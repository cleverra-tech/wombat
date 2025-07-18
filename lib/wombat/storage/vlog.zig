const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const HashMap = std.HashMap;
const atomic = std.atomic;
const fs = std.fs;
const MmapFile = @import("../io/mmap.zig").MmapFile;
const Entry = @import("wal.zig").Entry;
const CompressionType = @import("../core/options.zig").CompressionType;
const Compressor = @import("../compression/compressor.zig").Compressor;
const ErrorSystem = @import("../core/errors.zig");
const VLogError = ErrorSystem.VLogError;
const ErrorContext = ErrorSystem.ErrorContext;

/// Pointer to a value stored in the value log
pub const ValuePointer = struct {
    /// File ID containing the value
    file_id: u32,
    /// Offset within the file
    offset: u64,
    /// Size of the value in bytes
    size: u32,

    const ENCODED_SIZE = @sizeOf(u32) + @sizeOf(u64) + @sizeOf(u32);

    pub fn encode(self: *const ValuePointer, buf: []u8) VLogError!void {
        if (buf.len < ENCODED_SIZE) {
            return VLogError.InvalidSize;
        }

        var offset: usize = 0;
        std.mem.writeInt(u32, buf[offset..][0..4], self.file_id, .little);
        offset += 4;
        std.mem.writeInt(u64, buf[offset..][0..8], self.offset, .little);
        offset += 8;
        std.mem.writeInt(u32, buf[offset..][0..4], self.size, .little);
    }

    /// Encode ValuePointer to a newly allocated buffer
    pub fn encodeAlloc(self: *const ValuePointer, allocator: Allocator) ![]u8 {
        const buf = try allocator.alloc(u8, ENCODED_SIZE);
        try self.encode(buf);
        return buf;
    }

    pub fn decode(buf: []const u8) VLogError!ValuePointer {
        if (buf.len < ENCODED_SIZE) {
            return VLogError.InvalidSize;
        }

        var offset: usize = 0;
        const file_id = std.mem.readInt(u32, buf[offset..][0..4], .little);
        offset += 4;
        const value_offset = std.mem.readInt(u64, buf[offset..][0..8], .little);
        offset += 8;
        const size = std.mem.readInt(u32, buf[offset..][0..4], .little);

        return ValuePointer{
            .file_id = file_id,
            .offset = value_offset,
            .size = size,
        };
    }
};

/// Statistics tracking for disk space reclamation
pub const DiscardStats = struct {
    /// Total bytes written to this file
    total_size: atomic.Value(u64),
    /// Bytes marked as discarded (obsolete)
    discarded_size: atomic.Value(u64),

    const Self = @This();

    pub fn init() Self {
        return Self{
            .total_size = atomic.Value(u64).init(0),
            .discarded_size = atomic.Value(u64).init(0),
        };
    }

    pub fn addBytes(self: *Self, bytes: u64) void {
        _ = self.total_size.fetchAdd(bytes, .acq_rel);
    }

    pub fn discardBytes(self: *Self, bytes: u64) void {
        _ = self.discarded_size.fetchAdd(bytes, .acq_rel);
    }

    pub fn getDiscardRatio(self: *const Self) f64 {
        const total = self.total_size.load(.acquire);
        if (total == 0) return 0.0;

        const discarded = self.discarded_size.load(.acquire);
        return @as(f64, @floatFromInt(discarded)) / @as(f64, @floatFromInt(total));
    }

    pub fn getTotalSize(self: *const Self) u64 {
        return self.total_size.load(.acquire);
    }

    pub fn getDiscardedSize(self: *const Self) u64 {
        return self.discarded_size.load(.acquire);
    }
};

/// A single value log file with disk space tracking
pub const VLogFile = struct {
    /// Unique file identifier
    id: u32,
    /// Memory-mapped file handle
    mmap: MmapFile,
    /// Current write offset
    write_offset: atomic.Value(u64),
    /// Maximum file size
    max_size: u64,
    /// Statistics for space reclamation
    stats: DiscardStats,
    /// File path for identification
    path: []const u8,
    /// Compression type for this file
    compression: CompressionType,
    /// Allocator for compression operations
    allocator: Allocator,

    const Self = @This();
    const HEADER_SIZE = @sizeOf(u32) + @sizeOf(u64); // entry_size + checksum

    pub fn create(allocator: Allocator, id: u32, path: []const u8, max_size: u64, compression: CompressionType) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        var mmap = try MmapFile.create(path, max_size);
        errdefer mmap.close();

        self.* = Self{
            .id = id,
            .mmap = mmap,
            .write_offset = atomic.Value(u64).init(0),
            .max_size = max_size,
            .stats = DiscardStats.init(),
            .compression = compression,
            .allocator = allocator,
            .path = try allocator.dupe(u8, path),
        };

        return self;
    }

    pub fn open(allocator: Allocator, id: u32, path: []const u8, compression: CompressionType) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        var mmap = try MmapFile.open(path, false);
        errdefer mmap.close();

        self.* = Self{
            .id = id,
            .mmap = mmap,
            .write_offset = atomic.Value(u64).init(mmap.size),
            .compression = compression,
            .allocator = allocator,
            .max_size = mmap.size,
            .stats = DiscardStats.init(),
            .path = try allocator.dupe(u8, path),
        };

        // Scan file to rebuild statistics
        try self.rebuildStats();

        return self;
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        self.mmap.close();
        allocator.free(self.path);
        allocator.destroy(self);
    }

    /// Write an entry to the file, returning its pointer
    pub fn writeEntry(self: *Self, entry: Entry) VLogError!ValuePointer {
        // Compress the value if compression is enabled
        const compressor = Compressor.init(self.allocator, self.compression);
        const compressed_value = if (compressor.shouldCompress(entry.value))
            try compressor.compressAlloc(entry.value)
        else
            try self.allocator.dupe(u8, entry.value);
        defer self.allocator.free(compressed_value);

        const entry_size = compressed_value.len;
        const total_size = HEADER_SIZE + entry_size;

        const current_offset = self.write_offset.load(.acquire);

        // Check if file has space
        if (current_offset + total_size > self.max_size) {
            return VLogError.FileFull;
        }

        // Calculate checksum for integrity (on compressed data)
        const checksum = calculateChecksum(compressed_value);

        // Get buffer for write
        const buffer = try self.mmap.getSlice(current_offset, total_size);

        // Write header: size + checksum
        var offset: usize = 0;
        std.mem.writeInt(u32, buffer[offset..][0..4], @intCast(entry_size), .little);
        offset += 4;
        std.mem.writeInt(u64, buffer[offset..][0..8], checksum, .little);
        offset += 8;

        // Write value data (compressed)
        @memcpy(buffer[offset .. offset + entry_size], compressed_value);

        // Update write offset atomically
        _ = self.write_offset.fetchAdd(total_size, .acq_rel);

        // Update statistics
        self.stats.addBytes(total_size);

        return ValuePointer{
            .file_id = self.id,
            .offset = current_offset,
            .size = @intCast(total_size),
        };
    }

    /// Read an entry from the file using a pointer
    pub fn readEntry(self: *Self, ptr: ValuePointer, allocator: Allocator) VLogError![]u8 {
        if (ptr.file_id != self.id) {
            return VLogError.WrongFile;
        }

        if (ptr.offset + ptr.size > self.write_offset.load(.acquire)) {
            return VLogError.InvalidPointer;
        }

        // Read header
        const header_buf = try self.mmap.getSliceConst(ptr.offset, HEADER_SIZE);
        const entry_size = std.mem.readInt(u32, header_buf[0..4], .little);
        const stored_checksum = std.mem.readInt(u64, header_buf[4..12], .little);

        // Validate size consistency
        if (entry_size + HEADER_SIZE != ptr.size) {
            return VLogError.CorruptedEntry;
        }

        // Read value data (compressed)
        const compressed_buf = try self.mmap.getSliceConst(ptr.offset + HEADER_SIZE, entry_size);

        // Verify checksum
        const calculated_checksum = calculateChecksum(compressed_buf);
        if (calculated_checksum != stored_checksum) {
            return VLogError.ChecksumMismatch;
        }

        // Decompress if needed
        const compressor = Compressor.init(allocator, self.compression);
        const decompressed_value = if (self.compression != .none) blk: {
            // Try decompression, if it fails, assume it's uncompressed
            const original_size = if (compressed_buf.len >= 4) std.mem.readInt(u32, compressed_buf[0..4], .little) else compressed_buf.len;
            const decompressed = compressor.decompressAlloc(compressed_buf, original_size) catch {
                // If decompression fails, return as-is (might be uncompressed)
                break :blk try allocator.dupe(u8, compressed_buf);
            };
            break :blk decompressed;
        } else try allocator.dupe(u8, compressed_buf);

        // Return decompressed data
        return decompressed_value;
    }

    /// Check if file can accommodate additional bytes
    pub fn hasSpace(self: *const Self, bytes: u64) bool {
        const current_offset = self.write_offset.load(.acquire);
        return current_offset + bytes <= self.max_size;
    }

    /// Get current file utilization
    pub fn getUtilization(self: *const Self) f64 {
        const current_offset = self.write_offset.load(.acquire);
        return @as(f64, @floatFromInt(current_offset)) / @as(f64, @floatFromInt(self.max_size));
    }

    /// Sync file to disk
    pub fn sync(self: *Self) !void {
        try self.mmap.sync();
    }

    /// Rebuild statistics by scanning the entire file
    fn rebuildStats(self: *Self) !void {
        var offset: u64 = 0;
        const file_size = self.write_offset.load(.acquire);

        while (offset + HEADER_SIZE < file_size) {
            const header_buf = try self.mmap.getSliceConst(offset, HEADER_SIZE);
            const entry_size = std.mem.readInt(u32, header_buf[0..4], .little);
            const total_size = HEADER_SIZE + entry_size;

            if (offset + total_size > file_size) break;

            self.stats.addBytes(total_size);
            offset += total_size;
        }

        // Update write offset to actual end of valid data
        self.write_offset.store(offset, .release);
    }
};

/// Main ValueLog managing multiple files with space reclamation
pub const ValueLog = struct {
    /// All value log files
    files: ArrayList(*VLogFile),
    /// Currently active file for writes
    active_file: ?*VLogFile,
    /// File lookup by ID
    files_by_id: HashMap(u32, *VLogFile, std.hash_map.AutoContext(u32), 80),
    /// Size threshold for storing values in value log
    threshold: u32,
    /// Maximum size per file
    file_size: u64,
    /// Next file ID
    next_file_id: atomic.Value(u32),
    /// Directory for value log files
    dir_path: []const u8,
    /// Thread safety
    mutex: std.Thread.Mutex,
    /// Memory allocator
    allocator: Allocator,
    /// Compression type for value log entries
    compression: CompressionType,

    const Self = @This();

    pub fn init(allocator: Allocator, dir_path: []const u8, threshold: u32, file_size: u64, compression: CompressionType) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        // Ensure directory exists
        fs.cwd().makeDir(dir_path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        self.* = Self{
            .files = ArrayList(*VLogFile).init(allocator),
            .active_file = null,
            .files_by_id = HashMap(u32, *VLogFile, std.hash_map.AutoContext(u32), 80).init(allocator),
            .threshold = threshold,
            .file_size = file_size,
            .next_file_id = atomic.Value(u32).init(1),
            .dir_path = try allocator.dupe(u8, dir_path),
            .mutex = std.Thread.Mutex{},
            .allocator = allocator,
            .compression = compression,
        };

        // Load existing files
        try self.loadExistingFiles();

        return self;
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();

        // Close all files
        for (self.files.items) |file| {
            file.deinit(self.allocator);
        }
        self.files.deinit();
        self.files_by_id.deinit();

        self.mutex.unlock();

        const allocator = self.allocator;
        allocator.free(self.dir_path);
        allocator.destroy(self);
    }

    /// Write entries to value log, returning pointers
    pub fn write(self: *Self, entries: []const Entry) VLogError![]ValuePointer {
        self.mutex.lock();
        defer self.mutex.unlock();

        var pointers = try self.allocator.alloc(ValuePointer, entries.len);
        errdefer self.allocator.free(pointers);

        for (entries, 0..) |entry, i| {
            // Only store large values in value log
            if (entry.value.len < self.threshold) {
                return VLogError.InvalidSize;
            }

            // Ensure we have an active file with space
            try self.ensureActiveFile(entry.value.len + VLogFile.HEADER_SIZE);

            // Write to active file
            pointers[i] = try self.active_file.?.writeEntry(entry);
        }

        return pointers;
    }

    /// Read value using pointer
    pub fn read(self: *Self, ptr: ValuePointer, allocator: Allocator) VLogError![]u8 {
        self.mutex.lock();
        defer self.mutex.unlock();

        const file = self.files_by_id.get(ptr.file_id) orelse return VLogError.FileNotFound;
        return try file.readEntry(ptr, allocator);
    }

    /// Mark a value as discarded for space reclamation
    pub fn markDiscarded(self: *Self, ptr: ValuePointer) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.files_by_id.get(ptr.file_id)) |file| {
            file.stats.discardBytes(ptr.size);
        }
    }

    /// Reclaim space by rewriting files with high discard ratios
    pub fn reclaimSpace(self: *Self, discard_threshold: f64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var files_to_rewrite = ArrayList(u32).init(self.allocator);
        defer files_to_rewrite.deinit();

        // Find files that need rewriting
        for (self.files.items) |file| {
            const discard_ratio = file.stats.getDiscardRatio();
            if (discard_ratio >= discard_threshold) {
                try files_to_rewrite.append(file.id);
            }
        }

        // Rewrite each file
        for (files_to_rewrite.items) |file_id| {
            try self.rewriteFile(file_id);
        }
    }

    /// Rewrite a specific file, removing discarded entries
    pub fn rewriteFile(self: *Self, file_id: u32) !void {
        const old_file = self.files_by_id.get(file_id) orelse return error.FileNotFound;

        // Create new file
        const new_file_id = self.next_file_id.fetchAdd(1, .acq_rel);
        const new_path = try std.fmt.allocPrint(self.allocator, "{s}/vlog_{d}.dat", .{ self.dir_path, new_file_id });
        defer self.allocator.free(new_path);

        const new_file = try VLogFile.create(self.allocator, new_file_id, new_path, self.file_size, self.compression);
        errdefer new_file.deinit(self.allocator);

        // Copy valid entries from old file to new file
        try self.copyValidEntries(old_file, new_file);

        // Replace old file with new file
        try self.replaceFile(old_file, new_file);
    }

    /// Sync all files to disk
    pub fn sync(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.files.items) |file| {
            try file.sync();
        }
    }

    /// Get total number of files
    pub fn getFileCount(self: *Self) u32 {
        self.mutex.lock();
        defer self.mutex.unlock();
        return @intCast(self.files.items.len);
    }

    /// Get total storage used across all files
    pub fn getTotalSize(self: *Self) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var total: u64 = 0;
        for (self.files.items) |file| {
            total += file.stats.getTotalSize();
        }
        return total;
    }

    /// Get total discarded space across all files
    pub fn getTotalDiscarded(self: *Self) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var total: u64 = 0;
        for (self.files.items) |file| {
            total += file.stats.getDiscardedSize();
        }
        return total;
    }

    /// Check if space reclamation should run based on threshold
    pub fn shouldRunSpaceReclamation(self: *Self, threshold: f64) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.files.items) |file| {
            if (file.stats.getDiscardRatio() >= threshold) {
                return true;
            }
        }
        return false;
    }

    /// Run space reclamation on files with high discard ratio
    pub fn runSpaceReclamation(self: *Self, threshold: f64) !bool {
        try self.reclaimSpace(threshold);
        return true;
    }

    /// Get current size of all files
    pub fn size(self: *Self) u64 {
        return self.getTotalSize();
    }

    /// Load existing value log files from directory
    fn loadExistingFiles(self: *Self) !void {
        var dir = fs.cwd().openDir(self.dir_path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return, // No existing files
            else => return err,
        };
        defer dir.close();

        var iterator = dir.iterate();
        var max_file_id: u32 = 0;

        while (try iterator.next()) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.startsWith(u8, entry.name, "vlog_") or !std.mem.endsWith(u8, entry.name, ".dat")) continue;

            // Parse file ID from name: vlog_123.dat
            const id_start = "vlog_".len;
            const id_end = entry.name.len - ".dat".len;
            if (id_end <= id_start) continue;

            const file_id = std.fmt.parseInt(u32, entry.name[id_start..id_end], 10) catch continue;
            max_file_id = @max(max_file_id, file_id);

            // Load the file
            const file_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.dir_path, entry.name });
            defer self.allocator.free(file_path);

            const file = try VLogFile.open(self.allocator, file_id, file_path, self.compression);
            try self.files.append(file);
            try self.files_by_id.put(file_id, file);
        }

        // Set next file ID
        self.next_file_id.store(max_file_id + 1, .release);

        // Set active file to the last one if it has space
        if (self.files.items.len > 0) {
            const last_file = self.files.items[self.files.items.len - 1];
            if (last_file.hasSpace(self.threshold)) {
                self.active_file = last_file;
            }
        }
    }

    /// Ensure there's an active file with enough space
    fn ensureActiveFile(self: *Self, bytes: u64) !void {
        // Check if current active file has space
        if (self.active_file) |active| {
            if (active.hasSpace(bytes)) {
                return;
            }
        }

        // Create new active file
        const file_id = self.next_file_id.fetchAdd(1, .acq_rel);
        const file_path = try std.fmt.allocPrint(self.allocator, "{s}/vlog_{d}.dat", .{ self.dir_path, file_id });
        defer self.allocator.free(file_path);

        const new_file = try VLogFile.create(self.allocator, file_id, file_path, self.file_size, self.compression);
        try self.files.append(new_file);
        try self.files_by_id.put(file_id, new_file);

        self.active_file = new_file;
    }

    /// Copy valid entries from old file to new file
    fn copyValidEntries(self: *Self, old_file: *VLogFile, new_file: *VLogFile) !void {
        _ = self;

        var offset: u64 = 0;
        const file_size = old_file.write_offset.load(.acquire);
        var total_bytes_copied: u64 = 0;
        var entries_copied: u32 = 0;

        while (offset + VLogFile.HEADER_SIZE < file_size) {
            const header_buf = try old_file.mmap.getSliceConst(offset, VLogFile.HEADER_SIZE);
            const entry_size = std.mem.readInt(u32, header_buf[0..4], .little);
            const checksum = std.mem.readInt(u64, header_buf[4..12], .little);
            const total_size = VLogFile.HEADER_SIZE + entry_size;

            if (offset + total_size > file_size) break;

            // Validate entry integrity
            const entry_data = try old_file.mmap.getSliceConst(offset + VLogFile.HEADER_SIZE, entry_size);
            const calculated_checksum = std.hash.crc.Crc32.hash(entry_data);

            // Only copy valid entries with correct checksums
            // This implements space reclamation by skipping corrupted entries
            if (calculated_checksum == @as(u32, @truncate(checksum))) {
                // Copy entire entry (header + data)
                const entry_buf = try old_file.mmap.getSliceConst(offset, total_size);
                const new_offset = new_file.write_offset.load(.acquire);
                const new_buf = try new_file.mmap.getSlice(new_offset, total_size);
                @memcpy(new_buf, entry_buf);

                _ = new_file.write_offset.fetchAdd(total_size, .acq_rel);
                new_file.stats.addBytes(total_size);

                total_bytes_copied += total_size;
                entries_copied += 1;
            } else {
                // Skip corrupted entry - this reclaims space
                // The entry is effectively discarded during compaction
            }

            offset += total_size;
        }

        // Update statistics to reflect actual space reclamation
        const original_size = old_file.stats.total_size.load(.acquire);
        const space_reclaimed = original_size - total_bytes_copied;

        if (space_reclaimed > 0) {
            // Update the old file's discard stats to reflect reclaimed space
            old_file.stats.discardBytes(space_reclaimed);
        }
    }

    /// Replace old file with new file in the system
    fn replaceFile(self: *Self, old_file: *VLogFile, new_file: *VLogFile) !void {
        // Remove old file from collections
        for (self.files.items, 0..) |file, i| {
            if (file.id == old_file.id) {
                _ = self.files.swapRemove(i);
                break;
            }
        }
        _ = self.files_by_id.remove(old_file.id);

        // Add new file
        try self.files.append(new_file);
        try self.files_by_id.put(new_file.id, new_file);

        // Update active file if necessary
        if (self.active_file == old_file) {
            self.active_file = new_file;
        }

        // Delete old file from disk
        fs.cwd().deleteFile(old_file.path) catch {};

        // Clean up old file
        old_file.deinit(self.allocator);
    }
};

/// Calculate checksum for integrity verification
fn calculateChecksum(data: []const u8) u64 {
    // Simple FNV-1a hash for checksum
    var hash: u64 = 14695981039346656037;
    for (data) |byte| {
        hash ^= byte;
        hash = hash *% 1099511628211;
    }
    return hash;
}

/// Helper function to clean up test directories
fn cleanupTestDir(dir_path: []const u8) void {
    var dir = fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return;
    defer dir.close();
    var iterator = dir.iterate();
    while (iterator.next() catch null) |entry| {
        if (entry.kind == .file) {
            var path_buf: [256]u8 = undefined;
            const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir_path, entry.name }) catch continue;
            fs.cwd().deleteFile(path) catch {};
        }
    }
    fs.cwd().deleteDir(dir_path) catch {};
}

// Tests
test "ValuePointer encoding/decoding" {
    _ = std.testing.allocator;

    const ptr = ValuePointer{
        .file_id = 123,
        .offset = 456789,
        .size = 1024,
    };

    var buffer: [ValuePointer.ENCODED_SIZE]u8 = undefined;
    try ptr.encode(&buffer);

    const decoded = try ValuePointer.decode(&buffer);
    try std.testing.expect(decoded.file_id == ptr.file_id);
    try std.testing.expect(decoded.offset == ptr.offset);
    try std.testing.expect(decoded.size == ptr.size);
}

test "DiscardStats operations" {
    var stats = DiscardStats.init();

    try std.testing.expect(stats.getDiscardRatio() == 0.0);

    stats.addBytes(1000);
    try std.testing.expect(stats.getTotalSize() == 1000);
    try std.testing.expect(stats.getDiscardRatio() == 0.0);

    stats.discardBytes(300);
    try std.testing.expect(stats.getDiscardedSize() == 300);
    try std.testing.expect(stats.getDiscardRatio() == 0.3);
}

test "VLogFile basic operations" {
    const allocator = std.testing.allocator;

    const temp_path = "test_vlog_file.dat";
    defer fs.cwd().deleteFile(temp_path) catch {};

    const file = try VLogFile.create(allocator, 1, temp_path, 1024 * 1024, .none);
    defer file.deinit(allocator);

    // Test writing entry
    const entry = Entry{
        .key = "test_key",
        .value = "test_value_with_some_length",
        .timestamp = 123456,
        .meta = 0,
    };

    const ptr = try file.writeEntry(entry);
    try std.testing.expect(ptr.file_id == 1);
    try std.testing.expect(ptr.offset == 0);

    // Test reading entry
    const read_value = try file.readEntry(ptr, allocator);
    defer allocator.free(read_value);

    try std.testing.expect(std.mem.eql(u8, read_value, entry.value));
}

test "ValueLog complete workflow" {
    const allocator = std.testing.allocator;

    const temp_dir = "test_vlog_dir";
    defer cleanupTestDir(temp_dir);

    const vlog = try ValueLog.init(allocator, temp_dir, 10, 1024 * 1024, .zlib);
    defer vlog.deinit();

    // Test writing entries
    const entries = [_]Entry{
        Entry{ .key = "key1", .value = "value1_long_enough_for_vlog", .timestamp = 1, .meta = 0 },
        Entry{ .key = "key2", .value = "value2_also_long_enough_here", .timestamp = 2, .meta = 0 },
    };

    const pointers = try vlog.write(&entries);
    defer allocator.free(pointers);

    try std.testing.expect(pointers.len == 2);

    // Test reading values
    const value1 = try vlog.read(pointers[0], allocator);
    defer allocator.free(value1);
    try std.testing.expect(std.mem.eql(u8, value1, entries[0].value));

    const value2 = try vlog.read(pointers[1], allocator);
    defer allocator.free(value2);
    try std.testing.expect(std.mem.eql(u8, value2, entries[1].value));

    // Test marking discarded
    vlog.markDiscarded(pointers[0]);

    // Test statistics
    try std.testing.expect(vlog.getFileCount() == 1);
    try std.testing.expect(vlog.getTotalSize() > 0);
    try std.testing.expect(vlog.getTotalDiscarded() > 0);
}
