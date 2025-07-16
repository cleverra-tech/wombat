const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const MmapFile = @import("../io/mmap.zig").MmapFile;
const BloomFilter = @import("../core/bloom.zig").BloomFilter;
const ValueStruct = @import("../core/skiplist.zig").ValueStruct;
const SkipList = @import("../core/skiplist.zig").SkipList;
const Options = @import("../core/options.zig").Options;
const CompressionType = @import("../core/options.zig").CompressionType;

/// Table errors
pub const TableError = error{
    InvalidTableFile,
    UnsupportedVersion,
    CorruptedData,
    CompressionNotImplemented,
    BlockSizeExceeded,
    InvalidKeyOrder,
    ChecksumMismatch,
    OutOfMemory,
    IOError,
};

/// Table statistics for monitoring
pub const TableStats = struct {
    file_size: u64,
    key_count: u32,
    block_count: u32,
    bloom_false_positives: u64,
    cache_hits: u64,
    cache_misses: u64,
    reads_total: u64,
};

/// Index entry for fast key lookups
pub const IndexEntry = struct {
    key: []const u8,
    offset: u32,
    size: u32,
};

/// Table index for efficient key lookups with binary search
pub const TableIndex = struct {
    entries: ArrayList(IndexEntry),
    allocator: Allocator,

    const Self = @This();

    /// Initialize a new table index
    pub fn init(allocator: Allocator, capacity: usize) !Self {
        var entries = ArrayList(IndexEntry).init(allocator);
        try entries.ensureTotalCapacity(capacity);

        return Self{
            .entries = entries,
            .allocator = allocator,
        };
    }

    /// Clean up index resources
    pub fn deinit(self: *Self) void {
        // Free all key strings
        for (self.entries.items) |entry| {
            self.allocator.free(entry.key);
        }
        self.entries.deinit();
    }

    /// Add an index entry (keys must be added in sorted order)
    pub fn addEntry(self: *Self, key: []const u8, offset: u32, size: u32) !void {
        // Validate key ordering
        if (self.entries.items.len > 0) {
            const last_key = self.entries.items[self.entries.items.len - 1].key;
            if (std.mem.order(u8, last_key, key) != .lt) {
                return TableError.InvalidKeyOrder;
            }
        }

        // Create owned copy of key
        const owned_key = try self.allocator.dupe(u8, key);

        try self.entries.append(IndexEntry{
            .key = owned_key,
            .offset = offset,
            .size = size,
        });
    }

    /// Binary search for key, returns block index that might contain the key
    pub fn search(self: *const Self, key: []const u8) ?usize {
        if (self.entries.items.len == 0) {
            return null;
        }

        var left: usize = 0;
        var right: usize = self.entries.items.len;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const cmp = std.mem.order(u8, self.entries.items[mid].key, key);

            switch (cmp) {
                .lt => left = mid + 1,
                .gt => right = mid,
                .eq => return mid,
            }
        }

        // Return the block that might contain the key
        if (left >= self.entries.items.len) {
            // Key is beyond the last entry, no block can contain it
            return null;
        }
        return if (left > 0) left - 1 else 0;
    }

    /// Get the number of index entries
    pub fn getEntryCount(self: *const Self) usize {
        return self.entries.items.len;
    }

    /// Get an index entry by position
    pub fn getEntry(self: *const Self, index: usize) ?IndexEntry {
        if (index >= self.entries.items.len) {
            return null;
        }
        return self.entries.items[index];
    }

    /// Get the smallest key in the index
    pub fn getSmallestKey(self: *const Self) ?[]const u8 {
        if (self.entries.items.len == 0) {
            return null;
        }
        return self.entries.items[0].key;
    }

    /// Get the largest key in the index
    pub fn getLargestKey(self: *const Self) ?[]const u8 {
        if (self.entries.items.len == 0) {
            return null;
        }
        return self.entries.items[self.entries.items.len - 1].key;
    }
};

/// SSTable implementation for persistent storage
pub const Table = struct {
    mmap_file: MmapFile,
    index: TableIndex,
    bloom_filter: BloomFilter,
    smallest_key: []const u8,
    biggest_key: []const u8,
    compression: CompressionType,
    file_id: u64,
    level: u32,
    stats: TableStats,
    allocator: Allocator,

    const Self = @This();
    const MAGIC_NUMBER: u32 = 0xCAFEBABE;
    const VERSION: u32 = 1;
    const MIN_FILE_SIZE: usize = 64; // Minimum valid table file size

    /// Open an existing SSTable file
    pub fn open(allocator: Allocator, path: []const u8, file_id: u64) TableError!Self {
        const mmap_file = MmapFile.open(path, true) catch return TableError.IOError;

        if (mmap_file.size < MIN_FILE_SIZE) {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.InvalidTableFile;
        }

        // Read and validate footer
        const footer_offset = mmap_file.size - 32;
        const footer = mmap_file.getSliceConst(footer_offset, 32) catch {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.CorruptedData;
        };

        const magic = std.mem.readInt(u32, footer[0..4][0..4], .little);
        if (magic != MAGIC_NUMBER) {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.InvalidTableFile;
        }

        const version = std.mem.readInt(u32, footer[4..8][0..4], .little);
        if (version != VERSION) {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.UnsupportedVersion;
        }

        const index_offset = std.mem.readInt(u64, footer[8..16][0..8], .little);
        const bloom_offset = std.mem.readInt(u64, footer[16..24][0..8], .little);
        const compression = @as(CompressionType, @enumFromInt(std.mem.readInt(u32, footer[24..28][0..4], .little)));
        const level = std.mem.readInt(u32, footer[28..32][0..4], .little);

        // Validate offsets
        if (bloom_offset >= index_offset or index_offset >= footer_offset) {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.CorruptedData;
        }

        // Load bloom filter
        const bloom_size = index_offset - bloom_offset;
        const bloom_data = mmap_file.getSliceConst(bloom_offset, bloom_size) catch {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.CorruptedData;
        };
        const bloom_filter = BloomFilter.deserialize(bloom_data, allocator) catch {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.CorruptedData;
        };

        // Initialize table
        var table = Self{
            .mmap_file = mmap_file,
            .index = undefined,
            .bloom_filter = bloom_filter,
            .smallest_key = &[_]u8{},
            .biggest_key = &[_]u8{},
            .compression = compression,
            .file_id = file_id,
            .level = level,
            .stats = TableStats{
                .file_size = mmap_file.size,
                .key_count = 0,
                .block_count = 0,
                .bloom_false_positives = 0,
                .cache_hits = 0,
                .cache_misses = 0,
                .reads_total = 0,
            },
            .allocator = allocator,
        };

        // Load index
        const index_size = footer_offset - index_offset;
        table.loadIndex(index_offset, index_size) catch |err| {
            var mutable_bloom = table.bloom_filter;
            mutable_bloom.deinit(allocator);
            var mutable_mmap = table.mmap_file;
            mutable_mmap.close();
            return err;
        };

        return table;
    }

    /// Close the table and clean up resources
    pub fn close(self: *Self) void {
        self.index.deinit();
        var mutable_bloom = self.bloom_filter;
        mutable_bloom.deinit(self.allocator);
        var mutable_mmap = self.mmap_file;
        mutable_mmap.close();
    }

    /// Load the table index from file
    fn loadIndex(self: *Self, offset: u64, size: u64) TableError!void {
        const index_data = self.mmap_file.getSliceConst(offset, size) catch return TableError.CorruptedData;

        if (index_data.len < 4) {
            return TableError.CorruptedData;
        }

        var data_offset: usize = 0;
        const entry_count = std.mem.readInt(u32, index_data[data_offset .. data_offset + 4][0..4], .little);
        data_offset += 4;

        self.index = TableIndex.init(self.allocator, entry_count) catch return TableError.OutOfMemory;
        self.stats.key_count = entry_count;
        self.stats.block_count = entry_count;

        // Read index entries
        for (0..entry_count) |_| {
            if (data_offset + 12 > index_data.len) {
                return TableError.CorruptedData;
            }

            const entry_offset = std.mem.readInt(u32, index_data[data_offset .. data_offset + 4][0..4], .little);
            data_offset += 4;

            const block_size = std.mem.readInt(u32, index_data[data_offset .. data_offset + 4][0..4], .little);
            data_offset += 4;

            const key_len = std.mem.readInt(u32, index_data[data_offset .. data_offset + 4][0..4], .little);
            data_offset += 4;

            if (data_offset + key_len > index_data.len) {
                return TableError.CorruptedData;
            }

            const key = index_data[data_offset .. data_offset + key_len];
            data_offset += key_len;

            self.index.addEntry(key, entry_offset, block_size) catch return TableError.OutOfMemory;
        }

        // Set key range from index
        if (entry_count > 0) {
            self.smallest_key = self.index.getSmallestKey().?;
            self.biggest_key = self.index.getLargestKey().?;
        }
    }

    /// Get a value for the given key
    pub fn get(self: *Self, key: []const u8) TableError!?ValueStruct {
        self.stats.reads_total += 1;

        // Check bloom filter first
        if (!self.bloom_filter.contains(key)) {
            return null;
        }

        // Find the block that might contain the key
        const block_index = self.index.search(key) orelse {
            self.stats.bloom_false_positives += 1;
            return null;
        };

        // Read and search the block
        const block_data = self.readBlock(block_index) catch return TableError.IOError;
        defer self.allocator.free(block_data);

        const result = self.searchInBlock(block_data, key);
        if (result == null) {
            self.stats.bloom_false_positives += 1;
        }

        return result;
    }

    /// Read a block from the table
    fn readBlock(self: *Self, block_index: usize) TableError![]u8 {
        const entry = self.index.getEntry(block_index) orelse return TableError.CorruptedData;

        const compressed_data = self.mmap_file.getSliceConst(entry.offset, entry.size) catch return TableError.IOError;

        return switch (self.compression) {
            .none => self.allocator.dupe(u8, compressed_data) catch return TableError.OutOfMemory,
            .snappy, .zstd => return TableError.CompressionNotImplemented,
        };
    }

    /// Search for a key within a block
    fn searchInBlock(self: *Self, block_data: []const u8, key: []const u8) ?ValueStruct {
        var offset: usize = 0;

        while (offset < block_data.len) {
            // Ensure we have enough data for the header
            if (offset + 16 > block_data.len) break;

            const key_len = std.mem.readInt(u32, block_data[offset .. offset + 4][0..4], .little);
            offset += 4;

            const value_len = std.mem.readInt(u32, block_data[offset .. offset + 4][0..4], .little);
            offset += 4;

            const timestamp = std.mem.readInt(u64, block_data[offset .. offset + 8][0..8], .little);
            offset += 8;

            // Validate entry bounds
            if (offset + key_len + value_len > block_data.len) break;
            if (key_len == 0 or key_len > 1024 * 1024) break; // Reasonable key size limits
            if (value_len > 1024 * 1024 * 1024) break; // Reasonable value size limits

            const entry_key = block_data[offset .. offset + key_len];
            offset += key_len;

            const entry_value = block_data[offset .. offset + value_len];
            offset += value_len;

            const key_cmp = std.mem.order(u8, entry_key, key);
            if (key_cmp == .eq) {
                // Copy the value to owned memory to avoid use-after-free
                const owned_value = self.allocator.dupe(u8, entry_value) catch return null;
                return ValueStruct{
                    .value = owned_value,
                    .timestamp = timestamp,
                    .meta = 0,
                };
            }

            // Keys are sorted, so we can stop if we've passed the target
            if (key_cmp == .gt) {
                break;
            }
        }

        return null;
    }

    /// Create an iterator for the table
    pub fn iterator(self: *Self) TableIterator {
        return TableIterator.init(self);
    }

    /// Verify table integrity (checksum validation)
    pub fn verifyChecksum(self: *Self) TableError!void {
        // TODO: Implement checksum verification
        _ = self;
    }

    /// Check if this table overlaps with the given key range
    pub fn overlapsWithKeyRange(self: *const Self, start_key: []const u8, end_key: []const u8) bool {
        return std.mem.order(u8, self.biggest_key, start_key) != .lt and
            std.mem.order(u8, self.smallest_key, end_key) != .gt;
    }

    /// Get table statistics
    pub fn getStats(self: *const Self) TableStats {
        return self.stats;
    }

    /// Get file ID
    pub fn getFileId(self: *const Self) u64 {
        return self.file_id;
    }

    /// Get level
    pub fn getLevel(self: *const Self) u32 {
        return self.level;
    }

    /// Get smallest key
    pub fn getSmallestKey(self: *const Self) []const u8 {
        return self.smallest_key;
    }

    /// Get biggest key
    pub fn getBiggestKey(self: *const Self) []const u8 {
        return self.biggest_key;
    }

    /// Get file size
    pub fn getFileSize(self: *const Self) u64 {
        return self.mmap_file.size;
    }
};

/// Iterator for traversing table entries
pub const TableIterator = struct {
    table: *Table,
    current_block: usize,
    block_data: ?[]u8,
    block_offset: usize,
    finished: bool,

    const Self = @This();

    /// Initialize a new table iterator
    pub fn init(table: *Table) Self {
        return Self{
            .table = table,
            .current_block = 0,
            .block_data = null,
            .block_offset = 0,
            .finished = false,
        };
    }

    /// Clean up iterator resources
    pub fn deinit(self: *Self) void {
        if (self.block_data) |data| {
            self.table.allocator.free(data);
            self.block_data = null;
        }
    }

    /// Get the next key-value pair
    pub fn next(self: *Self) TableError!?struct { key: []const u8, value: ValueStruct } {
        if (self.finished) {
            return null;
        }

        // Load first block if needed
        if (self.block_data == null and self.current_block < self.table.index.getEntryCount()) {
            try self.loadCurrentBlock();
        }

        while (self.current_block < self.table.index.getEntryCount()) {
            if (self.block_data) |data| {
                if (self.block_offset + 16 <= data.len) {
                    const key_len = std.mem.readInt(u32, data[self.block_offset .. self.block_offset + 4][0..4], .little);
                    self.block_offset += 4;

                    const value_len = std.mem.readInt(u32, data[self.block_offset .. self.block_offset + 4][0..4], .little);
                    self.block_offset += 4;

                    const timestamp = std.mem.readInt(u64, data[self.block_offset .. self.block_offset + 8][0..8], .little);
                    self.block_offset += 8;

                    // Validate entry bounds and sizes
                    if (self.block_offset + key_len + value_len <= data.len and
                        key_len > 0 and key_len <= 1024 * 1024 and
                        value_len <= 1024 * 1024 * 1024)
                    {
                        const key = data[self.block_offset .. self.block_offset + key_len];
                        self.block_offset += key_len;

                        const value = data[self.block_offset .. self.block_offset + value_len];
                        self.block_offset += value_len;

                        return .{
                            .key = key,
                            .value = ValueStruct{
                                .value = value,
                                .timestamp = timestamp,
                                .meta = 0,
                            },
                        };
                    }
                }
            }

            // Move to next block
            self.current_block += 1;
            try self.loadCurrentBlock();
        }

        self.finished = true;
        return null;
    }

    /// Load the current block into memory
    fn loadCurrentBlock(self: *Self) TableError!void {
        // Free previous block data
        if (self.block_data) |data| {
            self.table.allocator.free(data);
            self.block_data = null;
        }

        if (self.current_block < self.table.index.getEntryCount()) {
            self.block_data = try self.table.readBlock(self.current_block);
            self.block_offset = 0;
        }
    }
};

/// Builder for creating SSTable files with proper format and compression
pub const TableBuilder = struct {
    file: std.fs.File,
    path: []const u8,
    allocator: Allocator,
    block_data: ArrayList(u8),
    index: TableIndex,
    bloom_filter: BloomFilter,
    current_block_offset: u64,
    entries_in_block: u32,
    total_entries: u32,
    block_size: usize,
    compression: CompressionType,
    level: u32,
    last_key: ?[]const u8,
    first_key: ?[]const u8,
    smallest_key: ?[]const u8,
    biggest_key: ?[]const u8,
    finished: bool,

    const Self = @This();

    /// Initialize a new table builder
    pub fn init(allocator: Allocator, path: []const u8, options: Options, level: u32) TableError!Self {
        const file = std.fs.cwd().createFile(path, .{ .read = true, .truncate = true }) catch return TableError.IOError;

        const bloom_filter = BloomFilter.init(allocator, 10000, options.bloom_false_positive) catch {
            file.close();
            return TableError.OutOfMemory;
        };

        const index = TableIndex.init(allocator, 1000) catch {
            var mutable_bloom = bloom_filter;
            mutable_bloom.deinit(allocator);
            file.close();
            return TableError.OutOfMemory;
        };

        return Self{
            .file = file,
            .path = path,
            .allocator = allocator,
            .block_data = ArrayList(u8).init(allocator),
            .index = index,
            .bloom_filter = bloom_filter,
            .current_block_offset = 0,
            .entries_in_block = 0,
            .total_entries = 0,
            .block_size = options.block_size,
            .compression = options.compression,
            .level = level,
            .last_key = null,
            .first_key = null,
            .smallest_key = null,
            .biggest_key = null,
            .finished = false,
        };
    }

    /// Clean up builder resources
    pub fn deinit(self: *Self) void {
        if (!self.finished) {
            // If not finished properly, delete the partial file
            self.file.close();
            std.fs.cwd().deleteFile(self.path) catch {};
        } else {
            self.file.close();
        }

        self.block_data.deinit();
        self.index.deinit();
        var mutable_bloom = self.bloom_filter;
        mutable_bloom.deinit(self.allocator);

        if (self.last_key) |key| {
            self.allocator.free(key);
        }
        if (self.first_key) |key| {
            self.allocator.free(key);
        }
        if (self.smallest_key) |key| {
            self.allocator.free(key);
        }
        if (self.biggest_key) |key| {
            self.allocator.free(key);
        }
    }

    /// Add a key-value pair to the table (keys must be in sorted order)
    pub fn add(self: *Self, key: []const u8, value: ValueStruct) TableError!void {
        if (self.finished) {
            return TableError.InvalidKeyOrder;
        }

        // Validate key ordering
        if (self.last_key) |last| {
            if (std.mem.order(u8, last, key) != .lt) {
                return TableError.InvalidKeyOrder;
            }
        }

        // Calculate entry size
        const entry_size = 4 + 4 + 8 + key.len + value.value.len;

        // Validate reasonable sizes
        if (key.len == 0 or key.len > 1024 * 1024) {
            return TableError.InvalidKeyOrder;
        }
        if (value.value.len > 1024 * 1024 * 1024) {
            return TableError.BlockSizeExceeded;
        }

        // Finish current block if it would exceed block size
        if (self.block_data.items.len + entry_size > self.block_size and self.entries_in_block > 0) {
            try self.finishCurrentBlock();
        }

        // Add entry to current block
        var entry_header: [16]u8 = undefined;
        std.mem.writeInt(u32, entry_header[0..4], @intCast(key.len), .little);
        std.mem.writeInt(u32, entry_header[4..8], @intCast(value.value.len), .little);
        std.mem.writeInt(u64, entry_header[8..16], value.timestamp, .little);

        self.block_data.appendSlice(&entry_header) catch return TableError.OutOfMemory;
        self.block_data.appendSlice(key) catch return TableError.OutOfMemory;
        self.block_data.appendSlice(value.value) catch return TableError.OutOfMemory;

        // Update bloom filter
        self.bloom_filter.add(key);

        // Update counters
        self.entries_in_block += 1;
        self.total_entries += 1;

        // Update key tracking
        if (self.first_key == null) {
            self.first_key = self.allocator.dupe(u8, key) catch return TableError.OutOfMemory;
        }

        if (self.last_key) |old_key| {
            self.allocator.free(old_key);
        }
        self.last_key = self.allocator.dupe(u8, key) catch return TableError.OutOfMemory;

        // Track overall smallest and biggest keys
        if (self.smallest_key == null) {
            self.smallest_key = self.allocator.dupe(u8, key) catch return TableError.OutOfMemory;
        }

        if (self.biggest_key) |old_key| {
            self.allocator.free(old_key);
        }
        self.biggest_key = self.allocator.dupe(u8, key) catch return TableError.OutOfMemory;
    }

    /// Finish the current block and add it to the index
    fn finishCurrentBlock(self: *Self) TableError!void {
        if (self.entries_in_block == 0) {
            return;
        }

        // Compress block data
        const compressed_block = switch (self.compression) {
            .none => self.block_data.toOwnedSlice() catch return TableError.OutOfMemory,
            .snappy, .zstd => return TableError.CompressionNotImplemented,
        };
        defer self.allocator.free(compressed_block);

        // Write block to file
        const bytes_written = self.file.write(compressed_block) catch return TableError.IOError;
        if (bytes_written != compressed_block.len) {
            return TableError.IOError;
        }

        // Add index entry (use first key of the block)
        if (self.first_key) |block_first_key| {
            try self.index.addEntry(block_first_key, @intCast(self.current_block_offset), @intCast(compressed_block.len));

            // If this is the only block (all entries fit), also add the biggest key for proper range
            if (self.biggest_key) |biggest_key| {
                if (!std.mem.eql(u8, block_first_key, biggest_key)) {
                    try self.index.addEntry(biggest_key, @intCast(self.current_block_offset), @intCast(compressed_block.len));
                }
            }
        }

        // Update offsets and reset block
        self.current_block_offset += compressed_block.len;
        self.entries_in_block = 0;
        self.block_data.clearRetainingCapacity();

        // Reset first_key for next block
        if (self.first_key) |key| {
            self.allocator.free(key);
            self.first_key = null;
        }
    }

    /// Finish building the table and write metadata
    pub fn finish(self: *Self) TableError!void {
        if (self.finished) {
            return;
        }

        // Finish any remaining block
        if (self.entries_in_block > 0) {
            try self.finishCurrentBlock();
        }

        // Serialize and write bloom filter
        const bloom_data = self.allocator.alloc(u8, self.bloom_filter.serializedSize()) catch return TableError.OutOfMemory;
        defer self.allocator.free(bloom_data);

        const bloom_bytes = self.bloom_filter.serialize(bloom_data) catch return TableError.IOError;
        if (bloom_bytes != bloom_data.len) {
            return TableError.IOError;
        }

        const bloom_offset = self.file.getPos() catch return TableError.IOError;
        const bloom_written = self.file.write(bloom_data) catch return TableError.IOError;
        if (bloom_written != bloom_data.len) {
            return TableError.IOError;
        }

        // Write index
        const index_offset = self.file.getPos() catch return TableError.IOError;

        // Write index header
        var header_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &header_buf, @intCast(self.index.getEntryCount()), .little);
        self.file.writeAll(&header_buf) catch return TableError.IOError;

        // Write index entries
        for (self.index.entries.items) |entry| {
            var entry_buf: [12]u8 = undefined;
            std.mem.writeInt(u32, entry_buf[0..4], entry.offset, .little);
            std.mem.writeInt(u32, entry_buf[4..8], entry.size, .little);
            std.mem.writeInt(u32, entry_buf[8..12], @intCast(entry.key.len), .little);
            self.file.writeAll(&entry_buf) catch return TableError.IOError;
            self.file.writeAll(entry.key) catch return TableError.IOError;
        }

        // Write footer
        var footer_buf: [32]u8 = undefined;
        std.mem.writeInt(u32, footer_buf[0..4], Table.MAGIC_NUMBER, .little);
        std.mem.writeInt(u32, footer_buf[4..8], Table.VERSION, .little);
        std.mem.writeInt(u64, footer_buf[8..16], index_offset, .little);
        std.mem.writeInt(u64, footer_buf[16..24], bloom_offset, .little);
        std.mem.writeInt(u32, footer_buf[24..28], @intFromEnum(self.compression), .little);
        std.mem.writeInt(u32, footer_buf[28..32], self.level, .little);
        self.file.writeAll(&footer_buf) catch return TableError.IOError;

        // Sync to disk
        self.file.sync() catch return TableError.IOError;

        self.finished = true;
    }

    /// Get the number of entries added so far
    pub fn getEntryCount(self: *const Self) u32 {
        return self.total_entries;
    }

    /// Get the estimated file size
    pub fn getEstimatedSize(self: *const Self) u64 {
        return self.current_block_offset + self.block_data.items.len;
    }

    /// Check if the builder is finished
    pub fn isFinished(self: *const Self) bool {
        return self.finished;
    }
};

// Tests
test "TableIndex basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var index = try TableIndex.init(allocator, 10);
    defer index.deinit();

    // Test adding entries in order
    try index.addEntry("key1", 0, 100);
    try index.addEntry("key2", 100, 150);
    try index.addEntry("key3", 250, 200);

    std.testing.expect(index.getEntryCount() == 3) catch unreachable;

    // Test search functionality
    std.testing.expect(index.search("key1").? == 0) catch unreachable;
    std.testing.expect(index.search("key2").? == 1) catch unreachable;
    std.testing.expect(index.search("key3").? == 2) catch unreachable;
    std.testing.expect(index.search("key0").? == 0) catch unreachable; // Should return block that might contain key
    std.testing.expect(index.search("key4") == null) catch unreachable; // Key beyond range

    // Test key range
    std.testing.expect(std.mem.eql(u8, index.getSmallestKey().?, "key1")) catch unreachable;
    std.testing.expect(std.mem.eql(u8, index.getLargestKey().?, "key3")) catch unreachable;
}

test "TableIndex key ordering validation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var index = try TableIndex.init(allocator, 10);
    defer index.deinit();

    // Test valid ordering
    try index.addEntry("key1", 0, 100);
    try index.addEntry("key2", 100, 150);

    // Test invalid ordering (should fail)
    const result = index.addEntry("key1", 250, 200);
    std.testing.expect(result == TableError.InvalidKeyOrder) catch unreachable;
}

test "TableBuilder basic functionality" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_path = "test_table.sst";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    const options = Options{
        .dir = ".",
        .value_dir = ".",
        .mem_table_size = 1024 * 1024,
        .num_memtables = 5,
        .max_levels = 7,
        .level_size_multiplier = 10,
        .base_table_size = 2 * 1024 * 1024,
        .num_compactors = 4,
        .num_level_zero_tables = 5,
        .num_level_zero_tables_stall = 15,
        .value_threshold = 1024 * 1024,
        .value_log_file_size = 1024 * 1024 * 1024,
        .block_size = 4 * 1024,
        .bloom_false_positive = 0.01,
        .compression = .none,
        .sync_writes = false,
        .detect_conflicts = true,
        .verify_checksums = true,
    };

    var builder = try TableBuilder.init(allocator, test_path, options, 0);
    defer builder.deinit();

    // Add some test data
    try builder.add("key1", ValueStruct{ .value = "value1", .timestamp = 1, .meta = 0 });
    try builder.add("key2", ValueStruct{ .value = "value2", .timestamp = 2, .meta = 0 });
    try builder.add("key3", ValueStruct{ .value = "value3", .timestamp = 3, .meta = 0 });

    std.testing.expect(builder.getEntryCount() == 3) catch unreachable;
    std.testing.expect(!builder.isFinished()) catch unreachable;

    // Finish the table
    try builder.finish();
    std.testing.expect(builder.isFinished()) catch unreachable;
}

test "TableBuilder key ordering validation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_path = "test_table_order.sst";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    const options = Options{
        .dir = ".",
        .value_dir = ".",
        .mem_table_size = 1024 * 1024,
        .num_memtables = 5,
        .max_levels = 7,
        .level_size_multiplier = 10,
        .base_table_size = 2 * 1024 * 1024,
        .num_compactors = 4,
        .num_level_zero_tables = 5,
        .num_level_zero_tables_stall = 15,
        .value_threshold = 1024 * 1024,
        .value_log_file_size = 1024 * 1024 * 1024,
        .block_size = 4 * 1024,
        .bloom_false_positive = 0.01,
        .compression = .none,
        .sync_writes = false,
        .detect_conflicts = true,
        .verify_checksums = true,
    };

    var builder = try TableBuilder.init(allocator, test_path, options, 0);
    defer builder.deinit();

    // Add keys in correct order
    try builder.add("key1", ValueStruct{ .value = "value1", .timestamp = 1, .meta = 0 });
    try builder.add("key2", ValueStruct{ .value = "value2", .timestamp = 2, .meta = 0 });

    // Try to add key out of order (should fail)
    const result = builder.add("key1", ValueStruct{ .value = "value1", .timestamp = 3, .meta = 0 });
    std.testing.expect(result == TableError.InvalidKeyOrder) catch unreachable;
}
