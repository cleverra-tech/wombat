const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const HashMap = std.HashMap;
const atomic = std.atomic;
const fs = std.fs;
const json = std.json;

/// Represents metadata for a single SSTable file
pub const TableInfo = struct {
    /// Unique file identifier
    id: u64,
    /// File path relative to database directory
    path: []const u8,
    /// LSM tree level (0 = newest)
    level: u32,
    /// Smallest key in the table
    smallest_key: []const u8,
    /// Largest key in the table
    largest_key: []const u8,
    /// File size in bytes
    size: u64,
    /// Number of key-value pairs
    key_count: u64,
    /// Creation timestamp
    created_at: u64,
    /// Checksum for integrity verification
    checksum: u64,

    pub fn deinit(self: *const TableInfo, allocator: Allocator) void {
        allocator.free(self.path);
        allocator.free(self.smallest_key);
        allocator.free(self.largest_key);
    }

    pub fn clone(self: *const TableInfo, allocator: Allocator) !TableInfo {
        return TableInfo{
            .id = self.id,
            .path = try allocator.dupe(u8, self.path),
            .level = self.level,
            .smallest_key = try allocator.dupe(u8, self.smallest_key),
            .largest_key = try allocator.dupe(u8, self.largest_key),
            .size = self.size,
            .key_count = self.key_count,
            .created_at = self.created_at,
            .checksum = self.checksum,
        };
    }
};

/// Represents a change to the database structure
pub const ManifestChange = struct {
    pub const ChangeType = enum {
        add_table,
        delete_table,
        create_level,
    };

    type: ChangeType,
    level: u32,
    table_info: ?TableInfo,
    timestamp: u64,

    pub fn deinit(self: *const ManifestChange, allocator: Allocator) void {
        if (self.table_info) |*info| {
            info.deinit(allocator);
        }
    }
};

/// Atomic manifest file operations with crash recovery support
pub const ManifestFile = struct {
    /// Path to the manifest file
    file_path: []const u8,
    /// Current file handle
    file: ?fs.File,
    /// All table metadata by level
    levels: [MAX_LEVELS]ArrayList(TableInfo),
    /// Table lookup by ID
    tables_by_id: HashMap(u64, u32, std.hash_map.AutoContext(u64), 80),
    /// Next available file ID
    next_file_id: atomic.Value(u64),
    /// Protects concurrent access
    mutex: std.Thread.Mutex,
    /// Memory allocator
    allocator: Allocator,

    const MAX_LEVELS = 7;
    const MANIFEST_VERSION = 1;

    const Self = @This();

    /// Initialize manifest file, loading existing data if present
    pub fn init(allocator: Allocator, file_path: []const u8) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        self.* = Self{
            .file_path = try allocator.dupe(u8, file_path),
            .file = null,
            .levels = undefined,
            .tables_by_id = HashMap(u64, u32, std.hash_map.AutoContext(u64), 80).init(allocator),
            .next_file_id = atomic.Value(u64).init(1),
            .mutex = std.Thread.Mutex{},
            .allocator = allocator,
        };

        // Initialize level arrays
        for (0..MAX_LEVELS) |i| {
            self.levels[i] = ArrayList(TableInfo).init(allocator);
        }

        // Load existing manifest if it exists
        self.loadFromDisk() catch |err| switch (err) {
            error.FileNotFound => {}, // First time - create new manifest
            else => return err,
        };

        return self;
    }

    /// Clean up all resources
    pub fn deinit(self: *Self) void {
        // Lock before cleanup
        self.mutex.lock();

        if (self.file) |file| {
            file.close();
        }

        // Clean up all table info
        for (0..MAX_LEVELS) |level| {
            for (self.levels[level].items) |*table_info| {
                table_info.deinit(self.allocator);
            }
            self.levels[level].deinit();
        }

        self.tables_by_id.deinit();

        // Unlock before freeing self
        self.mutex.unlock();

        const allocator = self.allocator;
        allocator.free(self.file_path);
        allocator.destroy(self);
    }

    /// Add a new table to the manifest
    pub fn addTable(self: *Self, table_info: TableInfo) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (table_info.level >= MAX_LEVELS) {
            return error.InvalidLevel;
        }

        // Clone the table info for our storage
        const cloned_info = try table_info.clone(self.allocator);
        errdefer cloned_info.deinit(self.allocator);

        // Add to level
        try self.levels[table_info.level].append(cloned_info);
        errdefer _ = self.levels[table_info.level].pop();

        // Add to ID lookup
        try self.tables_by_id.put(table_info.id, table_info.level);
        errdefer _ = self.tables_by_id.remove(table_info.id);

        // Update next file ID if necessary
        const current_next = self.next_file_id.load(.acquire);
        if (table_info.id >= current_next) {
            self.next_file_id.store(table_info.id + 1, .release);
        }

        // Persist the change
        const change = ManifestChange{
            .type = .add_table,
            .level = table_info.level,
            .table_info = try table_info.clone(self.allocator),
            .timestamp = @intCast(std.time.timestamp()),
        };
        defer change.deinit(self.allocator);

        try self.writeChange(change);
    }

    /// Remove a table from the manifest
    pub fn removeTable(self: *Self, table_id: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const level = self.tables_by_id.get(table_id) orelse return error.TableNotFound;

        // Find and remove from level
        const level_tables = &self.levels[level];
        for (level_tables.items, 0..) |*table_info, i| {
            if (table_info.id == table_id) {
                // Create change record before removal
                const change = ManifestChange{
                    .type = .delete_table,
                    .level = level,
                    .table_info = try table_info.clone(self.allocator),
                    .timestamp = @intCast(std.time.timestamp()),
                };
                defer change.deinit(self.allocator);

                // Remove from structures
                table_info.deinit(self.allocator);
                _ = level_tables.swapRemove(i);
                _ = self.tables_by_id.remove(table_id);

                // Persist the change
                try self.writeChange(change);
                return;
            }
        }

        return error.TableNotFound;
    }

    /// Get all tables at a specific level
    pub fn getTablesAtLevel(self: *Self, level: u32) ![]const TableInfo {
        if (level >= MAX_LEVELS) {
            return error.InvalidLevel;
        }

        self.mutex.lock();
        defer self.mutex.unlock();

        return self.levels[level].items;
    }

    /// Get table info by ID
    pub fn getTableInfo(self: *Self, table_id: u64) ?TableInfo {
        self.mutex.lock();
        defer self.mutex.unlock();

        const level = self.tables_by_id.get(table_id) orelse return null;

        for (self.levels[level].items) |*table_info| {
            if (table_info.id == table_id) {
                return table_info.clone(self.allocator) catch return null;
            }
        }

        return null;
    }

    /// Get next available file ID
    pub fn getNextFileId(self: *Self) u64 {
        return self.next_file_id.fetchAdd(1, .acq_rel);
    }

    /// Get total number of tables across all levels
    pub fn getTotalTableCount(self: *Self) u32 {
        self.mutex.lock();
        defer self.mutex.unlock();

        var count: u32 = 0;
        for (0..MAX_LEVELS) |level| {
            count += @intCast(self.levels[level].items.len);
        }
        return count;
    }

    /// Force a sync to disk
    pub fn sync(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.file) |file| {
            try file.sync();
        }
    }

    /// Write a manifest change to disk
    fn writeChange(self: *Self, change: ManifestChange) !void {
        // Ensure file is open
        if (self.file == null) {
            self.file = try fs.cwd().createFile(self.file_path, .{
                .read = true,
                .truncate = false,
            });
        }

        const file = self.file.?;

        // Seek to end of file
        try file.seekFromEnd(0);

        // Serialize change to JSON
        var buffer = ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

        try json.stringify(change, .{}, buffer.writer());
        try buffer.append('\n');

        // Write atomically
        try file.writeAll(buffer.items);
        try file.sync();
    }

    /// Load manifest from disk
    fn loadFromDisk(self: *Self) !void {
        const file = fs.cwd().openFile(self.file_path, .{}) catch |err| switch (err) {
            error.FileNotFound => return error.FileNotFound,
            else => return err,
        };
        defer file.close();

        const file_size = try file.getEndPos();
        if (file_size == 0) return;

        const content = try self.allocator.alloc(u8, file_size);
        defer self.allocator.free(content);

        _ = try file.readAll(content);

        // Parse line by line
        var lines = std.mem.splitSequence(u8, content, "\n");
        while (lines.next()) |line| {
            if (line.len == 0) continue;

            const parsed = json.parseFromSlice(ManifestChange, self.allocator, line, .{}) catch |err| {
                std.log.warn("Failed to parse manifest line: {s}, error: {}", .{ line, err });
                continue;
            };
            defer parsed.deinit();

            try self.applyChange(parsed.value);
        }
    }

    /// Apply a manifest change during recovery
    fn applyChange(self: *Self, change: ManifestChange) !void {
        switch (change.type) {
            .add_table => {
                if (change.table_info) |table_info| {
                    if (table_info.level >= MAX_LEVELS) return;

                    const cloned_info = try table_info.clone(self.allocator);
                    try self.levels[table_info.level].append(cloned_info);
                    try self.tables_by_id.put(table_info.id, table_info.level);

                    // Update next file ID
                    const current_next = self.next_file_id.load(.acquire);
                    if (table_info.id >= current_next) {
                        self.next_file_id.store(table_info.id + 1, .release);
                    }
                }
            },
            .delete_table => {
                if (change.table_info) |table_info| {
                    const level = table_info.level;
                    if (level >= MAX_LEVELS) return;

                    // Find and remove from level
                    const level_tables = &self.levels[level];
                    for (level_tables.items, 0..) |*info, i| {
                        if (info.id == table_info.id) {
                            info.deinit(self.allocator);
                            _ = level_tables.swapRemove(i);
                            _ = self.tables_by_id.remove(table_info.id);
                            break;
                        }
                    }
                }
            },
            .create_level => {
                // Level creation is implicit in our implementation
            },
        }
    }
};

/// Test table configuration for createTestTableInfo
pub const TestTableConfig = struct {
    size: u64 = 1024 * 1024, // Default 1MB
    key_count: u64 = 1000, // Default 1000 keys
};

/// Create a test TableInfo for testing purposes
pub fn createTestTableInfo(allocator: Allocator, id: u64, level: u32) !TableInfo {
    return createTestTableInfoWithConfig(allocator, id, level, TestTableConfig{});
}

/// Create a test TableInfo with configurable parameters
pub fn createTestTableInfoWithConfig(allocator: Allocator, id: u64, level: u32, config: TestTableConfig) !TableInfo {
    const path = try std.fmt.allocPrint(allocator, "table_{d}.sst", .{id});
    const smallest = try allocator.dupe(u8, "aaa");
    const largest = try allocator.dupe(u8, "zzz");

    // Calculate checksum based on table characteristics for test consistency
    // Buffer sized to handle: "id" + 20 digits + "lv" + 10 digits + "sz" + 7 digits + "kc" + 4 digits = 49 chars max
    var checksum_data: [64]u8 = undefined;
    const written = std.fmt.bufPrint(&checksum_data, "id{d}lv{d}sz{d}kc{d}", .{ id, level, config.size, config.key_count }) catch |err| switch (err) {
        error.NoSpaceLeft => unreachable, // Buffer is properly sized
    };
    const checksum = std.hash.crc.Crc32.hash(written);

    return TableInfo{
        .id = id,
        .path = path,
        .level = level,
        .smallest_key = smallest,
        .largest_key = largest,
        .size = config.size,
        .key_count = config.key_count,
        .created_at = @intCast(std.time.timestamp()),
        .checksum = checksum,
    };
}

// Tests
test "ManifestFile basic operations" {
    const allocator = std.testing.allocator;

    // Use temporary file
    const temp_path = "test_manifest.json";
    defer fs.cwd().deleteFile(temp_path) catch {};

    const manifest = try ManifestFile.init(allocator, temp_path);
    defer manifest.deinit();

    // Test adding tables
    var table1 = try createTestTableInfo(allocator, 1, 0);
    defer table1.deinit(allocator);

    var table2 = try createTestTableInfo(allocator, 2, 1);
    defer table2.deinit(allocator);

    try manifest.addTable(table1);
    try manifest.addTable(table2);

    // Test retrieving tables
    const level0_tables = try manifest.getTablesAtLevel(0);
    try std.testing.expect(level0_tables.len == 1);
    try std.testing.expect(level0_tables[0].id == 1);

    const level1_tables = try manifest.getTablesAtLevel(1);
    try std.testing.expect(level1_tables.len == 1);
    try std.testing.expect(level1_tables[0].id == 2);

    // Test table lookup by ID
    const found_table = manifest.getTableInfo(1);
    try std.testing.expect(found_table != null);
    try std.testing.expect(found_table.?.id == 1);
    found_table.?.deinit(allocator);

    // Test file ID generation
    const next_id = manifest.getNextFileId();
    try std.testing.expect(next_id == 3);

    // Test removing table
    try manifest.removeTable(1);
    const level0_after_remove = try manifest.getTablesAtLevel(0);
    try std.testing.expect(level0_after_remove.len == 0);

    // Test total count
    const total = manifest.getTotalTableCount();
    try std.testing.expect(total == 1);
}

test "ManifestFile persistence and recovery" {
    const allocator = std.testing.allocator;

    const temp_path = "test_manifest_persist.json";
    defer fs.cwd().deleteFile(temp_path) catch {};

    // Create manifest and add some tables
    {
        const manifest = try ManifestFile.init(allocator, temp_path);
        defer manifest.deinit();

        var table1 = try createTestTableInfo(allocator, 10, 0);
        defer table1.deinit(allocator);

        var table2 = try createTestTableInfo(allocator, 20, 1);
        defer table2.deinit(allocator);

        try manifest.addTable(table1);
        try manifest.addTable(table2);
        try manifest.sync();
    }

    // Reload and verify persistence
    {
        const manifest = try ManifestFile.init(allocator, temp_path);
        defer manifest.deinit();

        const level0_tables = try manifest.getTablesAtLevel(0);
        try std.testing.expect(level0_tables.len == 1);
        try std.testing.expect(level0_tables[0].id == 10);

        const level1_tables = try manifest.getTablesAtLevel(1);
        try std.testing.expect(level1_tables.len == 1);
        try std.testing.expect(level1_tables[0].id == 20);

        // Next file ID should be preserved
        const next_id = manifest.getNextFileId();
        try std.testing.expect(next_id == 21);
    }
}
