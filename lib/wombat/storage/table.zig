const std = @import("std");
const MmapFile = @import("../io/mmap.zig").MmapFile;
const BloomFilter = @import("../core/bloom.zig").BloomFilter;
const ValueStruct = @import("../core/skiplist.zig").ValueStruct;
const SkipList = @import("../core/skiplist.zig").SkipList;
const Options = @import("../core/options.zig").Options;
const CompressionType = @import("../core/options.zig").CompressionType;

pub const TableIndex = struct {
    offsets: []u32,
    keys: [][]const u8,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !Self {
        const offsets = try allocator.alloc(u32, capacity);
        const keys = try allocator.alloc([]const u8, capacity);

        return Self{
            .offsets = offsets,
            .keys = keys,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.offsets);
        allocator.free(self.keys);
    }

    pub fn search(self: *const Self, key: []const u8) ?usize {
        var left: usize = 0;
        var right: usize = self.keys.len;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const cmp = std.mem.order(u8, self.keys[mid], key);

            switch (cmp) {
                .lt => left = mid + 1,
                .gt => right = mid,
                .eq => return mid,
            }
        }

        return if (left > 0) left - 1 else null;
    }
};

pub const Table = struct {
    mmap_file: MmapFile,
    index: TableIndex,
    bloom_filter: BloomFilter,
    smallest_key: []const u8,
    biggest_key: []const u8,
    compression: CompressionType,
    file_id: u64,
    level: u32,

    const Self = @This();
    const MAGIC_NUMBER: u32 = 0xCAFEBABE;
    const VERSION: u32 = 1;

    pub fn open(allocator: std.mem.Allocator, path: []const u8, file_id: u64) !Self {
        const mmap_file = try MmapFile.open(path, true);

        if (mmap_file.size < 32) {
            return error.InvalidTableFile;
        }

        const footer_offset = mmap_file.size - 32;
        const footer = try mmap_file.getSliceConst(footer_offset, 32);

        const magic = std.mem.readInt(u32, footer[0..4], .little);
        if (magic != MAGIC_NUMBER) {
            return error.InvalidTableFile;
        }

        const version = std.mem.readInt(u32, footer[4..8], .little);
        if (version != VERSION) {
            return error.UnsupportedVersion;
        }

        const index_offset = std.mem.readInt(u64, footer[8..16], .little);
        const bloom_offset = std.mem.readInt(u64, footer[16..24], .little);
        const compression = @as(CompressionType, @enumFromInt(std.mem.readInt(u32, footer[24..28], .little)));
        const level = std.mem.readInt(u32, footer[28..32], .little);

        const bloom_size = index_offset - bloom_offset;
        const bloom_data = try mmap_file.getSliceConst(bloom_offset, bloom_size);
        const bloom_filter = try BloomFilter.deserialize(bloom_data, allocator);

        var table = Self{
            .mmap_file = mmap_file,
            .index = undefined,
            .bloom_filter = bloom_filter,
            .smallest_key = &[_]u8{},
            .biggest_key = &[_]u8{},
            .compression = compression,
            .file_id = file_id,
            .level = level,
        };

        try table.loadIndex(allocator, index_offset, footer_offset - index_offset);

        return table;
    }

    pub fn close(self: *Self, allocator: std.mem.Allocator) void {
        self.index.deinit(allocator);
        self.bloom_filter.deinit(allocator);
        self.mmap_file.close();
    }

    fn loadIndex(self: *Self, allocator: std.mem.Allocator, offset: u64, size: u64) !void {
        const index_data = try self.mmap_file.getSliceConst(offset, size);

        var data_offset: usize = 0;
        const entry_count = std.mem.readInt(u32, index_data[data_offset .. data_offset + 4], .little);
        data_offset += 4;

        self.index = try TableIndex.init(allocator, entry_count);

        for (0..entry_count) |i| {
            self.index.offsets[i] = std.mem.readInt(u32, index_data[data_offset .. data_offset + 4], .little);
            data_offset += 4;
        }

        for (0..entry_count) |i| {
            const key_len = std.mem.readInt(u32, index_data[data_offset .. data_offset + 4], .little);
            data_offset += 4;

            self.index.keys[i] = index_data[data_offset .. data_offset + key_len];
            data_offset += key_len;
        }

        if (entry_count > 0) {
            self.smallest_key = self.index.keys[0];
            self.biggest_key = self.index.keys[entry_count - 1];
        }
    }

    pub fn get(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?ValueStruct {
        if (!self.bloom_filter.contains(key)) {
            return null;
        }

        const block_index = self.index.search(key) orelse return null;

        const block_data = try self.readBlock(allocator, block_index);
        defer allocator.free(block_data);

        return self.searchInBlock(block_data, key);
    }

    fn readBlock(self: *Self, allocator: std.mem.Allocator, block_index: usize) ![]u8 {
        const start_offset = self.index.offsets[block_index];
        const end_offset = if (block_index + 1 < self.index.offsets.len)
            self.index.offsets[block_index + 1]
        else
            @as(u32, @intCast(self.mmap_file.size - 32 - self.bloom_filter.serializedSize()));

        const block_size = end_offset - start_offset;
        const compressed_data = try self.mmap_file.getSliceConst(start_offset, block_size);

        return switch (self.compression) {
            .none => try allocator.dupe(u8, compressed_data),
            .snappy, .zstd => return error.CompressionNotImplemented,
        };
    }

    fn searchInBlock(self: *Self, block_data: []const u8, key: []const u8) ?ValueStruct {
        _ = self;
        var offset: usize = 0;

        while (offset < block_data.len) {
            if (offset + 16 > block_data.len) break;

            const key_len = std.mem.readInt(u32, block_data[offset .. offset + 4], .little);
            offset += 4;

            const value_len = std.mem.readInt(u32, block_data[offset .. offset + 4], .little);
            offset += 4;

            const timestamp = std.mem.readInt(u64, block_data[offset .. offset + 8], .little);
            offset += 8;

            if (offset + key_len + value_len > block_data.len) break;

            const entry_key = block_data[offset .. offset + key_len];
            offset += key_len;

            const entry_value = block_data[offset .. offset + value_len];
            offset += value_len;

            if (std.mem.eql(u8, entry_key, key)) {
                return ValueStruct{
                    .value = entry_value,
                    .timestamp = timestamp,
                    .meta = 0,
                };
            }

            if (std.mem.order(u8, entry_key, key) == .gt) {
                break;
            }
        }

        return null;
    }

    pub fn iterator(self: *Self, allocator: std.mem.Allocator) TableIterator {
        return TableIterator.init(self, allocator);
    }

    pub fn verifyChecksum(self: *Self) !void {
        _ = self;
    }

    pub fn overlapsWithKeyRange(self: *const Self, start_key: []const u8, end_key: []const u8) bool {
        return std.mem.order(u8, self.biggest_key, start_key) != .lt and
            std.mem.order(u8, self.smallest_key, end_key) != .gt;
    }
};

pub const TableIterator = struct {
    table: *Table,
    allocator: std.mem.Allocator,
    current_block: usize,
    block_data: ?[]u8,
    block_offset: usize,

    const Self = @This();

    pub fn init(table: *Table, allocator: std.mem.Allocator) Self {
        return Self{
            .table = table,
            .allocator = allocator,
            .current_block = 0,
            .block_data = null,
            .block_offset = 0,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.block_data) |data| {
            self.allocator.free(data);
        }
    }

    pub fn next(self: *Self) !?struct { key: []const u8, value: ValueStruct } {
        if (self.block_data == null) {
            try self.loadBlock();
        }

        while (self.current_block < self.table.index.offsets.len) {
            if (self.block_data) |data| {
                if (self.block_offset + 16 <= data.len) {
                    const key_len = std.mem.readInt(u32, data[self.block_offset .. self.block_offset + 4], .little);
                    self.block_offset += 4;

                    const value_len = std.mem.readInt(u32, data[self.block_offset .. self.block_offset + 4], .little);
                    self.block_offset += 4;

                    const timestamp = std.mem.readInt(u64, data[self.block_offset .. self.block_offset + 8], .little);
                    self.block_offset += 8;

                    if (self.block_offset + key_len + value_len <= data.len) {
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

            self.current_block += 1;
            try self.loadBlock();
        }

        return null;
    }

    fn loadBlock(self: *Self) !void {
        if (self.block_data) |data| {
            self.allocator.free(data);
            self.block_data = null;
        }

        if (self.current_block < self.table.index.offsets.len) {
            self.block_data = try self.table.readBlock(self.allocator, self.current_block);
            self.block_offset = 0;
        }
    }
};

pub const TableBuilder = struct {
    file: std.fs.File,
    block_data: std.ArrayList(u8),
    index_data: std.ArrayList(u8),
    bloom_filter: BloomFilter,
    current_block_offset: u32,
    entries_in_block: u32,
    block_size: usize,
    compression: CompressionType,
    level: u32,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, path: []const u8, options: Options, level: u32) !Self {
        const file = try std.fs.cwd().createFile(path, .{ .read = true, .truncate = true });

        const bloom_filter = try BloomFilter.init(allocator, 10000, options.bloom_false_positive);

        return Self{
            .file = file,
            .block_data = std.ArrayList(u8).init(allocator),
            .index_data = std.ArrayList(u8).init(allocator),
            .bloom_filter = bloom_filter,
            .current_block_offset = 0,
            .entries_in_block = 0,
            .block_size = options.block_size,
            .compression = options.compression,
            .level = level,
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        self.file.close();
        self.block_data.deinit();
        self.index_data.deinit();
        self.bloom_filter.deinit(allocator);
    }

    pub fn add(self: *Self, key: []const u8, value: ValueStruct) !void {
        const entry_size = 4 + 4 + 8 + key.len + value.value.len;

        if (self.block_data.items.len + entry_size > self.block_size and self.entries_in_block > 0) {
            try self.finishBlock(key);
        }

        try self.block_data.writer().writeInt(u32, @intCast(key.len), .little);
        try self.block_data.writer().writeInt(u32, @intCast(value.value.len), .little);
        try self.block_data.writer().writeInt(u64, value.timestamp, .little);
        try self.block_data.appendSlice(key);
        try self.block_data.appendSlice(value.value);

        self.bloom_filter.add(key);
        self.entries_in_block += 1;
    }

    fn finishBlock(self: *Self, next_key: []const u8) !void {
        const compressed_block = switch (self.compression) {
            .none => try self.block_data.toOwnedSlice(),
            .snappy, .zstd => return error.CompressionNotImplemented,
        };
        defer self.block_data.allocator.free(compressed_block);

        _ = try self.file.write(compressed_block);

        try self.index_data.writer().writeInt(u32, self.current_block_offset, .little);
        try self.index_data.writer().writeInt(u32, @intCast(next_key.len), .little);
        try self.index_data.appendSlice(next_key);

        self.current_block_offset += @intCast(compressed_block.len);
        self.entries_in_block = 0;
        self.block_data.clearRetainingCapacity();
    }

    pub fn finish(self: *Self, allocator: std.mem.Allocator) !void {
        if (self.entries_in_block > 0) {
            const compressed_block = switch (self.compression) {
                .none => try self.block_data.toOwnedSlice(),
                .snappy, .zstd => return error.CompressionNotImplemented,
            };
            defer allocator.free(compressed_block);

            _ = try self.file.write(compressed_block);
        }

        const bloom_data = try allocator.alloc(u8, self.bloom_filter.serializedSize());
        defer allocator.free(bloom_data);
        _ = try self.bloom_filter.serialize(bloom_data);

        const bloom_offset = try self.file.getPos();
        _ = try self.file.write(bloom_data);

        const index_offset = try self.file.getPos();
        try self.file.writer().writeInt(u32, @intCast(self.index_data.items.len / (4 + 4)), .little);
        _ = try self.file.write(self.index_data.items);

        const footer_offset = try self.file.getPos();
        try self.file.writer().writeInt(u32, Table.MAGIC_NUMBER, .little);
        try self.file.writer().writeInt(u32, Table.VERSION, .little);
        try self.file.writer().writeInt(u64, index_offset, .little);
        try self.file.writer().writeInt(u64, bloom_offset, .little);
        try self.file.writer().writeInt(u32, @intFromEnum(self.compression), .little);
        try self.file.writer().writeInt(u32, self.level, .little);

        _ = footer_offset;
    }
};
