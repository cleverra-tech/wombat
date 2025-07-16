const std = @import("std");
const atomic = std.atomic;
const MmapFile = @import("../io/mmap.zig").MmapFile;

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
    timestamp: u64,
    meta: u8,

    const HEADER_SIZE = @sizeOf(u32) + @sizeOf(u32) + @sizeOf(u64) + @sizeOf(u8);

    pub fn encodedSize(self: *const Entry) usize {
        return HEADER_SIZE + self.key.len + self.value.len;
    }

    pub fn encode(self: *const Entry, buf: []u8) !usize {
        if (buf.len < self.encodedSize()) {
            return error.BufferTooSmall;
        }

        var offset: usize = 0;

        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.key.len), .little);
        offset += 4;

        std.mem.writeInt(u32, buf[offset..][0..4], @intCast(self.value.len), .little);
        offset += 4;

        std.mem.writeInt(u64, buf[offset..][0..8], self.timestamp, .little);
        offset += 8;

        buf[offset] = self.meta;
        offset += 1;

        @memcpy(buf[offset .. offset + self.key.len], self.key);
        offset += self.key.len;

        @memcpy(buf[offset .. offset + self.value.len], self.value);
        offset += self.value.len;

        return offset;
    }

    pub fn decode(buf: []const u8, allocator: std.mem.Allocator) !Entry {
        if (buf.len < HEADER_SIZE) {
            return error.BufferTooSmall;
        }

        var offset: usize = 0;

        const key_len = std.mem.readInt(u32, buf[offset .. offset + 4], .little);
        offset += 4;

        const value_len = std.mem.readInt(u32, buf[offset .. offset + 4], .little);
        offset += 4;

        const timestamp = std.mem.readInt(u64, buf[offset .. offset + 8], .little);
        offset += 8;

        const meta = buf[offset];
        offset += 1;

        if (buf.len < offset + key_len + value_len) {
            return error.BufferTooSmall;
        }

        const key = try allocator.dupe(u8, buf[offset .. offset + key_len]);
        offset += key_len;

        const value = try allocator.dupe(u8, buf[offset .. offset + value_len]);

        return Entry{
            .key = key,
            .value = value,
            .timestamp = timestamp,
            .meta = meta,
        };
    }
};

pub const WriteAheadLog = struct {
    file: MmapFile,
    write_offset: atomic.Value(u64),
    encryption_key: ?[]const u8,

    const Self = @This();

    pub fn open(path: []const u8) !Self {
        const file = try MmapFile.open(path, false);

        return Self{
            .file = file,
            .write_offset = atomic.Value(u64).init(file.size),
            .encryption_key = null,
        };
    }

    pub fn create(path: []const u8, file_size: usize) !Self {
        const file = try MmapFile.create(path, file_size);

        return Self{
            .file = file,
            .write_offset = atomic.Value(u64).init(0),
            .encryption_key = null,
        };
    }

    pub fn close(self: *Self) void {
        self.file.close();
    }

    pub fn writeEntry(self: *Self, entry: Entry) !void {
        const encoded_size = entry.encodedSize();
        const current_offset = self.write_offset.load(.acquire);

        if (current_offset + encoded_size > self.file.size) {
            try self.file.resize(self.file.size * 2);
        }

        const buffer = try self.file.getSlice(current_offset, encoded_size);
        _ = try entry.encode(buffer);

        _ = self.write_offset.fetchAdd(encoded_size, .acq_rel);
    }

    pub fn sync(self: *Self) !void {
        try self.file.sync();
    }

    pub fn replay(self: *Self, allocator: std.mem.Allocator, context: anytype, comptime callback: fn (@TypeOf(context), Entry) anyerror!void) !void {
        var offset: usize = 0;

        while (offset < self.write_offset.load(.acquire)) {
            if (offset + Entry.HEADER_SIZE > self.file.size) break;

            const header_buf = try self.file.getSliceConst(offset, Entry.HEADER_SIZE);

            var header_offset: usize = 0;
            const key_len = std.mem.readInt(u32, header_buf[header_offset .. header_offset + 4], .little);
            header_offset += 4;

            const value_len = std.mem.readInt(u32, header_buf[header_offset .. header_offset + 4], .little);
            header_offset += 4;

            const entry_size = Entry.HEADER_SIZE + key_len + value_len;

            if (offset + entry_size > self.file.size) break;

            const entry_buf = try self.file.getSliceConst(offset, entry_size);
            const entry = try Entry.decode(entry_buf, allocator);

            try callback(context, entry);

            offset += entry_size;
        }
    }

    pub fn reset(self: *Self) void {
        self.write_offset.store(0, .release);
    }

    pub fn size(self: *Self) u64 {
        return self.write_offset.load(.acquire);
    }
};
