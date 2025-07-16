const std = @import("std");
const fs = std.fs;
const posix = std.posix;

pub const MmapFile = struct {
    file: fs.File,
    data: []u8,
    size: usize,
    read_only: bool,

    const Self = @This();

    pub fn open(path: []const u8, read_only: bool) !Self {
        const file = if (read_only)
            try fs.cwd().openFile(path, .{})
        else
            try fs.cwd().createFile(path, .{ .read = true, .truncate = false });

        const stat = try file.stat();
        const size = stat.size;

        if (size == 0) {
            return Self{
                .file = file,
                .data = &[_]u8{},
                .size = 0,
                .read_only = read_only,
            };
        }

        const prot = if (read_only) @as(u32, posix.PROT.READ) else @as(u32, posix.PROT.READ | posix.PROT.WRITE);
        const data = try posix.mmap(null, size, prot, .{ .TYPE = .SHARED }, file.handle, 0);

        return Self{
            .file = file,
            .data = data,
            .size = size,
            .read_only = read_only,
        };
    }

    pub fn create(path: []const u8, size: usize) !Self {
        const file = try fs.cwd().createFile(path, .{ .read = true, .truncate = true });
        try file.setEndPos(size);

        const data = try posix.mmap(null, size, posix.PROT.READ | posix.PROT.WRITE, .{ .TYPE = .SHARED }, file.handle, 0);

        return Self{
            .file = file,
            .data = data,
            .size = size,
            .read_only = false,
        };
    }

    pub fn close(self: *Self) void {
        if (self.data.len > 0) {
            posix.munmap(@alignCast(self.data));
        }
        self.file.close();
    }

    pub fn sync(self: *Self) !void {
        if (self.data.len > 0 and !self.read_only) {
            try posix.msync(self.data, posix.MSF.SYNC);
        }
    }

    pub fn resize(self: *Self, new_size: usize) !void {
        if (self.read_only) {
            return error.ReadOnlyFile;
        }

        if (self.data.len > 0) {
            posix.munmap(@alignCast(self.data));
        }

        try self.file.setEndPos(new_size);

        self.data = try posix.mmap(null, new_size, posix.PROT.READ | posix.PROT.WRITE, .{ .TYPE = .SHARED }, self.file.handle, 0);
        self.size = new_size;
    }

    pub fn readAt(self: *Self, offset: usize, buf: []u8) !usize {
        if (offset >= self.size) {
            return 0;
        }

        const available = self.size - offset;
        const to_read = @min(buf.len, available);

        @memcpy(buf[0..to_read], self.data[offset .. offset + to_read]);
        return to_read;
    }

    pub fn writeAt(self: *Self, offset: usize, data: []const u8) !void {
        if (self.read_only) {
            return error.ReadOnlyFile;
        }

        if (offset + data.len > self.size) {
            return error.OutOfBounds;
        }

        @memcpy(self.data[offset .. offset + data.len], data);
    }

    pub fn getSlice(self: *Self, offset: usize, len: usize) ![]u8 {
        if (offset + len > self.size) {
            return error.OutOfBounds;
        }

        return self.data[offset .. offset + len];
    }

    pub fn getSliceConst(self: *const Self, offset: usize, len: usize) ![]const u8 {
        if (offset + len > self.size) {
            return error.OutOfBounds;
        }

        return self.data[offset .. offset + len];
    }
};
