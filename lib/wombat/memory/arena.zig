const std = @import("std");
const Allocator = std.mem.Allocator;
const atomic = std.atomic;

pub const Arena = struct {
    buffer: []u8,
    offset: atomic.Value(usize),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, size: usize) !Self {
        const buffer = try allocator.alloc(u8, size);
        return Self{
            .buffer = buffer,
            .offset = atomic.Value(usize).init(0),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.buffer);
    }

    pub fn alloc(self: *Self, comptime T: type, count: usize) ![]T {
        const size = @sizeOf(T) * count;
        const alignment = @alignOf(T);

        const current_offset = self.offset.load(.acquire);
        const aligned_offset = std.mem.alignForward(usize, current_offset, alignment);
        const new_offset = aligned_offset + size;

        if (new_offset > self.buffer.len) {
            return error.OutOfMemory;
        }

        const result = self.offset.cmpxchgWeak(current_offset, new_offset, .acq_rel, .acquire);
        if (result == null) {
            const ptr = @as([*]T, @ptrCast(@alignCast(&self.buffer[aligned_offset])));
            return ptr[0..count];
        } else {
            return self.alloc(T, count);
        }
    }

    pub fn allocBytes(self: *Self, size: usize) ![]u8 {
        return self.alloc(u8, size);
    }

    pub fn reset(self: *Self) void {
        self.offset.store(0, .release);
    }

    pub fn bytesUsed(self: *Self) usize {
        return self.offset.load(.acquire);
    }

    pub fn bytesRemaining(self: *Self) usize {
        return self.buffer.len - self.bytesUsed();
    }
};
