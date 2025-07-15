const std = @import("std");

pub fn Channel(comptime T: type) type {
    return struct {
        buffer: []T,
        read_index: std.atomic.Value(usize),
        write_index: std.atomic.Value(usize),
        capacity: usize,
        allocator: std.mem.Allocator,
        mutex: std.Thread.Mutex,
        not_empty: std.Thread.Condition,
        not_full: std.Thread.Condition,

        const Self = @This();

        pub fn init(allocator: std.mem.Allocator, capacity: usize) Self {
            const buffer = allocator.alloc(T, capacity) catch unreachable;

            return Self{
                .buffer = buffer,
                .read_index = std.atomic.Value(usize).init(0),
                .write_index = std.atomic.Value(usize).init(0),
                .capacity = capacity,
                .allocator = allocator,
                .mutex = std.Thread.Mutex{},
                .not_empty = std.Thread.Condition{},
                .not_full = std.Thread.Condition{},
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.buffer);
        }

        pub fn send(self: *Self, item: T) !void {
            self.mutex.lock();
            defer self.mutex.unlock();

            const write_idx = self.write_index.load(.acquire);
            const read_idx = self.read_index.load(.acquire);

            while ((write_idx + 1) % self.capacity == read_idx) {
                self.not_full.wait(&self.mutex);
            }

            self.buffer[write_idx] = item;
            self.write_index.store((write_idx + 1) % self.capacity, .release);

            self.not_empty.signal();
        }

        pub fn receive(self: *Self) !T {
            self.mutex.lock();
            defer self.mutex.unlock();

            const read_idx = self.read_index.load(.acquire);
            const write_idx = self.write_index.load(.acquire);

            while (read_idx == write_idx) {
                self.not_empty.wait(&self.mutex);
            }

            const item = self.buffer[read_idx];
            self.read_index.store((read_idx + 1) % self.capacity, .release);

            self.not_full.signal();

            return item;
        }

        pub fn tryReceive(self: *Self) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            const read_idx = self.read_index.load(.acquire);
            const write_idx = self.write_index.load(.acquire);

            if (read_idx == write_idx) {
                return null;
            }

            const item = self.buffer[read_idx];
            self.read_index.store((read_idx + 1) % self.capacity, .release);

            self.not_full.signal();

            return item;
        }

        pub fn isEmpty(self: *const Self) bool {
            const read_idx = self.read_index.load(.acquire);
            const write_idx = self.write_index.load(.acquire);
            return read_idx == write_idx;
        }

        pub fn isFull(self: *const Self) bool {
            const read_idx = self.read_index.load(.acquire);
            const write_idx = self.write_index.load(.acquire);
            return (write_idx + 1) % self.capacity == read_idx;
        }
    };
}
