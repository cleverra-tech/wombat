const std = @import("std");
const Allocator = std.mem.Allocator;
const atomic = std.atomic;
const Mutex = std.Thread.Mutex;
const Condition = std.Thread.Condition;

/// Channel errors
pub const ChannelError = error{
    ChannelClosed,
    SendTimeout,
    ReceiveTimeout,
    OutOfMemory,
    InvalidCapacity,
};

/// Channel statistics for monitoring
pub const ChannelStats = struct {
    sends_total: u64,
    receives_total: u64,
    send_timeouts: u64,
    receive_timeouts: u64,
    current_size: usize,
    max_size_reached: usize,
};

/// Thread-safe channel for async coordination between components
/// Supports blocking and non-blocking operations with timeout capabilities
pub fn Channel(comptime T: type) type {
    return struct {
        buffer: []T,
        read_index: usize,
        write_index: usize,
        current_size: usize,
        capacity: usize,
        allocator: Allocator,
        mutex: Mutex,
        not_empty: Condition,
        not_full: Condition,
        closed: bool,
        stats: ChannelStats,

        const Self = @This();

        /// Initialize a new channel with the specified capacity
        pub fn init(allocator: Allocator, capacity: usize) !Self {
            if (capacity == 0) {
                return ChannelError.InvalidCapacity;
            }

            const buffer = try allocator.alloc(T, capacity);

            return Self{
                .buffer = buffer,
                .read_index = 0,
                .write_index = 0,
                .current_size = 0,
                .capacity = capacity,
                .allocator = allocator,
                .mutex = Mutex{},
                .not_empty = Condition{},
                .not_full = Condition{},
                .closed = false,
                .stats = ChannelStats{
                    .sends_total = 0,
                    .receives_total = 0,
                    .send_timeouts = 0,
                    .receive_timeouts = 0,
                    .current_size = 0,
                    .max_size_reached = 0,
                },
            };
        }

        /// Clean up channel resources
        pub fn deinit(self: *Self) void {
            self.close();
            self.allocator.free(self.buffer);
        }

        /// Send an item to the channel (blocking)
        pub fn send(self: *Self, item: T) ChannelError!void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.closed) {
                return ChannelError.ChannelClosed;
            }

            // Wait until there's space available
            while (self.current_size >= self.capacity and !self.closed) {
                self.not_full.wait(&self.mutex);
            }

            if (self.closed) {
                return ChannelError.ChannelClosed;
            }

            // Add item to buffer
            self.buffer[self.write_index] = item;
            self.write_index = (self.write_index + 1) % self.capacity;
            self.current_size += 1;

            // Update statistics
            self.stats.sends_total += 1;
            self.stats.current_size = self.current_size;
            if (self.current_size > self.stats.max_size_reached) {
                self.stats.max_size_reached = self.current_size;
            }

            // Wake up waiting receivers
            self.not_empty.signal();
        }

        /// Try to send an item without blocking
        pub fn trySend(self: *Self, item: T) ChannelError!bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.closed) {
                return ChannelError.ChannelClosed;
            }

            if (self.current_size >= self.capacity) {
                return false; // Channel is full
            }

            // Add item to buffer
            self.buffer[self.write_index] = item;
            self.write_index = (self.write_index + 1) % self.capacity;
            self.current_size += 1;

            // Update statistics
            self.stats.sends_total += 1;
            self.stats.current_size = self.current_size;
            if (self.current_size > self.stats.max_size_reached) {
                self.stats.max_size_reached = self.current_size;
            }

            // Wake up waiting receivers
            self.not_empty.signal();

            return true;
        }

        /// Send an item with timeout (in nanoseconds)
        pub fn sendTimeout(self: *Self, item: T, timeout_ns: u64) ChannelError!void {
            const start_time = std.time.nanoTimestamp();

            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.closed) {
                return ChannelError.ChannelClosed;
            }

            // Wait until there's space available or timeout
            while (self.current_size >= self.capacity and !self.closed) {
                const elapsed = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
                if (elapsed >= timeout_ns) {
                    self.stats.send_timeouts += 1;
                    return ChannelError.SendTimeout;
                }

                const remaining = timeout_ns - elapsed;
                self.not_full.timedWait(&self.mutex, remaining) catch {
                    self.stats.send_timeouts += 1;
                    return ChannelError.SendTimeout;
                };
            }

            if (self.closed) {
                return ChannelError.ChannelClosed;
            }

            // Add item to buffer
            self.buffer[self.write_index] = item;
            self.write_index = (self.write_index + 1) % self.capacity;
            self.current_size += 1;

            // Update statistics
            self.stats.sends_total += 1;
            self.stats.current_size = self.current_size;
            if (self.current_size > self.stats.max_size_reached) {
                self.stats.max_size_reached = self.current_size;
            }

            // Wake up waiting receivers
            self.not_empty.signal();
        }

        /// Receive an item from the channel (blocking)
        pub fn receive(self: *Self) ChannelError!T {
            self.mutex.lock();
            defer self.mutex.unlock();

            // Wait until there's an item available
            while (self.current_size == 0 and !self.closed) {
                self.not_empty.wait(&self.mutex);
            }

            if (self.current_size == 0 and self.closed) {
                return ChannelError.ChannelClosed;
            }

            // Get item from buffer
            const item = self.buffer[self.read_index];
            self.read_index = (self.read_index + 1) % self.capacity;
            self.current_size -= 1;

            // Update statistics
            self.stats.receives_total += 1;
            self.stats.current_size = self.current_size;

            // Wake up waiting senders
            self.not_full.signal();

            return item;
        }

        /// Try to receive an item without blocking
        pub fn tryReceive(self: *Self) ChannelError!?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.closed and self.current_size == 0) {
                return ChannelError.ChannelClosed;
            }

            if (self.current_size == 0) {
                return null; // Channel is empty
            }

            // Get item from buffer
            const item = self.buffer[self.read_index];
            self.read_index = (self.read_index + 1) % self.capacity;
            self.current_size -= 1;

            // Update statistics
            self.stats.receives_total += 1;
            self.stats.current_size = self.current_size;

            // Wake up waiting senders
            self.not_full.signal();

            return item;
        }

        /// Receive an item with timeout (in nanoseconds)
        pub fn receiveTimeout(self: *Self, timeout_ns: u64) ChannelError!T {
            const start_time = std.time.nanoTimestamp();

            self.mutex.lock();
            defer self.mutex.unlock();

            // Wait until there's an item available or timeout
            while (self.current_size == 0 and !self.closed) {
                const elapsed = @as(u64, @intCast(std.time.nanoTimestamp() - start_time));
                if (elapsed >= timeout_ns) {
                    self.stats.receive_timeouts += 1;
                    return ChannelError.ReceiveTimeout;
                }

                const remaining = timeout_ns - elapsed;
                self.not_empty.timedWait(&self.mutex, remaining) catch {
                    self.stats.receive_timeouts += 1;
                    return ChannelError.ReceiveTimeout;
                };
            }

            if (self.current_size == 0 and self.closed) {
                return ChannelError.ChannelClosed;
            }

            // Get item from buffer
            const item = self.buffer[self.read_index];
            self.read_index = (self.read_index + 1) % self.capacity;
            self.current_size -= 1;

            // Update statistics
            self.stats.receives_total += 1;
            self.stats.current_size = self.current_size;

            // Wake up waiting senders
            self.not_full.signal();

            return item;
        }

        /// Close the channel - prevents new sends and wakes all waiting threads
        pub fn close(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.closed) {
                return;
            }

            self.closed = true;

            // Wake up all waiting threads
            self.not_empty.broadcast();
            self.not_full.broadcast();
        }

        /// Check if the channel is empty
        pub fn isEmpty(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.current_size == 0;
        }

        /// Check if the channel is full
        pub fn isFull(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.current_size >= self.capacity;
        }

        /// Check if the channel is closed
        pub fn isClosed(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.closed;
        }

        /// Get current size of the channel
        pub fn size(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.current_size;
        }

        /// Get channel capacity
        pub fn getCapacity(self: *Self) usize {
            return self.capacity;
        }

        /// Get channel statistics (creates a copy for thread safety)
        pub fn getStats(self: *Self) ChannelStats {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.stats;
        }

        /// Reset channel statistics
        pub fn resetStats(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();
            self.stats = ChannelStats{
                .sends_total = 0,
                .receives_total = 0,
                .send_timeouts = 0,
                .receive_timeouts = 0,
                .current_size = self.current_size,
                .max_size_reached = 0,
            };
        }

        /// Select-like operation: try to receive from multiple channels
        /// Returns the index of the channel that had data available, or null if none
        pub fn selectReceive(channels: []*Self) ?struct { index: usize, value: T } {
            // Try each channel once without blocking
            for (channels, 0..) |channel, i| {
                if (channel.tryReceive()) |maybe_value| {
                    if (maybe_value) |value| {
                        return .{ .index = i, .value = value };
                    }
                } else |_| {
                    // Channel closed or error, skip
                    continue;
                }
            }
            return null;
        }

        /// Drain all items from the channel without blocking
        /// Returns the number of items drained
        pub fn drain(self: *Self, allocator: Allocator) ![]T {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.current_size == 0) {
                return try allocator.alloc(T, 0);
            }

            const items = try allocator.alloc(T, self.current_size);
            var count: usize = 0;

            while (self.current_size > 0 and count < items.len) {
                items[count] = self.buffer[self.read_index];
                self.read_index = (self.read_index + 1) % self.capacity;
                self.current_size -= 1;
                count += 1;
            }

            // Update statistics
            self.stats.receives_total += count;
            self.stats.current_size = self.current_size;

            // Wake up all waiting senders
            self.not_full.broadcast();

            return items[0..count];
        }
    };
}

// Tests
test "Channel basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var channel = try Channel(u32).init(allocator, 3);
    defer channel.deinit();

    // Test initial state
    std.testing.expect(channel.isEmpty()) catch unreachable;
    std.testing.expect(!channel.isFull()) catch unreachable;
    std.testing.expect(!channel.isClosed()) catch unreachable;
    std.testing.expect(channel.size() == 0) catch unreachable;
    std.testing.expect(channel.getCapacity() == 3) catch unreachable;

    // Test send and receive
    try channel.send(42);
    std.testing.expect(!channel.isEmpty()) catch unreachable;
    std.testing.expect(channel.size() == 1) catch unreachable;

    const received = try channel.receive();
    std.testing.expect(received == 42) catch unreachable;
    std.testing.expect(channel.isEmpty()) catch unreachable;
}

test "Channel try operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var channel = try Channel(u32).init(allocator, 2);
    defer channel.deinit();

    // Test trySend when not full
    const sent1 = try channel.trySend(1);
    std.testing.expect(sent1) catch unreachable;

    const sent2 = try channel.trySend(2);
    std.testing.expect(sent2) catch unreachable;

    // Test trySend when full
    const sent3 = try channel.trySend(3);
    std.testing.expect(!sent3) catch unreachable;

    // Test tryReceive when not empty
    const received1 = try channel.tryReceive();
    std.testing.expect(received1 != null and received1.? == 1) catch unreachable;

    const received2 = try channel.tryReceive();
    std.testing.expect(received2 != null and received2.? == 2) catch unreachable;

    // Test tryReceive when empty
    const received3 = try channel.tryReceive();
    std.testing.expect(received3 == null) catch unreachable;
}

test "Channel close operation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var channel = try Channel(u32).init(allocator, 2);
    defer channel.deinit();

    try channel.send(42);
    channel.close();

    std.testing.expect(channel.isClosed()) catch unreachable;

    // Should still be able to receive existing items
    const received = try channel.receive();
    std.testing.expect(received == 42) catch unreachable;

    // Should fail to send on closed channel
    const send_result = channel.send(99);
    std.testing.expect(send_result == ChannelError.ChannelClosed) catch unreachable;

    // Should fail to receive from empty closed channel
    const receive_result = channel.receive();
    std.testing.expect(receive_result == ChannelError.ChannelClosed) catch unreachable;
}

test "Channel statistics" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var channel = try Channel(u32).init(allocator, 3);
    defer channel.deinit();

    // Test initial stats
    var stats = channel.getStats();
    std.testing.expect(stats.sends_total == 0) catch unreachable;
    std.testing.expect(stats.receives_total == 0) catch unreachable;

    // Send some items
    try channel.send(1);
    try channel.send(2);

    stats = channel.getStats();
    std.testing.expect(stats.sends_total == 2) catch unreachable;
    std.testing.expect(stats.current_size == 2) catch unreachable;
    std.testing.expect(stats.max_size_reached == 2) catch unreachable;

    // Receive an item
    _ = try channel.receive();

    stats = channel.getStats();
    std.testing.expect(stats.receives_total == 1) catch unreachable;
    std.testing.expect(stats.current_size == 1) catch unreachable;

    // Reset stats
    channel.resetStats();
    stats = channel.getStats();
    std.testing.expect(stats.sends_total == 0) catch unreachable;
    std.testing.expect(stats.receives_total == 0) catch unreachable;
}

test "Channel drain operation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var channel = try Channel(u32).init(allocator, 5);
    defer channel.deinit();

    // Add items to channel
    try channel.send(1);
    try channel.send(2);
    try channel.send(3);

    // Drain all items
    const items = try channel.drain(allocator);
    defer allocator.free(items);

    std.testing.expect(items.len == 3) catch unreachable;
    std.testing.expect(items[0] == 1) catch unreachable;
    std.testing.expect(items[1] == 2) catch unreachable;
    std.testing.expect(items[2] == 3) catch unreachable;
    std.testing.expect(channel.isEmpty()) catch unreachable;
}

test "Channel capacity validation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Test zero capacity
    const result = Channel(u32).init(allocator, 0);
    std.testing.expect(result == ChannelError.InvalidCapacity) catch unreachable;
}
