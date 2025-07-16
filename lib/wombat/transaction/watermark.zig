const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const atomic = std.atomic;
const Thread = std.Thread;

/// Represents a watermark for tracking transaction progress
/// Used to determine safe timestamps for garbage collection and consistency
pub const WaterMark = struct {
    /// Minimum timestamp that is safe for operations
    min_ts: atomic.Value(u64),
    /// List of pending transactions with their timestamps
    pending: ArrayList(u64),
    /// Protects pending list modifications
    mutex: Thread.Mutex,
    /// Memory allocator
    allocator: Allocator,
    /// Name for debugging purposes
    name: []const u8,

    const Self = @This();

    /// Initialize a new watermark
    pub fn init(allocator: Allocator, name: []const u8) !*Self {
        const self = try allocator.create(Self);
        errdefer allocator.destroy(self);

        self.* = Self{
            .min_ts = atomic.Value(u64).init(std.math.maxInt(u64) - 1),
            .pending = ArrayList(u64).init(allocator),
            .mutex = Thread.Mutex{},
            .allocator = allocator,
            .name = try allocator.dupe(u8, name),
        };

        return self;
    }

    /// Clean up resources
    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        self.pending.deinit();
        self.mutex.unlock();

        const allocator = self.allocator;
        allocator.free(self.name);
        allocator.destroy(self);
    }

    /// Begin tracking a transaction with the given timestamp
    pub fn begin(self: *Self, ts: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Insert timestamp in sorted order for efficient watermark calculation
        var insert_pos: usize = 0;
        for (self.pending.items, 0..) |pending_ts, i| {
            if (ts < pending_ts) {
                insert_pos = i;
                break;
            }
            insert_pos = i + 1;
        }

        try self.pending.insert(insert_pos, ts);
        self.updateMinTimestamp();
    }

    /// Stop tracking a transaction and update watermark
    pub fn done(self: *Self, ts: u64) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Remove the timestamp from pending list
        for (self.pending.items, 0..) |pending_ts, i| {
            if (pending_ts == ts) {
                _ = self.pending.orderedRemove(i);
                break;
            }
        }

        self.updateMinTimestamp();
    }

    /// Get the current watermark (safe minimum timestamp)
    pub fn getWaterMark(self: *Self) u64 {
        return self.min_ts.load(.acquire);
    }

    /// Wait until watermark advances past the given timestamp
    pub fn waitForMark(self: *Self, ts: u64, timeout_ms: u32) !bool {
        const start_time = std.time.milliTimestamp();
        const timeout_time = start_time + timeout_ms;

        while (self.getWaterMark() <= ts) {
            const current_time = std.time.milliTimestamp();
            if (current_time >= timeout_time) {
                return false; // Timeout
            }

            // Small sleep to avoid busy waiting
            std.Thread.sleep(1 * std.time.ns_per_ms);
        }

        return true; // Watermark advanced past ts
    }

    /// Check if a timestamp is safe (below current watermark)
    pub fn isSafe(self: *Self, ts: u64) bool {
        return ts < self.getWaterMark();
    }

    /// Get count of pending transactions
    pub fn getPendingCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.pending.items.len;
    }

    /// Get the oldest pending timestamp (for debugging)
    pub fn getOldestPending(self: *Self) ?u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.pending.items.len == 0) {
            return null;
        }

        return self.pending.items[0]; // First item is oldest due to sorted insertion
    }

    /// Force advance watermark to a specific timestamp
    /// Only use when you're certain all transactions below this timestamp are complete
    pub fn forceAdvanceTo(self: *Self, ts: u64) void {
        self.min_ts.store(ts, .release);
    }

    /// Get snapshot of current state for debugging
    pub fn getSnapshot(self: *Self, allocator: Allocator) !WaterMarkSnapshot {
        self.mutex.lock();
        defer self.mutex.unlock();

        return WaterMarkSnapshot{
            .name = try allocator.dupe(u8, self.name),
            .min_ts = self.min_ts.load(.acquire),
            .pending = try allocator.dupe(u64, self.pending.items),
            .pending_count = self.pending.items.len,
        };
    }

    /// Update the minimum timestamp based on pending transactions
    /// Must be called with mutex held
    fn updateMinTimestamp(self: *Self) void {
        if (self.pending.items.len == 0) {
            // No pending transactions, watermark advances to a very high value
            // indicating all timestamps are safe for garbage collection
            self.min_ts.store(std.math.maxInt(u64) - 1, .release);
        } else {
            // Watermark is the oldest pending transaction
            const oldest_pending = self.pending.items[0];
            self.min_ts.store(oldest_pending, .release);
        }
    }
};

/// Snapshot of watermark state for debugging and monitoring
pub const WaterMarkSnapshot = struct {
    name: []const u8,
    min_ts: u64,
    pending: []u64,
    pending_count: usize,

    pub fn deinit(self: *WaterMarkSnapshot, allocator: Allocator) void {
        allocator.free(self.name);
        allocator.free(self.pending);
    }

    pub fn format(
        self: WaterMarkSnapshot,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        try writer.print("WaterMark(name='{s}', min_ts={}, pending_count={}, oldest={})", .{
            self.name,
            self.min_ts,
            self.pending_count,
            if (self.pending.len > 0) self.pending[0] else 0,
        });
    }
};

/// WaterMark group for managing multiple related watermarks
/// Useful for coordinating read and transaction watermarks
pub const WaterMarkGroup = struct {
    watermarks: ArrayList(*WaterMark),
    allocator: Allocator,
    mutex: Thread.Mutex,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .watermarks = ArrayList(*WaterMark).init(allocator),
            .allocator = allocator,
            .mutex = Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.watermarks.items) |wm| {
            wm.deinit();
        }
        self.watermarks.deinit();
    }

    /// Add a watermark to the group
    pub fn add(self: *Self, watermark: *WaterMark) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.watermarks.append(watermark);
    }

    /// Remove a watermark from the group
    pub fn remove(self: *Self, watermark: *WaterMark) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.watermarks.items, 0..) |wm, i| {
            if (wm == watermark) {
                _ = self.watermarks.swapRemove(i);
                break;
            }
        }
    }

    /// Get the minimum watermark across all watermarks in the group
    pub fn getMinWaterMark(self: *Self) u64 {
        self.mutex.lock();
        defer self.mutex.unlock();

        if (self.watermarks.items.len == 0) {
            return std.math.maxInt(u64);
        }

        var min_mark: u64 = std.math.maxInt(u64);
        for (self.watermarks.items) |wm| {
            const mark = wm.getWaterMark();
            min_mark = @min(min_mark, mark);
        }

        return min_mark;
    }

    /// Wait for all watermarks to advance past the given timestamp
    pub fn waitForAllMarks(self: *Self, ts: u64, timeout_ms: u32) !bool {
        const start_time = std.time.milliTimestamp();

        while (true) {
            const min_mark = self.getMinWaterMark();
            if (min_mark > ts) {
                return true;
            }

            const current_time = std.time.milliTimestamp();
            if (current_time >= start_time + timeout_ms) {
                return false; // Timeout
            }

            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
    }

    /// Get snapshots of all watermarks
    pub fn getAllSnapshots(self: *Self, allocator: Allocator) ![]WaterMarkSnapshot {
        self.mutex.lock();
        defer self.mutex.unlock();

        var snapshots = try allocator.alloc(WaterMarkSnapshot, self.watermarks.items.len);
        errdefer allocator.free(snapshots);

        for (self.watermarks.items, 0..) |wm, i| {
            snapshots[i] = try wm.getSnapshot(allocator);
        }

        return snapshots;
    }
};

// Tests
test "WaterMark basic operations" {
    const allocator = std.testing.allocator;

    const wm = try WaterMark.init(allocator, "test_mark");
    defer wm.deinit();

    // Initially no pending transactions, watermark should be at max-1
    try std.testing.expect(wm.getWaterMark() == std.math.maxInt(u64) - 1);
    try std.testing.expect(wm.getPendingCount() == 0);

    // Begin tracking transaction at timestamp 100
    try wm.begin(100);
    try std.testing.expect(wm.getWaterMark() == 100);
    try std.testing.expect(wm.getPendingCount() == 1);
    try std.testing.expect(wm.getOldestPending().? == 100);

    // Begin another transaction at timestamp 50 (older)
    try wm.begin(50);
    try std.testing.expect(wm.getWaterMark() == 50); // Should be the older one
    try std.testing.expect(wm.getPendingCount() == 2);
    try std.testing.expect(wm.getOldestPending().? == 50);

    // Complete the older transaction
    wm.done(50);
    try std.testing.expect(wm.getWaterMark() == 100); // Should advance to next oldest
    try std.testing.expect(wm.getPendingCount() == 1);

    // Complete the remaining transaction
    wm.done(100);
    try std.testing.expect(wm.getWaterMark() == std.math.maxInt(u64) - 1); // No pending
    try std.testing.expect(wm.getPendingCount() == 0);
}

test "WaterMark ordering" {
    const allocator = std.testing.allocator;

    const wm = try WaterMark.init(allocator, "ordering_test");
    defer wm.deinit();

    // Add transactions in non-sequential order
    try wm.begin(300);
    try wm.begin(100);
    try wm.begin(200);
    try wm.begin(150);

    // Watermark should be the minimum (100)
    try std.testing.expect(wm.getWaterMark() == 100);
    try std.testing.expect(wm.getPendingCount() == 4);

    // Complete transactions in different order
    wm.done(200);
    try std.testing.expect(wm.getWaterMark() == 100); // Still blocked by 100

    wm.done(100);
    try std.testing.expect(wm.getWaterMark() == 150); // Advances to next oldest

    wm.done(300);
    try std.testing.expect(wm.getWaterMark() == 150); // Still blocked by 150

    wm.done(150);
    try std.testing.expect(wm.getWaterMark() == std.math.maxInt(u64) - 1); // All done
}

test "WaterMark safety checks" {
    const allocator = std.testing.allocator;

    const wm = try WaterMark.init(allocator, "safety_test");
    defer wm.deinit();

    try wm.begin(100);

    // Test safety checks
    try std.testing.expect(wm.isSafe(50) == true); // Below watermark
    try std.testing.expect(wm.isSafe(100) == false); // At watermark
    try std.testing.expect(wm.isSafe(150) == false); // Above watermark

    wm.done(100);

    // After completing, much higher timestamps are safe
    try std.testing.expect(wm.isSafe(1000) == true);
}

test "WaterMark snapshots" {
    const allocator = std.testing.allocator;

    const wm = try WaterMark.init(allocator, "snapshot_test");
    defer wm.deinit();

    try wm.begin(100);
    try wm.begin(200);

    var snapshot = try wm.getSnapshot(allocator);
    defer snapshot.deinit(allocator);

    try std.testing.expect(std.mem.eql(u8, snapshot.name, "snapshot_test"));
    try std.testing.expect(snapshot.min_ts == 100);
    try std.testing.expect(snapshot.pending_count == 2);
    try std.testing.expect(snapshot.pending[0] == 100);
    try std.testing.expect(snapshot.pending[1] == 200);
}

test "WaterMarkGroup operations" {
    const allocator = std.testing.allocator;

    var group = WaterMarkGroup.init(allocator);
    defer group.deinit();

    const wm1 = try WaterMark.init(allocator, "wm1");
    const wm2 = try WaterMark.init(allocator, "wm2");

    try group.add(wm1);
    try group.add(wm2);

    // Initially both at max-1, group min should be max-1
    try std.testing.expect(group.getMinWaterMark() == std.math.maxInt(u64) - 1);

    // Begin transactions on both
    try wm1.begin(100);
    try wm2.begin(200);

    // Group minimum should be the lower one
    try std.testing.expect(group.getMinWaterMark() == 100);

    // Complete one transaction
    wm1.done(100);
    try std.testing.expect(group.getMinWaterMark() == 200);

    // Complete the other
    wm2.done(200);
    try std.testing.expect(group.getMinWaterMark() == std.math.maxInt(u64) - 1);

    // Test snapshots
    try wm1.begin(300);
    const snapshots = try group.getAllSnapshots(allocator);
    defer {
        for (snapshots) |*snapshot| {
            snapshot.deinit(allocator);
        }
        allocator.free(snapshots);
    }

    try std.testing.expect(snapshots.len == 2);
}

test "WaterMark concurrent operations" {
    const allocator = std.testing.allocator;

    const wm = try WaterMark.init(allocator, "concurrent_test");
    defer wm.deinit();

    // Simulate concurrent transaction lifecycle
    const TestContext = struct {
        wm: *WaterMark,
        ts: u64,
    };

    const worker = struct {
        fn run(ctx: *TestContext) void {
            ctx.wm.begin(ctx.ts) catch return;

            // Simulate some work
            std.Thread.sleep(1 * std.time.ns_per_ms);

            ctx.wm.done(ctx.ts);
        }
    }.run;

    var ctx1 = TestContext{ .wm = wm, .ts = 100 };
    var ctx2 = TestContext{ .wm = wm, .ts = 200 };

    const thread1 = try Thread.spawn(.{}, worker, .{&ctx1});
    const thread2 = try Thread.spawn(.{}, worker, .{&ctx2});

    thread1.join();
    thread2.join();

    // After both complete, watermark should be at max-1
    try std.testing.expect(wm.getWaterMark() == std.math.maxInt(u64) - 1);
    try std.testing.expect(wm.getPendingCount() == 0);
}
