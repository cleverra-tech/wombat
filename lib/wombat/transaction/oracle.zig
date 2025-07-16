const std = @import("std");
const atomic = std.atomic;
const ArrayList = std.ArrayList;
const WaterMark = @import("watermark.zig").WaterMark;
const WaterMarkGroup = @import("watermark.zig").WaterMarkGroup;

/// Write operation entry for tracking transaction modifications
pub const WriteEntry = struct {
    key: []const u8,
    value: []const u8,
    timestamp: u64,
    deleted: bool = false,
};

/// Transaction state for MVCC operations
pub const TransactionState = enum {
    active,
    committed,
    aborted,
};

/// Transaction represents a single database transaction with MVCC support
pub const Transaction = struct {
    /// Read timestamp assigned at transaction start
    read_ts: u64,
    /// Commit timestamp assigned during commit
    commit_ts: u64,
    /// Set of key hashes that this transaction has read
    reads: std.HashMap(u64, void, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    /// List of write operations in this transaction
    writes: ArrayList(WriteEntry),
    /// Set of key hashes that could cause conflicts
    conflict_keys: std.HashMap(u64, void, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    /// Current transaction state
    state: TransactionState,
    /// Database reference
    db: *anyopaque,
    /// Memory allocator
    allocator: std.mem.Allocator,

    const Self = @This();

    /// Initialize a new transaction with the given read timestamp
    pub fn init(allocator: std.mem.Allocator, read_ts: u64, db: *anyopaque) Self {
        return Self{
            .read_ts = read_ts,
            .commit_ts = 0,
            .reads = std.HashMap(u64, void, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .writes = ArrayList(WriteEntry).init(allocator),
            .conflict_keys = std.HashMap(u64, void, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .state = .active,
            .db = db,
            .allocator = allocator,
        };
    }

    /// Clean up transaction resources
    pub fn deinit(self: *Self) void {
        self.reads.deinit();
        self.writes.deinit();
        self.conflict_keys.deinit();
    }

    /// Record that this transaction read a key
    pub fn addRead(self: *Self, key_hash: u64) !void {
        try self.reads.put(key_hash, {});
    }

    /// Add a write operation to this transaction
    pub fn addWrite(self: *Self, key: []const u8, value: []const u8, timestamp: u64, deleted: bool) !void {
        try self.writes.append(WriteEntry{
            .key = key,
            .value = value,
            .timestamp = timestamp,
            .deleted = deleted,
        });

        // Add to conflict keys for write-write conflict detection
        const key_hash = std.hash_map.hashString(key);
        try self.conflict_keys.put(key_hash, {});
    }

    /// Add a key that could cause conflicts
    pub fn addConflictKey(self: *Self, key_hash: u64) !void {
        try self.conflict_keys.put(key_hash, {});
    }

    /// Check if this transaction has read a specific key
    pub fn hasRead(self: *Self, key_hash: u64) bool {
        return self.reads.contains(key_hash);
    }

    /// Check if this transaction has written to a specific key
    pub fn hasWritten(self: *Self, key: []const u8) bool {
        for (self.writes.items) |write| {
            if (std.mem.eql(u8, write.key, key)) {
                return true;
            }
        }
        return false;
    }

    /// Get the latest write for a key in this transaction
    pub fn getWrite(self: *Self, key: []const u8) ?WriteEntry {
        var i = self.writes.items.len;
        while (i > 0) {
            i -= 1;
            if (std.mem.eql(u8, self.writes.items[i].key, key)) {
                return self.writes.items[i];
            }
        }
        return null;
    }

    /// Mark transaction as committed with the given timestamp
    pub fn markCommitted(self: *Self, commit_ts: u64) void {
        self.commit_ts = commit_ts;
        self.state = .committed;
    }

    /// Mark transaction as aborted
    pub fn markAborted(self: *Self) void {
        self.state = .aborted;
    }

    /// Check if transaction is active
    pub fn isActive(self: *Self) bool {
        return self.state == .active;
    }

    /// Check if transaction is committed
    pub fn isCommitted(self: *Self) bool {
        return self.state == .committed;
    }

    /// Get the number of read operations
    pub fn getReadCount(self: *Self) u32 {
        return @intCast(self.reads.count());
    }

    /// Get the number of write operations
    pub fn getWriteCount(self: *Self) u32 {
        return @intCast(self.writes.items.len);
    }
};

pub const CommittedTxn = struct {
    commit_ts: u64,
    conflict_keys: []u64,
};

/// Statistics for monitoring Oracle performance
pub const OracleStats = struct {
    transactions_started: atomic.Value(u64),
    transactions_committed: atomic.Value(u64),
    transactions_aborted: atomic.Value(u64),
    conflicts_detected: atomic.Value(u64),
    cleanup_runs: atomic.Value(u64),

    const Self = @This();

    pub fn init() Self {
        return Self{
            .transactions_started = atomic.Value(u64).init(0),
            .transactions_committed = atomic.Value(u64).init(0),
            .transactions_aborted = atomic.Value(u64).init(0),
            .conflicts_detected = atomic.Value(u64).init(0),
            .cleanup_runs = atomic.Value(u64).init(0),
        };
    }

    pub fn incrementStarted(self: *Self) void {
        _ = self.transactions_started.fetchAdd(1, .monotonic);
    }

    pub fn incrementCommitted(self: *Self) void {
        _ = self.transactions_committed.fetchAdd(1, .monotonic);
    }

    pub fn incrementAborted(self: *Self) void {
        _ = self.transactions_aborted.fetchAdd(1, .monotonic);
    }

    pub fn incrementConflicts(self: *Self) void {
        _ = self.conflicts_detected.fetchAdd(1, .monotonic);
    }

    pub fn incrementCleanup(self: *Self) void {
        _ = self.cleanup_runs.fetchAdd(1, .monotonic);
    }

    pub fn getStarted(self: *const Self) u64 {
        return self.transactions_started.load(.monotonic);
    }

    pub fn getCommitted(self: *const Self) u64 {
        return self.transactions_committed.load(.monotonic);
    }

    pub fn getAborted(self: *const Self) u64 {
        return self.transactions_aborted.load(.monotonic);
    }

    pub fn getConflicts(self: *const Self) u64 {
        return self.conflicts_detected.load(.monotonic);
    }

    pub fn getCleanupRuns(self: *const Self) u64 {
        return self.cleanup_runs.load(.monotonic);
    }
};

/// Oracle manages transaction timestamps and conflict detection for MVCC
pub const Oracle = struct {
    /// Next transaction timestamp to assign
    next_txn_ts: atomic.Value(u64),
    /// List of recently committed transactions for conflict detection
    committed_txns: ArrayList(CommittedTxn),
    /// WaterMark for tracking read transactions
    read_mark: *WaterMark,
    /// WaterMark for tracking commit transactions
    txn_mark: *WaterMark,
    /// WaterMark group for coordinating both watermarks
    watermark_group: WaterMarkGroup,
    /// Memory allocator
    allocator: std.mem.Allocator,
    /// Mutex for protecting committed transactions list
    mutex: std.Thread.Mutex,
    /// Statistics for monitoring
    stats: OracleStats,
    /// Maximum number of committed transactions to keep for conflict detection
    max_committed_txns: usize,
    /// Cleanup threshold for old transactions
    cleanup_threshold: u64,

    const Self = @This();

    /// Initialize Oracle with proper WaterMark integration
    pub fn init(allocator: std.mem.Allocator) !Self {
        const read_mark = try WaterMark.init(allocator, "read_transactions");
        errdefer read_mark.deinit();

        const txn_mark = try WaterMark.init(allocator, "commit_transactions");
        errdefer txn_mark.deinit();

        var watermark_group = WaterMarkGroup.init(allocator);
        try watermark_group.add(read_mark);
        try watermark_group.add(txn_mark);

        return Self{
            .next_txn_ts = atomic.Value(u64).init(1),
            .committed_txns = ArrayList(CommittedTxn).init(allocator),
            .read_mark = read_mark,
            .txn_mark = txn_mark,
            .watermark_group = watermark_group,
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
            .stats = OracleStats.init(),
            .max_committed_txns = 10000,
            .cleanup_threshold = 1000,
        };
    }

    /// Clean up Oracle resources
    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.committed_txns.items) |txn| {
            self.allocator.free(txn.conflict_keys);
        }
        self.committed_txns.deinit();

        // WaterMarkGroup.deinit will clean up the individual watermarks
        self.watermark_group.deinit();
    }

    /// Generate a new read timestamp and begin tracking in read watermark
    pub fn newReadTs(self: *Self) !u64 {
        const ts = self.next_txn_ts.fetchAdd(1, .acq_rel);
        try self.read_mark.begin(ts);
        self.stats.incrementStarted();
        return ts;
    }

    /// Generate a new commit timestamp after conflict detection
    pub fn newCommitTs(self: *Self, txn: *Transaction) !u64 {
        if (self.hasConflict(txn)) {
            self.stats.incrementConflicts();
            self.stats.incrementAborted();
            txn.markAborted();
            return error.TransactionConflict;
        }

        const commit_ts = self.next_txn_ts.fetchAdd(1, .acq_rel);
        txn.markCommitted(commit_ts);

        try self.txn_mark.begin(commit_ts);
        try self.recordCommittedTxn(txn);

        self.stats.incrementCommitted();
        return commit_ts;
    }

    /// Enhanced conflict detection using watermarks and read-write conflicts
    pub fn hasConflict(self: *Self, txn: *Transaction) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Check for read-write conflicts: if any committed transaction
        // with commit_ts > txn.read_ts wrote to keys that txn read
        for (self.committed_txns.items) |committed| {
            // Only check transactions that committed after this transaction's read timestamp
            if (committed.commit_ts <= txn.read_ts) {
                continue;
            }

            // Check if any of the committed transaction's writes conflict with our reads
            for (committed.conflict_keys) |conflict_key| {
                if (txn.reads.contains(conflict_key)) {
                    return true;
                }
            }
        }

        return false;
    }

    /// Record a committed transaction for conflict detection
    fn recordCommittedTxn(self: *Self, txn: *Transaction) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Only record if there are conflict keys to track
        if (txn.conflict_keys.count() == 0) {
            return;
        }

        var conflict_keys = try self.allocator.alloc(u64, txn.conflict_keys.count());
        var i: usize = 0;

        var iter = txn.conflict_keys.iterator();
        while (iter.next()) |entry| {
            conflict_keys[i] = entry.key_ptr.*;
            i += 1;
        }

        const committed_txn = CommittedTxn{
            .commit_ts = txn.commit_ts,
            .conflict_keys = conflict_keys,
        };

        try self.committed_txns.append(committed_txn);

        // Clean up old transactions periodically
        try self.maybeCleanupOldTxns(txn.commit_ts);
    }

    /// Clean up old committed transactions that are no longer needed for conflict detection
    fn maybeCleanupOldTxns(self: *Self, current_ts: u64) !void {
        // Use watermarks to determine safe cleanup threshold
        const min_read_ts = self.read_mark.getWaterMark();
        const safe_cleanup_ts = if (min_read_ts == std.math.maxInt(u64) - 1)
            if (current_ts > self.cleanup_threshold) current_ts - self.cleanup_threshold else 0
        else
            @min(min_read_ts, if (current_ts > self.cleanup_threshold) current_ts - self.cleanup_threshold else 0);

        var cleaned_count: usize = 0;
        while (self.committed_txns.items.len > 0 and
            self.committed_txns.items[0].commit_ts < safe_cleanup_ts and
            cleaned_count < self.max_committed_txns / 4) // Limit cleanup per run
        {
            const old_txn = self.committed_txns.orderedRemove(0);
            self.allocator.free(old_txn.conflict_keys);
            cleaned_count += 1;
        }

        if (cleaned_count > 0) {
            self.stats.incrementCleanup();
        }

        // Force cleanup if we have too many committed transactions
        if (self.committed_txns.items.len > self.max_committed_txns) {
            const excess = self.committed_txns.items.len - self.max_committed_txns / 2;
            var force_cleaned: usize = 0;
            while (force_cleaned < excess and self.committed_txns.items.len > 0) {
                const old_txn = self.committed_txns.orderedRemove(0);
                self.allocator.free(old_txn.conflict_keys);
                force_cleaned += 1;
            }
        }
    }

    /// Complete a read transaction and update read watermark
    pub fn completeReadTxn(self: *Self, ts: u64) void {
        self.read_mark.done(ts);
    }

    /// Complete a commit transaction and update commit watermark
    pub fn completeCommitTxn(self: *Self, ts: u64) void {
        self.txn_mark.done(ts);
    }

    /// Get current read watermark
    pub fn getReadMark(self: *Self) u64 {
        return self.read_mark.getWaterMark();
    }

    /// Get current commit watermark
    pub fn getTxnMark(self: *Self) u64 {
        return self.txn_mark.getWaterMark();
    }

    /// Get minimum watermark across all transaction types
    pub fn getMinWaterMark(self: *Self) u64 {
        return self.watermark_group.getMinWaterMark();
    }

    /// Wait for all watermarks to advance past the given timestamp
    pub fn waitForWaterMark(self: *Self, ts: u64, timeout_ms: u32) !bool {
        return self.watermark_group.waitForAllMarks(ts, timeout_ms);
    }

    /// Check if a timestamp is safe for garbage collection
    pub fn isSafeForGC(self: *Self, ts: u64) bool {
        return ts < self.getMinWaterMark();
    }

    /// Get Oracle statistics
    pub fn getStats(self: *Self) OracleStats {
        return self.stats;
    }

    /// Get the number of committed transactions being tracked
    pub fn getCommittedTxnCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.committed_txns.items.len;
    }

    /// Force cleanup of old committed transactions
    pub fn forceCleanup(self: *Self) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const current_ts = self.next_txn_ts.load(.acquire);
        try self.maybeCleanupOldTxns(current_ts);
    }
};

// Tests
test "Transaction basic operations" {
    const allocator = std.testing.allocator;

    var txn = Transaction.init(allocator, 100, undefined);
    defer txn.deinit();

    try std.testing.expect(txn.read_ts == 100);
    try std.testing.expect(txn.commit_ts == 0);
    try std.testing.expect(txn.state == .active);
    try std.testing.expect(txn.isActive());
    try std.testing.expect(!txn.isCommitted());

    // Test read tracking
    try txn.addRead(12345);
    try std.testing.expect(txn.hasRead(12345));
    try std.testing.expect(!txn.hasRead(54321));
    try std.testing.expect(txn.getReadCount() == 1);

    // Test write tracking
    try txn.addWrite("key1", "value1", 100, false);
    try std.testing.expect(txn.hasWritten("key1"));
    try std.testing.expect(!txn.hasWritten("key2"));
    try std.testing.expect(txn.getWriteCount() == 1);

    const write = txn.getWrite("key1");
    try std.testing.expect(write != null);
    try std.testing.expect(std.mem.eql(u8, write.?.value, "value1"));
    try std.testing.expect(!write.?.deleted);

    // Test state transitions
    txn.markCommitted(200);
    try std.testing.expect(txn.commit_ts == 200);
    try std.testing.expect(txn.state == .committed);
    try std.testing.expect(txn.isCommitted());
    try std.testing.expect(!txn.isActive());
}

test "Oracle basic operations" {
    const allocator = std.testing.allocator;

    var oracle = try Oracle.init(allocator);
    defer oracle.deinit();

    // Test timestamp generation
    const ts1 = try oracle.newReadTs();
    const ts2 = try oracle.newReadTs();
    try std.testing.expect(ts2 > ts1);

    // Complete read transactions
    oracle.completeReadTxn(ts1);
    oracle.completeReadTxn(ts2);

    // Test watermarks
    try std.testing.expect(oracle.getReadMark() == std.math.maxInt(u64) - 1);
}

test "Oracle conflict detection" {
    const allocator = std.testing.allocator;

    var oracle = try Oracle.init(allocator);
    defer oracle.deinit();

    // Create first transaction that reads key1
    const read_ts1 = try oracle.newReadTs();
    var txn1 = Transaction.init(allocator, read_ts1, undefined);
    defer txn1.deinit();

    const key1_hash = std.hash_map.hashString("key1");
    try txn1.addRead(key1_hash);

    // Create second transaction that starts after txn1 and writes to key1
    const read_ts2 = try oracle.newReadTs();
    var txn2 = Transaction.init(allocator, read_ts2, undefined);
    defer txn2.deinit();

    // Transaction 2 writes to key1
    try txn2.addWrite("key1", "new_value", read_ts2, false);

    // Transaction 2 commits first
    const commit_ts2 = try oracle.newCommitTs(&txn2);
    try std.testing.expect(commit_ts2 > read_ts2);
    oracle.completeCommitTxn(commit_ts2);

    // Now txn1 tries to commit - should detect conflict because:
    // - txn1 read key1 at read_ts1
    // - txn2 wrote key1 and committed at commit_ts2 > read_ts1
    try std.testing.expectError(error.TransactionConflict, oracle.newCommitTs(&txn1));
    try std.testing.expect(txn1.state == .aborted);

    oracle.completeReadTxn(read_ts1);
    oracle.completeReadTxn(read_ts2);
}

test "Oracle watermark integration" {
    const allocator = std.testing.allocator;

    var oracle = try Oracle.init(allocator);
    defer oracle.deinit();

    // Start several read transactions
    const read_ts1 = try oracle.newReadTs();
    const read_ts2 = try oracle.newReadTs();
    const read_ts3 = try oracle.newReadTs();

    // Read watermark should be at the minimum pending transaction
    try std.testing.expect(oracle.getReadMark() == read_ts1);

    // Complete middle transaction
    oracle.completeReadTxn(read_ts2);
    try std.testing.expect(oracle.getReadMark() == read_ts1); // Still blocked by ts1

    // Complete first transaction
    oracle.completeReadTxn(read_ts1);
    try std.testing.expect(oracle.getReadMark() == read_ts3); // Advances to ts3

    // Complete last transaction
    oracle.completeReadTxn(read_ts3);
    try std.testing.expect(oracle.getReadMark() == std.math.maxInt(u64) - 1); // No pending
}

test "Oracle cleanup and statistics" {
    const allocator = std.testing.allocator;

    var oracle = try Oracle.init(allocator);
    defer oracle.deinit();

    // Create and commit several transactions
    for (0..10) |i| {
        const read_ts = try oracle.newReadTs();
        var txn = Transaction.init(allocator, read_ts, undefined);
        defer txn.deinit();

        // Add some writes to create conflict keys
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "key_{d}", .{i});
        try txn.addWrite(key, "value", read_ts, false);

        const commit_ts = try oracle.newCommitTs(&txn);
        oracle.completeCommitTxn(commit_ts);
        oracle.completeReadTxn(read_ts);
    }

    // Check statistics
    const stats = oracle.getStats();
    try std.testing.expect(stats.getStarted() == 10);
    try std.testing.expect(stats.getCommitted() == 10);
    try std.testing.expect(stats.getAborted() == 0);
    try std.testing.expect(stats.getConflicts() == 0);

    // Check committed transaction tracking
    try std.testing.expect(oracle.getCommittedTxnCount() <= 10);

    // Force cleanup
    try oracle.forceCleanup();
}

test "Oracle concurrent operations" {
    const allocator = std.testing.allocator;

    var oracle = try Oracle.init(allocator);
    defer oracle.deinit();

    const TestContext = struct {
        oracle: *Oracle,
        txn_id: u32,
        allocator: std.mem.Allocator,
    };

    const worker = struct {
        fn run(ctx: *TestContext) void {
            const read_ts = ctx.oracle.newReadTs() catch return;
            var txn = Transaction.init(ctx.allocator, read_ts, undefined);
            defer txn.deinit();

            // Add some operations
            var key_buf: [32]u8 = undefined;
            const key = std.fmt.bufPrint(&key_buf, "key_{d}", .{ctx.txn_id}) catch return;
            txn.addWrite(key, "value", read_ts, false) catch return;

            // Simulate some work
            std.Thread.sleep(1 * std.time.ns_per_ms);

            // Try to commit
            if (ctx.oracle.newCommitTs(&txn)) |commit_ts| {
                ctx.oracle.completeCommitTxn(commit_ts);
            } else |_| {
                // Transaction aborted due to conflict
            }

            ctx.oracle.completeReadTxn(read_ts);
        }
    }.run;

    var contexts: [5]TestContext = undefined;
    var threads: [5]std.Thread = undefined;

    // Start multiple concurrent transactions
    for (0..5) |i| {
        contexts[i] = TestContext{
            .oracle = &oracle,
            .txn_id = @intCast(i),
            .allocator = allocator,
        };
        threads[i] = try std.Thread.spawn(.{}, worker, .{&contexts[i]});
    }

    // Wait for all threads to complete
    for (threads) |thread| {
        thread.join();
    }

    // All transactions should be complete
    try std.testing.expect(oracle.getReadMark() == std.math.maxInt(u64) - 1);
    try std.testing.expect(oracle.getTxnMark() == std.math.maxInt(u64) - 1);
}
