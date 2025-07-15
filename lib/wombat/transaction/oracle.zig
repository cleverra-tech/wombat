const std = @import("std");
const atomic = std.atomic;
const ArrayList = std.ArrayList;

pub const Transaction = struct {
    read_ts: u64,
    commit_ts: u64,
    reads: std.HashSet(u64),
    writes: ArrayList(WriteEntry),
    conflict_keys: std.HashSet(u64),
    db: *anyopaque,

    const WriteEntry = struct {
        key: []const u8,
        value: []const u8,
        timestamp: u64,
    };

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, read_ts: u64, db: *anyopaque) Self {
        return Self{
            .read_ts = read_ts,
            .commit_ts = 0,
            .reads = std.HashSet(u64).init(allocator),
            .writes = ArrayList(WriteEntry).init(allocator),
            .conflict_keys = std.HashSet(u64).init(allocator),
            .db = db,
        };
    }

    pub fn deinit(self: *Self) void {
        self.reads.deinit();
        self.writes.deinit();
        self.conflict_keys.deinit();
    }

    pub fn addRead(self: *Self, key_hash: u64) !void {
        try self.reads.put(key_hash, {});
    }

    pub fn addWrite(self: *Self, key: []const u8, value: []const u8, timestamp: u64) !void {
        try self.writes.append(WriteEntry{
            .key = key,
            .value = value,
            .timestamp = timestamp,
        });
    }

    pub fn addConflictKey(self: *Self, key_hash: u64) !void {
        try self.conflict_keys.put(key_hash, {});
    }
};

pub const CommittedTxn = struct {
    commit_ts: u64,
    conflict_keys: []u64,
};

pub const WaterMark = struct {
    value: atomic.Value(u64),

    const Self = @This();

    pub fn init(initial: u64) Self {
        return Self{
            .value = atomic.Value(u64).init(initial),
        };
    }

    pub fn get(self: *const Self) u64 {
        return self.value.load(.acquire);
    }

    pub fn set(self: *Self, new_value: u64) void {
        self.value.store(new_value, .release);
    }

    pub fn compareAndSwap(self: *Self, expected: u64, new_value: u64) bool {
        return self.value.cmpxchgWeak(expected, new_value, .acq_rel, .acquire) == null;
    }
};

pub const Oracle = struct {
    next_txn_ts: atomic.Value(u64),
    committed_txns: ArrayList(CommittedTxn),
    read_mark: WaterMark,
    txn_mark: WaterMark,
    allocator: std.mem.Allocator,
    mutex: std.Thread.Mutex,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .next_txn_ts = atomic.Value(u64).init(1),
            .committed_txns = ArrayList(CommittedTxn).init(allocator),
            .read_mark = WaterMark.init(0),
            .txn_mark = WaterMark.init(0),
            .allocator = allocator,
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.committed_txns.items) |txn| {
            self.allocator.free(txn.conflict_keys);
        }
        self.committed_txns.deinit();
    }

    pub fn newReadTs(self: *Self) u64 {
        return self.next_txn_ts.fetchAdd(1, .acq_rel);
    }

    pub fn newCommitTs(self: *Self, txn: *Transaction) !u64 {
        if (self.hasConflict(txn)) {
            return error.TransactionConflict;
        }

        txn.commit_ts = self.next_txn_ts.fetchAdd(1, .acq_rel);

        try self.recordCommittedTxn(txn);

        return txn.commit_ts;
    }

    pub fn hasConflict(self: *Self, txn: *Transaction) bool {
        self.mutex.lock();
        defer self.mutex.unlock();

        for (self.committed_txns.items) |committed| {
            if (committed.commit_ts <= txn.read_ts) {
                continue;
            }

            for (committed.conflict_keys) |conflict_key| {
                if (txn.reads.contains(conflict_key)) {
                    return true;
                }
            }
        }

        return false;
    }

    fn recordCommittedTxn(self: *Self, txn: *Transaction) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

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

        const cleanup_threshold = txn.commit_ts - 1000;
        while (self.committed_txns.items.len > 0 and
            self.committed_txns.items[0].commit_ts < cleanup_threshold)
        {
            const old_txn = self.committed_txns.orderedRemove(0);
            self.allocator.free(old_txn.conflict_keys);
        }
    }

    pub fn updateReadMark(self: *Self, ts: u64) void {
        var current = self.read_mark.get();
        while (ts > current) {
            if (self.read_mark.compareAndSwap(current, ts)) {
                break;
            }
            current = self.read_mark.get();
        }
    }

    pub fn updateTxnMark(self: *Self, ts: u64) void {
        var current = self.txn_mark.get();
        while (ts > current) {
            if (self.txn_mark.compareAndSwap(current, ts)) {
                break;
            }
            current = self.txn_mark.get();
        }
    }

    pub fn getReadMark(self: *const Self) u64 {
        return self.read_mark.get();
    }

    pub fn getTxnMark(self: *const Self) u64 {
        return self.txn_mark.get();
    }
};
