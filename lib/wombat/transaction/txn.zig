const std = @import("std");
const ArrayList = std.ArrayList;
const HashMap = std.HashMap;
const Allocator = std.mem.Allocator;
const atomic = std.atomic;

const Oracle = @import("oracle.zig").Oracle;
const WriteEntry = @import("oracle.zig").WriteEntry;
const TransactionState = @import("oracle.zig").TransactionState;
const ValueStruct = @import("../core/skiplist.zig").ValueStruct;

/// Transaction errors
pub const TxnError = error{
    TransactionConflict,
    TransactionAborted,
    TransactionReadOnly,
    KeyNotFound,
    OutOfMemory,
    InvalidOperation,
};

/// Transaction options for customizing behavior
pub const TxnOptions = struct {
    /// Whether this is a read-only transaction
    read_only: bool = false,
    /// Maximum number of keys this transaction can read
    max_read_keys: u32 = 10000,
    /// Maximum number of keys this transaction can write
    max_write_keys: u32 = 1000,
    /// Whether to detect conflicts
    detect_conflicts: bool = true,
};

/// Enhanced Transaction API with full MVCC support
pub const Txn = struct {
    /// Read timestamp assigned at transaction start
    read_ts: u64,
    /// Commit timestamp assigned during commit
    commit_ts: u64,
    /// Set of key hashes that this transaction has read
    reads: HashMap(u64, void, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    /// List of write operations in this transaction
    writes: ArrayList(WriteEntry),
    /// Set of key hashes that could cause conflicts
    conflict_keys: HashMap(u64, void, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    /// Local write cache for read-your-writes consistency
    write_cache: HashMap(u64, WriteEntry, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    /// Current transaction state
    state: TransactionState,
    /// Transaction options
    options: TxnOptions,
    /// Database reference (opaque to avoid circular imports)
    db: *anyopaque,
    /// Oracle reference for timestamp management
    oracle: *Oracle,
    /// Memory allocator
    allocator: Allocator,
    /// Sequence number for write ordering
    write_sequence: u32,

    const Self = @This();

    /// Initialize a new transaction with the given read timestamp
    pub fn init(allocator: Allocator, read_ts: u64, db: *anyopaque, oracle: *Oracle, options: TxnOptions) Self {
        return Self{
            .read_ts = read_ts,
            .commit_ts = 0,
            .reads = HashMap(u64, void, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .writes = ArrayList(WriteEntry).init(allocator),
            .conflict_keys = HashMap(u64, void, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .write_cache = HashMap(u64, WriteEntry, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .state = .active,
            .options = options,
            .db = db,
            .oracle = oracle,
            .allocator = allocator,
            .write_sequence = 0,
        };
    }

    /// Clean up transaction resources
    pub fn deinit(self: *Self) void {
        self.reads.deinit();
        self.writes.deinit();
        self.conflict_keys.deinit();
        self.write_cache.deinit();
    }

    /// Get a value for the given key with MVCC semantics
    pub fn get(self: *Self, key: []const u8) TxnError!?[]const u8 {
        if (self.state != .active) {
            return TxnError.TransactionAborted;
        }

        const key_hash = std.hash_map.hashString(key);

        // Check local write cache first (read-your-writes)
        if (self.write_cache.get(key_hash)) |write_entry| {
            if (write_entry.deleted) {
                return null;
            }
            return write_entry.value;
        }

        // Track this read for conflict detection
        if (self.options.detect_conflicts) {
            if (self.reads.count() >= self.options.max_read_keys) {
                return TxnError.OutOfMemory;
            }
            try self.reads.put(key_hash, {});
        }

        // Get from database with timestamp visibility
        // This is a mock implementation - in reality would call DB.getAt(key, read_ts)
        return self.getFromDB(key);
    }

    /// Set a value for the given key
    pub fn set(self: *Self, key: []const u8, value: []const u8) TxnError!void {
        if (self.state != .active) {
            return TxnError.TransactionAborted;
        }

        if (self.options.read_only) {
            return TxnError.TransactionReadOnly;
        }

        if (self.writes.items.len >= self.options.max_write_keys) {
            return TxnError.OutOfMemory;
        }

        const key_hash = std.hash_map.hashString(key);
        self.write_sequence += 1;

        // Create write entry
        const write_entry = WriteEntry{
            .key = try self.allocator.dupe(u8, key),
            .value = try self.allocator.dupe(u8, value),
            .timestamp = self.read_ts,
            .deleted = false,
        };

        // Add to writes list
        try self.writes.append(write_entry);

        // Update write cache for read-your-writes
        try self.write_cache.put(key_hash, write_entry);

        // Add to conflict keys for conflict detection
        if (self.options.detect_conflicts) {
            try self.conflict_keys.put(key_hash, {});
        }
    }

    /// Delete a key
    pub fn delete(self: *Self, key: []const u8) TxnError!void {
        if (self.state != .active) {
            return TxnError.TransactionAborted;
        }

        if (self.options.read_only) {
            return TxnError.TransactionReadOnly;
        }

        if (self.writes.items.len >= self.options.max_write_keys) {
            return TxnError.OutOfMemory;
        }

        const key_hash = std.hash_map.hashString(key);
        self.write_sequence += 1;

        // Create delete entry
        const write_entry = WriteEntry{
            .key = try self.allocator.dupe(u8, key),
            .value = try self.allocator.dupe(u8, ""),
            .timestamp = self.read_ts,
            .deleted = true,
        };

        // Add to writes list
        try self.writes.append(write_entry);

        // Update write cache
        try self.write_cache.put(key_hash, write_entry);

        // Add to conflict keys
        if (self.options.detect_conflicts) {
            try self.conflict_keys.put(key_hash, {});
        }
    }

    /// Commit the transaction
    pub fn commit(self: *Self) TxnError!void {
        if (self.state != .active) {
            return TxnError.TransactionAborted;
        }

        // Read-only transactions can commit immediately
        if (self.options.read_only or self.writes.items.len == 0) {
            self.state = .committed;
            self.oracle.completeReadTxn(self.read_ts);
            return;
        }

        // Create Oracle transaction for conflict detection
        var oracle_txn = @import("oracle.zig").Transaction.init(self.allocator, self.read_ts, self.db);
        defer oracle_txn.deinit();

        // Copy reads and conflict keys to Oracle transaction
        var read_iter = self.reads.iterator();
        while (read_iter.next()) |entry| {
            try oracle_txn.addRead(entry.key_ptr.*);
        }

        var conflict_iter = self.conflict_keys.iterator();
        while (conflict_iter.next()) |entry| {
            try oracle_txn.addConflictKey(entry.key_ptr.*);
        }

        // Add writes to Oracle transaction
        for (self.writes.items) |write| {
            try oracle_txn.addWrite(write.key, write.value, write.timestamp, write.deleted);
        }

        // Try to get commit timestamp
        const commit_ts = self.oracle.newCommitTs(&oracle_txn) catch |err| switch (err) {
            error.TransactionConflict => {
                self.state = .aborted;
                self.oracle.completeReadTxn(self.read_ts);
                return TxnError.TransactionConflict;
            },
            else => return err,
        };

        // Apply writes to database
        try self.applyWrites();

        // Mark as committed
        self.commit_ts = commit_ts;
        self.state = .committed;

        // Complete watermarks
        self.oracle.completeReadTxn(self.read_ts);
        self.oracle.completeCommitTxn(commit_ts);
    }

    /// Discard/abort the transaction
    pub fn discard(self: *Self) void {
        if (self.state == .active) {
            self.state = .aborted;
            self.oracle.completeReadTxn(self.read_ts);
        }

        // Clean up allocated keys and values
        for (self.writes.items) |write| {
            self.allocator.free(write.key);
            self.allocator.free(write.value);
        }
        self.writes.clearAndFree();
        self.write_cache.clearAndFree();
    }

    /// Check if transaction is active
    pub fn isActive(self: *Self) bool {
        return self.state == .active;
    }

    /// Check if transaction is committed
    pub fn isCommitted(self: *Self) bool {
        return self.state == .committed;
    }

    /// Check if transaction is aborted
    pub fn isAborted(self: *Self) bool {
        return self.state == .aborted;
    }

    /// Get transaction read timestamp
    pub fn getReadTs(self: *Self) u64 {
        return self.read_ts;
    }

    /// Get transaction commit timestamp (only valid after commit)
    pub fn getCommitTs(self: *Self) u64 {
        return self.commit_ts;
    }

    /// Get the number of reads performed
    pub fn getReadCount(self: *Self) u32 {
        return @intCast(self.reads.count());
    }

    /// Get the number of writes performed
    pub fn getWriteCount(self: *Self) u32 {
        return @intCast(self.writes.items.len);
    }

    /// Check if the transaction has any writes
    pub fn hasWrites(self: *Self) bool {
        return self.writes.items.len > 0;
    }

    /// Get transaction size estimate in bytes
    pub fn getSizeEstimate(self: *Self) usize {
        var size: usize = 0;
        for (self.writes.items) |write| {
            size += write.key.len + write.value.len;
        }
        return size;
    }

    /// Mock implementation - would be replaced with actual DB calls
    fn getFromDB(self: *Self, key: []const u8) TxnError!?[]const u8 {
        _ = self;
        _ = key;
        // In real implementation, this would:
        // 1. Check memtables for key with timestamp <= read_ts
        // 2. Check SST files for key with timestamp <= read_ts
        // 3. Return most recent visible version
        return null; // Mock: key not found
    }

    /// Mock implementation - would apply writes to DB
    fn applyWrites(self: *Self) TxnError!void {
        _ = self;
        // In real implementation, this would:
        // 1. Add writes to current memtable with commit timestamp
        // 2. Ensure writes are durable (WAL)
        // 3. Update indexes if needed
    }
};

/// Transaction iterator for scanning operations
pub const TxnIterator = struct {
    txn: *Txn,
    current_key: ?[]const u8,
    start_key: ?[]const u8,
    end_key: ?[]const u8,
    reverse: bool,

    const Self = @This();

    pub fn init(txn: *Txn, start_key: ?[]const u8, end_key: ?[]const u8, reverse: bool) Self {
        return Self{
            .txn = txn,
            .current_key = null,
            .start_key = start_key,
            .end_key = end_key,
            .reverse = reverse,
        };
    }

    pub fn next(self: *Self) TxnError!bool {
        // Mock implementation
        _ = self;
        return false;
    }

    pub fn key(self: *Self) []const u8 {
        return self.current_key orelse "";
    }

    pub fn value(self: *Self) TxnError![]const u8 {
        if (self.current_key) |k| {
            return self.txn.get(k) orelse return TxnError.KeyNotFound;
        }
        return TxnError.KeyNotFound;
    }
};

/// Transaction builder for creating transactions with custom options
pub const TxnBuilder = struct {
    options: TxnOptions,

    const Self = @This();

    pub fn init() Self {
        return Self{
            .options = TxnOptions{},
        };
    }

    pub fn readOnly(self: *Self, read_only: bool) *Self {
        self.options.read_only = read_only;
        return self;
    }

    pub fn maxReadKeys(self: *Self, max_keys: u32) *Self {
        self.options.max_read_keys = max_keys;
        return self;
    }

    pub fn maxWriteKeys(self: *Self, max_keys: u32) *Self {
        self.options.max_write_keys = max_keys;
        return self;
    }

    pub fn detectConflicts(self: *Self, detect: bool) *Self {
        self.options.detect_conflicts = detect;
        return self;
    }

    pub fn build(self: *Self, allocator: Allocator, read_ts: u64, db: *anyopaque, oracle: *Oracle) Txn {
        return Txn.init(allocator, read_ts, db, oracle, self.options);
    }
};

/// Transaction manager for coordinating multiple transactions
pub const TxnManager = struct {
    oracle: *Oracle,
    allocator: Allocator,
    active_txns: HashMap(u64, *Txn, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage),
    txn_counter: atomic.Value(u64),
    mutex: std.Thread.Mutex,

    const Self = @This();

    pub fn init(allocator: Allocator, oracle: *Oracle) Self {
        return Self{
            .oracle = oracle,
            .allocator = allocator,
            .active_txns = HashMap(u64, *Txn, std.hash_map.AutoContext(u64), std.hash_map.default_max_load_percentage).init(allocator),
            .txn_counter = atomic.Value(u64).init(1),
            .mutex = std.Thread.Mutex{},
        };
    }

    pub fn deinit(self: *Self) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var iter = self.active_txns.valueIterator();
        while (iter.next()) |txn_ptr| {
            const txn = txn_ptr.*;
            txn.discard();
            txn.deinit();
            self.allocator.destroy(txn);
        }
        self.active_txns.deinit();
    }

    pub fn newTransaction(self: *Self, db: *anyopaque, options: TxnOptions) !*Txn {
        const read_ts = try self.oracle.newReadTs();
        const txn_id = self.txn_counter.fetchAdd(1, .acq_rel);

        const txn = try self.allocator.create(Txn);
        txn.* = Txn.init(self.allocator, read_ts, db, self.oracle, options);

        self.mutex.lock();
        defer self.mutex.unlock();
        try self.active_txns.put(txn_id, txn);

        return txn;
    }

    pub fn commitTransaction(self: *Self, txn: *Txn) !void {
        try txn.commit();
        self.removeTransaction(txn);
    }

    pub fn discardTransaction(self: *Self, txn: *Txn) void {
        txn.discard();
        self.removeTransaction(txn);
    }

    fn removeTransaction(self: *Self, txn: *Txn) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var iter = self.active_txns.iterator();
        while (iter.next()) |entry| {
            if (entry.value_ptr.* == txn) {
                _ = self.active_txns.remove(entry.key_ptr.*);
                txn.deinit();
                self.allocator.destroy(txn);
                break;
            }
        }
    }

    pub fn getActiveCount(self: *Self) usize {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self.active_txns.count();
    }
};
