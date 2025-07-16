const std = @import("std");
const atomic = std.atomic;
const Random = std.Random;
const Arena = @import("../memory/arena.zig").Arena;

/// Atomic ValueStruct for concurrent access
pub const ValueStruct = struct {
    value: []const u8,
    timestamp: u64,
    meta: u8,

    const Self = @This();

    /// Metadata flags
    pub const DELETED_FLAG: u8 = 0x01;
    pub const TOMBSTONE_FLAG: u8 = 0x02;
    pub const COMPRESSED_FLAG: u8 = 0x04;
    pub const EXTERNAL_FLAG: u8 = 0x08; // Value stored externally (ValueLog)

    pub fn isDeleted(self: *const ValueStruct) bool {
        return (self.meta & DELETED_FLAG) != 0;
    }

    pub fn setDeleted(self: *ValueStruct) void {
        self.meta |= DELETED_FLAG;
    }

    pub fn isTombstone(self: *const ValueStruct) bool {
        return (self.meta & TOMBSTONE_FLAG) != 0;
    }

    pub fn setTombstone(self: *ValueStruct) void {
        self.meta |= TOMBSTONE_FLAG;
    }

    pub fn isCompressed(self: *const ValueStruct) bool {
        return (self.meta & COMPRESSED_FLAG) != 0;
    }

    pub fn setCompressed(self: *ValueStruct) void {
        self.meta |= COMPRESSED_FLAG;
    }

    pub fn isExternal(self: *const ValueStruct) bool {
        return (self.meta & EXTERNAL_FLAG) != 0;
    }

    pub fn setExternal(self: *ValueStruct) void {
        self.meta |= EXTERNAL_FLAG;
    }

    /// Create a copy with owned memory
    pub fn clone(self: *const ValueStruct, allocator: std.mem.Allocator) !ValueStruct {
        const value_copy = try allocator.dupe(u8, self.value);
        return ValueStruct{
            .value = value_copy,
            .timestamp = self.timestamp,
            .meta = self.meta,
        };
    }

    /// Free the owned memory
    pub fn deinit(self: *const ValueStruct, allocator: std.mem.Allocator) void {
        allocator.free(self.value);
    }

    /// Compare two ValueStructs for ordering (by timestamp)
    pub fn compare(self: *const ValueStruct, other: *const ValueStruct) std.math.Order {
        return std.math.order(self.timestamp, other.timestamp);
    }

    /// Check if this ValueStruct is newer than other
    pub fn isNewerThan(self: *const ValueStruct, other: *const ValueStruct) bool {
        return self.timestamp > other.timestamp;
    }

    /// Get serialized size
    pub fn serializedSize(self: *const ValueStruct) usize {
        return @sizeOf(u64) + @sizeOf(u8) + @sizeOf(u32) + self.value.len;
    }
};

/// Statistics for SkipList monitoring
pub const SkipListStats = struct {
    entries_count: u64,
    memory_used: u64,
    puts_total: u64,
    gets_total: u64,
    deletes_total: u64,
    iterations_total: u64,
    cas_failures: u64,
    search_steps_total: u64,
    max_height_reached: u32,
};

/// SkipList error types
pub const SkipListError = error{
    KeyTooLarge,
    ValueTooLarge,
    OutOfMemory,
    Corrupted,
};

/// Configuration for SkipList behavior
pub const SkipListConfig = struct {
    max_key_size: usize = 1024 * 1024, // 1MB max key size
    max_value_size: usize = 1024 * 1024 * 1024, // 1GB max value size
    enable_statistics: bool = true,
    enable_validation: bool = false, // Expensive validation checks
};

/// Lock-free SkipList with atomic ValueStruct operations
pub const SkipList = struct {
    const MAX_HEIGHT = 20;
    const PROBABILITY = 0.25; // 1/4 probability for each level

    head: *Node,
    height: atomic.Value(u32),
    arena: Arena,
    rng: Random,
    stats: SkipListStats,
    stats_mutex: std.Thread.Mutex,
    config: SkipListConfig,
    allocator: std.mem.Allocator, // For value copying

    const Self = @This();

    const Node = struct {
        key: []const u8,
        value: ValueStruct,
        height: u16,
        tower: [MAX_HEIGHT]atomic.Value(?*Node),
        deleted: atomic.Value(bool),
        ref_count: atomic.Value(u32), // Reference counting for safe memory management
        value_mutex: std.Thread.Mutex, // Protect value updates

        fn init(key: []const u8, value: ValueStruct, height: u16) Node {
            var node = Node{
                .key = key,
                .value = value,
                .height = height,
                .tower = undefined,
                .deleted = atomic.Value(bool).init(false),
                .ref_count = atomic.Value(u32).init(1),
                .value_mutex = std.Thread.Mutex{},
            };

            for (0..MAX_HEIGHT) |i| {
                node.tower[i] = atomic.Value(?*Node).init(null);
            }

            return node;
        }

        /// Atomically update the value using mutex protection
        fn compareAndSwapValue(self: *Node, expected: ValueStruct, new_value: ValueStruct) bool {
            self.value_mutex.lock();
            defer self.value_mutex.unlock();
            
            if (std.mem.eql(u8, self.value.value, expected.value) and 
                self.value.timestamp == expected.timestamp and 
                self.value.meta == expected.meta) {
                self.value = new_value;
                return true;
            }
            return false;
        }

        /// Load the value with mutex protection
        fn loadValue(self: *const Node) ValueStruct {
            // Since we're reading, we need to cast away const to use mutex
            const mutable_self = @constCast(self);
            mutable_self.value_mutex.lock();
            defer mutable_self.value_mutex.unlock();
            return self.value;
        }

        /// Store the value with mutex protection
        fn storeValue(self: *Node, new_value: ValueStruct) void {
            self.value_mutex.lock();
            defer self.value_mutex.unlock();
            self.value = new_value;
        }

        /// Check if node is logically deleted
        fn isDeleted(self: *const Node) bool {
            return self.deleted.load(.acquire);
        }

        /// Mark node as deleted
        fn markDeleted(self: *Node) void {
            self.deleted.store(true, .release);
        }

        /// Increment reference count
        fn addRef(self: *Node) void {
            _ = self.ref_count.fetchAdd(1, .acq_rel);
        }

        /// Decrement reference count and return true if it reaches zero
        fn release(self: *Node) bool {
            const old_count = self.ref_count.fetchSub(1, .acq_rel);
            return old_count == 1;
        }
    };

    pub fn init(allocator: std.mem.Allocator, arena_size: usize) !Self {
        return Self.initWithConfig(allocator, arena_size, SkipListConfig{});
    }

    pub fn initWithConfig(allocator: std.mem.Allocator, arena_size: usize, config: SkipListConfig) !Self {
        var arena = try Arena.init(allocator, arena_size);

        const head_key = try arena.allocBytes(0);
        const head_value = ValueStruct{
            .value = &[_]u8{},
            .timestamp = 0,
            .meta = 0,
        };

        const head = try arena.alloc(Node, 1);
        head[0] = Node.init(head_key, head_value, MAX_HEIGHT);

        var prng = std.Random.DefaultPrng.init(@intCast(std.time.milliTimestamp()));

        return Self{
            .head = &head[0],
            .height = atomic.Value(u32).init(1),
            .arena = arena,
            .rng = prng.random(),
            .stats = SkipListStats{
                .entries_count = 0,
                .memory_used = 0,
                .puts_total = 0,
                .gets_total = 0,
                .deletes_total = 0,
                .iterations_total = 0,
                .cas_failures = 0,
                .search_steps_total = 0,
                .max_height_reached = 1,
            },
            .stats_mutex = std.Thread.Mutex{},
            .config = config,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.arena.deinit();
    }

    fn randomHeight(self: *Self) u16 {
        var height: u16 = 1;
        while (height < MAX_HEIGHT and self.rng.float(f32) < PROBABILITY) {
            height += 1;
        }
        
        // Update statistics
        if (self.config.enable_statistics) {
            self.updateMaxHeight(height);
        }
        
        return height;
    }

    fn updateMaxHeight(self: *Self, height: u16) void {
        if (!self.config.enable_statistics) return;
        
        self.stats_mutex.lock();
        defer self.stats_mutex.unlock();
        
        if (height > self.stats.max_height_reached) {
            self.stats.max_height_reached = @intCast(height);
        }
    }

    fn findNodeWithStats(self: *Self, key: []const u8, prev: *[MAX_HEIGHT]?*Node) ?*Node {
        if (self.config.enable_statistics) {
            self.stats_mutex.lock();
            self.stats.gets_total += 1;
            self.stats_mutex.unlock();
        }
        
        return self.findNode(key, prev);
    }

    fn findNode(self: *Self, key: []const u8, prev: *[MAX_HEIGHT]?*Node) ?*Node {
        var current = self.head;
        var level = self.height.load(.acquire);
        var search_steps: u64 = 0;

        while (level > 0) {
            level -= 1;

            while (true) {
                search_steps += 1;
                const next = current.tower[level].load(.acquire);
                if (next == null) break;

                // Skip logically deleted nodes
                if (next.?.isDeleted()) {
                    // Try to help with physical deletion
                    _ = current.tower[level].cmpxchgWeak(next, next.?.tower[level].load(.acquire), .acq_rel, .acquire);
                    continue;
                }

                const cmp = std.mem.order(u8, next.?.key, key);
                if (cmp == .gt) break;

                current = next.?;
                if (cmp == .eq and !current.isDeleted()) {
                    prev[level] = current;
                    
                    // Update search statistics
                    if (self.config.enable_statistics) {
                        self.updateSearchStats(search_steps);
                    }
                    
                    return current;
                }
            }

            prev[level] = current;
        }

        // Update search statistics even for misses
        if (self.config.enable_statistics) {
            self.updateSearchStats(search_steps);
        }

        return null;
    }

    fn updateSearchStats(self: *Self, steps: u64) void {
        if (!self.config.enable_statistics) return;
        
        self.stats_mutex.lock();
        defer self.stats_mutex.unlock();
        
        self.stats.search_steps_total += steps;
    }

    pub fn put(self: *Self, key: []const u8, value: ValueStruct) SkipListError!void {
        // Validate input sizes
        if (key.len > self.config.max_key_size) {
            return SkipListError.KeyTooLarge;
        }
        if (value.value.len > self.config.max_value_size) {
            return SkipListError.ValueTooLarge;
        }

        // Update statistics
        if (self.config.enable_statistics) {
            self.stats_mutex.lock();
            self.stats.puts_total += 1;
            self.stats_mutex.unlock();
        }

        // Retry loop for lock-free insertion
        var retry_count: u32 = 0;
        const max_retries = 10;
        
        while (retry_count < max_retries) {
            var prev: [MAX_HEIGHT]?*Node = [_]?*Node{null} ** MAX_HEIGHT;

            if (self.findNode(key, &prev)) |existing| {
                // Key exists, atomically update the value
                return self.updateExistingNode(existing, value);
            }

            // Key doesn't exist, insert new node
            const insertion_result = self.insertNewNode(key, value, &prev);
            switch (insertion_result) {
                .success => return,
                .retry => {
                    retry_count += 1;
                    if (self.config.enable_statistics) {
                        self.stats_mutex.lock();
                        self.stats.cas_failures += 1;
                        self.stats_mutex.unlock();
                    }
                    continue;
                },
                .err => |err| return err,
            }
        }
        
        return SkipListError.Corrupted; // Too many retries
    }

    const InsertResult = union(enum) {
        success: void,
        retry: void,
        err: SkipListError,
    };

    fn updateExistingNode(self: *Self, node: *Node, new_value: ValueStruct) SkipListError!void {
        // For atomic updates, we need to ensure the new value has a higher timestamp
        var retry_count: u32 = 0;
        const max_retries = 5;
        
        while (retry_count < max_retries) {
            const current_value = node.loadValue();
            
            // Skip if current value is newer (based on timestamp)
            if (current_value.timestamp >= new_value.timestamp) {
                return; // Current value is newer or equal, no update needed
            }
            
            // Try to update atomically
            if (node.compareAndSwapValue(current_value, new_value)) {
                return; // Successfully updated
            }
            
            retry_count += 1;
            if (self.config.enable_statistics) {
                self.stats_mutex.lock();
                self.stats.cas_failures += 1;
                self.stats_mutex.unlock();
            }
        }
        
        return SkipListError.Corrupted; // Too many CAS failures
    }

    fn insertNewNode(self: *Self, key: []const u8, value: ValueStruct, prev: *[MAX_HEIGHT]?*Node) InsertResult {
        const node_height = self.randomHeight();

        // Allocate memory for key and node
        const key_copy = self.arena.allocBytes(key.len) catch return InsertResult{ .err = SkipListError.OutOfMemory };
        @memcpy(key_copy, key);

        const new_node = self.arena.alloc(Node, 1) catch return InsertResult{ .err = SkipListError.OutOfMemory };
        new_node[0] = Node.init(key_copy, value, node_height);

        // Update skiplist height if necessary
        const current_height = self.height.load(.acquire);
        if (node_height > current_height) {
            for (current_height..node_height) |i| {
                prev[i] = self.head;
            }
            // Try to update height atomically
            _ = self.height.cmpxchgWeak(current_height, node_height, .acq_rel, .acquire);
        }

        // Link the node at all levels atomically
        for (0..node_height) |level| {
            while (true) {
                const next = if (prev[level]) |p| p.tower[level].load(.acquire) else null;
                new_node[0].tower[level].store(next, .release);

                // Atomic insertion
                if (prev[level]) |p| {
                    if (p.tower[level].cmpxchgWeak(next, &new_node[0], .acq_rel, .acquire) == null) {
                        break; // Successfully linked at this level
                    }
                    // CAS failed, retry
                } else {
                    break; // No predecessor at this level
                }
            }
        }

        // Update statistics
        if (self.config.enable_statistics) {
            self.stats_mutex.lock();
            self.stats.entries_count += 1;
            self.stats.memory_used += key.len + @sizeOf(Node) + value.value.len;
            self.stats_mutex.unlock();
        }

        return InsertResult{ .success = {} };
    }

    pub fn get(self: *Self, key: []const u8) ?ValueStruct {
        var prev: [MAX_HEIGHT]?*Node = [_]?*Node{null} ** MAX_HEIGHT;

        if (self.findNodeWithStats(key, &prev)) |node| {
            const value = node.loadValue();
            
            // Don't return deleted values
            if (value.isDeleted() or node.isDeleted()) {
                return null;
            }
            
            return value;
        }

        return null;
    }

    /// Delete a key by marking it as deleted (tombstone)
    pub fn delete(self: *Self, key: []const u8) SkipListError!bool {
        // Update statistics
        if (self.config.enable_statistics) {
            self.stats_mutex.lock();
            self.stats.deletes_total += 1;
            self.stats_mutex.unlock();
        }

        var prev: [MAX_HEIGHT]?*Node = [_]?*Node{null} ** MAX_HEIGHT;

        if (self.findNode(key, &prev)) |node| {
            return self.markNodeDeleted(node);
        }

        return false; // Key not found
    }

    fn markNodeDeleted(self: *Self, node: *Node) SkipListError!bool {
        var retry_count: u32 = 0;
        const max_retries = 5;
        
        while (retry_count < max_retries) {
            var current_value = node.loadValue();
            
            // Already deleted
            if (current_value.isDeleted() or node.isDeleted()) {
                return true;
            }
            
            // Create tombstone value
            var tombstone_value = current_value;
            tombstone_value.setDeleted();
            tombstone_value.setTombstone();
            
            // Try to update atomically
            if (node.compareAndSwapValue(current_value, tombstone_value)) {
                // Mark node as logically deleted
                node.markDeleted();
                
                // Update statistics
                if (self.config.enable_statistics) {
                    self.stats_mutex.lock();
                    self.stats.entries_count = if (self.stats.entries_count > 0) self.stats.entries_count - 1 else 0;
                    self.stats_mutex.unlock();
                }
                
                return true;
            }
            
            retry_count += 1;
            if (self.config.enable_statistics) {
                self.stats_mutex.lock();
                self.stats.cas_failures += 1;
                self.stats_mutex.unlock();
            }
        }
        
        return SkipListError.Corrupted; // Too many CAS failures
    }

    /// Get statistics
    pub fn getStats(self: *const Self) SkipListStats {
        const mutable_self = @constCast(self);
        mutable_self.stats_mutex.lock();
        defer mutable_self.stats_mutex.unlock();
        return self.stats;
    }

    /// Reset statistics
    pub fn resetStats(self: *Self) void {
        if (!self.config.enable_statistics) return;
        
        self.stats_mutex.lock();
        defer self.stats_mutex.unlock();
        
        const entries_count = self.stats.entries_count;
        const memory_used = self.stats.memory_used;
        const max_height = self.stats.max_height_reached;
        
        self.stats = SkipListStats{
            .entries_count = entries_count, // Keep entry count
            .memory_used = memory_used, // Keep memory usage
            .puts_total = 0,
            .gets_total = 0,
            .deletes_total = 0,
            .iterations_total = 0,
            .cas_failures = 0,
            .search_steps_total = 0,
            .max_height_reached = max_height,
        };
    }

    /// Validate the skiplist structure (expensive operation)
    pub fn validate(self: *Self) SkipListError!void {
        if (!self.config.enable_validation) return;
        
        // Check that levels are properly linked
        var level: usize = 0;
        while (level < self.height.load(.acquire)) {
            var current = self.head;
            var prev_key: ?[]const u8 = null;
            
            while (current.tower[level].load(.acquire)) |next| {
                current = next;
                
                // Check ordering
                if (prev_key) |prev| {
                    if (std.mem.order(u8, prev, current.key) != .lt) {
                        return SkipListError.Corrupted;
                    }
                }
                prev_key = current.key;
            }
            
            level += 1;
        }
    }

    pub fn iterator(self: *Self) SkipListIterator {
        return SkipListIterator.init(self);
    }

    pub fn size(self: *Self) usize {
        return self.arena.bytesUsed();
    }
};

pub const SkipListIterator = struct {
    skiplist: *SkipList,
    current: ?*SkipList.Node,

    const Self = @This();

    pub fn init(skiplist: *SkipList) Self {
        return Self{
            .skiplist = skiplist,
            .current = skiplist.head.tower[0].load(.acquire),
        };
    }

    pub fn next(self: *Self) ?struct { key: []const u8, value: ValueStruct } {
        // Update statistics
        if (self.skiplist.config.enable_statistics) {
            self.skiplist.stats_mutex.lock();
            self.skiplist.stats.iterations_total += 1;
            self.skiplist.stats_mutex.unlock();
        }

        while (self.current) |node| {
            const value = node.loadValue();
            
            // Skip deleted nodes and move to next
            if (value.isDeleted() or node.isDeleted()) {
                self.current = node.tower[0].load(.acquire);
                continue;
            }
            
            const result = .{
                .key = node.key,
                .value = value,
            };
            self.current = node.tower[0].load(.acquire);
            return result;
        }
        return null;
    }

    pub fn seek(self: *Self, key: []const u8) void {
        var prev: [SkipList.MAX_HEIGHT]?*SkipList.Node = [_]?*SkipList.Node{null} ** SkipList.MAX_HEIGHT;

        if (self.skiplist.findNode(key, &prev)) |node| {
            self.current = node;
        } else {
            self.current = prev[0];
            if (self.current) |c| {
                self.current = c.tower[0].load(.acquire);
            }
        }
    }
};
