const std = @import("std");
const ArrayList = std.ArrayList;
const HashMap = std.HashMap;
const atomic = std.atomic;
const Mutex = std.Thread.Mutex;
const fs = std.fs;
const Channel = @import("core/channel.zig").Channel;
const ChannelError = @import("core/channel.zig").ChannelError;
const MemTable = @import("storage/memtable.zig").MemTable;
const LevelsController = @import("storage/levels.zig").LevelsController;
const Table = @import("storage/table.zig").Table;
const Oracle = @import("transaction/oracle.zig").Oracle;
const Transaction = @import("transaction/oracle.zig").Transaction;
const ValueStruct = @import("core/skiplist.zig").ValueStruct;
const Options = @import("core/options.zig").Options;
const Txn = @import("transaction/txn.zig").Txn;
const TxnOptions = @import("transaction/txn.zig").TxnOptions;
const TxnError = @import("transaction/txn.zig").TxnError;
const TxnManager = @import("transaction/txn.zig").TxnManager;
const ValueLog = @import("storage/vlog.zig").ValueLog;
const ValuePointer = @import("storage/vlog.zig").ValuePointer;
const ManifestFile = @import("storage/manifest.zig").ManifestFile;
const TableInfo = @import("storage/manifest.zig").TableInfo;
const WaterMark = @import("transaction/watermark.zig").WaterMark;
const Entry = @import("storage/wal.zig").Entry;
const CompactionJob = @import("storage/compaction.zig").CompactionJob;
const CompactionPicker = @import("storage/compaction.zig").CompactionPicker;
const CompactionStrategy = @import("storage/compaction.zig").CompactionStrategy;

/// Database errors
pub const DBError = error{
    DatabaseClosed,
    InvalidOptions,
    CorruptedData,
    OutOfMemory,
    IOError,
    TransactionError,
    InvalidKey,
    KeyTooLarge,
    ValueTooLarge,
};

/// Database statistics for monitoring
pub const DBStats = struct {
    memtable_size: usize,
    immutable_count: u32,
    level_sizes: [7]u64,
    gets_total: u64,
    puts_total: u64,
    deletes_total: u64,
    txn_commits: u64,
    txn_aborts: u64,
    compactions_total: u64,
    vlog_size: u64,
    vlog_space_reclaim_runs: u64,
};

/// Enhanced worker performance statistics
pub const WorkerStats = struct {
    compaction_jobs_queued: atomic.Value(u64),
    compaction_jobs_completed: atomic.Value(u64),
    compaction_jobs_failed: atomic.Value(u64),
    compaction_bytes_processed: atomic.Value(u64),
    compaction_duration_ms: atomic.Value(u64),
    writer_batches_processed: atomic.Value(u64),
    writer_queue_depth: atomic.Value(u32),
    vlog_space_reclaimed_bytes: atomic.Value(u64),
    active_compaction_workers: atomic.Value(u32),

    const Self = @This();

    pub fn init() Self {
        return Self{
            .compaction_jobs_queued = atomic.Value(u64).init(0),
            .compaction_jobs_completed = atomic.Value(u64).init(0),
            .compaction_jobs_failed = atomic.Value(u64).init(0),
            .compaction_bytes_processed = atomic.Value(u64).init(0),
            .compaction_duration_ms = atomic.Value(u64).init(0),
            .writer_batches_processed = atomic.Value(u64).init(0),
            .writer_queue_depth = atomic.Value(u32).init(0),
            .vlog_space_reclaimed_bytes = atomic.Value(u64).init(0),
            .active_compaction_workers = atomic.Value(u32).init(0),
        };
    }

    pub fn recordCompactionJob(self: *Self, bytes_processed: u64, duration_ms: u64) void {
        _ = self.compaction_jobs_completed.fetchAdd(1, .acq_rel);
        _ = self.compaction_bytes_processed.fetchAdd(bytes_processed, .acq_rel);
        _ = self.compaction_duration_ms.fetchAdd(duration_ms, .acq_rel);
    }

    pub fn recordFailedCompaction(self: *Self) void {
        _ = self.compaction_jobs_failed.fetchAdd(1, .acq_rel);
    }

    pub fn setWorkerQueueDepth(self: *Self, depth: u32) void {
        self.writer_queue_depth.store(depth, .release);
    }

    pub fn incrementActiveWorkers(self: *Self) void {
        _ = self.active_compaction_workers.fetchAdd(1, .acq_rel);
    }

    pub fn decrementActiveWorkers(self: *Self) void {
        _ = self.active_compaction_workers.fetchSub(1, .acq_rel);
    }
};

/// Compaction throttling configuration
pub const CompactionThrottleConfig = struct {
    max_compaction_workers: u32,
    target_cpu_percent: u32,
    io_throttle_bytes_per_sec: u64,
    adaptive_throttling: bool,
    priority_boost_threshold: f64,
    max_pending_jobs: u32,

    const Self = @This();

    pub fn init() Self {
        return Self{
            .max_compaction_workers = 4,
            .target_cpu_percent = 80,
            .io_throttle_bytes_per_sec = 100 * 1024 * 1024, // 100MB/s
            .adaptive_throttling = true,
            .priority_boost_threshold = 0.8,
            .max_pending_jobs = 32,
        };
    }

    pub fn shouldThrottle(self: *const Self, current_workers: u32, pending_jobs: u32) bool {
        return current_workers >= self.max_compaction_workers or pending_jobs >= self.max_pending_jobs;
    }

    pub fn calculateDelay(self: *const Self, job_priority: f64) u64 {
        if (!self.adaptive_throttling) {
            return 0;
        }

        const base_delay_ms = 10;
        const priority_factor = if (job_priority > self.priority_boost_threshold) 0.5 else 1.0;
        return @intFromFloat(@as(f64, @floatFromInt(base_delay_ms)) * priority_factor);
    }
};

/// Write request for async operations
pub const WriteRequest = struct {
    key: []const u8,
    value: []const u8,
    timestamp: u64,
    meta: u8,
    callback: *const fn (err: ?DBError) void,
};

/// Batch write request for better performance
pub const BatchRequest = struct {
    entries: []WriteRequest,
    callback: *const fn (err: ?DBError) void,
};

/// Enhanced database engine with full LSM-Tree implementation
pub const DB = struct {
    // Core configuration
    options: Options,
    allocator: std.mem.Allocator,

    // Storage layers
    levels: LevelsController,
    memtable: *MemTable,
    immutable_tables: ArrayList(*MemTable),
    value_log: *ValueLog,
    manifest: *ManifestFile,

    // Transaction management
    oracle: Oracle,
    txn_manager: TxnManager,
    read_watermark: *WaterMark,
    commit_watermark: *WaterMark,

    // Async coordination
    write_channel: Channel(WriteRequest),
    batch_channel: Channel(BatchRequest),

    // State management
    next_memtable_id: atomic.Value(u64),
    next_table_id: atomic.Value(u64),
    close_signal: atomic.Value(bool),
    stats: DBStats,
    stats_mutex: Mutex,

    // Background workers
    writer_thread: ?std.Thread,
    compaction_thread: ?std.Thread,
    vlog_space_reclaim_thread: ?std.Thread,

    // Enhanced worker management
    compaction_pool: ArrayList(std.Thread),
    compaction_jobs: Channel(CompactionJob),
    compaction_picker: CompactionPicker,
    worker_stats: WorkerStats,
    compaction_throttle: CompactionThrottleConfig,

    // Synchronization
    memtable_mutex: Mutex,
    flush_signal: std.Thread.Condition,

    const Self = @This();

    /// Open or create a database with enhanced LSM-Tree architecture
    pub fn open(allocator: std.mem.Allocator, options: Options) !*Self {
        // Enhanced runtime validation
        try validateOptions(options);

        // Create directory structure
        try std.fs.cwd().makePath(options.dir);
        if (!std.mem.eql(u8, options.dir, options.value_dir)) {
            try std.fs.cwd().makePath(options.value_dir);
        }

        // Initialize database instance
        const db = try allocator.create(Self);
        errdefer allocator.destroy(db);

        // Initialize core components
        var oracle = try Oracle.init(allocator);
        errdefer oracle.deinit();

        var txn_manager = TxnManager.init(allocator, &oracle);
        errdefer txn_manager.deinit();

        const read_watermark = try WaterMark.init(allocator, "read_transactions");
        errdefer read_watermark.deinit();

        const commit_watermark = try WaterMark.init(allocator, "commit_transactions");
        errdefer commit_watermark.deinit();

        // Initialize storage components
        const value_log = try ValueLog.init(allocator, options.value_dir, 10, options.value_log_file_size, options.compression);
        errdefer value_log.deinit();

        var manifest_path_buf: [512]u8 = undefined;
        const manifest_path = try std.fmt.bufPrint(&manifest_path_buf, "{s}/MANIFEST", .{options.dir});
        const manifest = try ManifestFile.init(allocator, manifest_path);

        var levels = LevelsController.init(allocator, &options);
        errdefer levels.deinit();

        // Initialize channels
        var write_channel = try Channel(WriteRequest).init(allocator, 1000);
        errdefer write_channel.deinit();

        var batch_channel = try Channel(BatchRequest).init(allocator, 100);
        errdefer batch_channel.deinit();

        // Initialize database structure
        db.* = Self{
            .options = options,
            .allocator = allocator,
            .levels = levels,
            .memtable = undefined, // Will be set below
            .immutable_tables = ArrayList(*MemTable).init(allocator),
            .value_log = value_log,
            .manifest = manifest,
            .oracle = oracle,
            .txn_manager = txn_manager,
            .read_watermark = read_watermark,
            .commit_watermark = commit_watermark,
            .write_channel = write_channel,
            .batch_channel = batch_channel,
            .next_memtable_id = atomic.Value(u64).init(1),
            .next_table_id = atomic.Value(u64).init(1),
            .close_signal = atomic.Value(bool).init(false),
            .stats = DBStats{
                .memtable_size = 0,
                .immutable_count = 0,
                .level_sizes = [_]u64{0} ** 7,
                .gets_total = 0,
                .puts_total = 0,
                .deletes_total = 0,
                .txn_commits = 0,
                .txn_aborts = 0,
                .compactions_total = 0,
                .vlog_size = 0,
                .vlog_space_reclaim_runs = 0,
            },
            .stats_mutex = Mutex{},
            .writer_thread = null,
            .compaction_thread = null,
            .vlog_space_reclaim_thread = null,

            // Enhanced worker management
            .compaction_pool = ArrayList(std.Thread).init(allocator),
            .compaction_jobs = Channel(CompactionJob).init(allocator, 64),
            .compaction_picker = CompactionPicker.init(allocator, .level),
            .worker_stats = WorkerStats.init(),
            .compaction_throttle = CompactionThrottleConfig.init(),
            .memtable_mutex = Mutex{},
            .flush_signal = std.Thread.Condition{},
        };

        // Create initial memtable
        db.memtable = try db.createMemTable();
        errdefer {
            db.memtable.deinit();
            allocator.destroy(db.memtable);
        }

        // Recover from manifest and WAL
        try db.recover();

        // Start background workers
        db.writer_thread = try std.Thread.spawn(.{}, writerWorker, .{db});
        db.compaction_thread = try std.Thread.spawn(.{}, enhancedCompactionCoordinator, .{db});
        db.vlog_space_reclaim_thread = try std.Thread.spawn(.{}, vlogSpaceReclaimWorker, .{db});

        // Start enhanced compaction worker pool
        try db.startCompactionPool();

        return db;
    }

    fn validateOptions(options: Options) !void {
        if (options.mem_table_size == 0) return DBError.InvalidOptions;
        if (options.max_levels == 0) return DBError.InvalidOptions;
        if (options.block_size == 0) return DBError.InvalidOptions;
        if (options.num_memtables == 0) return DBError.InvalidOptions;
        if (options.value_log_file_size == 0) return DBError.InvalidOptions;
        if (options.bloom_false_positive <= 0.0 or options.bloom_false_positive >= 1.0) {
            return DBError.InvalidOptions;
        }
    }

    /// Start the enhanced compaction worker pool
    fn startCompactionPool(self: *Self) !void {
        const num_workers = self.options.num_compactors;
        try self.compaction_pool.ensureTotalCapacity(num_workers);

        for (0..num_workers) |i| {
            const worker_id = @as(u32, @intCast(i));
            const thread = try std.Thread.spawn(.{}, enhancedCompactionWorker, .{ self, worker_id });
            self.compaction_pool.appendAssumeCapacity(thread);
        }
    }

    fn recover(self: *Self) !void {
        // Validate directory structure exists
        fs.cwd().access(self.options.dir, .{}) catch {
            return DBError.IOError;
        };

        if (!std.mem.eql(u8, self.options.dir, self.options.value_dir)) {
            fs.cwd().access(self.options.value_dir, .{}) catch {
                return DBError.IOError;
            };
        }

        // Recover from manifest first - this loads all table metadata
        try self.recoverFromManifest();

        // Recover from WAL files - replay uncommitted operations
        try self.recoverFromWAL();

        // Verify data integrity and clean up orphaned files
        try self.verifyAndCleanup();
    }

    /// Recover database state from manifest file
    fn recoverFromManifest(self: *Self) !void {
        // The manifest file initialization already handles recovery
        // We just need to load existing tables into the levels controller
        for (0..self.options.max_levels) |level| {
            const level_u32 = @as(u32, @intCast(level));
            const tables = self.manifest.getTablesAtLevel(level_u32) catch continue;

            for (tables) |table_info| {
                // Verify table file exists
                var table_path_buf: [512]u8 = undefined;
                const full_path = std.fmt.bufPrint(&table_path_buf, "{s}/{s}", .{ self.options.dir, table_info.path }) catch continue;

                fs.cwd().access(full_path, .{}) catch |err| switch (err) {
                    error.FileNotFound => {
                        // Table file missing - remove from manifest
                        self.manifest.removeTable(table_info.id) catch {};
                        continue;
                    },
                    else => continue,
                };

                // Load table into levels controller
                const table = try self.allocator.create(Table);
                table.* = Table.open(self.allocator, full_path, table_info.id) catch {
                    self.allocator.destroy(table);
                    continue;
                };

                // Add table to the appropriate level
                if (level_u32 == 0) {
                    self.levels.addLevel0Table(table) catch {
                        table.close();
                        self.allocator.destroy(table);
                        continue;
                    };
                } else {
                    self.levels.levels[level_u32].addTable(table) catch {
                        table.close();
                        self.allocator.destroy(table);
                        continue;
                    };
                }
            }
        }
    }

    /// Recover uncommitted operations from WAL files
    fn recoverFromWAL(self: *Self) !void {
        // Scan directory for WAL files
        var dir = fs.cwd().openDir(self.options.dir, .{ .iterate = true }) catch return;
        defer dir.close();

        var iterator = dir.iterate();
        while (iterator.next() catch null) |entry| {
            if (entry.kind != .file) continue;

            // Check if it's a WAL file (pattern: wal_*.log)
            if (!std.mem.startsWith(u8, entry.name, "wal_") or !std.mem.endsWith(u8, entry.name, ".log")) {
                continue;
            }

            // During recovery, we want to replay ALL WAL files from previous runs
            // We'll skip WAL files that are still being used by active memtables
            // For now, replay all WAL files since we're in recovery mode

            // Replay WAL file
            var wal_path_buf: [512]u8 = undefined;
            const wal_path = std.fmt.bufPrint(&wal_path_buf, "{s}/{s}", .{ self.options.dir, entry.name }) catch continue;

            std.log.info("Replaying WAL file: {s}", .{entry.name});
            self.replayWAL(wal_path) catch |err| {
                std.log.warn("Failed to replay WAL file {s}: {}", .{ entry.name, err });
                continue;
            };
            std.log.info("Successfully replayed WAL file: {s}", .{entry.name});

            // After successful replay, delete the WAL file
            fs.cwd().deleteFile(wal_path) catch {};
        }
    }

    /// Replay a single WAL file
    fn replayWAL(self: *Self, wal_path: []const u8) !void {
        const wal_file = fs.cwd().openFile(wal_path, .{}) catch return;
        defer wal_file.close();

        const file_size = wal_file.getEndPos() catch return;
        if (file_size == 0) return;

        const content = self.allocator.alloc(u8, file_size) catch return;
        defer self.allocator.free(content);

        _ = wal_file.readAll(content) catch return;

        // Parse WAL entries - format: key_len|value_len|timestamp|meta|key|value
        var offset: usize = 0;
        while (offset < content.len) {
            if (offset + 17 > content.len) break; // 4 + 4 + 8 + 1 = 17 bytes header

            // Read key length
            const key_len = std.mem.readInt(u32, content[offset .. offset + 4][0..4], .little);
            offset += 4;

            // Read value length
            const value_len = std.mem.readInt(u32, content[offset .. offset + 4][0..4], .little);
            offset += 4;

            // Read timestamp
            const timestamp = std.mem.readInt(u64, content[offset .. offset + 8][0..8], .little);
            offset += 8;

            // Read meta
            const meta = content[offset];
            offset += 1;

            // Read key
            if (offset + key_len > content.len) break;
            const key = content[offset .. offset + key_len];
            offset += key_len;

            // Read value
            if (offset + value_len > content.len) break;
            const value = content[offset .. offset + value_len];
            offset += value_len;

            // Apply the operation
            const value_struct = ValueStruct{
                .value = value,
                .timestamp = timestamp,
                .meta = meta,
            };

            self.memtable.put(key, value_struct) catch continue;
        }
    }

    /// Verify data integrity and clean up orphaned files
    fn verifyAndCleanup(self: *Self) !void {
        // Scan directory for orphaned files
        var dir = fs.cwd().openDir(self.options.dir, .{ .iterate = true }) catch return;
        defer dir.close();

        var iterator = dir.iterate();
        while (iterator.next() catch null) |entry| {
            if (entry.kind != .file) continue;

            // Check for orphaned SST files (not in manifest)
            if (std.mem.endsWith(u8, entry.name, ".sst")) {
                var is_orphaned = true;

                // Check if file is referenced in manifest
                for (0..self.options.max_levels) |level| {
                    const level_u32 = @as(u32, @intCast(level));
                    const tables = self.manifest.getTablesAtLevel(level_u32) catch continue;

                    for (tables) |table_info| {
                        if (std.mem.eql(u8, table_info.path, entry.name)) {
                            is_orphaned = false;
                            break;
                        }
                    }

                    if (!is_orphaned) break;
                }

                // Remove orphaned SST files
                if (is_orphaned) {
                    var file_path_buf: [512]u8 = undefined;
                    const file_path = std.fmt.bufPrint(&file_path_buf, "{s}/{s}", .{ self.options.dir, entry.name }) catch continue;
                    fs.cwd().deleteFile(file_path) catch {};
                }
            }

            // Check for orphaned temporary files
            if (std.mem.startsWith(u8, entry.name, "tmp_") or std.mem.endsWith(u8, entry.name, ".tmp")) {
                var file_path_buf: [512]u8 = undefined;
                const file_path = std.fmt.bufPrint(&file_path_buf, "{s}/{s}", .{ self.options.dir, entry.name }) catch continue;
                fs.cwd().deleteFile(file_path) catch {};
            }
        }

        // Verify value log files integrity
        if (fs.cwd().openDir(self.options.value_dir, .{ .iterate = true })) |vlog_dir| {
            var mutable_vlog_dir = vlog_dir;
            defer mutable_vlog_dir.close();

            var vlog_iterator = vlog_dir.iterate();
            while (vlog_iterator.next() catch null) |entry| {
                if (entry.kind != .file) continue;

                // Check for orphaned value log files
                if (std.mem.startsWith(u8, entry.name, "vlog_") and std.mem.endsWith(u8, entry.name, ".vlog")) {
                    // Parse file ID from filename
                    const id_str = entry.name[5 .. entry.name.len - 5]; // Remove "vlog_" and ".vlog"
                    const file_id = std.fmt.parseInt(u32, id_str, 10) catch continue;

                    // Check if this file is still referenced
                    var is_referenced = false;
                    for (self.value_log.files.items) |vlog_file| {
                        if (vlog_file.id == file_id) {
                            is_referenced = true;
                            break;
                        }
                    }

                    // Remove unreferenced value log files
                    if (!is_referenced) {
                        var file_path_buf: [512]u8 = undefined;
                        const file_path = std.fmt.bufPrint(&file_path_buf, "{s}/{s}", .{ self.options.value_dir, entry.name }) catch continue;
                        fs.cwd().deleteFile(file_path) catch {};
                    }
                }
            }
        } else |_| {}
    }

    /// Gracefully close the database and cleanup all resources
    pub fn close(self: *Self) !void {
        // Signal all workers to stop
        self.close_signal.store(true, .release);

        // Close channels to wake up workers
        self.write_channel.close();
        self.batch_channel.close();
        self.compaction_jobs.close();

        // Wait for all background workers to finish
        if (self.writer_thread) |thread| {
            thread.join();
        }
        if (self.compaction_thread) |thread| {
            thread.join();
        }
        if (self.vlog_space_reclaim_thread) |thread| {
            thread.join();
        }

        // Wait for all compaction pool workers to finish
        for (self.compaction_pool.items) |thread| {
            thread.join();
        }
        self.compaction_pool.deinit();

        // Ensure all pending writes are flushed
        try self.sync();

        // Close transaction manager
        self.txn_manager.deinit();

        // Cleanup memtables
        self.memtable.deinit();
        self.allocator.destroy(self.memtable);

        for (self.immutable_tables.items) |memtable| {
            memtable.deinit();
            self.allocator.destroy(memtable);
        }
        self.immutable_tables.deinit();

        // Cleanup storage components
        self.levels.deinit();
        self.value_log.deinit();
        self.manifest.deinit();

        // Cleanup transaction infrastructure
        self.oracle.deinit();
        self.read_watermark.deinit();
        self.commit_watermark.deinit();

        // Cleanup channels
        self.write_channel.deinit();
        self.batch_channel.deinit();
        self.compaction_jobs.deinit();

        // Cleanup enhanced worker management
        self.compaction_picker.deinit();

        self.allocator.destroy(self);
    }

    /// Get a value for the given key with proper MVCC semantics
    pub fn get(self: *Self, key: []const u8) !?[]const u8 {
        if (self.close_signal.load(.acquire)) {
            return DBError.DatabaseClosed;
        }

        // Update statistics
        self.updateStats(.gets_total, 1);

        // Validate key
        if (key.len == 0 or key.len > 1024 * 1024) {
            return DBError.InvalidKey;
        }

        // Check current memtable first
        if (self.memtable.get(key)) |value| {
            if (value.isDeleted()) {
                return null;
            }

            // Check if value is in ValueLog
            if (value.isExternal()) {
                const ptr = ValuePointer.decode(value.value) catch {
                    return error.CorruptedData;
                };
                return self.value_log.read(ptr, self.allocator);
            }

            return try self.allocator.dupe(u8, value.value);
        }

        // Check immutable memtables
        for (self.immutable_tables.items) |memtable| {
            if (memtable.get(key)) |value| {
                if (value.isDeleted()) {
                    return null;
                }

                if (value.isExternal()) {
                    const ptr = ValuePointer.decode(value.value) catch {
                        return error.CorruptedData;
                    };
                    return self.value_log.read(ptr, self.allocator);
                }

                return try self.allocator.dupe(u8, value.value);
            }
        }

        // Check SST levels
        if (try self.levels.get(key)) |value| {
            if (value.isDeleted()) {
                return null;
            }

            if (value.isExternal()) {
                const ptr = ValuePointer.decode(value.value) catch {
                    return error.CorruptedData;
                };
                return self.value_log.read(ptr, self.allocator);
            }

            return try self.allocator.dupe(u8, value.value);
        }

        return null;
    }

    /// Set a key-value pair with enhanced error handling
    pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
        if (self.close_signal.load(.acquire)) {
            return DBError.DatabaseClosed;
        }

        // Validate inputs
        if (key.len == 0 or key.len > 1024 * 1024) {
            return DBError.KeyTooLarge;
        }
        if (value.len > 1024 * 1024 * 1024) {
            return DBError.ValueTooLarge;
        }

        // For simplicity, let's implement synchronous writes directly
        // This bypasses the async channel system for now
        return self.setSynchronous(key, value);
    }

    /// Synchronous set operation that writes directly to memtable
    fn setSynchronous(self: *Self, key: []const u8, value: []const u8) !void {
        const timestamp = try self.oracle.newReadTs();

        // Check if we should store in ValueLog
        const use_vlog = value.len >= self.options.value_threshold;

        var value_struct = ValueStruct{
            .value = value,
            .timestamp = timestamp,
            .meta = 0,
        };

        // Handle ValueLog storage for large values
        if (use_vlog and value.len > 0) {
            const entries = [_]Entry{Entry{
                .key = key,
                .value = value,
                .timestamp = timestamp,
                .meta = 0,
            }};

            const pointers = try self.value_log.write(&entries);
            defer self.allocator.free(pointers);

            // Serialize ValuePointer into value_struct.value
            const encoded_ptr = try pointers[0].encodeAlloc(self.allocator);
            value_struct.value = encoded_ptr;
            value_struct.setExternal();
        }

        // Write to memtable
        self.memtable_mutex.lock();
        defer self.memtable_mutex.unlock();

        try self.memtable.put(key, value_struct);

        // Update statistics
        if (value.len == 0) {
            self.updateStats(.deletes_total, 1);
        } else {
            self.updateStats(.puts_total, 1);
        }

        // Check if memtable is full and rotate if needed
        if (self.memtable.isFull()) {
            try self.rotateMemTable();
        }
    }

    /// Delete a key with tombstone marking
    pub fn delete(self: *Self, key: []const u8) !void {
        if (self.close_signal.load(.acquire)) {
            return DBError.DatabaseClosed;
        }

        if (key.len == 0 or key.len > 1024 * 1024) {
            return DBError.InvalidKey;
        }

        return self.deleteSynchronous(key);
    }

    /// Synchronous delete operation that writes directly to memtable
    fn deleteSynchronous(self: *Self, key: []const u8) !void {
        const timestamp = try self.oracle.newReadTs();

        const value_struct = ValueStruct{
            .value = &[_]u8{},
            .timestamp = timestamp,
            .meta = ValueStruct.DELETED_FLAG | ValueStruct.TOMBSTONE_FLAG,
        };

        // Write to memtable
        self.memtable_mutex.lock();
        defer self.memtable_mutex.unlock();

        try self.memtable.put(key, value_struct);

        // Update statistics
        self.updateStats(.deletes_total, 1);

        // Check if memtable is full and rotate if needed
        if (self.memtable.isFull()) {
            try self.rotateMemTable();
        }
    }

    pub fn sync(self: *Self) !void {
        try self.memtable.sync();

        for (self.immutable_tables.items) |memtable| {
            try memtable.sync();
        }
    }

    /// Create a new transaction with enhanced MVCC support
    pub fn newTransaction(self: *Self) !*Txn {
        if (self.close_signal.load(.acquire)) {
            return DBError.DatabaseClosed;
        }

        const options = TxnOptions{
            .read_only = false,
            .detect_conflicts = self.options.detect_conflicts,
        };

        return self.txn_manager.newTransaction(@ptrCast(self), options);
    }

    /// Create a read-only transaction for snapshots
    pub fn newReadTransaction(self: *Self) !*Txn {
        if (self.close_signal.load(.acquire)) {
            return DBError.DatabaseClosed;
        }

        const options = TxnOptions{
            .read_only = true,
            .detect_conflicts = false,
        };

        return self.txn_manager.newTransaction(@ptrCast(self), options);
    }

    /// Commit a transaction
    pub fn commitTransaction(self: *Self, txn: *Txn) !void {
        return self.txn_manager.commitTransaction(txn);
    }

    /// Discard a transaction
    pub fn discardTransaction(self: *Self, txn: *Txn) void {
        self.txn_manager.discardTransaction(txn);
    }

    /// Get database statistics
    pub fn getStats(self: *Self) DBStats {
        self.stats_mutex.lock();
        defer self.stats_mutex.unlock();

        var stats = self.stats;

        // Update current state
        stats.memtable_size = self.memtable.size();
        stats.immutable_count = @intCast(self.immutable_tables.items.len);

        // Update level sizes
        for (0..self.options.max_levels) |level| {
            stats.level_sizes[level] = self.levels.levelSize(@intCast(level));
        }

        stats.vlog_size = self.value_log.size();

        return stats;
    }

    /// Perform manual compaction
    pub fn compact(self: *Self) !void {
        for (0..self.options.max_levels - 1) |level| {
            try self.levels.compactLevel(@intCast(level));
        }

        self.updateStats(.compactions_total, 1);
    }

    /// Perform value log space reclamation (remove obsolete entries)
    pub fn reclaimValueLogSpace(self: *Self) !void {
        _ = try self.value_log.runSpaceReclamation(0.5); // Reclaim space if 50% waste
        self.updateStats(.vlog_space_reclaim_runs, 1);
    }

    fn createMemTable(self: *Self) !*MemTable {
        const memtable_id = self.next_memtable_id.fetchAdd(1, .acq_rel);

        var path_buffer: [512]u8 = undefined;
        const wal_path = try std.fmt.bufPrint(&path_buffer, "{s}/wal_{d}.log", .{ self.options.dir, memtable_id });

        const memtable = try self.allocator.create(MemTable);
        errdefer self.allocator.destroy(memtable);

        memtable.* = try MemTable.init(self.allocator, wal_path, self.options.mem_table_size);

        return memtable;
    }

    /// Helper function to update statistics atomically
    fn updateStats(self: *Self, field: std.meta.FieldEnum(DBStats), delta: u64) void {
        self.stats_mutex.lock();
        defer self.stats_mutex.unlock();

        switch (field) {
            .gets_total => self.stats.gets_total += delta,
            .puts_total => self.stats.puts_total += delta,
            .deletes_total => self.stats.deletes_total += delta,
            .txn_commits => self.stats.txn_commits += delta,
            .txn_aborts => self.stats.txn_aborts += delta,
            .compactions_total => self.stats.compactions_total += delta,
            .vlog_space_reclaim_runs => self.stats.vlog_space_reclaim_runs += delta,
            else => {}, // Other fields updated elsewhere
        }
    }

    /// Batch write operation for better performance
    pub fn writeBatch(self: *Self, entries: []const struct { key: []const u8, value: []const u8 }) !void {
        if (self.close_signal.load(.acquire)) {
            return DBError.DatabaseClosed;
        }

        var write_reqs = try self.allocator.alloc(WriteRequest, entries.len);
        defer self.allocator.free(write_reqs);

        for (entries, 0..) |entry, i| {
            const timestamp = try self.oracle.newReadTs();
            write_reqs[i] = WriteRequest{
                .key = entry.key,
                .value = entry.value,
                .timestamp = timestamp,
                .meta = 0,
                .callback = &struct {
                    fn callback(err: ?DBError) void {
                        _ = err;
                    }
                }.callback,
            };
        }

        const batch_req = BatchRequest{
            .entries = write_reqs,
            .callback = &struct {
                fn callback(err: ?DBError) void {
                    _ = err;
                }
            }.callback,
        };

        self.batch_channel.send(batch_req) catch |err| switch (err) {
            ChannelError.ChannelClosed => return DBError.DatabaseClosed,
            else => return DBError.IOError,
        };
    }

    fn handleWrite(self: *Self, req: WriteRequest) void {
        // Check if we should store in ValueLog
        const use_vlog = req.value.len >= self.options.value_threshold;

        var value_struct = ValueStruct{
            .value = req.value,
            .timestamp = req.timestamp,
            .meta = req.meta,
        };

        // Handle ValueLog storage for large values
        if (use_vlog and req.value.len > 0) {
            const entries = [_]Entry{Entry{
                .key = req.key,
                .value = req.value,
                .timestamp = req.timestamp,
                .meta = req.meta,
            }};

            const pointers = self.value_log.write(&entries) catch |err| {
                req.callback(switch (err) {
                    error.OutOfMemory => DBError.OutOfMemory,
                    else => DBError.IOError,
                });
                return;
            };
            defer self.allocator.free(pointers);

            // Serialize ValuePointer into value_struct.value
            const encoded_ptr = pointers[0].encodeAlloc(self.allocator) catch |err| {
                req.callback(switch (err) {
                    error.OutOfMemory => DBError.OutOfMemory,
                    else => DBError.IOError,
                });
                return;
            };
            value_struct.value = encoded_ptr;
            value_struct.setExternal();
        }

        // Set deletion flags if needed
        if (req.value.len == 0 or (req.meta & ValueStruct.DELETED_FLAG) != 0) {
            value_struct.setDeleted();
            if ((req.meta & ValueStruct.TOMBSTONE_FLAG) != 0) {
                value_struct.setTombstone();
            }
        }

        // Write to memtable
        self.memtable_mutex.lock();
        defer self.memtable_mutex.unlock();

        if (self.memtable.put(req.key, value_struct)) {
            // Update statistics
            if (req.value.len == 0) {
                self.updateStats(.deletes_total, 1);
            } else {
                self.updateStats(.puts_total, 1);
            }

            // Check if memtable is full
            if (self.memtable.isFull()) {
                self.rotateMemTable() catch |err| {
                    req.callback(switch (err) {
                        error.OutOfMemory => DBError.OutOfMemory,
                        else => DBError.IOError,
                    });
                    return;
                };
            }

            req.callback(null);
        } else |err| {
            req.callback(switch (err) {
                error.OutOfMemory => DBError.OutOfMemory,
                error.KeyTooLarge => DBError.KeyTooLarge,
                error.ValueTooLarge => DBError.ValueTooLarge,
                else => DBError.IOError,
            });
        }
    }

    fn rotateMemTable(self: *Self) !void {
        // Move current memtable to immutable list
        try self.immutable_tables.append(self.memtable);

        // Create new memtable
        self.memtable = try self.createMemTable();

        // Wake up flush workers
        self.flush_signal.broadcast();

        // Check if we need to flush oldest immutable
        if (self.immutable_tables.items.len > self.options.num_memtables) {
            const old_memtable = self.immutable_tables.orderedRemove(0);

            // Generate new table ID
            const table_id = self.next_table_id.fetchAdd(1, .acq_rel);

            // Create table info for manifest
            var table_path_buf: [512]u8 = undefined;
            const table_path = try std.fmt.bufPrint(&table_path_buf, "table_{}.sst", .{table_id});

            const table_info = TableInfo{
                .id = table_id,
                .path = try self.allocator.dupe(u8, table_path),
                .level = 0,
                .smallest_key = try self.allocator.dupe(u8, ""), // Will be updated after flush
                .largest_key = try self.allocator.dupe(u8, ""), // Will be updated after flush
                .size = 0, // Will be updated after flush
                .key_count = 0, // Will be updated after flush
                .created_at = @intCast(std.time.milliTimestamp()),
                .checksum = 0,
            };

            // Flush to SST
            try self.levels.flushMemTable(old_memtable);

            // Update manifest
            try self.manifest.addTable(table_info);

            // Cleanup
            old_memtable.deinit();
            self.allocator.destroy(old_memtable);
        }
    }

    /// Enhanced writer worker with batch processing
    fn writerWorker(self: *Self) void {
        while (!self.close_signal.load(.acquire)) {
            // Try to receive batch requests first for better performance
            if (self.batch_channel.tryReceive()) |maybe_batch| {
                if (maybe_batch) |batch| {
                    self.handleBatch(batch);
                    continue;
                }
            } else |_| {}

            // Handle individual write requests
            if (self.write_channel.receiveTimeout(1000000)) |req| { // 1ms timeout
                self.handleWrite(req);
            } else |err| switch (err) {
                ChannelError.ReceiveTimeout => continue,
                ChannelError.ChannelClosed => break,
                else => continue,
            }
        }
    }

    fn handleBatch(self: *Self, batch: BatchRequest) void {
        const first_error: ?DBError = null;

        for (batch.entries) |req| {
            self.handleWrite(req);
            // Note: individual callbacks are called in handleWrite
        }

        // Call batch callback
        batch.callback(first_error);
    }

    /// Enhanced compaction worker with smarter scheduling
    fn compactionWorker(self: *Self) void {
        while (!self.close_signal.load(.acquire)) {
            var compacted = false;

            // Check each level for compaction needs
            for (0..self.options.max_levels - 1) |level| {
                const level_u32 = @as(u32, @intCast(level));

                if (self.levels.needsCompaction()) {
                    self.levels.compactLevel(level_u32) catch continue;

                    compacted = true;
                    self.updateStats(.compactions_total, 1);

                    // Only compact one level per iteration to avoid overwhelming
                    break;
                }
            }

            // Sleep longer if no compaction was needed
            const sleep_time: u64 = if (compacted) 10000000 else 100000000; // 10ms vs 100ms
            std.Thread.sleep(sleep_time);
        }
    }

    /// Value log space reclamation worker
    fn vlogSpaceReclaimWorker(self: *Self) void {
        while (!self.close_signal.load(.acquire)) {
            // Reclaim space if value log is getting full
            const reclaim_threshold = 0.7; // Reclaim when 70% full

            if (self.value_log.shouldRunSpaceReclamation(reclaim_threshold)) {
                const start_time = std.time.milliTimestamp();
                const bytes_reclaimed = self.value_log.runSpaceReclamation(0.5) catch 0;
                const end_time = std.time.milliTimestamp();

                self.updateStats(.vlog_space_reclaim_runs, 1);
                _ = self.worker_stats.vlog_space_reclaimed_bytes.fetchAdd(bytes_reclaimed, .acq_rel);

                // Log space reclamation performance
                const duration = @as(u64, @intCast(end_time - start_time));
                if (duration > 1000) { // Log if reclamation took more than 1 second
                    std.log.info("VLog space reclamation completed: {} bytes reclaimed in {} ms", .{ bytes_reclaimed, duration });
                }
            }

            // Sleep for 30 seconds between space reclamation checks
            std.Thread.sleep(30000000000);
        }
    }

    /// Enhanced compaction coordinator that manages job selection and distribution
    fn enhancedCompactionCoordinator(self: *Self) void {
        while (!self.close_signal.load(.acquire)) {
            // Pick compaction jobs using the advanced compaction picker
            if (self.compaction_picker.pickCompaction(&self.levels)) |job| {
                // Queue the job for worker threads to process
                _ = self.worker_stats.compaction_jobs_queued.fetchAdd(1, .acq_rel);

                // Try to send the job to the worker pool
                if (self.compaction_jobs.trySend(job)) {
                    // Job successfully queued
                    continue;
                } else {
                    // Job queue is full, execute directly or wait
                    const active_workers = self.worker_stats.active_compaction_workers.load(.acquire);
                    const pending_jobs = self.compaction_jobs.size();

                    if (self.compaction_throttle.shouldThrottle(active_workers, pending_jobs)) {
                        // Apply throttling - delay before trying again
                        const delay_ms = self.compaction_throttle.calculateDelay(job.priority);
                        std.Thread.sleep(delay_ms * 1000000); // Convert to nanoseconds

                        // Try to send again after delay
                        if (self.compaction_jobs.trySend(job)) {
                            continue;
                        } else {
                            // Still can't send, record failure and cleanup
                            self.worker_stats.recordFailedCompaction();
                            job.deinit(self.allocator);
                        }
                    } else {
                        // Execute job directly in coordinator thread as fallback
                        self.executeCompactionJob(job);
                    }
                }
            } else {
                // No compaction needed, sleep for a while
                std.Thread.sleep(100000000); // 100ms
            }
        }
    }

    /// Enhanced compaction worker that processes jobs from the queue
    fn enhancedCompactionWorker(self: *Self, worker_id: u32) void {
        while (!self.close_signal.load(.acquire)) {
            // Try to receive a compaction job with timeout
            if (self.compaction_jobs.receiveTimeout(1000000)) |job| { // 1ms timeout
                self.worker_stats.incrementActiveWorkers();
                defer self.worker_stats.decrementActiveWorkers();

                // Record start time for performance metrics
                const start_time = std.time.milliTimestamp();

                // Execute the compaction job
                self.executeCompactionJob(job);

                // Record completion metrics
                const end_time = std.time.milliTimestamp();
                const duration = @as(u64, @intCast(end_time - start_time));

                // Estimate bytes processed (simplified calculation)
                const bytes_processed = job.estimated_size;
                self.worker_stats.recordCompactionJob(bytes_processed, duration);

                // Log performance for monitoring
                if (duration > 5000) { // Log slow compactions (>5s)
                    std.log.warn("Slow compaction on worker {}: level {} took {} ms", .{ worker_id, job.level, duration });
                }
            } else |err| switch (err) {
                ChannelError.ReceiveTimeout => {
                    // Normal timeout, continue polling
                    continue;
                },
                ChannelError.ChannelClosed => {
                    // Channel closed, worker should exit
                    break;
                },
                else => {
                    // Other error, continue with next iteration
                    continue;
                },
            }
        }
    }

    /// Execute a compaction job with enhanced error handling and metrics
    fn executeCompactionJob(self: *Self, job: CompactionJob) void {
        defer job.deinit(self.allocator);

        // Execute the actual compaction through the levels controller
        self.levels.compactLevel(job.level) catch |err| {
            self.worker_stats.recordFailedCompaction();

            switch (err) {
                error.OutOfMemory => {
                    std.log.err("Compaction failed due to OOM: level {}", .{job.level});
                    // Trigger space reclamation to free disk space
                    _ = self.value_log.runSpaceReclamation(0.3) catch {};
                },
                error.IOError => {
                    std.log.err("Compaction failed due to IO error: level {}", .{job.level});
                },
                else => {
                    std.log.err("Compaction failed with error: {} level {}", .{ err, job.level });
                },
            }
            return;
        };

        // Update global statistics
        self.updateStats(.compactions_total, 1);

        // Log successful compaction
        std.log.info("Compaction completed: level {} -> {} ({} bytes)", .{ job.level, job.target_level, job.estimated_size });
    }

    /// Enhanced writer worker with better batch processing and queue management
    fn enhancedWriterWorker(self: *Self) void {
        while (!self.close_signal.load(.acquire)) {
            var batch_processed = false;

            // Process batches with priority over individual writes
            if (self.batch_channel.tryReceive()) |maybe_batch| {
                if (maybe_batch) |batch| {
                    self.handleBatch(batch);
                    _ = self.worker_stats.writer_batches_processed.fetchAdd(1, .acq_rel);
                    batch_processed = true;
                }
            } else |_| {}

            // Process individual writes if no batch was available
            if (!batch_processed) {
                if (self.write_channel.receiveTimeout(1000000)) |req| { // 1ms timeout
                    self.handleWrite(req);
                } else |err| switch (err) {
                    ChannelError.ReceiveTimeout => {
                        // Update queue depth statistics
                        self.worker_stats.setWorkerQueueDepth(self.write_channel.size());
                        continue;
                    },
                    ChannelError.ChannelClosed => break,
                    else => continue,
                }
            }
        }
    }
};
