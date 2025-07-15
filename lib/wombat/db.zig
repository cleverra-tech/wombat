const std = @import("std");
const ArrayList = std.ArrayList;
const Channel = @import("core/channel.zig").Channel;
const MemTable = @import("storage/memtable.zig").MemTable;
const LevelsController = @import("storage/levels.zig").LevelsController;
const Oracle = @import("transaction/oracle.zig").Oracle;
const Transaction = @import("transaction/oracle.zig").Transaction;
const ValueStruct = @import("core/skiplist.zig").ValueStruct;
const Options = @import("core/options.zig").Options;

pub const WriteRequest = struct {
    key: []const u8,
    value: []const u8,
    callback: *const fn (err: ?anyerror) void,
};

pub const DB = struct {
    options: Options,
    levels: LevelsController,
    memtable: *MemTable,
    immutable_tables: ArrayList(*MemTable),
    oracle: Oracle,
    write_channel: Channel(WriteRequest),
    allocator: std.mem.Allocator,
    next_memtable_id: std.atomic.Value(u64),
    close_signal: std.atomic.Value(bool),
    compaction_thread: ?std.Thread,
    writer_thread: ?std.Thread,

    const Self = @This();

    pub fn open(allocator: std.mem.Allocator, options: Options) !*Self {
        // Basic runtime validation
        if (options.mem_table_size == 0) return error.InvalidOptions;
        if (options.max_levels == 0) return error.InvalidOptions;
        if (options.block_size == 0) return error.InvalidOptions;

        try std.fs.cwd().makePath(options.dir);
        if (!std.mem.eql(u8, options.dir, options.value_dir)) {
            try std.fs.cwd().makePath(options.value_dir);
        }

        const db = try allocator.create(Self);

        db.* = Self{
            .options = options,
            .levels = LevelsController.init(allocator, &options),
            .memtable = undefined,
            .immutable_tables = ArrayList(*MemTable).init(allocator),
            .oracle = Oracle.init(allocator),
            .write_channel = Channel(WriteRequest).init(allocator, 1000),
            .allocator = allocator,
            .next_memtable_id = std.atomic.Value(u64).init(1),
            .close_signal = std.atomic.Value(bool).init(false),
            .compaction_thread = null,
            .writer_thread = null,
        };

        db.memtable = try db.createMemTable();

        db.writer_thread = try std.Thread.spawn(.{}, writerWorker, .{db});
        db.compaction_thread = try std.Thread.spawn(.{}, compactionWorker, .{db});

        return db;
    }

    pub fn close(self: *Self) !void {
        self.close_signal.store(true, .release);

        if (self.writer_thread) |thread| {
            thread.join();
        }

        if (self.compaction_thread) |thread| {
            thread.join();
        }

        try self.sync();

        self.memtable.deinit();
        self.allocator.destroy(self.memtable);

        for (self.immutable_tables.items) |memtable| {
            memtable.deinit();
            self.allocator.destroy(memtable);
        }
        self.immutable_tables.deinit();

        self.levels.deinit();
        self.oracle.deinit();
        self.write_channel.deinit();

        self.allocator.destroy(self);
    }

    pub fn get(self: *Self, key: []const u8) !?[]const u8 {
        if (self.memtable.get(key)) |value| {
            if (value.isDeleted()) {
                return null;
            }
            return value.value;
        }

        for (self.immutable_tables.items) |memtable| {
            if (memtable.get(key)) |value| {
                if (value.isDeleted()) {
                    return null;
                }
                return value.value;
            }
        }

        if (try self.levels.get(key)) |value| {
            if (value.isDeleted()) {
                return null;
            }
            return value.value;
        }

        return null;
    }

    pub fn set(self: *Self, key: []const u8, value: []const u8) !void {
        const write_req = WriteRequest{
            .key = key,
            .value = value,
            .callback = &struct {
                fn callback(err: ?anyerror) void {
                    _ = err;
                }
            }.callback,
        };

        try self.write_channel.send(write_req);
    }

    pub fn delete(self: *Self, key: []const u8) !void {
        const write_req = WriteRequest{
            .key = key,
            .value = &[_]u8{},
            .callback = &struct {
                fn callback(err: ?anyerror) void {
                    _ = err;
                }
            }.callback,
        };

        try self.write_channel.send(write_req);
    }

    pub fn sync(self: *Self) !void {
        try self.memtable.sync();

        for (self.immutable_tables.items) |memtable| {
            try memtable.sync();
        }
    }

    pub fn newTransaction(self: *Self) Transaction {
        const read_ts = self.oracle.newReadTs();
        return Transaction.init(self.allocator, read_ts, self);
    }

    fn createMemTable(self: *Self) !*MemTable {
        const memtable_id = self.next_memtable_id.fetchAdd(1, .acq_rel);

        var path_buffer: [256]u8 = undefined;
        const wal_path = try std.fmt.bufPrint(path_buffer[0..], "{s}/wal_{d}.log", .{ self.options.dir, memtable_id });

        const memtable = try self.allocator.create(MemTable);
        memtable.* = try MemTable.init(self.allocator, wal_path, self.options.mem_table_size);

        return memtable;
    }

    fn handleWrite(self: *Self, req: WriteRequest) void {
        const timestamp = self.oracle.newReadTs();

        const value_struct = if (req.value.len == 0) blk: {
            var vs = ValueStruct{
                .value = req.value,
                .timestamp = timestamp,
                .meta = 0,
            };
            vs.setDeleted();
            break :blk vs;
        } else ValueStruct{
            .value = req.value,
            .timestamp = timestamp,
            .meta = 0,
        };

        if (self.memtable.put(req.key, value_struct)) {
            if (self.memtable.isFull()) {
                self.rotateMemTable() catch |err| {
                    req.callback(err);
                    return;
                };
            }
            req.callback(null);
        } else |err| {
            req.callback(err);
        }
    }

    fn rotateMemTable(self: *Self) !void {
        try self.immutable_tables.append(self.memtable);
        self.memtable = try self.createMemTable();

        if (self.immutable_tables.items.len > self.options.num_memtables) {
            const old_memtable = self.immutable_tables.orderedRemove(0);
            try self.levels.flushMemTable(old_memtable);

            old_memtable.deinit();
            self.allocator.destroy(old_memtable);
        }
    }

    fn writerWorker(self: *Self) void {
        while (!self.close_signal.load(.acquire)) {
            if (self.write_channel.receive()) |req| {
                self.handleWrite(req);
            } else |_| {
                std.time.sleep(1000000);
            }
        }
    }

    fn compactionWorker(self: *Self) void {
        while (!self.close_signal.load(.acquire)) {
            if (self.levels.needsCompaction()) {
                for (0..self.options.max_levels - 1) |level| {
                    self.levels.compactLevel(@intCast(level)) catch continue;
                }
            }

            std.time.sleep(100000000);
        }
    }
};
