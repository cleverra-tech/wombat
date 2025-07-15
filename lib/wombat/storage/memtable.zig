const std = @import("std");
const SkipList = @import("../core/skiplist.zig").SkipList;
const ValueStruct = @import("../core/skiplist.zig").ValueStruct;
const WriteAheadLog = @import("wal.zig").WriteAheadLog;
const Entry = @import("wal.zig").Entry;

pub const MemTable = struct {
    skiplist: SkipList,
    wal: WriteAheadLog,
    max_version: u64,
    size_bytes: std.atomic.Value(usize),
    max_size: usize,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8, max_size: usize) !Self {
        const arena_size = max_size * 2;
        const skiplist = try SkipList.init(allocator, arena_size);
        const wal = try WriteAheadLog.create(wal_path, max_size);

        return Self{
            .skiplist = skiplist,
            .wal = wal,
            .max_version = 0,
            .size_bytes = std.atomic.Value(usize).init(0),
            .max_size = max_size,
        };
    }

    pub fn open(allocator: std.mem.Allocator, wal_path: []const u8, max_size: usize) !Self {
        const arena_size = max_size * 2;
        const skiplist = try SkipList.init(allocator, arena_size);
        const wal = try WriteAheadLog.open(wal_path);

        var memtable = Self{
            .skiplist = skiplist,
            .wal = wal,
            .max_version = 0,
            .size_bytes = std.atomic.Value(usize).init(0),
            .max_size = max_size,
        };

        try memtable.recoverFromWAL(allocator);

        return memtable;
    }

    pub fn deinit(self: *Self) void {
        self.skiplist.deinit();
        self.wal.close();
    }

    fn recoverFromWAL(self: *Self, allocator: std.mem.Allocator) !void {
        const Context = struct {
            memtable: *Self,

            pub fn callback(ctx: *@This(), entry: Entry) !void {
                const value_struct = ValueStruct{
                    .value = entry.value,
                    .timestamp = entry.timestamp,
                    .meta = entry.meta,
                };

                try ctx.memtable.skiplist.put(entry.key, value_struct);
                ctx.memtable.updateSize(entry.key.len + entry.value.len);

                if (entry.timestamp > ctx.memtable.max_version) {
                    ctx.memtable.max_version = entry.timestamp;
                }
            }
        };

        var context = Context{ .memtable = self };
        try self.wal.replay(allocator, &context, Context.callback);
    }

    pub fn put(self: *Self, key: []const u8, value: ValueStruct) !void {
        const entry = Entry{
            .key = key,
            .value = value.value,
            .timestamp = value.timestamp,
            .meta = value.meta,
        };

        try self.wal.writeEntry(entry);
        try self.skiplist.put(key, value);

        self.updateSize(key.len + value.value.len);

        if (value.timestamp > self.max_version) {
            self.max_version = value.timestamp;
        }
    }

    pub fn get(self: *Self, key: []const u8) ?ValueStruct {
        return self.skiplist.get(key);
    }

    pub fn isFull(self: *Self) bool {
        return self.size_bytes.load(.acquire) >= self.max_size;
    }

    pub fn sync(self: *Self) !void {
        try self.wal.sync();
    }

    pub fn iterator(self: *Self) SkipList.SkipListIterator {
        return self.skiplist.iterator();
    }

    pub fn size(self: *Self) usize {
        return self.size_bytes.load(.acquire);
    }

    fn updateSize(self: *Self, delta: usize) void {
        _ = self.size_bytes.fetchAdd(delta, .acq_rel);
    }

    pub fn estimateSize(key: []const u8, value: []const u8) usize {
        return key.len + value.len + @sizeOf(ValueStruct) + 64;
    }
};
