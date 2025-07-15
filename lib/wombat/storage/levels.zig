const std = @import("std");
const atomic = std.atomic;
const ArrayList = std.ArrayList;
const Table = @import("table.zig").Table;
const TableBuilder = @import("table.zig").TableBuilder;
const ValueStruct = @import("../core/skiplist.zig").ValueStruct;
const MemTable = @import("memtable.zig").MemTable;
const Options = @import("../core/options.zig").Options;

const MAX_LEVELS = 7;

pub const LevelHandler = struct {
    tables: ArrayList(*Table),
    level: u32,
    total_size: atomic.Value(u64),

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, level: u32) Self {
        return Self{
            .tables = ArrayList(*Table).init(allocator),
            .level = level,
            .total_size = atomic.Value(u64).init(0),
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        for (self.tables.items) |table| {
            table.close(allocator);
            allocator.destroy(table);
        }
        self.tables.deinit();
    }

    pub fn addTable(self: *Self, table: *Table) !void {
        try self.tables.append(table);
        _ = self.total_size.fetchAdd(table.mmap_file.size, .acq_rel);
    }

    pub fn removeTable(self: *Self, table: *Table) void {
        for (self.tables.items, 0..) |t, i| {
            if (t == table) {
                _ = self.tables.swapRemove(i);
                _ = self.total_size.fetchSub(table.mmap_file.size, .acq_rel);
                break;
            }
        }
    }

    pub fn get(self: *Self, allocator: std.mem.Allocator, key: []const u8) !?ValueStruct {
        for (self.tables.items) |table| {
            if (table.overlapsWithKeyRange(key, key)) {
                if (try table.get(allocator, key)) |value| {
                    return value;
                }
            }
        }
        return null;
    }

    pub fn overlapsWithKeyRange(self: *const Self, start_key: []const u8, end_key: []const u8) bool {
        for (self.tables.items) |table| {
            if (table.overlapsWithKeyRange(start_key, end_key)) {
                return true;
            }
        }
        return false;
    }

    pub fn size(self: *const Self) u64 {
        return self.total_size.load(.acquire);
    }

    pub fn sortTables(self: *Self) void {
        std.mem.sort(*Table, self.tables.items, {}, tableCompare);
    }

    fn tableCompare(_: void, a: *Table, b: *Table) bool {
        return std.mem.order(u8, a.smallest_key, b.smallest_key) == .lt;
    }
};

pub const CompactionJob = struct {
    level: u32,
    input_tables: ArrayList(*Table),
    key_range: struct {
        start: []const u8,
        end: []const u8,
    },

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, level: u32) Self {
        return Self{
            .level = level,
            .input_tables = ArrayList(*Table).init(allocator),
            .key_range = .{
                .start = &[_]u8{},
                .end = &[_]u8{},
            },
        };
    }

    pub fn deinit(self: *Self) void {
        self.input_tables.deinit();
    }
};

pub const CompactionPicker = struct {
    levels: *[MAX_LEVELS]LevelHandler,
    options: *const Options,

    const Self = @This();

    pub fn init(levels: *[MAX_LEVELS]LevelHandler, options: *const Options) Self {
        return Self{
            .levels = levels,
            .options = options,
        };
    }

    pub fn pickCompaction(self: *Self, allocator: std.mem.Allocator) ?CompactionJob {
        for (0..MAX_LEVELS - 1) |level| {
            const level_size = self.levels[level].size();
            const max_size = self.options.tableSizeForLevel(@intCast(level));

            if (level_size > max_size) {
                var job = CompactionJob.init(allocator, @intCast(level));

                const table = self.levels[level].tables.items[0];
                job.input_tables.append(table) catch return null;
                job.key_range.start = table.smallest_key;
                job.key_range.end = table.biggest_key;

                for (self.levels[level + 1].tables.items) |next_table| {
                    if (next_table.overlapsWithKeyRange(job.key_range.start, job.key_range.end)) {
                        job.input_tables.append(next_table) catch continue;
                    }
                }

                return job;
            }
        }

        return null;
    }
};

pub const LevelsController = struct {
    levels: [MAX_LEVELS]LevelHandler,
    compaction_picker: CompactionPicker,
    next_file_id: atomic.Value(u64),
    options: *const Options,
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, options: *const Options) Self {
        var levels: [MAX_LEVELS]LevelHandler = undefined;

        for (0..MAX_LEVELS) |i| {
            levels[i] = LevelHandler.init(allocator, @intCast(i));
        }

        const compaction_picker = CompactionPicker.init(&levels, options);

        return Self{
            .levels = levels,
            .compaction_picker = compaction_picker,
            .next_file_id = atomic.Value(u64).init(1),
            .options = options,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        for (&self.levels) |*level| {
            level.deinit(self.allocator);
        }
    }

    pub fn get(self: *Self, key: []const u8) !?ValueStruct {
        for (&self.levels) |*level| {
            if (try level.get(self.allocator, key)) |value| {
                return value;
            }
        }
        return null;
    }

    pub fn addLevel0Table(self: *Self, table: *Table) !void {
        try self.levels[0].addTable(table);
        self.levels[0].sortTables();
    }

    pub fn flushMemTable(self: *Self, memtable: *MemTable) !void {
        const file_id = self.next_file_id.fetchAdd(1, .acq_rel);

        var path_buffer: [256]u8 = undefined;
        const table_path = try std.fmt.bufPrint(path_buffer[0..], "{s}/{d}.sst", .{ self.options.dir, file_id });

        var builder = try TableBuilder.init(self.allocator, table_path, self.options.*, 0);
        defer builder.deinit(self.allocator);

        var iter = memtable.iterator();
        while (iter.next()) |entry| {
            try builder.add(entry.key, entry.value);
        }

        try builder.finish(self.allocator);

        const table = try self.allocator.create(Table);
        table.* = try Table.open(self.allocator, table_path, file_id);

        try self.addLevel0Table(table);
    }

    pub fn compactLevel(self: *Self, level: u32) !void {
        if (level >= MAX_LEVELS - 1) return;

        var job = self.compaction_picker.pickCompaction(self.allocator) orelse return;
        defer job.deinit();

        if (job.level != level) return;

        const output_level = level + 1;
        const file_id = self.next_file_id.fetchAdd(1, .acq_rel);

        var path_buffer: [256]u8 = undefined;
        const output_path = try std.fmt.bufPrint(path_buffer[0..], "{s}/{d}.sst", .{ self.options.dir, file_id });

        var builder = try TableBuilder.init(self.allocator, output_path, self.options.*, output_level);
        defer builder.deinit(self.allocator);

        var iterators = ArrayList(@import("table.zig").TableIterator).init(self.allocator);
        defer {
            for (iterators.items) |*iter| {
                iter.deinit();
            }
            iterators.deinit();
        }

        for (job.input_tables.items) |table| {
            const iter = table.iterator(self.allocator);
            try iterators.append(iter);
        }

        var entries = ArrayList(struct { key: []const u8, value: ValueStruct, iter_index: usize }).init(self.allocator);
        defer entries.deinit();

        for (iterators.items, 0..) |*iter, i| {
            if (try iter.next()) |entry| {
                try entries.append(.{
                    .key = entry.key,
                    .value = entry.value,
                    .iter_index = i,
                });
            }
        }

        while (entries.items.len > 0) {
            var min_index: usize = 0;
            for (entries.items, 0..) |entry, i| {
                if (std.mem.order(u8, entry.key, entries.items[min_index].key) == .lt) {
                    min_index = i;
                }
            }

            const min_entry = entries.swapRemove(min_index);
            try builder.add(min_entry.key, min_entry.value);

            if (try iterators.items[min_entry.iter_index].next()) |next_entry| {
                try entries.append(.{
                    .key = next_entry.key,
                    .value = next_entry.value,
                    .iter_index = min_entry.iter_index,
                });
            }
        }

        try builder.finish(self.allocator);

        for (job.input_tables.items) |table| {
            self.levels[job.level].removeTable(table);
            if (job.level < MAX_LEVELS - 1) {
                for (&self.levels[job.level + 1].tables.items) |next_table| {
                    if (next_table == table) {
                        self.levels[job.level + 1].removeTable(table);
                        break;
                    }
                }
            }

            table.close(self.allocator);
            self.allocator.destroy(table);
        }

        const new_table = try self.allocator.create(Table);
        new_table.* = try Table.open(self.allocator, output_path, file_id);

        try self.levels[output_level].addTable(new_table);
        self.levels[output_level].sortTables();
    }

    pub fn pickCompaction(self: *Self) ?CompactionJob {
        return self.compaction_picker.pickCompaction(self.allocator);
    }

    pub fn needsCompaction(self: *const Self) bool {
        for (0..MAX_LEVELS - 1) |level| {
            const level_size = self.levels[level].size();
            const max_size = self.options.tableSizeForLevel(@intCast(level));

            if (level_size > max_size) {
                return true;
            }
        }

        return false;
    }
};
