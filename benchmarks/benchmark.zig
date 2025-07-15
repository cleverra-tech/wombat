const std = @import("std");
const wombat = @import("wombat");

const BenchResult = struct {
    operations: u64,
    duration_ns: u64,
    ops_per_sec: f64,
};

fn benchmark(comptime name: []const u8, comptime operation: fn (allocator: std.mem.Allocator, i: u64) anyerror!void, allocator: std.mem.Allocator, count: u64) !BenchResult {
    std.log.info("Starting benchmark: {s}", .{name});

    const start_time = std.time.nanoTimestamp();

    for (0..count) |i| {
        try operation(allocator, i);
    }

    const end_time = std.time.nanoTimestamp();
    const duration = @as(u64, @intCast(end_time - start_time));
    const ops_per_sec = @as(f64, @floatFromInt(count)) / (@as(f64, @floatFromInt(duration)) / 1_000_000_000.0);

    const result = BenchResult{
        .operations = count,
        .duration_ns = duration,
        .ops_per_sec = ops_per_sec,
    };

    std.log.info("Benchmark {s}: {} ops in {d}ns ({d:.2} ops/sec)", .{ name, result.operations, result.duration_ns, result.ops_per_sec });

    return result;
}

var skiplist_global: ?wombat.SkipList = null;

fn benchmarkSkipListPut(allocator: std.mem.Allocator, i: u64) !void {
    _ = allocator;
    var key_buf: [32]u8 = undefined;
    var value_buf: [64]u8 = undefined;

    const key = try std.fmt.bufPrint(key_buf[0..], "key_{d:0>8}", .{i});
    const value_data = try std.fmt.bufPrint(value_buf[0..], "value_{d:0>16}", .{i});

    const value_struct = wombat.ValueStruct{
        .value = value_data,
        .timestamp = i,
        .meta = 0,
    };

    if (skiplist_global) |*sl| {
        try sl.put(key, value_struct);
    }
}

fn benchmarkSkipListGet(allocator: std.mem.Allocator, i: u64) !void {
    _ = allocator;
    var key_buf: [32]u8 = undefined;
    const key = try std.fmt.bufPrint(key_buf[0..], "key_{d:0>8}", .{i});

    if (skiplist_global) |*sl| {
        _ = sl.get(key);
    }
}

fn benchmarkArenaAlloc(allocator: std.mem.Allocator, i: u64) !void {
    _ = allocator;
    _ = i;
    if (arena_global) |*arena| {
        _ = try arena.alloc(u8, 64);
    }
}

var arena_global: ?wombat.Arena = null;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.log.info("Wombat LSM-Tree Component Benchmarks", .{});

    // Arena benchmark
    std.log.info("Setting up Arena benchmark...", .{});
    arena_global = try wombat.Arena.init(allocator, 64 * 1024 * 1024);
    defer if (arena_global) |*arena| arena.deinit();

    _ = try benchmark("Arena Allocations", benchmarkArenaAlloc, allocator, 100000);

    // SkipList benchmark
    std.log.info("Setting up SkipList benchmark...", .{});
    skiplist_global = try wombat.SkipList.init(allocator, 64 * 1024 * 1024);
    defer if (skiplist_global) |*sl| sl.deinit();

    const write_count = 10000;
    const read_count = 50000;

    _ = try benchmark("SkipList Writes", benchmarkSkipListPut, allocator, write_count);
    _ = try benchmark("SkipList Reads", benchmarkSkipListGet, allocator, read_count);

    std.log.info("All benchmarks completed", .{});
}
