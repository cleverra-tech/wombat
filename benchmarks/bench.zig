const std = @import("std");
const wombat = @import("wombat");

const BenchResult = struct {
    operations: u64,
    duration_ns: u64,
    ops_per_sec: f64,
};

fn benchmark(comptime name: []const u8, comptime operation: fn (db: *wombat.DB, i: u64) anyerror!void, db: *wombat.DB, count: u64) !BenchResult {
    std.log.info("Starting benchmark: {s}", .{name});

    const start_time = std.time.nanoTimestamp();

    for (0..count) |i| {
        try operation(db, i);
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

fn benchmarkSet(db: *wombat.DB, i: u64) !void {
    var key_buf: [32]u8 = undefined;
    var value_buf: [64]u8 = undefined;

    const key = try std.fmt.bufPrint(key_buf[0..], "key_{d:0>8}", .{i});
    const value = try std.fmt.bufPrint(value_buf[0..], "value_{d:0>16}_test_data", .{i});

    try db.set(key, value);
}

fn benchmarkGet(db: *wombat.DB, i: u64) !void {
    var key_buf: [32]u8 = undefined;
    const key = try std.fmt.bufPrint(key_buf[0..], "key_{d:0>8}", .{i});

    _ = try db.get(key);
}

fn benchmarkMixed(db: *wombat.DB, i: u64) !void {
    if (i % 10 == 0) {
        try benchmarkSet(db, i);
    } else {
        try benchmarkGet(db, i % 1000);
    }
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.log.info("Wombat LSM-Tree Database Benchmarks", .{});

    const options = wombat.Options.default("./wombat_bench_data")
        .withMemTableSize(64 * 1024 * 1024)
        .withSyncWrites(false);

    const db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};

    const write_count = 10000;
    const read_count = 50000;
    const mixed_count = 100000;

    _ = try benchmark("Sequential Writes", benchmarkSet, db, write_count);

    try db.sync();

    _ = try benchmark("Random Reads", benchmarkGet, db, read_count);

    _ = try benchmark("Mixed Workload (90% reads, 10% writes)", benchmarkMixed, db, mixed_count);

    std.log.info("All benchmarks completed", .{});
}
