const std = @import("std");
const wombat = @import("wombat");
const print = std.debug.print;

const BenchResult = struct {
    operations: u64,
    duration_ns: u64,
    ops_per_sec: f64,
    memory_used: usize,
    memory_peak: usize,
};

const RegressionThreshold = struct {
    ops_per_sec_min: f64,
    memory_max: usize,
    duration_max_ns: u64,
};

const BenchmarkSuite = struct {
    name: []const u8,
    operation: *const fn (allocator: std.mem.Allocator, i: u64) anyerror!void,
    count: u64,
    threshold: RegressionThreshold,
    setup: ?*const fn (allocator: std.mem.Allocator) anyerror!void,
    cleanup: ?*const fn (allocator: std.mem.Allocator) anyerror!void,
};

const HistoricalResult = struct {
    timestamp: u64,
    result: BenchResult,
};

const BenchmarkHistory = struct {
    results: std.ArrayList(HistoricalResult),
    allocator: std.mem.Allocator,

    fn init(allocator: std.mem.Allocator) BenchmarkHistory {
        return BenchmarkHistory{
            .results = std.ArrayList(HistoricalResult).init(allocator),
            .allocator = allocator,
        };
    }

    fn deinit(self: *BenchmarkHistory) void {
        self.results.deinit();
    }

    fn addResult(self: *BenchmarkHistory, result: BenchResult) !void {
        const timestamp = @as(u64, @intCast(std.time.milliTimestamp()));
        try self.results.append(HistoricalResult{
            .timestamp = timestamp,
            .result = result,
        });
    }

    fn getRegression(self: *const BenchmarkHistory) ?f64 {
        if (self.results.items.len < 2) return null;

        const latest = self.results.items[self.results.items.len - 1];
        const previous = self.results.items[self.results.items.len - 2];

        const performance_change = (latest.result.ops_per_sec - previous.result.ops_per_sec) / previous.result.ops_per_sec;
        return performance_change;
    }

    fn saveToFile(self: *const BenchmarkHistory, filename: []const u8) !void {
        const file = try std.fs.cwd().createFile(filename, .{});
        defer file.close();

        var buffer = std.ArrayList(u8).init(self.allocator);
        defer buffer.deinit();

        const writer = buffer.writer();
        try writer.print("timestamp,operations,duration_ns,ops_per_sec,memory_used,memory_peak\n", .{});

        for (self.results.items) |entry| {
            try writer.print("{d},{d},{d},{d:.2},{d},{d}\n", .{
                entry.timestamp,
                entry.result.operations,
                entry.result.duration_ns,
                entry.result.ops_per_sec,
                entry.result.memory_used,
                entry.result.memory_peak,
            });
        }

        try file.writeAll(buffer.items);
    }
};

fn benchmark(name: []const u8, operation: *const fn (allocator: std.mem.Allocator, i: u64) anyerror!void, bench_allocator: std.mem.Allocator, count: u64) !BenchResult {
    std.log.info("Starting benchmark: {s}", .{name});

    // Memory tracking
    var memory_tracker = std.heap.GeneralPurposeAllocator(.{ .enable_memory_limit = true }){};
    defer _ = memory_tracker.deinit();
    const tracking_allocator = memory_tracker.allocator();

    const start_memory = memory_tracker.total_requested_bytes;
    const start_time = std.time.nanoTimestamp();

    for (0..count) |i| {
        try operation(tracking_allocator, i);
    }

    _ = bench_allocator; // Mark as used for API compatibility

    const end_time = std.time.nanoTimestamp();
    const end_memory = memory_tracker.total_requested_bytes;
    const peak_memory = memory_tracker.total_requested_bytes;

    const duration = @as(u64, @intCast(end_time - start_time));
    const ops_per_sec = @as(f64, @floatFromInt(count)) / (@as(f64, @floatFromInt(duration)) / 1_000_000_000.0);

    const result = BenchResult{
        .operations = count,
        .duration_ns = duration,
        .ops_per_sec = ops_per_sec,
        .memory_used = end_memory - start_memory,
        .memory_peak = peak_memory,
    };

    std.log.info("Benchmark {s}: {} ops in {d}ns ({d:.2} ops/sec) Memory: {d}KB peak", .{ name, result.operations, result.duration_ns, result.ops_per_sec, result.memory_peak / 1024 });

    return result;
}

fn runBenchmarkSuite(suite: BenchmarkSuite, allocator: std.mem.Allocator, history: *BenchmarkHistory) !bool {
    print("\n=== Running {s} Benchmark Suite ===\n", .{suite.name});

    // Setup if needed
    if (suite.setup) |setup_fn| {
        try setup_fn(allocator);
    }
    defer {
        if (suite.cleanup) |cleanup_fn| {
            cleanup_fn(allocator) catch {};
        }
    }

    // Run benchmark
    const result = try benchmark(suite.name, suite.operation, allocator, suite.count);

    // Add to history
    try history.addResult(result);

    // Check for regression
    const passed = checkRegressionThreshold(result, suite.threshold);

    if (history.getRegression()) |regression| {
        const regression_pct = regression * 100.0;
        if (regression < -0.05) { // 5% regression threshold
            print("WARNING: REGRESSION DETECTED: {d:.2}% performance decrease\n", .{-regression_pct});
        } else if (regression > 0.05) {
            print("INFO: IMPROVEMENT: {d:.2}% performance increase\n", .{regression_pct});
        }
    }

    return passed;
}

fn checkRegressionThreshold(result: BenchResult, threshold: RegressionThreshold) bool {
    var passed = true;

    if (result.ops_per_sec < threshold.ops_per_sec_min) {
        print("FAILED: Performance below threshold ({d:.2} < {d:.2} ops/sec)\n", .{ result.ops_per_sec, threshold.ops_per_sec_min });
        passed = false;
    } else {
        print("PASSED: Performance threshold ({d:.2} >= {d:.2} ops/sec)\n", .{ result.ops_per_sec, threshold.ops_per_sec_min });
    }

    if (result.memory_peak > threshold.memory_max) {
        print("FAILED: Memory usage above threshold ({d} > {d} KB)\n", .{ result.memory_peak / 1024, threshold.memory_max / 1024 });
        passed = false;
    } else {
        print("PASSED: Memory threshold ({d} <= {d} KB)\n", .{ result.memory_peak / 1024, threshold.memory_max / 1024 });
    }

    if (result.duration_ns > threshold.duration_max_ns) {
        print("FAILED: Duration above threshold ({d} > {d} ns)\n", .{ result.duration_ns, threshold.duration_max_ns });
        passed = false;
    } else {
        print("PASSED: Duration threshold ({d} <= {d} ns)\n", .{ result.duration_ns, threshold.duration_max_ns });
    }

    return passed;
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
    if (arena_global) |*arena| {
        const size = 64 + (i % 128); // Variable allocation sizes
        _ = try arena.alloc(u8, size);
    }
}

var arena_global: ?wombat.Arena = null;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    print("Wombat LSM-Tree Performance Benchmark Suite\n", .{});
    print("===============================================\n", .{});

    // Initialize benchmark history
    var arena_history = BenchmarkHistory.init(allocator);
    defer arena_history.deinit();

    var skiplist_write_history = BenchmarkHistory.init(allocator);
    defer skiplist_write_history.deinit();

    var skiplist_read_history = BenchmarkHistory.init(allocator);
    defer skiplist_read_history.deinit();

    // Define benchmark suites with regression thresholds
    const suites = [_]BenchmarkSuite{
        BenchmarkSuite{
            .name = "Arena Allocations",
            .operation = benchmarkArenaAlloc,
            .count = 100000,
            .threshold = RegressionThreshold{
                .ops_per_sec_min = 500000.0, // 500K ops/sec minimum
                .memory_max = 128 * 1024 * 1024, // 128MB max
                .duration_max_ns = 1000000000, // 1 second max
            },
            .setup = setupArena,
            .cleanup = cleanupArena,
        },
        BenchmarkSuite{
            .name = "SkipList Writes",
            .operation = benchmarkSkipListPut,
            .count = 10000,
            .threshold = RegressionThreshold{
                .ops_per_sec_min = 50000.0, // 50K ops/sec minimum
                .memory_max = 128 * 1024 * 1024, // 128MB max
                .duration_max_ns = 1000000000, // 1 second max
            },
            .setup = setupSkipList,
            .cleanup = cleanupSkipList,
        },
        BenchmarkSuite{
            .name = "SkipList Reads",
            .operation = benchmarkSkipListGet,
            .count = 50000,
            .threshold = RegressionThreshold{
                .ops_per_sec_min = 200000.0, // 200K ops/sec minimum
                .memory_max = 128 * 1024 * 1024, // 128MB max
                .duration_max_ns = 1000000000, // 1 second max
            },
            .setup = setupSkipList,
            .cleanup = cleanupSkipList,
        },
    };

    const histories = [_]*BenchmarkHistory{ &arena_history, &skiplist_write_history, &skiplist_read_history };

    var all_passed = true;
    for (suites, 0..) |suite, i| {
        const passed = try runBenchmarkSuite(suite, allocator, histories[i]);
        all_passed = all_passed and passed;
    }

    // Ensure results directory exists
    std.fs.cwd().makeDir("benchmark_results") catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    // Save results to CSV files
    try arena_history.saveToFile("benchmark_results/arena_benchmark_history.csv");
    try skiplist_write_history.saveToFile("benchmark_results/skiplist_write_history.csv");
    try skiplist_read_history.saveToFile("benchmark_results/skiplist_read_history.csv");

    // Final summary
    print("\n=== Benchmark Summary ===\n", .{});
    if (all_passed) {
        print("ALL BENCHMARKS PASSED\n", .{});
    } else {
        print("SOME BENCHMARKS FAILED\n", .{});
        std.process.exit(1);
    }

    print("Results saved to benchmark_results/ directory\n", .{});
    print("Run with --compare to see performance trends\n", .{});
}

fn setupArena(allocator: std.mem.Allocator) !void {
    arena_global = try wombat.Arena.init(allocator, 64 * 1024 * 1024);
}

fn cleanupArena(allocator: std.mem.Allocator) !void {
    _ = allocator;
    if (arena_global) |*arena| arena.deinit();
}

fn setupSkipList(allocator: std.mem.Allocator) !void {
    skiplist_global = try wombat.SkipList.init(allocator, 64 * 1024 * 1024);
}

fn cleanupSkipList(allocator: std.mem.Allocator) !void {
    _ = allocator;
    if (skiplist_global) |*sl| sl.deinit();
}
