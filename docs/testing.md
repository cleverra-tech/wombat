# Testing Guide

This guide covers the comprehensive testing framework in Wombat, including unit tests, integration tests, recovery tests, and performance testing.

## Overview

Wombat includes a multi-layered testing strategy:

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test component interactions
- **Recovery Tests**: Test crash recovery and persistence
- **Performance Tests**: Validate performance characteristics
- **Benchmark Suite**: Regression testing framework

## Test Structure

```
tests/
├── unit/                    # Unit tests (embedded in source)
├── integration/            # Integration test files
├── recovery/              # Recovery and persistence tests
├── performance/           # Performance validation tests
└── benchmarks/            # Benchmark test framework
```

## Running Tests

### Basic Test Commands

```bash
# Run all tests
zig build test

# Run all test suites
zig build test-all

# Run specific test suite
zig build test-recovery

# Run benchmarks
zig build benchmark
```

### Test Options

```bash
# Run tests with coverage
zig build test -- --test-coverage

# Run tests with memory leak detection
zig build test -- --test-leak-check

# Run specific test by name
zig build test -- --test-filter "skiplist"

# Run tests with verbose output
zig build test -- --verbose

# Run tests with custom thread count
zig build test -- --test-thread-count=1
```

## Unit Tests

### Test Organization

Unit tests are embedded directly in source files using Zig's `test` blocks:

```zig
const std = @import("std");
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;

pub const SkipList = struct {
    // Implementation...
    
    test "skiplist basic operations" {
        var gpa = std.heap.GeneralPurposeAllocator(.{}){};
        defer _ = gpa.deinit();
        const allocator = gpa.allocator();
        
        var skiplist = try SkipList.init(allocator, 1024 * 1024);
        defer skiplist.deinit();
        
        // Test put operation
        const value = ValueStruct{
            .value = "test_value",
            .timestamp = 123,
            .meta = 0,
        };
        try skiplist.put("test_key", value);
        
        // Test get operation
        const result = skiplist.get("test_key");
        try expect(result != null);
        try expectEqual(result.?.timestamp, 123);
        
        // Test delete operation
        try skiplist.delete("test_key");
        const deleted_result = skiplist.get("test_key");
        try expect(deleted_result == null);
    }
};
```

### Test Categories

#### Data Structure Tests
```zig
test "arena allocator" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    var arena = try Arena.init(allocator, 64 * 1024);
    defer arena.deinit();
    
    // Test allocations
    const ptr1 = try arena.alloc(u8, 100);
    const ptr2 = try arena.alloc(u64, 10);
    
    // Verify allocations
    try expect(ptr1.len == 100);
    try expect(ptr2.len == 10);
    
    // Test reset
    arena.reset();
    const ptr3 = try arena.alloc(u8, 50);
    try expect(ptr3.len == 50);
}
```

#### Compression Tests
```zig
test "compression roundtrip" {
    const allocator = testing.allocator;
    
    const original_data = "Hello, World! This is test data for compression.";
    const compressor = Compressor.init(allocator, .zlib);
    
    // Compress data
    const compressed = try compressor.compressAlloc(original_data);
    defer allocator.free(compressed);
    
    // Decompress data
    const decompressed = try compressor.decompressAlloc(compressed, original_data.len);
    defer allocator.free(decompressed);
    
    // Verify roundtrip
    try expectEqual(original_data.len, decompressed.len);
    try expect(std.mem.eql(u8, original_data, decompressed));
}
```

#### Error Handling Tests
```zig
test "error handling" {
    const allocator = testing.allocator;
    
    // Test invalid options
    const invalid_options = Options{
        .dir = "",
        .mem_table_size = 0,
    };
    
    const result = DB.open(allocator, invalid_options);
    try expectError(error.InvalidOptions, result);
}
```

## Integration Tests

### Database Integration Tests

```zig
// tests/integration/db_integration_test.zig
const std = @import("std");
const testing = std.testing;
const wombat = @import("wombat");

test "database lifecycle" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const test_dir = "/tmp/wombat_integration_test";
    defer std.fs.cwd().deleteTree(test_dir) catch {};
    
    // Test database creation
    const options = wombat.Options.default(test_dir);
    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};
    
    // Test write operations
    try db.set("key1", "value1");
    try db.set("key2", "value2");
    
    // Test read operations
    const value1 = try db.get("key1");
    try testing.expect(value1 != null);
    try testing.expectEqualStrings("value1", value1.?);
    defer allocator.free(value1.?);
    
    // Test delete operations
    try db.delete("key1");
    const deleted_value = try db.get("key1");
    try testing.expect(deleted_value == null);
    
    // Test persistence
    try db.sync();
}
```

### Compaction Integration Tests

```zig
test "compaction integration" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const test_dir = "/tmp/wombat_compaction_test";
    defer std.fs.cwd().deleteTree(test_dir) catch {};
    
    // Configure for frequent compaction
    const options = wombat.Options.default(test_dir)
        .withMemTableSize(1024 * 1024)  // 1MB
        .withNumLevelZeroTables(2)       // Trigger compaction early
        .withNumCompactors(1);
    
    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};
    
    // Write enough data to trigger compaction
    for (0..10000) |i| {
        const key = try std.fmt.allocPrint(allocator, "key_{d:0>6}", .{i});
        defer allocator.free(key);
        
        const value = try std.fmt.allocPrint(allocator, "value_{d:0>10}", .{i});
        defer allocator.free(value);
        
        try db.set(key, value);
    }
    
    // Force compaction
    try db.runCompaction();
    
    // Verify data integrity after compaction
    for (0..10000) |i| {
        const key = try std.fmt.allocPrint(allocator, "key_{d:0>6}", .{i});
        defer allocator.free(key);
        
        const expected_value = try std.fmt.allocPrint(allocator, "value_{d:0>10}", .{i});
        defer allocator.free(expected_value);
        
        const actual_value = try db.get(key);
        try testing.expect(actual_value != null);
        try testing.expectEqualStrings(expected_value, actual_value.?);
        defer allocator.free(actual_value.?);
    }
}
```

## Recovery Tests

### Crash Recovery Tests

```zig
// tests/recovery/crash_recovery_test.zig
const std = @import("std");
const testing = std.testing;
const wombat = @import("wombat");

test "crash recovery with WAL" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const test_dir = "/tmp/wombat_crash_test";
    defer std.fs.cwd().deleteTree(test_dir) catch {};
    
    // Phase 1: Write data and simulate crash
    {
        const options = wombat.Options.default(test_dir)
            .withSyncWrites(true)
            .withRecoveryConfig(.{
                .enable_wal = true,
                .wal_sync_writes = true,
            });
        
        var db = try wombat.DB.open(allocator, options);
        
        // Write data
        try db.set("persistent_key", "persistent_value");
        try db.set("crash_key", "crash_value");
        
        // Simulate crash by not calling close()
        // In real crash, the database would be forcibly terminated
    }
    
    // Phase 2: Recovery and verification
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};
        
        // Verify data was recovered
        const value1 = try db.get("persistent_key");
        try testing.expect(value1 != null);
        try testing.expectEqualStrings("persistent_value", value1.?);
        defer allocator.free(value1.?);
        
        const value2 = try db.get("crash_key");
        try testing.expect(value2 != null);
        try testing.expectEqualStrings("crash_value", value2.?);
        defer allocator.free(value2.?);
    }
}
```

### Corruption Recovery Tests

```zig
test "corruption recovery" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const test_dir = "/tmp/wombat_corruption_test";
    defer std.fs.cwd().deleteTree(test_dir) catch {};
    
    // Create database with data
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};
        
        try db.set("key1", "value1");
        try db.set("key2", "value2");
        try db.sync();
    }
    
    // Simulate corruption by modifying files
    // (In practice, this would test checksum validation)
    
    // Attempt recovery
    {
        const options = wombat.Options.default(test_dir)
            .withRecoveryConfig(.{
                .verify_checksums = true,
                .repair_corrupted_data = true,
            });
        
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};
        
        // Verify what can be recovered
        const value1 = try db.get("key1");
        defer if (value1) |v| allocator.free(v);
        
        const value2 = try db.get("key2");
        defer if (value2) |v| allocator.free(v);
    }
}
```

## Performance Tests

### Baseline Performance Tests

```zig
test "performance baseline" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const test_dir = "/tmp/wombat_perf_test";
    defer std.fs.cwd().deleteTree(test_dir) catch {};
    
    const options = wombat.Options.highPerformance(test_dir);
    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};
    
    // Write performance test
    const write_start = std.time.nanoTimestamp();
    for (0..1000) |i| {
        const key = try std.fmt.allocPrint(allocator, "perf_key_{d}", .{i});
        defer allocator.free(key);
        
        const value = try std.fmt.allocPrint(allocator, "perf_value_{d}", .{i});
        defer allocator.free(value);
        
        try db.set(key, value);
    }
    const write_end = std.time.nanoTimestamp();
    
    const write_duration = write_end - write_start;
    const write_ops_per_sec = 1000.0 / (@as(f64, @floatFromInt(write_duration)) / 1e9);
    
    // Verify minimum performance
    try testing.expect(write_ops_per_sec > 1000.0); // 1K ops/sec minimum
    
    // Read performance test
    const read_start = std.time.nanoTimestamp();
    for (0..1000) |i| {
        const key = try std.fmt.allocPrint(allocator, "perf_key_{d}", .{i});
        defer allocator.free(key);
        
        const value = try db.get(key);
        defer if (value) |v| allocator.free(v);
    }
    const read_end = std.time.nanoTimestamp();
    
    const read_duration = read_end - read_start;
    const read_ops_per_sec = 1000.0 / (@as(f64, @floatFromInt(read_duration)) / 1e9);
    
    // Verify minimum performance
    try testing.expect(read_ops_per_sec > 5000.0); // 5K ops/sec minimum
}
```

### Memory Usage Tests

```zig
test "memory usage limits" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const test_dir = "/tmp/wombat_memory_test";
    defer std.fs.cwd().deleteTree(test_dir) catch {};
    
    const options = wombat.Options.memoryOptimized(test_dir);
    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};
    
    // Write data and monitor memory usage
    const initial_memory = gpa.total_requested_bytes;
    
    for (0..10000) |i| {
        const key = try std.fmt.allocPrint(allocator, "mem_key_{d}", .{i});
        defer allocator.free(key);
        
        const value = try std.fmt.allocPrint(allocator, "mem_value_{d}", .{i});
        defer allocator.free(value);
        
        try db.set(key, value);
    }
    
    const final_memory = gpa.total_requested_bytes;
    const memory_used = final_memory - initial_memory;
    
    // Verify memory usage is within expected bounds
    try testing.expect(memory_used < 100 * 1024 * 1024); // 100MB limit
}
```

## Benchmark Framework

### Running Benchmarks

```bash
# Run all benchmarks
zig build benchmark

# Check benchmark results
ls benchmark_results/
cat benchmark_results/arena_benchmark_history.csv
```

### Custom Benchmarks

```zig
// benchmarks/custom_benchmark.zig
const std = @import("std");
const wombat = @import("wombat");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    const test_dir = "/tmp/custom_benchmark";
    defer std.fs.cwd().deleteTree(test_dir) catch {};
    
    const options = wombat.Options.default(test_dir);
    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};
    
    // Custom benchmark implementation
    const start_time = std.time.nanoTimestamp();
    
    // Your workload here
    for (0..100000) |i| {
        const key = try std.fmt.allocPrint(allocator, "bench_key_{d}", .{i});
        defer allocator.free(key);
        
        const value = try std.fmt.allocPrint(allocator, "bench_value_{d}", .{i});
        defer allocator.free(value);
        
        try db.set(key, value);
    }
    
    const end_time = std.time.nanoTimestamp();
    const duration = end_time - start_time;
    const ops_per_sec = 100000.0 / (@as(f64, @floatFromInt(duration)) / 1e9);
    
    std.log.info("Custom benchmark: {d:.0} ops/sec", .{ops_per_sec});
}
```

## Test Configuration

### Test Environment Variables

```bash
# Set test timeout
export ZIG_TEST_TIMEOUT=60

# Set test thread count
export ZIG_TEST_THREADS=4

# Enable verbose output
export ZIG_TEST_VERBOSE=1
```

### Custom Test Allocator

```zig
test "with custom allocator" {
    var failing_allocator = std.testing.FailingAllocator.init(
        std.testing.allocator,
        .{ .fail_index = 10 }
    );
    const allocator = failing_allocator.allocator();
    
    // Test allocation failure handling
    const result = wombat.DB.open(allocator, options);
    try testing.expectError(error.OutOfMemory, result);
}
```

## Continuous Integration

### CI Test Configuration

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        zig-version: [0.15.0]
        
    steps:
    - uses: actions/checkout@v3
    
    - name: Setup Zig
      uses: goto-bus-stop/setup-zig@v2
      with:
        version: ${{ matrix.zig-version }}
    
    - name: Run tests
      run: |
        zig build test
        zig build test-recovery
        zig build benchmark
    
    - name: Upload benchmark results
      uses: actions/upload-artifact@v3
      with:
        name: benchmark-results
        path: benchmark_results/
```

## Test Best Practices

### Test Organization

1. **Unit tests** in source files for immediate feedback
2. **Integration tests** in separate files for complex scenarios
3. **Recovery tests** for durability verification
4. **Performance tests** for regression detection

### Test Data Management

```zig
// Use unique test directories
const test_dir = try std.fmt.allocPrint(
    allocator, 
    "/tmp/wombat_test_{d}", 
    .{std.time.milliTimestamp()}
);
defer allocator.free(test_dir);
defer std.fs.cwd().deleteTree(test_dir) catch {};
```

### Error Testing

```zig
// Test error conditions
test "error conditions" {
    const result = someFunction(invalid_input);
    try testing.expectError(error.InvalidInput, result);
}
```

### Resource Cleanup

```zig
// Always clean up resources
test "resource cleanup" {
    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {}; // Always close
    
    // Test implementation
}
```

## Debugging Tests

### Test Debugging

```bash
# Run single test with debugging
zig build test -- --test-filter "specific_test" --verbose

# Run with GDB
gdb --args zig build test -- --test-filter "specific_test"
```

### Test Profiling

```bash
# Profile test execution
perf record zig build test
perf report
```

This comprehensive testing guide covers all aspects of testing in Wombat, from basic unit tests to complex integration and performance testing scenarios.