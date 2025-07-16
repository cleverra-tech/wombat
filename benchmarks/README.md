# Wombat LSM-Tree Benchmark Suite

A comprehensive performance benchmarking and regression testing framework for the Wombat LSM-Tree database.

## Features

- **Performance Benchmarking**: Measure operations per second, memory usage, and execution time
- **Regression Testing**: Automatically detect performance regressions with configurable thresholds
- **Historical Tracking**: Save benchmark results to CSV files for trend analysis
- **Memory Profiling**: Track memory usage and peak memory consumption
- **Automated Thresholds**: Fail builds when performance drops below acceptable levels

## Usage

### Basic Benchmarking

```bash
# Run all benchmarks
zig run benchmarks/benchmark.zig

# Build and run optimized benchmarks
zig build-exe benchmarks/benchmark.zig -O ReleaseFast
./benchmark
```

### Benchmark Suites

The framework includes the following benchmark suites:

1. **Arena Allocations**: Tests memory arena allocation performance
   - Minimum: 500K ops/sec
   - Memory limit: 128MB
   - Duration limit: 1 second

2. **SkipList Writes**: Tests skiplist insertion performance
   - Minimum: 50K ops/sec
   - Memory limit: 128MB
   - Duration limit: 1 second

3. **SkipList Reads**: Tests skiplist read performance
   - Minimum: 200K ops/sec
   - Memory limit: 128MB
   - Duration limit: 1 second

### Output Files

The benchmark suite generates CSV files for historical analysis:

- `benchmark_results/arena_benchmark_history.csv`: Arena allocation benchmark results
- `benchmark_results/skiplist_write_history.csv`: SkipList write benchmark results
- `benchmark_results/skiplist_read_history.csv`: SkipList read benchmark results

### CSV Format

```csv
timestamp,operations,duration_ns,ops_per_sec,memory_used,memory_peak
1642678800000,100000,200000000,500000.00,1048576,2097152
```

## Regression Detection

The framework automatically detects performance regressions by:

1. **Threshold Checking**: Each benchmark has minimum performance requirements
2. **Historical Comparison**: Compares current run to previous results
3. **Regression Alerts**: Warns when performance drops by >5%
4. **Improvement Detection**: Celebrates when performance improves by >5%

## Integration with CI/CD

The benchmark suite is designed to integrate with continuous integration:

```bash
# In your CI pipeline
zig run benchmarks/benchmark.zig || exit 1
```

The program exits with code 1 if any benchmark fails its regression threshold.

## Extending the Framework

### Adding New Benchmarks

```zig
// Add to the suites array in main()
BenchmarkSuite{
    .name = "Your Benchmark",
    .operation = yourBenchmarkFunction,
    .count = 10000,
    .threshold = RegressionThreshold{
        .ops_per_sec_min = 100000.0,
        .memory_max = 64 * 1024 * 1024,
        .duration_max_ns = 500000000,
    },
    .setup = optionalSetupFunction,
    .cleanup = optionalCleanupFunction,
},
```

### Benchmark Function Signature

```zig
fn yourBenchmarkFunction(allocator: std.mem.Allocator, i: u64) !void {
    // Your benchmark code here
    // Parameter 'i' is the iteration number
}
```

## Performance Monitoring

The framework tracks:

- **Operations per second**: Primary performance metric
- **Memory usage**: Total memory allocated during benchmark
- **Peak memory**: Maximum memory usage during benchmark
- **Duration**: Total execution time in nanoseconds

## Best Practices

1. **Consistent Environment**: Run benchmarks in consistent environments
2. **Warm-up**: Consider adding warm-up iterations for more stable results
3. **Multiple Runs**: Run benchmarks multiple times and take averages
4. **Baseline Establishment**: Establish performance baselines before making changes
5. **Threshold Tuning**: Adjust thresholds based on realistic performance expectations

## Troubleshooting

### Common Issues

1. **Memory Limit Exceeded**: Increase memory threshold or optimize memory usage
2. **Performance Regression**: Investigate recent changes that may impact performance
3. **Inconsistent Results**: Ensure system load is consistent during benchmarking

### Debug Mode

For debugging benchmark issues:

```bash
# Run with debug logging
zig run benchmarks/benchmark.zig --release=debug
```

## Contributing

When adding new benchmarks:

1. Follow the existing naming conventions
2. Set realistic performance thresholds
3. Include appropriate setup/cleanup functions
4. Document the benchmark purpose and expected performance
5. Test the benchmark in different environments