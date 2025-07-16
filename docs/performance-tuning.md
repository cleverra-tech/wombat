# Performance Tuning Guide

This guide provides comprehensive strategies for optimizing Wombat performance across different workload patterns and hardware configurations.

## Performance Fundamentals

### LSM-Tree Performance Characteristics

**Write Performance:**
- Sequential writes are optimal
- Write amplification affects throughput
- MemTable size impacts flush frequency
- Compaction affects sustained write performance

**Read Performance:**
- Point queries: O(log n) per level
- Range queries: Sequential scan efficiency
- Cache hit rates critical for performance
- Bloom filters reduce false positives

**Space Amplification:**
- Compaction reduces space overhead
- Compression trades CPU for storage
- Value log separation helps with large values

## Workload-Specific Optimizations

### Write-Heavy Workloads

#### Configuration
```zig
const options = Options.default("/path/to/db")
    .withMemTableSize(256 * 1024 * 1024)        // Large MemTable
    .withNumMemtables(8)                         // More memtables
    .withSyncWrites(false)                       // Async writes
    .withNumCompactors(8)                        // More compactors
    .withCompression(.zlib)                      // Reduce I/O
    .withCompressionLevel(3)                     // Fast compression
    .withCompactionThrottleBytesPerSec(200 * 1024 * 1024); // 200MB/s
```

#### Key Strategies
1. **Increase MemTable size** to reduce flush frequency
2. **Use more memtables** to buffer writes during flushes
3. **Disable sync writes** for maximum throughput
4. **Scale compaction workers** to handle background work
5. **Enable compression** to reduce disk I/O

#### Monitoring
```zig
const stats = db.getStats();
if (stats.puts_total > 0) {
    const avg_write_latency = stats.put_latency_ns / stats.puts_total;
    if (avg_write_latency > 10_000_000) { // 10ms
        // Consider increasing MemTable size
    }
}
```

### Read-Heavy Workloads

#### Configuration
```zig
const options = Options.default("/path/to/db")
    .withBlockCacheSize(1024 * 1024 * 1024)     // 1GB block cache
    .withTableCacheSize(512 * 1024 * 1024)      // 512MB table cache
    .withFilterCacheSize(128 * 1024 * 1024)     // 128MB filter cache
    .withEnablePrefetch(true)                    // Read-ahead
    .withPrefetchDistance(16)                    // Aggressive prefetch
    .withMaxTableSize(256 * 1024 * 1024)        // Larger tables
    .withReadAheadSize(2 * 1024 * 1024);        // 2MB read-ahead
```

#### Key Strategies
1. **Maximize cache sizes** for better hit rates
2. **Enable prefetching** for sequential access patterns
3. **Use larger SSTables** to reduce file count
4. **Optimize compaction** for read performance
5. **Monitor cache hit rates** and adjust accordingly

#### Cache Optimization
```zig
// Monitor cache effectiveness
const stats = db.getStats();
if (stats.cache_hit_ratio < 0.8) {
    // Increase cache sizes
    const new_options = current_options
        .withBlockCacheSize(current_options.cache_config.block_cache_size * 2);
}
```

### Mixed Workloads

#### Balanced Configuration
```zig
const options = Options.default("/path/to/db")
    .withMemTableSize(128 * 1024 * 1024)        // Balanced MemTable
    .withNumMemtables(5)                         // Standard count
    .withSyncWrites(true)                        // Durability
    .withNumCompactors(4)                        // Moderate compaction
    .withCompression(.zlib)                      // Balanced compression
    .withCompressionLevel(6)                     // Standard level
    .withBlockCacheSize(256 * 1024 * 1024)      // Reasonable cache
    .withTableCacheSize(128 * 1024 * 1024);     // Moderate cache
```

#### Adaptive Tuning
```zig
// Adjust based on runtime metrics
const stats = db.getStats();
const read_ratio = @as(f64, @floatFromInt(stats.gets_total)) / 
                  @as(f64, @floatFromInt(stats.gets_total + stats.puts_total));

if (read_ratio > 0.8) {
    // Read-heavy: increase caches
} else if (read_ratio < 0.2) {
    // Write-heavy: increase MemTable
}
```

## Hardware-Specific Optimizations

### SSD Storage

#### Configuration
```zig
const options = Options.default("/fast/ssd/db")
    .withDirectIO(true)                          // Bypass page cache
    .withUseMmap(true)                           // Memory mapping
    .withMaxOpenFiles(2000)                      // More open files
    .withReadAheadSize(4 * 1024 * 1024)         // 4MB read-ahead
    .withWriteBufferSize(8 * 1024 * 1024)       // 8MB write buffer
    .withCompactionThrottleBytesPerSec(500 * 1024 * 1024); // 500MB/s
```

#### Optimizations
1. **Enable direct I/O** to bypass OS cache
2. **Use memory mapping** for efficient access
3. **Increase open file limits** for better concurrency
4. **Larger read-ahead** for sequential workloads
5. **Higher compaction throughput** limits

### HDD Storage

#### Configuration
```zig
const options = Options.default("/slow/hdd/db")
    .withDirectIO(false)                         // Use page cache
    .withUseMmap(false)                          // Avoid memory mapping
    .withMaxOpenFiles(100)                       // Limit open files
    .withReadAheadSize(256 * 1024)              // 256KB read-ahead
    .withWriteBufferSize(1 * 1024 * 1024)       // 1MB write buffer
    .withCompactionThrottleBytesPerSec(50 * 1024 * 1024); // 50MB/s
```

#### Optimizations
1. **Disable direct I/O** to use OS caching
2. **Avoid memory mapping** on slower storage
3. **Limit open files** to reduce seeking
4. **Smaller read-ahead** to avoid wasted I/O
5. **Lower compaction limits** to reduce interference

### High-Memory Systems

#### Configuration
```zig
const options = Options.default("/path/to/db")
    .withMemTableSize(1024 * 1024 * 1024)       // 1GB MemTable
    .withNumMemtables(16)                        // Many memtables
    .withBlockCacheSize(4 * 1024 * 1024 * 1024) // 4GB block cache
    .withTableCacheSize(2 * 1024 * 1024 * 1024) // 2GB table cache
    .withFilterCacheSize(1 * 1024 * 1024 * 1024) // 1GB filter cache
    .withEnableCacheCompression(false)           // Disable compression
    .withNumCompactors(16);                      // Many compactors
```

#### Memory Optimization
1. **Large MemTables** for fewer flushes
2. **Multiple memtables** for better concurrency
3. **Massive caches** for high hit rates
4. **Disable cache compression** with abundant memory
5. **Parallel compaction** for CPU utilization

### Memory-Constrained Systems

#### Configuration
```zig
const options = Options.memoryOptimized("/path/to/db")
    .withMemTableSize(8 * 1024 * 1024)          // 8MB MemTable
    .withNumMemtables(2)                         // Minimal memtables
    .withBlockCacheSize(16 * 1024 * 1024)       // 16MB block cache
    .withTableCacheSize(8 * 1024 * 1024)        // 8MB table cache
    .withFilterCacheSize(4 * 1024 * 1024)       // 4MB filter cache
    .withEnableCacheCompression(true)            // Compress cache
    .withNumCompactors(1);                       // Single compactor
```

#### Memory Efficiency
1. **Small MemTables** to minimize memory usage
2. **Fewer memtables** to reduce overhead
3. **Compact caches** with high density
4. **Enable cache compression** for efficiency
5. **Limited compaction** to control memory

## Key Design Patterns

### Sequential Key Patterns

For time-series or log-like data:

```zig
// Good: Sequential keys improve compaction
const timestamp = std.time.milliTimestamp();
const key = try std.fmt.allocPrint(allocator, "log:{d:0>13}", .{timestamp});

// Better: Include additional ordering
const key = try std.fmt.allocPrint(allocator, "log:{d:0>13}:{s}", .{timestamp, source});
```

Benefits:
- Better compaction efficiency
- Improved range query performance
- Reduced write amplification

### Value Size Optimization

#### Small Values (< 1KB)
```zig
// Store directly in LSM tree
try db.set("counter:users", "12345");
try db.set("config:timeout", "30000");
```

#### Large Values (> 10KB)
```zig
// Value log automatically handles large values
const large_data = try allocator.alloc(u8, 1024 * 1024); // 1MB
defer allocator.free(large_data);
try db.set("document:content", large_data);
```

#### Very Large Values (> 1MB)
```zig
// Consider storing references instead
const file_path = "/path/to/large/file.dat";
try db.set("document:path", file_path);
```

### Batch Processing

#### Efficient Batch Writes
```zig
// Group related operations
const batch_size = 1000;
for (0..100000) |i| {
    const key = try std.fmt.allocPrint(allocator, "batch:{d:0>6}", .{i});
    defer allocator.free(key);
    
    const value = try std.fmt.allocPrint(allocator, "value_{d}", .{i});
    defer allocator.free(value);
    
    try db.set(key, value);
    
    // Periodic sync for durability
    if (i % batch_size == 0) {
        try db.sync();
    }
}
```

#### Bulk Loading
```zig
// Disable sync for bulk loading
const options = Options.default("/path/to/db")
    .withSyncWrites(false)
    .withMemTableSize(512 * 1024 * 1024)        // Large MemTable
    .withNumCompactors(1);                       // Single compactor during load

var db = try DB.open(allocator, options);
defer db.close() catch {};

// Load data...
// Final sync
try db.sync();
```

## Performance Monitoring

### Key Metrics to Monitor

#### Throughput Metrics
```zig
const stats = db.getStats();
const ops_per_second = stats.gets_total + stats.puts_total + stats.deletes_total;
const avg_latency = (stats.get_latency_ns + stats.put_latency_ns + stats.delete_latency_ns) / 3;

std.log.info("Throughput: {} ops/sec, Avg latency: {d}ms", .{
    ops_per_second,
    avg_latency / 1_000_000
});
```

#### Resource Utilization
```zig
std.log.info("Memory: {d}MB, Disk: {d}MB, Cache hit: {d:.1}%", .{
    stats.memory_usage_bytes / (1024 * 1024),
    stats.disk_usage_bytes / (1024 * 1024),
    stats.cache_hit_ratio * 100
});
```

#### Compaction Health
```zig
std.log.info("Compactions: {}, Rate: {d:.1}MB/s", .{
    stats.compactions_total,
    // Calculate compaction rate from metrics
});
```

### Performance Regression Detection

Use the benchmark framework for continuous monitoring:

```bash
# Run benchmarks regularly
zig build benchmark

# Check for regressions
if [[ $(grep "FAILED" benchmark_results/*.csv | wc -l) -gt 0 ]]; then
    echo "Performance regression detected!"
    exit 1
fi
```

### Alerting Thresholds

```zig
const stats = db.getStats();

// Latency alerts
if (stats.get_latency_ns > 50_000_000) { // 50ms
    std.log.warn("High read latency: {d}ms", .{stats.get_latency_ns / 1_000_000});
}

// Cache hit rate alerts
if (stats.cache_hit_ratio < 0.8) {
    std.log.warn("Low cache hit rate: {d:.1}%", .{stats.cache_hit_ratio * 100});
}

// Memory usage alerts
if (stats.memory_usage_bytes > 2 * 1024 * 1024 * 1024) { // 2GB
    std.log.warn("High memory usage: {d}MB", .{stats.memory_usage_bytes / (1024 * 1024)});
}
```

## Common Performance Issues

### High Write Latency

**Symptoms:**
- Slow write operations
- High write latency metrics
- Blocking write operations

**Solutions:**
1. Increase MemTable size
2. Add more memtables
3. Disable sync writes (if acceptable)
4. Increase compaction workers
5. Enable compression

### Poor Read Performance

**Symptoms:**
- Slow read operations
- Low cache hit rates
- High read latency

**Solutions:**
1. Increase cache sizes
2. Enable prefetching
3. Optimize compaction for reads
4. Use larger SSTables
5. Monitor bloom filter effectiveness

### High Memory Usage

**Symptoms:**
- Excessive memory consumption
- Out of memory errors
- System slowdown

**Solutions:**
1. Reduce MemTable size
2. Decrease cache sizes
3. Enable cache compression
4. Reduce number of memtables
5. Use memory-optimized preset

### Disk Space Issues

**Symptoms:**
- Rapid disk space growth
- Compaction not keeping up
- Storage alerts

**Solutions:**
1. Enable compression
2. Increase compaction workers
3. Adjust compaction thresholds
4. Monitor value log growth
5. Implement data retention policies

## Benchmarking and Testing

### Custom Benchmarks

```zig
const std = @import("std");
const wombat = @import("wombat");

pub fn benchmarkWorkload(allocator: std.mem.Allocator, options: wombat.Options) !void {
    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};

    const start_time = std.time.nanoTimestamp();
    
    // Your workload here
    for (0..10000) |i| {
        const key = try std.fmt.allocPrint(allocator, "key_{d}", .{i});
        defer allocator.free(key);
        
        const value = try std.fmt.allocPrint(allocator, "value_{d}", .{i});
        defer allocator.free(value);
        
        try db.set(key, value);
    }
    
    const end_time = std.time.nanoTimestamp();
    const duration = end_time - start_time;
    const ops_per_sec = 10000.0 / (@as(f64, @floatFromInt(duration)) / 1e9);
    
    std.log.info("Benchmark: {d:.0} ops/sec", .{ops_per_sec});
}
```

### A/B Testing Configurations

```zig
const config_a = Options.default("/tmp/db_a")
    .withMemTableSize(64 * 1024 * 1024);

const config_b = Options.default("/tmp/db_b")
    .withMemTableSize(128 * 1024 * 1024);

// Test both configurations
try benchmarkWorkload(allocator, config_a);
try benchmarkWorkload(allocator, config_b);
```

## Production Deployment Tips

### Gradual Rollout

1. **Start with conservative settings**
2. **Monitor key metrics closely**
3. **Adjust one parameter at a time**
4. **Validate performance improvements**
5. **Document configuration changes**

### Capacity Planning

```zig
// Estimate resource requirements
const estimated_keys = 100_000_000;
const avg_key_size = 32;
const avg_value_size = 256;

const estimated_data_size = estimated_keys * (avg_key_size + avg_value_size);
const estimated_memory = estimated_data_size / 10; // 10% in memory

std.log.info("Estimated storage: {d}GB, memory: {d}GB", .{
    estimated_data_size / (1024 * 1024 * 1024),
    estimated_memory / (1024 * 1024 * 1024)
});
```

### Monitoring in Production

1. **Set up automated monitoring**
2. **Configure performance alerts**
3. **Regular performance testing**
4. **Capacity trend analysis**
5. **Performance regression detection**

This comprehensive performance tuning guide should help you optimize Wombat for your specific use case and achieve optimal performance across different workload patterns and hardware configurations.