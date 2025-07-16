# Configuration Guide

This guide covers all configuration options available in Wombat and how to tune them for your specific use case.

## Overview

Wombat provides extensive configuration options through the `Options` struct. Configuration affects performance, durability, resource usage, and operational behavior.

## Core Configuration

### Basic Options

```zig
pub const Options = struct {
    // Directory settings
    dir: []const u8,                    // Main database directory
    value_dir: []const u8,              // Value log directory (optional)
    
    // Memory table settings
    mem_table_size: usize = 64 * 1024 * 1024,  // 64MB default
    num_memtables: u32 = 5,                     // Number of memtables
    
    // LSM tree structure
    max_levels: u32 = 7,                        // Maximum levels (L0-L6)
    num_level_zero_tables: u32 = 5,             // L0 table limit
    max_table_size: u64 = 64 * 1024 * 1024,    // SSTable size limit
    
    // Compaction settings
    num_compactors: u32 = 4,                    // Background compaction threads
    
    // I/O and persistence
    sync_writes: bool = true,                   // Force sync on writes
    compression: CompressionType = .none,       // Compression algorithm
    compression_level: u32 = 6,                 // Compression level (1-9)
    
    // Advanced configuration sections
    transaction_config: TransactionConfig = .{},
    retry_config: RetryConfig = .{},
    io_config: IOConfig = .{},
    cache_config: CacheConfig = .{},
    monitoring_config: MonitoringConfig = .{},
    recovery_config: RecoveryConfig = .{},
    encryption_config: EncryptionConfig = .{},
    validation_config: ValidationConfig = .{},
};
```

## Memory Configuration

### MemTable Settings

**mem_table_size**: Controls the size of the in-memory write buffer.

```zig
// Small memory footprint (16MB)
options.mem_table_size = 16 * 1024 * 1024;

// High performance (256MB)
options.mem_table_size = 256 * 1024 * 1024;

// Balanced (64MB - default)
options.mem_table_size = 64 * 1024 * 1024;
```

**Trade-offs:**
- **Larger**: Fewer flushes, better write performance, more memory usage
- **Smaller**: More frequent flushes, lower memory usage, potentially higher write amplification

**num_memtables**: Number of memtables to keep in memory.

```zig
// Minimal memory (2 memtables)
options.num_memtables = 2;

// High throughput (8 memtables)
options.num_memtables = 8;

// Balanced (5 memtables - default)
options.num_memtables = 5;
```

### Cache Configuration

```zig
pub const CacheConfig = struct {
    block_cache_size: usize = 32 * 1024 * 1024,      // 32MB
    table_cache_size: usize = 16 * 1024 * 1024,      // 16MB
    filter_cache_size: usize = 8 * 1024 * 1024,      // 8MB
    enable_cache_compression: bool = false,
    cache_ttl_seconds: u32 = 3600,                    // 1 hour
    
    // Advanced cache settings
    block_size: usize = 4 * 1024,                     // 4KB blocks
    cache_shards: u32 = 16,                           // Cache sharding
    enable_prefetch: bool = true,                     // Read-ahead
    prefetch_distance: u32 = 8,                       // Prefetch distance
};
```

**Example configurations:**

```zig
// Memory-optimized caching
options.cache_config = .{
    .block_cache_size = 8 * 1024 * 1024,      // 8MB
    .table_cache_size = 4 * 1024 * 1024,      // 4MB
    .filter_cache_size = 2 * 1024 * 1024,     // 2MB
    .enable_cache_compression = true,
};

// High-performance caching
options.cache_config = .{
    .block_cache_size = 512 * 1024 * 1024,    // 512MB
    .table_cache_size = 256 * 1024 * 1024,    // 256MB
    .filter_cache_size = 128 * 1024 * 1024,   // 128MB
    .enable_prefetch = true,
    .prefetch_distance = 16,
};
```

## LSM Tree Configuration

### Level Structure

**max_levels**: Maximum number of levels in the LSM tree.

```zig
// Compact structure (4 levels)
options.max_levels = 4;

// Standard structure (7 levels - default)
options.max_levels = 7;

// Extended structure (10 levels)
options.max_levels = 10;
```

**num_level_zero_tables**: Number of tables allowed in L0 before compaction.

```zig
// Frequent compaction (3 tables)
options.num_level_zero_tables = 3;

// Balanced (5 tables - default)
options.num_level_zero_tables = 5;

// Infrequent compaction (10 tables)
options.num_level_zero_tables = 10;
```

### Table Size Configuration

**max_table_size**: Maximum size of SSTables.

```zig
// Small tables (32MB)
options.max_table_size = 32 * 1024 * 1024;

// Large tables (256MB)
options.max_table_size = 256 * 1024 * 1024;

// Balanced (64MB - default)
options.max_table_size = 64 * 1024 * 1024;
```

**Trade-offs:**
- **Smaller tables**: Better point queries, more files, higher metadata overhead
- **Larger tables**: Better range queries, fewer files, more efficient compaction

## Compaction Configuration

### Basic Compaction Settings

**num_compactors**: Number of background compaction threads.

```zig
// Single-threaded (1 compactor)
options.num_compactors = 1;

// Multi-threaded (8 compactors)
options.num_compactors = 8;

// Balanced (4 compactors - default)
options.num_compactors = 4;
```

### Advanced Compaction Settings

Use the builder pattern for advanced compaction configuration:

```zig
const options = Options.default("/tmp/db")
    .withCompactionThrottleBytesPerSec(50 * 1024 * 1024)  // 50MB/s
    .withCompactionPriority(.balanced)
    .withCompactionWorkerQueueSize(100);
```

## I/O Configuration

### Sync Settings

**sync_writes**: Controls write durability.

```zig
// Maximum durability (slower writes)
options.sync_writes = true;

// Better performance (less durability)
options.sync_writes = false;
```

### I/O Configuration Section

```zig
pub const IOConfig = struct {
    use_mmap: bool = true,                          // Memory-mapped files
    direct_io: bool = false,                        // Direct I/O bypass
    read_ahead_size: usize = 256 * 1024,           // Read-ahead buffer
    write_buffer_size: usize = 1 * 1024 * 1024,    // Write buffer
    
    // Throttling
    compaction_throttle_bytes_per_sec: u64 = 100 * 1024 * 1024,  // 100MB/s
    write_throttle_bytes_per_sec: u64 = 0,          // No limit
    
    // Timeouts
    read_timeout_ms: u32 = 30000,                   // 30 seconds
    write_timeout_ms: u32 = 30000,                  // 30 seconds
    sync_timeout_ms: u32 = 60000,                   // 60 seconds
    
    // File handling
    max_open_files: u32 = 1000,                     // Open file limit
    use_file_lock: bool = true,                     // File locking
};
```

**Example I/O configurations:**

```zig
// High-performance I/O
options.io_config = .{
    .use_mmap = true,
    .direct_io = true,
    .read_ahead_size = 1 * 1024 * 1024,            // 1MB
    .write_buffer_size = 4 * 1024 * 1024,          // 4MB
    .compaction_throttle_bytes_per_sec = 200 * 1024 * 1024,  // 200MB/s
    .max_open_files = 2000,
};

// Conservative I/O
options.io_config = .{
    .use_mmap = false,
    .direct_io = false,
    .read_ahead_size = 64 * 1024,                   // 64KB
    .write_buffer_size = 256 * 1024,                // 256KB
    .compaction_throttle_bytes_per_sec = 20 * 1024 * 1024,   // 20MB/s
    .max_open_files = 100,
};
```

## Compression Configuration

### Compression Types

```zig
pub const CompressionType = enum {
    none,    // No compression
    zlib,    // DEFLATE compression
};
```

### Compression Settings

```zig
// No compression (fastest)
options.compression = .none;

// Zlib compression (balanced)
options.compression = .zlib;
options.compression_level = 6;  // 1-9, higher = better compression

// High compression
options.compression = .zlib;
options.compression_level = 9;
```

**Trade-offs:**
- **No compression**: Fastest, largest storage size
- **Zlib**: Balanced compression ratio and speed
- **Higher levels**: Better compression, slower compression/decompression

## Transaction Configuration

```zig
pub const TransactionConfig = struct {
    enable_transactions: bool = false,
    max_concurrent_transactions: u32 = 1000,
    transaction_timeout_ms: u64 = 30000,           // 30 seconds
    deadlock_detection_enabled: bool = true,
    conflict_resolution: ConflictResolution = .abort,
    
    // MVCC settings
    max_versions_per_key: u32 = 100,
    version_cleanup_interval_ms: u64 = 60000,      // 1 minute
    
    // Snapshot settings
    snapshot_isolation_level: IsolationLevel = .read_committed,
    max_snapshots: u32 = 1000,
};
```

## Monitoring Configuration

```zig
pub const MonitoringConfig = struct {
    enable_metrics: bool = true,
    metrics_interval_ms: u64 = 1000,               // 1 second
    log_level: LogLevel = .info,
    
    // Performance monitoring
    enable_performance_tracking: bool = true,
    histogram_buckets: u32 = 50,
    
    // Error monitoring
    enable_error_tracking: bool = true,
    error_sample_rate: f64 = 1.0,                  // 100%
    
    // Resource monitoring
    enable_resource_monitoring: bool = true,
    memory_check_interval_ms: u64 = 5000,          // 5 seconds
    disk_check_interval_ms: u64 = 10000,           // 10 seconds
};
```

## Recovery Configuration

```zig
pub const RecoveryConfig = struct {
    enable_wal: bool = true,
    wal_sync_writes: bool = true,
    wal_file_size: usize = 64 * 1024 * 1024,      // 64MB
    wal_compression: bool = false,
    
    // Recovery behavior
    verify_checksums: bool = true,
    repair_corrupted_data: bool = false,
    max_recovery_time_ms: u64 = 300000,            // 5 minutes
    
    // Backup settings
    auto_backup_enabled: bool = false,
    backup_interval_hours: u32 = 24,               // Daily
    backup_retention_days: u32 = 7,                // 1 week
};
```

## Encryption Configuration

```zig
pub const EncryptionConfig = struct {
    enable_encryption: bool = false,
    encryption_algorithm: EncryptionAlgorithm = .aes256,
    key_derivation_rounds: u32 = 10000,
    
    // Key management
    key_rotation_enabled: bool = false,
    key_rotation_interval_hours: u32 = 24 * 7,     // Weekly
    
    // Performance
    encrypt_in_memory: bool = false,
    encryption_batch_size: usize = 64 * 1024,      // 64KB
};
```

## Validation Configuration

```zig
pub const ValidationConfig = struct {
    enable_validation: bool = true,
    max_key_size: usize = 1024,                    // 1KB
    max_value_size: usize = 1024 * 1024,          // 1MB
    
    // Runtime validation
    validate_keys: bool = true,
    validate_values: bool = true,
    validate_checksums: bool = true,
    
    // Constraints
    allow_empty_keys: bool = false,
    allow_empty_values: bool = true,
    enforce_utf8_keys: bool = false,
};
```

## Configuration Presets

### Default Configuration

```zig
const options = Options.default("/path/to/db");
```
- Balanced performance and durability
- 64MB MemTable, 5 memtables
- 4 compaction threads
- Sync writes enabled
- No compression

### High Performance Configuration

```zig
const options = Options.highPerformance("/path/to/db");
```
- 256MB MemTable, 8 memtables
- 16 compaction threads
- Sync writes disabled
- Zlib compression
- Larger caches

### Memory Optimized Configuration

```zig
const options = Options.memoryOptimized("/path/to/db");
```
- 16MB MemTable, 2 memtables
- 2 compaction threads
- Smaller caches
- Compression enabled

### Durability Focused Configuration

```zig
const options = Options.durabilityFocused("/path/to/db");
```
- Sync writes enabled
- WAL enabled with sync
- Checksum verification
- Backup enabled

## Builder Pattern Examples

### Custom High-Performance Setup

```zig
const options = Options.default("/fast/ssd/db")
    .withMemTableSize(512 * 1024 * 1024)       // 512MB
    .withNumMemtables(8)
    .withCompression(.zlib)
    .withCompressionLevel(3)                    // Fast compression
    .withSyncWrites(false)                      // Async writes
    .withNumCompactors(16)                      // Many compactors
    .withBlockCacheSize(1024 * 1024 * 1024)    // 1GB cache
    .withTableCacheSize(512 * 1024 * 1024)     // 512MB cache
    .withDirectIO(true)                         // Direct I/O
    .withReadAheadSize(2 * 1024 * 1024);       // 2MB read-ahead
```

### Custom Memory-Constrained Setup

```zig
const options = Options.default("/slow/hdd/db")
    .withMemTableSize(8 * 1024 * 1024)         // 8MB
    .withNumMemtables(2)
    .withCompression(.zlib)
    .withCompressionLevel(9)                    // Max compression
    .withSyncWrites(true)                       // Durability
    .withNumCompactors(1)                       // Single compactor
    .withBlockCacheSize(4 * 1024 * 1024)       // 4MB cache
    .withTableCacheSize(2 * 1024 * 1024)       // 2MB cache
    .withCompactionThrottleBytesPerSec(10 * 1024 * 1024);  // 10MB/s
```

## Performance Tuning Guidelines

### For Write-Heavy Workloads

1. **Increase MemTable size** to reduce flush frequency
2. **Disable sync writes** for better throughput
3. **Use more compactors** for background processing
4. **Enable compression** to reduce I/O
5. **Increase write buffer size**

### For Read-Heavy Workloads

1. **Increase cache sizes** for better hit rates
2. **Enable prefetching** for sequential reads
3. **Use larger tables** for fewer files
4. **Optimize compaction** for read performance
5. **Enable bloom filters**

### For Mixed Workloads

1. **Balance MemTable size** and flush frequency
2. **Use moderate compression** levels
3. **Configure appropriate cache sizes**
4. **Monitor and adjust** based on metrics
5. **Use transaction isolation** as needed

## Validation and Testing

Always validate configuration changes:

```zig
const options = Options.default("/tmp/test_db")
    .withMemTableSize(128 * 1024 * 1024)
    .withCompression(.zlib);

// Validate configuration
try options.validate();

// Test with your workload
var db = try DB.open(allocator, options);
defer db.close() catch {};

// Monitor performance
const stats = db.getStats();
std.log.info("Cache hit ratio: {d:.2}%", .{stats.cache_hit_ratio * 100});
```

## Configuration Best Practices

1. **Start with presets** and adjust incrementally
2. **Monitor metrics** after configuration changes
3. **Test thoroughly** in your environment
4. **Document** your configuration choices
5. **Version control** your configuration
6. **Plan for growth** in data size and load
7. **Consider hardware** constraints and capabilities

This comprehensive configuration guide should help you optimize Wombat for your specific use case and requirements.