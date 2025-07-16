# API Reference

This document provides a complete reference for the Wombat database API.

## Core Database Operations

### DB.open()

Opens a database instance with the specified configuration.

```zig
pub fn open(allocator: std.mem.Allocator, options: Options) !*DB
```

**Parameters:**
- `allocator`: Memory allocator for database operations
- `options`: Database configuration options

**Returns:** Database instance pointer

**Example:**
```zig
const allocator = std.heap.page_allocator;
const options = Options.default("/path/to/db");
var db = try DB.open(allocator, options);
defer db.close() catch {};
```

### DB.close()

Closes the database and flushes all pending writes.

```zig
pub fn close(self: *Self) !void
```

**Example:**
```zig
try db.close();
```

### DB.get()

Retrieves a value for the given key.

```zig
pub fn get(self: *Self, key: []const u8) !?[]const u8
```

**Parameters:**
- `key`: Key to look up

**Returns:** 
- `[]const u8`: Value if found
- `null`: If key not found

**Example:**
```zig
const value = try db.get("my_key");
if (value) |v| {
    std.log.info("Found value: {s}", .{v});
    allocator.free(v); // Remember to free returned value
} else {
    std.log.info("Key not found");
}
```

### DB.set()

Stores a key-value pair in the database.

```zig
pub fn set(self: *Self, key: []const u8, value: []const u8) !void
```

**Parameters:**
- `key`: Key to store
- `value`: Value to associate with key

**Example:**
```zig
try db.set("my_key", "my_value");
```

### DB.delete()

Removes a key-value pair from the database.

```zig
pub fn delete(self: *Self, key: []const u8) !void
```

**Parameters:**
- `key`: Key to delete

**Example:**
```zig
try db.delete("my_key");
```

### DB.sync()

Forces all pending writes to disk.

```zig
pub fn sync(self: *Self) !void
```

**Example:**
```zig
try db.sync();
```

## Configuration Options

### Options

Database configuration structure.

```zig
pub const Options = struct {
    // Directory settings
    dir: []const u8,
    value_dir: []const u8,
    
    // Core LSM tree settings
    mem_table_size: usize = 64 * 1024 * 1024,
    num_memtables: u32 = 5,
    max_levels: u32 = 7,
    
    // Performance settings
    num_compactors: u32 = 4,
    num_level_zero_tables: u32 = 5,
    max_table_size: u64 = 64 * 1024 * 1024,
    
    // I/O settings
    sync_writes: bool = true,
    compression: CompressionType = .none,
    compression_level: u32 = 6,
    
    // Configuration sections
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

### Configuration Presets

#### Options.default()

Creates a balanced configuration suitable for most use cases.

```zig
pub fn default(dir: []const u8) Options
```

#### Options.highPerformance()

Optimized for maximum performance with higher resource usage.

```zig
pub fn highPerformance(dir: []const u8) Options
```

#### Options.memoryOptimized()

Optimized for minimal memory usage.

```zig
pub fn memoryOptimized(dir: []const u8) Options
```

#### Options.durabilityFocused()

Optimized for maximum durability guarantees.

```zig
pub fn durabilityFocused(dir: []const u8) Options
```

### Builder Pattern API

Options support a fluent builder pattern for configuration:

```zig
const options = Options.default("/path/to/db")
    .withMemTableSize(128 * 1024 * 1024)
    .withCompression(.zlib)
    .withSyncWrites(false)
    .withNumCompactors(8);
```

## Statistics and Monitoring

### DB.getStats()

Returns database statistics.

```zig
pub fn getStats(self: *Self) DBStats
```

**Returns:** `DBStats` structure with operation counts and performance metrics

### DBStats

```zig
pub const DBStats = struct {
    gets_total: u64,
    puts_total: u64,
    deletes_total: u64,
    compactions_total: u64,
    vlog_space_reclaim_runs: u64,
    
    // Performance metrics
    get_latency_ns: u64,
    put_latency_ns: u64,
    delete_latency_ns: u64,
    
    // Resource usage
    memory_usage_bytes: u64,
    disk_usage_bytes: u64,
    cache_hit_ratio: f64,
};
```

### DB.getErrorMetrics()

Returns error statistics.

```zig
pub fn getErrorMetrics(self: *const Self) *const ErrorMetrics
```

### Error Handling

The API uses Zig's error handling with comprehensive error types:

```zig
pub const DBError = error{
    DatabaseClosed,
    InvalidOptions,
    CorruptedData,
    OutOfMemory,
    IOError,
    TransactionError,
    InvalidKey,
    KeyTooLarge,
    ValueTooLarge,
    // ... more error types
};
```

## Data Structures

### ValueStruct

Internal representation of values with metadata.

```zig
pub const ValueStruct = struct {
    value: []const u8,
    timestamp: u64,
    meta: u8,
};
```

### SkipList

In-memory sorted data structure for the MemTable.

```zig
pub const SkipList = struct {
    pub fn init(allocator: Allocator, arena_size: usize) !SkipList
    pub fn deinit(self: *Self) void
    pub fn put(self: *Self, key: []const u8, value: ValueStruct) !void
    pub fn get(self: *Self, key: []const u8) ?ValueStruct
    pub fn delete(self: *Self, key: []const u8) !void
    pub fn iterator(self: *Self) Iterator
    pub fn getStats(self: *const Self) SkipListStats
};
```

### Arena

Memory allocator for efficient allocation patterns.

```zig
pub const Arena = struct {
    pub fn init(allocator: Allocator, size: usize) !Arena
    pub fn deinit(self: *Self) void
    pub fn alloc(self: *Self, comptime T: type, n: usize) ![]T
    pub fn reset(self: *Self) void
};
```

## Compression

### CompressionType

Supported compression algorithms.

```zig
pub const CompressionType = enum {
    none,
    zlib,
};
```

### Compressor

Compression interface for data compression.

```zig
pub const Compressor = struct {
    pub fn init(allocator: Allocator, compression_type: CompressionType) Compressor
    pub fn compress(self: *const Self, src: []const u8, dst: []u8) !usize
    pub fn decompress(self: *const Self, src: []const u8, dst: []u8) !usize
    pub fn compressAlloc(self: *const Self, src: []const u8) ![]u8
    pub fn decompressAlloc(self: *const Self, src: []const u8, original_size: usize) ![]u8
};
```

## Advanced Features

### Compaction Control

```zig
pub fn runCompaction(self: *Self) !void
pub fn pauseCompaction(self: *Self) !void
pub fn resumeCompaction(self: *Self) !void
```

### Backup and Restore

```zig
pub fn createBackup(self: *Self, backup_path: []const u8) !void
pub fn restoreFromBackup(allocator: Allocator, backup_path: []const u8, restore_path: []const u8) !void
```

### Iterator Support

```zig
pub const Iterator = struct {
    pub fn next(self: *Self) ?KeyValue
    pub fn seekToFirst(self: *Self) void
    pub fn seekToLast(self: *Self) void
    pub fn seek(self: *Self, key: []const u8) void
    pub fn valid(self: *const Self) bool
};
```

## Usage Examples

### Basic Usage

```zig
const std = @import("std");
const wombat = @import("wombat");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Open database
    const options = wombat.Options.default("/tmp/my_db");
    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};

    // Store data
    try db.set("key1", "value1");
    try db.set("key2", "value2");

    // Retrieve data
    const value = try db.get("key1");
    if (value) |v| {
        std.log.info("Retrieved: {s}", .{v});
        allocator.free(v);
    }

    // Delete data
    try db.delete("key1");

    // Force sync
    try db.sync();
}
```

### Performance Optimized Usage

```zig
const options = wombat.Options.highPerformance("/tmp/fast_db")
    .withMemTableSize(256 * 1024 * 1024)
    .withNumCompactors(16)
    .withCompression(.zlib)
    .withSyncWrites(false);

var db = try wombat.DB.open(allocator, options);
defer db.close() catch {};

// Batch operations for better performance
for (0..10000) |i| {
    const key = try std.fmt.allocPrint(allocator, "key_{d}", .{i});
    defer allocator.free(key);
    
    const value = try std.fmt.allocPrint(allocator, "value_{d}", .{i});
    defer allocator.free(value);
    
    try db.set(key, value);
}

// Periodic sync for durability
try db.sync();
```

### Error Handling

```zig
const result = db.get("nonexistent_key");
if (result) |value| {
    defer allocator.free(value);
    // Handle found value
} else |err| switch (err) {
    error.KeyNotFound => {
        // Handle key not found
    },
    error.IOError => {
        // Handle I/O error
    },
    else => {
        // Handle other errors
        return err;
    },
}
```

## Thread Safety

Wombat is designed for concurrent access:

- **Reads**: Multiple concurrent readers are supported
- **Writes**: Single-writer model with internal queuing
- **Compaction**: Background threads handle compaction automatically
- **Sync**: Thread-safe across all operations

## Memory Management

- **Returned Values**: Must be freed by caller using the same allocator
- **Keys/Values**: Copied internally, caller retains ownership
- **Database Instance**: Must be closed to free resources
- **Arena Usage**: Internal memory management for efficiency

## Performance Considerations

- **Batch Operations**: Group multiple operations for better performance
- **Key Ordering**: Sequential keys provide better compaction efficiency
- **Memory Sizing**: Larger MemTables reduce flush frequency
- **Compression**: Trade-off between CPU and storage efficiency
- **Sync Frequency**: Balance durability requirements with performance