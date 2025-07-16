# Getting Started with Wombat

This guide will help you get up and running with Wombat, a high-performance LSM-Tree key-value database implemented in Zig.

## Prerequisites

### System Requirements
- **Zig**: Version 0.15 or later
- **Operating System**: Linux, macOS, or Windows
- **Memory**: Minimum 4GB RAM recommended
- **Storage**: SSD recommended for optimal performance

### Installing Zig

If you don't have Zig installed, download it from [ziglang.org](https://ziglang.org/download/).

```bash
# Example for Linux x86_64
curl -L https://ziglang.org/download/0.15.0/zig-linux-x86_64-0.15.0.tar.xz | tar -xJ
export PATH=$PATH:$PWD/zig-linux-x86_64-0.15.0
```

## Installation

### From Source

1. **Clone the Repository**
   ```bash
   git clone https://github.com/cleverra-tech/wombat.git
   cd wombat
   ```

2. **Build the Library**
   ```bash
   zig build
   ```

3. **Run Tests**
   ```bash
   zig build test
   ```

4. **Run Benchmarks**
   ```bash
   zig build benchmark
   ```

### Using in Your Project

Add Wombat as a dependency in your `build.zig`:

```zig
const wombat = b.dependency("wombat", .{
    .target = target,
    .optimize = optimize,
});

exe.root_module.addImport("wombat", wombat.module("wombat"));
```

## Quick Start

### Basic Usage

Here's a simple example to get you started:

```zig
const std = @import("std");
const wombat = @import("wombat");

pub fn main() !void {
    // Initialize allocator
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Configure database options
    const options = wombat.Options.default("/tmp/my_database");
    
    // Open database
    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};

    // Store some data
    try db.set("hello", "world");
    try db.set("foo", "bar");

    // Retrieve data
    const value = try db.get("hello");
    if (value) |v| {
        std.log.info("Retrieved: {s}", .{v});
        allocator.free(v); // Don't forget to free!
    }

    // Delete data
    try db.delete("foo");

    // Ensure data is persisted
    try db.sync();
}
```

### Running the Example

Save the above code to `example.zig` and run:

```bash
zig run example.zig --deps wombat
```

## Configuration

### Basic Configuration

The `Options` struct provides comprehensive configuration:

```zig
const options = wombat.Options{
    .dir = "/path/to/database",
    .value_dir = "/path/to/values",
    .mem_table_size = 64 * 1024 * 1024,  // 64MB
    .num_memtables = 5,
    .max_levels = 7,
    .sync_writes = true,
    .compression = .zlib,
};
```

### Configuration Presets

Wombat provides several configuration presets:

```zig
// Balanced configuration (default)
const balanced = wombat.Options.default("/tmp/db");

// High performance (more memory, less durability)
const fast = wombat.Options.highPerformance("/tmp/db");

// Memory optimized (lower memory usage)
const compact = wombat.Options.memoryOptimized("/tmp/db");

// Durability focused (maximum data safety)
const durable = wombat.Options.durabilityFocused("/tmp/db");
```

### Builder Pattern

Use the fluent builder pattern for easy configuration:

```zig
const options = wombat.Options.default("/tmp/db")
    .withMemTableSize(128 * 1024 * 1024)
    .withCompression(.zlib)
    .withSyncWrites(false)
    .withNumCompactors(8);
```

## Basic Operations

### Writing Data

```zig
// Simple key-value store
try db.set("user:123", "john_doe");

// Binary data is supported
const binary_data = [_]u8{0x01, 0x02, 0x03, 0x04};
try db.set("binary_key", &binary_data);

// Large values are handled efficiently
const large_value = try allocator.alloc(u8, 1024 * 1024); // 1MB
defer allocator.free(large_value);
try db.set("large_data", large_value);
```

### Reading Data

```zig
// Simple retrieval
const value = try db.get("user:123");
if (value) |v| {
    std.log.info("User: {s}", .{v});
    allocator.free(v);
} else {
    std.log.info("User not found");
}

// Handle errors explicitly
const result = db.get("might_not_exist");
if (result) |value| {
    defer allocator.free(value);
    // Process value
} else |err| switch (err) {
    error.KeyNotFound => {
        // Handle missing key
    },
    error.IOError => {
        // Handle I/O error
    },
    else => return err,
}
```

### Deleting Data

```zig
// Delete a key
try db.delete("user:123");

// Verify deletion
const value = try db.get("user:123");
assert(value == null);
```

## Performance Tips

### Batch Operations

For better performance, group related operations:

```zig
// Efficient batch insertion
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

### Memory Management

```zig
// Always free returned values
const value = try db.get("key");
defer if (value) |v| allocator.free(v);

// Reuse allocations when possible
var buffer = std.ArrayList(u8).init(allocator);
defer buffer.deinit();

for (keys) |key| {
    buffer.clearRetainingCapacity();
    const value = try db.get(key);
    if (value) |v| {
        defer allocator.free(v);
        try buffer.appendSlice(v);
        // Process buffer...
    }
}
```

### Key Design

Design keys for optimal performance:

```zig
// Good: Sequential keys for better compaction
try db.set("user:000001", "alice");
try db.set("user:000002", "bob");
try db.set("user:000003", "charlie");

// Less optimal: Random keys create more fragmentation
try db.set("user:xyz123", "alice");
try db.set("user:abc456", "bob");
try db.set("user:def789", "charlie");
```

## Error Handling

### Common Error Types

```zig
const DBError = error{
    DatabaseClosed,
    InvalidOptions,
    CorruptedData,
    OutOfMemory,
    IOError,
    TransactionError,
    InvalidKey,
    KeyTooLarge,
    ValueTooLarge,
};
```

### Error Handling Patterns

```zig
// Pattern 1: Explicit error handling
const result = db.get("key");
if (result) |value| {
    defer allocator.free(value);
    // Handle success
} else |err| switch (err) {
    error.KeyNotFound => {
        // Not an error, just missing
    },
    error.IOError => {
        std.log.err("I/O error occurred");
        return err;
    },
    else => return err,
}

// Pattern 2: Propagate errors
const value = try db.get("key");
defer if (value) |v| allocator.free(v);

// Pattern 3: Provide defaults
const value = db.get("key") catch |err| switch (err) {
    error.KeyNotFound => null,
    else => return err,
};
```

## Database Lifecycle

### Opening and Closing

```zig
// Open database
var db = try wombat.DB.open(allocator, options);

// Always close when done
defer db.close() catch |err| {
    std.log.err("Error closing database: {}", .{err});
};

// Or handle explicitly
try db.close();
```

### Sync and Persistence

```zig
// Force write to disk
try db.sync();

// Check if sync is needed
const stats = db.getStats();
if (stats.gets_total > 1000) {
    try db.sync();
}
```

## Monitoring

### Getting Statistics

```zig
const stats = db.getStats();
std.log.info("Operations: {} gets, {} puts, {} deletes", .{
    stats.gets_total,
    stats.puts_total,
    stats.deletes_total,
});

std.log.info("Memory usage: {d} bytes", .{stats.memory_usage_bytes});
std.log.info("Cache hit ratio: {d:.2}%", .{stats.cache_hit_ratio * 100});
```

### Error Monitoring

```zig
const error_metrics = db.getErrorMetrics();
const error_rate = error_metrics.getErrorRate(60 * 1000); // Last minute
if (error_rate > 0.1) {
    std.log.warn("High error rate: {d:.2} errors/sec", .{error_rate});
}
```

## Next Steps

Now that you have Wombat up and running, explore these topics:

1. **[Configuration](configuration.md)** - Detailed configuration options
2. **[Performance Tuning](performance-tuning.md)** - Optimization strategies
3. **[API Reference](api-reference.md)** - Complete API documentation
4. **[Architecture](architecture.md)** - Understanding the internals

## Common Issues

### Permission Errors
```bash
# Ensure write permissions for database directory
chmod 755 /path/to/database/directory
```

### Memory Issues
```zig
// Reduce memory usage if needed
const options = wombat.Options.memoryOptimized("/tmp/db")
    .withMemTableSize(16 * 1024 * 1024); // 16MB instead of 64MB
```

### Performance Issues
```zig
// Enable compression for better I/O
const options = wombat.Options.default("/tmp/db")
    .withCompression(.zlib)
    .withSyncWrites(false); // For non-critical data
```

## Getting Help

- **Documentation**: Check the [docs/](.) directory
- **Issues**: Report bugs on GitHub Issues
- **Examples**: See the [examples/](../examples/) directory
- **Tests**: Reference implementation in [tests/](../tests/)

## Building and Testing

```bash
# Build library
zig build

# Run all tests
zig build test

# Run specific test
zig build test -- --filter "skiplist"

# Run benchmarks
zig build benchmark

# Build documentation
zig build docs
```

This should give you a solid foundation for using Wombat in your projects. The database is designed to be simple to use while providing high performance for demanding applications.