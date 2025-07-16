# Wombat

A high-performance LSM-Tree (Log-Structured Merge-Tree) key-value database implemented in Zig.

## Features

- **High Performance**: Optimized for both read and write workloads
- **Persistent Storage**: Durable key-value storage with crash recovery
- **Compression**: Built-in data compression with zlib support
- **Configurable**: Extensive configuration options for different use cases
- **Memory Efficient**: Arena-based memory management and configurable caching
- **Concurrent**: Safe concurrent access with internal synchronization
- **Monitoring**: Built-in performance metrics and statistics
- **Benchmarking**: Comprehensive performance testing framework

## Architecture

Wombat implements a classic LSM-Tree design with the following components:

- **MemTable**: In-memory write buffer using skip list data structure
- **Value Log**: Separate log for large values to reduce write amplification
- **SSTable Hierarchy**: Multi-level sorted tables (L0-L6) for efficient storage
- **Background Compaction**: Automatic background merge operations
- **Write-Ahead Log**: Durability guarantee for crash recovery
- **Bloom Filters**: Efficient negative lookup filtering

## Quick Start

### Installation

```bash
git clone https://github.com/cleverra-tech/wombat.git
cd wombat
zig build
```

### Basic Usage

```zig
const std = @import("std");
const wombat = @import("wombat");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Configure and open database
    const options = wombat.Options.default("/tmp/my_database");
    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};

    // Store data
    try db.set("hello", "world");
    try db.set("user:123", "alice");

    // Retrieve data
    const value = try db.get("hello");
    if (value) |v| {
        std.log.info("Retrieved: {s}", .{v});
        allocator.free(v);
    }

    // Delete data
    try db.delete("user:123");

    // Ensure persistence
    try db.sync();
}
```

## Configuration

Wombat provides flexible configuration through the `Options` struct:

```zig
// Default balanced configuration
const options = wombat.Options.default("/path/to/db");

// High-performance configuration
const fast_options = wombat.Options.highPerformance("/path/to/db");

// Memory-optimized configuration
const compact_options = wombat.Options.memoryOptimized("/path/to/db");

// Custom configuration using builder pattern
const custom_options = wombat.Options.default("/path/to/db")
    .withMemTableSize(128 * 1024 * 1024)  // 128MB MemTable
    .withCompression(.zlib)                // Enable compression
    .withSyncWrites(false)                 // Async writes
    .withNumCompactors(8);                 // 8 compaction threads
```

## Performance

Wombat is designed for high-performance workloads:

### Benchmark Results

Using the built-in benchmark framework:

```bash
zig build benchmark
```

**Typical performance on modern hardware:**
- **Arena Allocations**: ~10M ops/sec
- **SkipList Writes**: ~160K ops/sec
- **SkipList Reads**: ~1.4M ops/sec

### Performance Characteristics

- **Write Performance**: Optimized for sequential writes with minimal write amplification
- **Read Performance**: Efficient point queries with bloom filters and caching
- **Memory Usage**: Configurable memory footprint with arena allocation
- **Disk Usage**: Automatic compaction and optional compression

## API Reference

### Core Operations

```zig
// Database lifecycle
var db = try wombat.DB.open(allocator, options);
defer db.close() catch {};

// Basic operations
try db.set(key, value);           // Store key-value pair
const value = try db.get(key);    // Retrieve value (or null)
try db.delete(key);               // Delete key-value pair
try db.sync();                    // Force write to disk

// Statistics
const stats = db.getStats();      // Get performance metrics
```

### Configuration Options

```zig
pub const Options = struct {
    dir: []const u8,                          // Database directory
    mem_table_size: usize = 64 * 1024 * 1024, // MemTable size
    num_memtables: u32 = 5,                   // Number of memtables
    max_levels: u32 = 7,                      // LSM tree levels
    num_compactors: u32 = 4,                  // Compaction threads
    sync_writes: bool = true,                 // Write durability
    compression: CompressionType = .none,     // Compression algorithm
    
    // Advanced configuration sections
    cache_config: CacheConfig = .{},
    io_config: IOConfig = .{},
    monitoring_config: MonitoringConfig = .{},
    // ... additional configuration options
};
```

## Building and Testing

### Build System

```bash
# Build library
zig build

# Run all tests
zig build test

# Run specific test suite
zig build test-recovery

# Run benchmarks
zig build benchmark

# Generate documentation
zig build docs
```

### Testing

The project includes comprehensive tests:

- **Unit Tests**: Test individual components
- **Integration Tests**: Test complete workflows
- **Recovery Tests**: Test crash recovery scenarios
- **Performance Tests**: Validate performance characteristics
- **Benchmark Suite**: Regression testing framework

### Development

```bash
# Development build
zig build

# Debug build with assertions
zig build -Doptimize=Debug

# Release build
zig build -Doptimize=ReleaseFast

# Run specific test
zig build test -- --filter "skiplist"
```

## Documentation

Comprehensive documentation is available in the `docs/` directory:

- **[Getting Started](docs/getting-started.md)** - Installation and first steps
- **[Architecture](docs/architecture.md)** - System design and components
- **[API Reference](docs/api-reference.md)** - Complete API documentation
- **[Configuration](docs/configuration.md)** - Configuration options and tuning
- **[Performance Tuning](docs/performance-tuning.md)** - Optimization guidelines

## Project Status

Wombat is actively developed with the following implementation status:

### Completed Components

- **Core Data Structures**: Arena, SkipList, Write-Ahead Log
- **Storage Engine**: MemTable, SSTable, Value Log
- **LSM Tree**: Multi-level hierarchy with compaction
- **Compression**: zlib compression support
- **Error Handling**: Comprehensive error types and context
- **Configuration**: Extensive configuration options
- **Testing**: Comprehensive test suite
- **Benchmarking**: Performance regression testing
- **Documentation**: Complete API and usage documentation

### In Development

- **Transaction System**: ACID transaction support
- **Encryption**: Data-at-rest encryption
- **Monitoring**: Enhanced metrics and observability
- **Backup/Restore**: Data protection features

### Planned Features

- **Replication**: Multi-node replication support
- **Clustering**: Distributed deployment
- **SQL Interface**: SQL query layer
- **Streaming**: Change data capture

## Contributing

Contributions are welcome! Please see the development guidelines:

1. **Fork** the repository
2. **Create** a feature branch
3. **Implement** your changes with tests
4. **Run** the full test suite
5. **Submit** a pull request

### Development Setup

```bash
# Clone repository
git clone https://github.com/cleverra-tech/wombat.git
cd wombat

# Build and test
zig build test

# Run benchmarks
zig build benchmark

# Check code formatting
zig fmt --check lib/ benchmarks/ tests/
```

## Requirements

- **Zig**: Version 0.15 or later
- **Operating System**: Linux, macOS, Windows
- **Memory**: 4GB+ recommended
- **Storage**: SSD recommended for optimal performance

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Resources

- **Repository**: https://github.com/cleverra-tech/wombat
- **Documentation**: [docs/](docs/)
- **Issues**: GitHub Issues for bug reports and feature requests
- **Benchmarks**: [benchmarks/](benchmarks/) for performance testing

---

**Note**: Wombat is designed for applications requiring high-performance key-value storage with strong durability guarantees. It's suitable for use cases like time-series databases, caching layers, and persistent storage engines.