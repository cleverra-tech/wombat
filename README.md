# Wombat LSM-Tree Database

A high-performance LSM-tree key-value database implemented in Zig, designed for memory safety, performance, and scalability.

## Features

- **Memory-safe operations** with compile-time bounds checking
- **Lock-free data structures** using atomic operations
- **Memory-mapped file I/O** for efficient storage
- **Configurable compression** and bloom filters
- **Concurrent background compaction**
- **Transaction support** with MVCC and conflict detection
- **Write-ahead logging** for durability and crash recovery

## Architecture

The database follows a layered architecture with clear separation of concerns:

```
lib/wombat/
├── core/           # Core data structures and utilities
│   ├── skiplist.zig    # Lock-free skip list
│   ├── bloom.zig       # Bloom filter implementation
│   ├── channel.zig     # Thread-safe channels
│   └── options.zig     # Configuration management
├── memory/         # Memory management
│   └── arena.zig       # Arena allocator
├── io/             # I/O operations
│   └── mmap.zig        # Memory-mapped file operations
├── storage/        # Storage layer
│   ├── wal.zig         # Write-ahead log
│   ├── memtable.zig    # In-memory table
│   ├── table.zig       # SSTable implementation
│   └── levels.zig      # LSM level management
├── transaction/    # Transaction system
│   └── oracle.zig      # MVCC and conflict detection
└── db.zig          # Main database interface
```

## Quick Start

### Building

```bash
# Build the library and examples
zig build

# Run unit tests
zig build test

# Run component tests
zig build simple-test

# Run database integration tests
zig build db-test

# Run benchmarks
zig build bench
zig build component-bench
```

### Basic Usage

```zig
const std = @import("std");
const wombat = @import("wombat");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Configure database options
    const options = wombat.Options.default("./data")
        .withMemTableSize(64 * 1024 * 1024)
        .withSyncWrites(true);

    // Open database
    const db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};

    // Basic operations
    try db.set("hello", "world");
    
    if (try db.get("hello")) |value| {
        std.log.info("Retrieved: {s}", .{value});
    }
    
    try db.delete("hello");
}
```

## Examples

- `examples/main.zig` - Complete CLI example with database operations
- `examples/simple_test.zig` - Basic component testing
- `tests/test_db.zig` - Integration testing
- `benchmarks/` - Performance benchmarking

## Performance

Benchmarks on modern hardware show:

- **Arena Allocations**: ~100M ops/sec
- **SkipList Writes**: ~3M ops/sec  
- **SkipList Reads**: ~8M ops/sec

## Design Principles

The implementation follows Zig's design philosophy:

- **Explicit error handling** with error unions
- **Memory safety** without garbage collection
- **Zero-cost abstractions** for performance
- **Compile-time validation** where possible
- **Clear separation** of concerns

## Configuration

Database behavior can be customized through the `Options` struct:

```zig
const options = wombat.Options{
    .dir = "./data",
    .mem_table_size = 64 * 1024 * 1024,
    .max_levels = 7,
    .compression = .snappy,
    .sync_writes = true,
    .detect_conflicts = true,
    // ... more options
};
```

## Contributing

1. Follow Zig's style guidelines
2. Add tests for new functionality
3. Update documentation as needed
4. Ensure all tests pass: `zig build test-all`

## License

[Add your license here]

## References

- [LSM-Tree: The Log-Structured Merge-Tree](https://www.cs.umb.edu/~poneil/lsmtree.pdf)
- [Zig Language Reference](https://ziglang.org/documentation/master/)
- Architecture specification in `docs/wombat.md`