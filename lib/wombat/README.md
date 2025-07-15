# Wombat Library Structure

This directory contains the core Wombat LSM-tree database library, organized by functionality.

## Directory Structure

### `core/` - Core Data Structures and Utilities
- `skiplist.zig` - Lock-free skip list implementation for MemTable
- `bloom.zig` - Bloom filter for reducing disk reads
- `channel.zig` - Thread-safe communication channels
- `options.zig` - Configuration and validation

### `memory/` - Memory Management
- `arena.zig` - Arena allocator for efficient memory pooling

### `io/` - Input/Output Operations
- `mmap.zig` - Memory-mapped file operations for efficient I/O

### `storage/` - Storage Layer
- `wal.zig` - Write-ahead log for durability
- `memtable.zig` - In-memory table with WAL integration
- `table.zig` - SSTable format and operations
- `levels.zig` - LSM tree level management and compaction

### `transaction/` - Transaction System
- `oracle.zig` - MVCC oracle and transaction management

### `db.zig` - Main Database Interface
The main database engine that coordinates all components.

## Key Design Patterns

### Memory Safety
- Uses Zig's compile-time bounds checking
- Arena allocators for predictable memory usage
- RAII patterns with `defer` for cleanup

### Concurrency
- Lock-free data structures where possible
- Atomic operations for shared state
- Thread-safe channels for coordination

### Performance
- Memory-mapped files for efficient I/O
- Bloom filters to avoid unnecessary reads
- Comptime optimizations for hot paths

## Module Dependencies

```
db.zig
├── core/
│   ├── skiplist.zig ← memory/arena.zig
│   ├── bloom.zig
│   ├── channel.zig
│   └── options.zig
├── storage/
│   ├── wal.zig ← io/mmap.zig
│   ├── memtable.zig ← core/skiplist.zig, storage/wal.zig
│   ├── table.zig ← io/mmap.zig, core/bloom.zig, core/options.zig
│   └── levels.zig ← storage/table.zig, storage/memtable.zig
└── transaction/
    └── oracle.zig
```

## Usage

Import the main library:
```zig
const wombat = @import("wombat");
```

Or import specific modules:
```zig
const SkipList = @import("wombat").SkipList;
const DB = @import("wombat").DB;
```