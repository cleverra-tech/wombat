const std = @import("std");

// Core modules
pub const Arena = @import("wombat/memory/arena.zig").Arena;
pub const SkipList = @import("wombat/core/skiplist.zig").SkipList;
pub const ValueStruct = @import("wombat/core/skiplist.zig").ValueStruct;
pub const BloomFilter = @import("wombat/core/bloom.zig").BloomFilter;
pub const Channel = @import("wombat/core/channel.zig").Channel;
pub const Options = @import("wombat/core/options.zig").Options;
pub const CompressionType = @import("wombat/core/options.zig").CompressionType;

// I/O modules
pub const MmapFile = @import("wombat/io/mmap.zig").MmapFile;

// Storage modules
pub const WriteAheadLog = @import("wombat/storage/wal.zig").WriteAheadLog;
pub const Entry = @import("wombat/storage/wal.zig").Entry;
pub const MemTable = @import("wombat/storage/memtable.zig").MemTable;
pub const Table = @import("wombat/storage/table.zig").Table;
pub const TableBuilder = @import("wombat/storage/table.zig").TableBuilder;
pub const LevelsController = @import("wombat/storage/levels.zig").LevelsController;

// Transaction modules
pub const Oracle = @import("wombat/transaction/oracle.zig").Oracle;
pub const Transaction = @import("wombat/transaction/oracle.zig").Transaction;

// Main database
pub const DB = @import("wombat/db.zig").DB;

// Re-export commonly used types
pub const arena = @import("wombat/memory/arena.zig");
pub const skiplist = @import("wombat/core/skiplist.zig");
pub const mmap = @import("wombat/io/mmap.zig");
pub const options = @import("wombat/core/options.zig");
pub const wal = @import("wombat/storage/wal.zig");
pub const memtable = @import("wombat/storage/memtable.zig");
pub const bloom = @import("wombat/core/bloom.zig");
pub const table = @import("wombat/storage/table.zig");
pub const levels = @import("wombat/storage/levels.zig");
pub const oracle = @import("wombat/transaction/oracle.zig");
pub const channel = @import("wombat/core/channel.zig");
pub const db = @import("wombat/db.zig");

test "wombat basic test" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var arena_allocator = try Arena.init(allocator, 1024 * 1024);
    defer arena_allocator.deinit();

    const test_data = try arena_allocator.alloc(u8, 100);
    std.testing.expect(test_data.len == 100) catch unreachable;
}

test "skiplist basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var skiplist_impl = try SkipList.init(allocator, 1024 * 1024);
    defer skiplist_impl.deinit();

    const key = "test_key";
    const value = ValueStruct{
        .value = "test_value",
        .timestamp = 1,
        .meta = 0,
    };

    try skiplist_impl.put(key, value);

    const retrieved = skiplist_impl.get(key);
    std.testing.expect(retrieved != null) catch unreachable;
    std.testing.expect(std.mem.eql(u8, retrieved.?.value, "test_value")) catch unreachable;
}
