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
pub const ManifestFile = @import("wombat/storage/manifest.zig").ManifestFile;
pub const TableInfo = @import("wombat/storage/manifest.zig").TableInfo;
pub const ValueLog = @import("wombat/storage/vlog.zig").ValueLog;
pub const ValuePointer = @import("wombat/storage/vlog.zig").ValuePointer;
pub const VLogFile = @import("wombat/storage/vlog.zig").VLogFile;

// Transaction modules
pub const Oracle = @import("wombat/transaction/oracle.zig").Oracle;
pub const Transaction = @import("wombat/transaction/oracle.zig").Transaction;
pub const WriteEntry = @import("wombat/transaction/oracle.zig").WriteEntry;
pub const TransactionState = @import("wombat/transaction/oracle.zig").TransactionState;
pub const CommittedTxn = @import("wombat/transaction/oracle.zig").CommittedTxn;
pub const OracleStats = @import("wombat/transaction/oracle.zig").OracleStats;
pub const WaterMark = @import("wombat/transaction/watermark.zig").WaterMark;
pub const WaterMarkGroup = @import("wombat/transaction/watermark.zig").WaterMarkGroup;
pub const WaterMarkSnapshot = @import("wombat/transaction/watermark.zig").WaterMarkSnapshot;

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
pub const manifest = @import("wombat/storage/manifest.zig");
pub const vlog = @import("wombat/storage/vlog.zig");
pub const oracle = @import("wombat/transaction/oracle.zig");
pub const watermark = @import("wombat/transaction/watermark.zig");
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

test "ValueLog basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const temp_dir = "test_vlog_basic";
    defer cleanupVLogTestDir(temp_dir);

    const value_log = try ValueLog.init(allocator, temp_dir, 10, 1024 * 1024);
    defer value_log.deinit();

    // Test basic write/read
    const entries = [_]Entry{
        Entry{ .key = "key1", .value = "long_value_for_vlog_storage", .timestamp = 1, .meta = 0 },
    };

    const pointers = try value_log.write(&entries);
    defer allocator.free(pointers);

    const read_value = try value_log.read(pointers[0], allocator);
    defer allocator.free(read_value);

    std.testing.expect(std.mem.eql(u8, read_value, entries[0].value)) catch unreachable;
}

test "WaterMark transaction ordering" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const read_mark = try WaterMark.init(allocator, "read_transactions");
    defer read_mark.deinit();

    const txn_mark = try WaterMark.init(allocator, "commit_transactions");
    defer txn_mark.deinit();

    // Test transaction lifecycle with watermarks
    try read_mark.begin(100);
    try txn_mark.begin(100);

    std.testing.expect(read_mark.getWaterMark() == 100) catch unreachable;
    std.testing.expect(txn_mark.getWaterMark() == 100) catch unreachable;

    // Complete read transaction
    read_mark.done(100);
    std.testing.expect(read_mark.getWaterMark() == std.math.maxInt(u64) - 1) catch unreachable;

    // Commit transaction still pending
    std.testing.expect(txn_mark.getWaterMark() == 100) catch unreachable;

    // Complete commit transaction
    txn_mark.done(100);
    std.testing.expect(txn_mark.getWaterMark() == std.math.maxInt(u64) - 1) catch unreachable;
}

fn cleanupVLogTestDir(dir_path: []const u8) void {
    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return;
    defer dir.close();
    var iterator = dir.iterate();
    while (iterator.next() catch null) |entry| {
        if (entry.kind == .file) {
            var path_buf: [256]u8 = undefined;
            const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir_path, entry.name }) catch continue;
            std.fs.cwd().deleteFile(path) catch {};
        }
    }
    std.fs.cwd().deleteDir(dir_path) catch {};
}
