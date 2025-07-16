const std = @import("std");

// Core modules
pub const Arena = @import("wombat/memory/arena.zig").Arena;
pub const SkipList = @import("wombat/core/skiplist.zig").SkipList;
pub const ValueStruct = @import("wombat/core/skiplist.zig").ValueStruct;
pub const BloomFilter = @import("wombat/core/bloom.zig").BloomFilter;
pub const Channel = @import("wombat/core/channel.zig").Channel;
pub const ChannelError = @import("wombat/core/channel.zig").ChannelError;
pub const ChannelStats = @import("wombat/core/channel.zig").ChannelStats;
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
pub const TableIndex = @import("wombat/storage/table.zig").TableIndex;
pub const TableIterator = @import("wombat/storage/table.zig").TableIterator;
pub const TableError = @import("wombat/storage/table.zig").TableError;
pub const TableStats = @import("wombat/storage/table.zig").TableStats;
pub const IndexEntry = @import("wombat/storage/table.zig").IndexEntry;
pub const IndexStats = @import("wombat/storage/table.zig").IndexStats;
pub const RangeResult = @import("wombat/storage/table.zig").RangeResult;
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
pub const Txn = @import("wombat/transaction/txn.zig").Txn;
pub const TxnOptions = @import("wombat/transaction/txn.zig").TxnOptions;
pub const TxnError = @import("wombat/transaction/txn.zig").TxnError;
pub const TxnIterator = @import("wombat/transaction/txn.zig").TxnIterator;
pub const TxnBuilder = @import("wombat/transaction/txn.zig").TxnBuilder;
pub const TxnManager = @import("wombat/transaction/txn.zig").TxnManager;

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
pub const txn = @import("wombat/transaction/txn.zig");
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

test "Transaction API MVCC operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Initialize Oracle for transaction management
    var oracle_impl = try Oracle.init(allocator);
    defer oracle_impl.deinit();

    // Mock DB instance
    var mock_db: u8 = 0;

    // Test TxnBuilder pattern
    var builder = TxnBuilder.init();
    const txn_options = builder.readOnly(false)
        .maxReadKeys(1000)
        .maxWriteKeys(100)
        .detectConflicts(true)
        .options;

    // Create transaction with custom options
    const read_ts = try oracle_impl.newReadTs();
    var transaction = Txn.init(allocator, read_ts, &mock_db, &oracle_impl, txn_options);
    defer transaction.deinit();

    // Test transaction state
    std.testing.expect(transaction.isActive()) catch unreachable;
    std.testing.expect(!transaction.isCommitted()) catch unreachable;
    std.testing.expect(!transaction.isAborted()) catch unreachable;
    std.testing.expect(transaction.getReadTs() == read_ts) catch unreachable;

    // Test set operation
    try transaction.set("key1", "value1");
    std.testing.expect(transaction.getWriteCount() == 1) catch unreachable;
    std.testing.expect(transaction.hasWrites()) catch unreachable;

    // Test read-your-writes consistency
    const read_value = try transaction.get("key1");
    std.testing.expect(read_value != null) catch unreachable;
    std.testing.expect(std.mem.eql(u8, read_value.?, "value1")) catch unreachable;

    // Test delete operation
    try transaction.delete("key2");
    std.testing.expect(transaction.getWriteCount() == 2) catch unreachable;

    // Test delete read-your-writes
    const deleted_value = try transaction.get("key2");
    std.testing.expect(deleted_value == null) catch unreachable;

    // Test size estimate
    const size = transaction.getSizeEstimate();
    std.testing.expect(size > 0) catch unreachable;

    // Test discard
    transaction.discard();
    std.testing.expect(transaction.isAborted()) catch unreachable;
    std.testing.expect(!transaction.isActive()) catch unreachable;
}

test "TxnManager lifecycle" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Initialize Oracle
    var oracle_impl = try Oracle.init(allocator);
    defer oracle_impl.deinit();

    // Initialize TxnManager
    var txn_manager = TxnManager.init(allocator, &oracle_impl);
    defer txn_manager.deinit();

    // Mock DB instance
    var mock_db: u8 = 0;

    // Test creating transactions
    const txn_opts = TxnOptions{ .read_only = false };
    const txn1 = try txn_manager.newTransaction(&mock_db, txn_opts);
    std.testing.expect(txn_manager.getActiveCount() == 1) catch unreachable;

    const txn2 = try txn_manager.newTransaction(&mock_db, txn_opts);
    std.testing.expect(txn_manager.getActiveCount() == 2) catch unreachable;

    // Test committing transaction
    try txn_manager.commitTransaction(txn1);
    std.testing.expect(txn_manager.getActiveCount() == 1) catch unreachable;

    // Test discarding transaction
    txn_manager.discardTransaction(txn2);
    std.testing.expect(txn_manager.getActiveCount() == 0) catch unreachable;
}

test "Channel integration with database operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Test Channel with WriteRequest (from DB)
    const WriteRequest = struct {
        key: []const u8,
        value: []const u8,
        callback: *const fn (err: ?anyerror) void,
    };

    var write_channel = try Channel(WriteRequest).init(allocator, 10);
    defer write_channel.deinit();

    // Mock callback
    const callback = struct {
        fn cb(err: ?anyerror) void {
            _ = err;
        }
    }.cb;

    // Test sending write requests
    const req1 = WriteRequest{
        .key = "key1",
        .value = "value1",
        .callback = callback,
    };

    try write_channel.send(req1);
    std.testing.expect(write_channel.size() == 1) catch unreachable;

    // Test receiving write request
    const received_req = try write_channel.receive();
    std.testing.expect(std.mem.eql(u8, received_req.key, "key1")) catch unreachable;
    std.testing.expect(std.mem.eql(u8, received_req.value, "value1")) catch unreachable;
    std.testing.expect(write_channel.isEmpty()) catch unreachable;

    // Test channel statistics
    const stats = write_channel.getStats();
    std.testing.expect(stats.sends_total == 1) catch unreachable;
    std.testing.expect(stats.receives_total == 1) catch unreachable;
}

test "TableBuilder integration with SSTable format" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_path = "test_integration.sst";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    const db_options = Options{
        .dir = ".",
        .value_dir = ".",
        .mem_table_size = 1024 * 1024,
        .num_memtables = 5,
        .max_levels = 7,
        .level_size_multiplier = 10,
        .base_table_size = 2 * 1024 * 1024,
        .num_compactors = 4,
        .num_level_zero_tables = 5,
        .num_level_zero_tables_stall = 15,
        .value_threshold = 1024 * 1024,
        .value_log_file_size = 1024 * 1024 * 1024,
        .block_size = 4 * 1024,
        .bloom_false_positive = 0.01,
        .compression = .none,
        .sync_writes = false,
        .detect_conflicts = true,
        .verify_checksums = true,
    };

    // Build a table
    var builder = try TableBuilder.init(allocator, test_path, db_options, 1);
    defer builder.deinit();

    try builder.add("apple", ValueStruct{ .value = "fruit", .timestamp = 10, .meta = 0 });
    try builder.add("banana", ValueStruct{ .value = "yellow", .timestamp = 20, .meta = 0 });
    try builder.add("cherry", ValueStruct{ .value = "red", .timestamp = 30, .meta = 0 });
    try builder.finish();

    // Read the table back
    var sstable = try Table.open(allocator, test_path, 123);
    defer sstable.close();

    // Test basic properties
    std.testing.expect(sstable.getFileId() == 123) catch unreachable;
    std.testing.expect(sstable.getLevel() == 1) catch unreachable;

    std.testing.expect(std.mem.eql(u8, sstable.getSmallestKey(), "apple")) catch unreachable;
    std.testing.expect(std.mem.eql(u8, sstable.getBiggestKey(), "cherry")) catch unreachable;

    // Test key lookups
    const apple_result = try sstable.get("apple");
    std.testing.expect(apple_result != null) catch unreachable;
    std.testing.expect(std.mem.eql(u8, apple_result.?.value, "fruit")) catch unreachable;
    std.testing.expect(apple_result.?.timestamp == 10) catch unreachable;
    // Free the allocated value memory
    allocator.free(apple_result.?.value);

    const banana_result = try sstable.get("banana");
    std.testing.expect(banana_result != null) catch unreachable;
    std.testing.expect(std.mem.eql(u8, banana_result.?.value, "yellow")) catch unreachable;
    // Free the allocated value memory
    allocator.free(banana_result.?.value);

    // Test non-existent key
    const grape_result = try sstable.get("grape");
    std.testing.expect(grape_result == null) catch unreachable;

    // Test key range overlaps
    std.testing.expect(sstable.overlapsWithKeyRange("a", "b")) catch unreachable; // Should overlap with apple
    std.testing.expect(!sstable.overlapsWithKeyRange("d", "z")) catch unreachable; // Should not overlap

    // Test statistics
    const stats = sstable.getStats();
    std.testing.expect(stats.file_size > 0) catch unreachable;
    std.testing.expect(stats.key_count > 0) catch unreachable;
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
