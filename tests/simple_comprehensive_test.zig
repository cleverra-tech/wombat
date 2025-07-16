const std = @import("std");
const testing = std.testing;
const wombat = @import("wombat");

// Simplified comprehensive test suite for the Wombat LSM-Tree database

test "Database basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_db_basic";
    defer cleanupTestDir(test_dir);

    var options = wombat.Options.default(test_dir);
    options.mem_table_size = 1024 * 1024; // 1MB
    options.value_threshold = 1024; // Store values > 1KB in ValueLog
    options.sync_writes = false; // For testing performance

    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};

    // Test basic put operations
    try db.set("key1", "value1");
    try db.set("key2", "value2_with_longer_content");
    try db.set("key3", "value3");

    // Test basic get operations
    if (try db.get("key1")) |value| {
        defer allocator.free(value);
        try testing.expect(std.mem.eql(u8, value, "value1"));
    } else {
        try testing.expect(false);
    }

    // Test get non-existent key
    const non_existent = try db.get("non_existent_key");
    try testing.expect(non_existent == null);

    // Test delete operations
    try db.delete("key2");
    const deleted_value = try db.get("key2");
    try testing.expect(deleted_value == null);

    // Test statistics
    const stats = db.getStats();
    try testing.expect(stats.puts_total >= 3);
    try testing.expect(stats.gets_total >= 3);
    try testing.expect(stats.deletes_total >= 1);

    std.debug.print("Database basic operations test passed\n", .{});
}

test "Transaction basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_txn_basic";
    defer cleanupTestDir(test_dir);

    var options = wombat.Options.default(test_dir);
    options.detect_conflicts = true;

    var db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};

    // Test read-write transaction
    var txn1 = try db.newTransaction();
    defer db.discardTransaction(txn1);

    try txn1.set("txn_key1", "txn_value1");

    // Test read-your-writes within transaction
    if (try txn1.get("txn_key1")) |value| {
        try testing.expect(std.mem.eql(u8, value, "txn_value1"));
    } else {
        try testing.expect(false);
    }

    // Keys should not be visible outside transaction yet
    const external_read = try db.get("txn_key1");
    try testing.expect(external_read == null);

    // Commit transaction
    try db.commitTransaction(txn1);

    // Now keys should be visible
    if (try db.get("txn_key1")) |value| {
        defer allocator.free(value);
        try testing.expect(std.mem.eql(u8, value, "txn_value1"));
    } else {
        try testing.expect(false);
    }

    std.debug.print("Transaction basic operations test passed\n", .{});
}

test "Component basic verification" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Test Oracle basic functionality
    var oracle = try wombat.Oracle.init(allocator);
    defer oracle.deinit();

    const ts1 = try oracle.newReadTs();
    const ts2 = try oracle.newReadTs();
    try testing.expect(ts2 > ts1);

    std.debug.print("Component basic verification passed\n", .{});
}

/// Helper function to clean up test directories
fn cleanupTestDir(dir_path: []const u8) void {
    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return;
    defer dir.close();
    var iterator = dir.iterate();
    while (iterator.next() catch null) |entry| {
        if (entry.kind == .file) {
            var path_buf: [512]u8 = undefined;
            const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir_path, entry.name }) catch continue;
            std.fs.cwd().deleteFile(path) catch {};
        }
    }
    std.fs.cwd().deleteDir(dir_path) catch {};
}
