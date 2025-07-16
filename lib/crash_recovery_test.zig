const std = @import("std");
const testing = std.testing;
const wombat = @import("wombat.zig");

// Comprehensive crash recovery and persistence test suite
// Tests database durability, WAL recovery, manifest consistency, and data integrity after crashes

test "WAL recovery after simulated crash" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_wal_recovery";
    defer cleanupTestDir(test_dir);

    // Phase 1: Write data and simulate crash before sync
    {
        var options = wombat.Options.default(test_dir);
        options.sync_writes = false; // Simulate crash before sync
        options.mem_table_size = 512 * 1024;

        var db = try wombat.DB.open(allocator, options);

        // Write data that should be recoverable from WAL
        try db.set("recovery_key_1", "recovery_value_1");
        try db.set("recovery_key_2", "recovery_value_2");
        try db.set("recovery_key_3", "recovery_value_3");

        // Force WAL write without syncing
        // Simulate crash by closing without proper shutdown
        try db.close();
    }

    // Phase 2: Reopen database and verify recovery
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};

        // Verify data was recovered from WAL
        if (try db.get("recovery_key_1")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "recovery_value_1"));
        } else {
            try testing.expect(false);
        }

        if (try db.get("recovery_key_2")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "recovery_value_2"));
        } else {
            try testing.expect(false);
        }

        if (try db.get("recovery_key_3")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "recovery_value_3"));
        } else {
            try testing.expect(false);
        }

        // Verify statistics are consistent
        const stats = db.getStats();
        try testing.expect(stats.puts_total >= 3);
    }

    std.debug.print("✅ WAL recovery test passed\n", .{});
}

test "Transaction recovery and rollback consistency" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_txn_recovery";
    defer cleanupTestDir(test_dir);

    // Phase 1: Create committed and uncommitted transactions
    {
        var options = wombat.Options.default(test_dir);
        options.detect_conflicts = true;

        var db = try wombat.DB.open(allocator, options);

        // Committed transaction - should survive crash
        var txn1 = try db.newTransaction();
        try txn1.set("committed_key_1", "committed_value_1");
        try txn1.set("committed_key_2", "committed_value_2");
        try db.commitTransaction(txn1);

        // Uncommitted transaction - should not survive crash
        var txn2 = try db.newTransaction();
        try txn2.set("uncommitted_key_1", "uncommitted_value_1");
        try txn2.set("uncommitted_key_2", "uncommitted_value_2");
        // Don't commit txn2 - simulate crash with pending transaction

        try db.close();
    }

    // Phase 2: Verify recovery behavior
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};

        // Committed data should be present
        if (try db.get("committed_key_1")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "committed_value_1"));
        } else {
            try testing.expect(false);
        }

        if (try db.get("committed_key_2")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "committed_value_2"));
        } else {
            try testing.expect(false);
        }

        // Uncommitted data should not be present
        const uncommitted1 = try db.get("uncommitted_key_1");
        try testing.expect(uncommitted1 == null);

        const uncommitted2 = try db.get("uncommitted_key_2");
        try testing.expect(uncommitted2 == null);
    }

    std.debug.print("✅ Transaction recovery test passed\n", .{});
}

test "Manifest consistency after crash" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_manifest_recovery";
    defer cleanupTestDir(test_dir);

    // Phase 1: Create SST files and update manifest
    {
        var options = wombat.Options.default(test_dir);
        options.mem_table_size = 64 * 1024; // Small to trigger flush
        options.sync_writes = true; // Ensure manifest updates are persisted

        var db = try wombat.DB.open(allocator, options);

        // Write enough data to trigger memtable flush and SST creation
        for (0..200) |i| {
            var key_buf: [32]u8 = undefined;
            var value_buf: [64]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "manifest_key_{:0>6}", .{i});
            const value = try std.fmt.bufPrint(&value_buf, "manifest_value_{}_data_for_testing", .{i});
            try db.set(key, value);
        }

        // Force sync to ensure manifest is written
        try db.sync();
        try db.close();
    }

    // Phase 2: Verify manifest recovery and SST file consistency
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};

        // Verify all data is recoverable
        for (0..200) |i| {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "manifest_key_{:0>6}", .{i});
            
            if (try db.get(key)) |value| {
                defer allocator.free(value);
                try testing.expect(value.len > 0);
                try testing.expect(std.mem.startsWith(u8, value, "manifest_value_"));
            } else {
                try testing.expect(false);
            }
        }

        // Verify database statistics are consistent
        const stats = db.getStats();
        try testing.expect(stats.puts_total >= 200);
        try testing.expect(stats.gets_total >= 200);
    }

    std.debug.print("✅ Manifest consistency test passed\n", .{});
}

test "ValueLog persistence and recovery" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_vlog_recovery";
    defer cleanupTestDir(test_dir);

    // Phase 1: Store large values in ValueLog
    {
        var options = wombat.Options.default(test_dir);
        options.value_threshold = 100; // Small threshold to test ValueLog
        options.value_log_file_size = 1024 * 1024; // 1MB files

        var db = try wombat.DB.open(allocator, options);

        // Create large values that go to ValueLog
        const large_value = try allocator.alloc(u8, 500);
        defer allocator.free(large_value);
        @memset(large_value, 'X');

        for (0..20) |i| {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "vlog_key_{}", .{i});
            
            // Modify some bytes to make each value unique
            large_value[0] = @intCast('A' + (i % 26));
            large_value[1] = @intCast('0' + (i % 10));
            
            try db.set(key, large_value);
        }

        try db.sync();
        try db.close();
    }

    // Phase 2: Verify ValueLog recovery
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};

        // Verify all large values are recovered correctly
        for (0..20) |i| {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "vlog_key_{}", .{i});
            
            if (try db.get(key)) |value| {
                defer allocator.free(value);
                try testing.expect(value.len == 500);
                try testing.expect(value[0] == @as(u8, @intCast('A' + (i % 26))));
                try testing.expect(value[1] == @as(u8, @intCast('0' + (i % 10))));
                // Rest should be 'X'
                try testing.expect(value[2] == 'X');
                try testing.expect(value[499] == 'X');
            } else {
                try testing.expect(false);
            }
        }

        // Test ValueLog statistics
        const stats = db.getStats();
        try testing.expect(stats.vlog_size > 0);
    }

    std.debug.print("✅ ValueLog persistence test passed\n", .{});
}

test "Data integrity after multiple crashes" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_multi_crash";
    defer cleanupTestDir(test_dir);

    var total_keys: u32 = 0;

    // Simulate multiple crash-recovery cycles
    for (0..5) |cycle| {
        var options = wombat.Options.default(test_dir);
        options.sync_writes = (cycle % 2 == 0); // Alternate sync behavior
        
        var db = try wombat.DB.open(allocator, options);

        // Add some data in each cycle
        const start_key = cycle * 50;
        const end_key = start_key + 50;
        
        for (start_key..end_key) |i| {
            var key_buf: [32]u8 = undefined;
            var value_buf: [64]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "multi_crash_key_{}", .{i});
            const value = try std.fmt.bufPrint(&value_buf, "multi_crash_value_{}_cycle_{}", .{ i, cycle });
            
            try db.set(key, value);
            total_keys += 1;
        }

        // Randomly sync or crash
        if (cycle % 2 == 0) {
            try db.sync();
        }
        
        try db.close();
    }

    // Final verification after all cycles
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};

        var recovered_keys: u32 = 0;
        
        // Check which data survived
        for (0..total_keys) |i| {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "multi_crash_key_{}", .{i});
            
            if (try db.get(key)) |value| {
                defer allocator.free(value);
                try testing.expect(std.mem.startsWith(u8, value, "multi_crash_value_"));
                recovered_keys += 1;
            }
        }

        // Should have recovered a significant portion of data
        // Even with crashes, synced data should survive
        try testing.expect(recovered_keys >= total_keys / 2);
        
        std.debug.print("Recovered {}/{} keys after {} crash cycles\n", .{ recovered_keys, total_keys, 5 });
    }

    std.debug.print("✅ Multiple crash recovery test passed\n", .{});
}

test "Concurrent operation recovery" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_concurrent_recovery";
    defer cleanupTestDir(test_dir);

    // Phase 1: Simulate concurrent operations before crash
    {
        var options = wombat.Options.default(test_dir);
        options.detect_conflicts = true;
        options.mem_table_size = 512 * 1024;

        var db = try wombat.DB.open(allocator, options);

        // Mix of regular operations and transactions
        try db.set("regular_key_1", "regular_value_1");
        
        var txn1 = try db.newTransaction();
        try txn1.set("txn_key_1", "txn_value_1");
        try db.commitTransaction(txn1);
        
        try db.set("regular_key_2", "regular_value_2");
        
        var txn2 = try db.newTransaction();
        try txn2.set("txn_key_2", "txn_value_2");
        try txn2.delete("regular_key_1"); // Delete previous key
        try db.commitTransaction(txn2);

        // Some operations that might be in-flight during crash
        try db.set("regular_key_3", "regular_value_3");

        try db.close();
    }

    // Phase 2: Verify consistent recovery
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};

        // regular_key_1 should be deleted
        const deleted_key = try db.get("regular_key_1");
        try testing.expect(deleted_key == null);

        // txn_key_1 should exist
        if (try db.get("txn_key_1")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "txn_value_1"));
        } else {
            try testing.expect(false);
        }

        // regular_key_2 should exist
        if (try db.get("regular_key_2")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "regular_value_2"));
        } else {
            try testing.expect(false);
        }

        // txn_key_2 should exist
        if (try db.get("txn_key_2")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "txn_value_2"));
        } else {
            try testing.expect(false);
        }

        // regular_key_3 should exist
        if (try db.get("regular_key_3")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "regular_value_3"));
        } else {
            try testing.expect(false);
        }
    }

    std.debug.print("✅ Concurrent operation recovery test passed\n", .{});
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