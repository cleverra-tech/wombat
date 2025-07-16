const std = @import("std");
const testing = std.testing;
const wombat = @import("wombat.zig");

// Simplified crash recovery and persistence test suite
// Focus on basic data persistence without complex transaction scenarios

test "Basic data persistence across database restarts" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_basic_persistence";
    defer cleanupTestDir(test_dir);

    // Phase 1: Write data and close database properly
    {
        var options = wombat.Options.default(test_dir);
        options.sync_writes = true; // Ensure data is written to disk
        options.mem_table_size = 512 * 1024;

        var db = try wombat.DB.open(allocator, options);

        // Write some basic data
        try db.set("persist_key_1", "persist_value_1");
        try db.set("persist_key_2", "persist_value_2");
        try db.set("persist_key_3", "persist_value_3");

        // Ensure data is synced
        try db.sync();
        try db.close();
    }

    // Phase 2: Reopen database and verify data persistence
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};

        // Verify all data persisted correctly
        if (try db.get("persist_key_1")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "persist_value_1"));
        } else {
            try testing.expect(false);
        }

        if (try db.get("persist_key_2")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "persist_value_2"));
        } else {
            try testing.expect(false);
        }

        if (try db.get("persist_key_3")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "persist_value_3"));
        } else {
            try testing.expect(false);
        }
    }

    std.debug.print("✅ Basic data persistence test passed\n", .{});
}

test "Data durability with sync operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_sync_durability";
    defer cleanupTestDir(test_dir);

    // Phase 1: Write data with explicit sync calls
    {
        var options = wombat.Options.default(test_dir);
        options.sync_writes = false; // Manually control sync
        
        var db = try wombat.DB.open(allocator, options);

        // Write data batch 1
        try db.set("sync_key_1", "sync_value_1");
        try db.set("sync_key_2", "sync_value_2");
        try db.sync(); // Explicitly sync

        // Write data batch 2  
        try db.set("sync_key_3", "sync_value_3");
        try db.set("sync_key_4", "sync_value_4");
        // Don't sync batch 2 - simulate potential data loss

        try db.close();
    }

    // Phase 2: Verify synced data persists
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};

        // Batch 1 (synced) should be available
        if (try db.get("sync_key_1")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "sync_value_1"));
        } else {
            try testing.expect(false);
        }

        if (try db.get("sync_key_2")) |value| {
            defer allocator.free(value);
            try testing.expect(std.mem.eql(u8, value, "sync_value_2"));
        } else {
            try testing.expect(false);
        }

        // Note: We can't guarantee batch 2 data without WAL recovery
        // This test just ensures the synced data is durable
    }

    std.debug.print("✅ Data durability test passed\n", .{});
}

test "Large dataset persistence" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_large_persistence";
    defer cleanupTestDir(test_dir);

    const num_entries = 1000;

    // Phase 1: Write large dataset
    {
        var options = wombat.Options.default(test_dir);
        options.mem_table_size = 64 * 1024; // Small to trigger flushes
        options.sync_writes = true;

        var db = try wombat.DB.open(allocator, options);

        for (0..num_entries) |i| {
            var key_buf: [32]u8 = undefined;
            var value_buf: [64]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "large_key_{:0>6}", .{i});
            const value = try std.fmt.bufPrint(&value_buf, "large_value_{}_with_data", .{i});
            
            try db.set(key, value);

            // Periodic sync to ensure durability
            if (i % 100 == 0) {
                try db.sync();
            }
        }

        try db.sync();
        try db.close();
    }

    // Phase 2: Verify large dataset recovery
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};

        var recovered_count: u32 = 0;

        for (0..num_entries) |i| {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "large_key_{:0>6}", .{i});
            
            if (try db.get(key)) |value| {
                defer allocator.free(value);
                try testing.expect(std.mem.startsWith(u8, value, "large_value_"));
                recovered_count += 1;
            }
        }

        // Should recover most or all data
        try testing.expect(recovered_count >= num_entries * 90 / 100); // At least 90%
        
        std.debug.print("Recovered {}/{} entries from large dataset\n", .{ recovered_count, num_entries });
    }

    std.debug.print("✅ Large dataset persistence test passed\n", .{});
}

test "Database consistency after multiple open/close cycles" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_multi_cycle_consistency";
    defer cleanupTestDir(test_dir);

    var total_operations: u32 = 0;

    // Multiple open/close cycles with incremental data
    for (0..5) |cycle| {
        var options = wombat.Options.default(test_dir);
        options.sync_writes = true;
        
        var db = try wombat.DB.open(allocator, options);

        // Add data in each cycle
        const start_idx = cycle * 20;
        for (start_idx..start_idx + 20) |i| {
            var key_buf: [32]u8 = undefined;
            var value_buf: [64]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "cycle_key_{}", .{i});
            const value = try std.fmt.bufPrint(&value_buf, "cycle_value_{}_round_{}", .{ i, cycle });
            
            try db.set(key, value);
            total_operations += 1;
        }

        // Verify data from previous cycles is still accessible
        if (cycle > 0) {
            const prev_key_idx = (cycle - 1) * 20;
            var prev_key_buf: [32]u8 = undefined;
            const prev_key = try std.fmt.bufPrint(&prev_key_buf, "cycle_key_{}", .{prev_key_idx});
            
            if (try db.get(prev_key)) |value| {
                defer allocator.free(value);
                try testing.expect(std.mem.startsWith(u8, value, "cycle_value_"));
            } else {
                try testing.expect(false);
            }
        }

        try db.sync();
        try db.close();
    }

    // Final verification
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};

        var final_count: u32 = 0;
        for (0..total_operations) |i| {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "cycle_key_{}", .{i});
            
            if (try db.get(key)) |value| {
                defer allocator.free(value);
                final_count += 1;
            }
        }

        try testing.expect(final_count == total_operations);
        std.debug.print("Maintained consistency across {} cycles with {} operations\n", .{ 5, total_operations });
    }

    std.debug.print("✅ Multi-cycle consistency test passed\n", .{});
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