const std = @import("std");
const wombat = @import("wombat");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.log.info("Testing Wombat LSM-Tree Database", .{});

    // Clean up test directory
    std.fs.cwd().deleteTree("./test_data") catch {};

    const options = wombat.Options.default("./test_data")
        .withMemTableSize(1024 * 1024)
        .withSyncWrites(false);

    std.log.info("Opening database...", .{});
    const db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};

    std.log.info("Database opened successfully", .{});

    // Test basic set/get operations
    std.log.info("Testing basic operations...", .{});

    try db.set("key1", "value1");
    try db.set("key2", "value2");
    try db.set("key3", "value3");

    std.log.info("Set 3 key-value pairs", .{});

    if (try db.get("key1")) |value| {
        std.log.info("Get key1: {s}", .{value});
        if (!std.mem.eql(u8, value, "value1")) {
            std.log.err("Expected 'value1', got '{s}'", .{value});
            return;
        }
    } else {
        std.log.err("key1 not found", .{});
        return;
    }

    if (try db.get("key2")) |value| {
        std.log.info("Get key2: {s}", .{value});
        if (!std.mem.eql(u8, value, "value2")) {
            std.log.err("Expected 'value2', got '{s}'", .{value});
            return;
        }
    } else {
        std.log.err("key2 not found", .{});
        return;
    }

    // Test deletion
    std.log.info("Testing deletion...", .{});
    try db.delete("key2");

    if (try db.get("key2")) |value| {
        std.log.err("key2 should be deleted but found: {s}", .{value});
        return;
    } else {
        std.log.info("key2 correctly deleted", .{});
    }

    // Test overwriting
    std.log.info("Testing overwrite...", .{});
    try db.set("key1", "new_value1");

    if (try db.get("key1")) |value| {
        std.log.info("Updated key1: {s}", .{value});
        if (!std.mem.eql(u8, value, "new_value1")) {
            std.log.err("Expected 'new_value1', got '{s}'", .{value});
            return;
        }
    } else {
        std.log.err("key1 not found after update", .{});
        return;
    }

    // Test non-existent key
    if (try db.get("nonexistent")) |value| {
        std.log.err("nonexistent key should not be found but got: {s}", .{value});
        return;
    } else {
        std.log.info("nonexistent key correctly returns null", .{});
    }

    // Test multiple operations
    std.log.info("Testing bulk operations...", .{});
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        var key_buf: [32]u8 = undefined;
        var value_buf: [64]u8 = undefined;

        const key = try std.fmt.bufPrint(key_buf[0..], "bulk_key_{d}", .{i});
        const value = try std.fmt.bufPrint(value_buf[0..], "bulk_value_{d}", .{i});

        try db.set(key, value);
    }

    std.log.info("Set 100 bulk key-value pairs", .{});

    // Verify some bulk operations
    i = 0;
    var verified: u32 = 0;
    while (i < 100) : (i += 10) {
        var key_buf: [32]u8 = undefined;
        var expected_buf: [64]u8 = undefined;

        const key = try std.fmt.bufPrint(key_buf[0..], "bulk_key_{d}", .{i});
        const expected = try std.fmt.bufPrint(expected_buf[0..], "bulk_value_{d}", .{i});

        if (try db.get(key)) |value| {
            if (std.mem.eql(u8, value, expected)) {
                verified += 1;
            } else {
                std.log.err("Bulk key {s}: expected '{s}', got '{s}'", .{ key, expected, value });
                return;
            }
        } else {
            std.log.err("Bulk key {s} not found", .{key});
            return;
        }
    }

    std.log.info("Verified {d} bulk operations", .{verified});

    // Test sync
    std.log.info("Testing sync...", .{});
    try db.sync();
    std.log.info("Sync completed", .{});

    std.log.info("All database tests passed successfully!", .{});
}
