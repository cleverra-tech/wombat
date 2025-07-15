const std = @import("std");
const wombat = @import("wombat");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.log.info("Starting Wombat LSM-Tree Database CLI", .{});

    const options = wombat.Options.default("./wombat_data")
        .withMemTableSize(32 * 1024 * 1024)
        .withSyncWrites(true);

    const db = try wombat.DB.open(allocator, options);
    defer db.close() catch {};

    std.log.info("Database opened successfully", .{});

    try db.set("hello", "world");
    std.log.info("Set: hello -> world", .{});

    if (try db.get("hello")) |value| {
        std.log.info("Get: hello -> {s}", .{value});
    } else {
        std.log.info("Get: hello -> not found", .{});
    }

    try db.set("foo", "bar");
    try db.set("test", "value");

    std.log.info("Added more test data", .{});

    try db.delete("foo");
    std.log.info("Deleted: foo", .{});

    if (try db.get("foo")) |value| {
        std.log.info("Get: foo -> {s}", .{value});
    } else {
        std.log.info("Get: foo -> not found (deleted)", .{});
    }

    std.log.info("Database operations completed successfully", .{});
}
