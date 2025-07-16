const std = @import("std");
const testing = std.testing;
const wombat = @import("wombat");

// Minimal recovery test to validate basic functionality
test "Minimal database functionality" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "test_minimal";
    defer cleanupTestDir(test_dir);

    // Test basic database creation and simple operations
    {
        const options = wombat.Options.default(test_dir);
        var db = try wombat.DB.open(allocator, options);
        defer db.close() catch {};

        // Just test database creation succeeded
        const stats = db.getStats();
        try testing.expect(stats.puts_total == 0);
        try testing.expect(stats.gets_total == 0);

        std.debug.print("Database creation successful\n", .{});
    }

    std.debug.print("Minimal database test passed\n", .{});
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
