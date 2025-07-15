const std = @import("std");

pub fn build(b: *std.Build) void {
    // Wombat library module - this validates the library compiles
    _ = b.addModule("wombat", .{
        .root_source_file = b.path("lib/wombat.zig"),
    });

    // Test step using built-in test runner
    const test_cmd = b.addSystemCommand(&[_][]const u8{ "zig", "test", "lib/wombat.zig" });
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&test_cmd.step);
}
