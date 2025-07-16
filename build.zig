const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Wombat module for other projects to use
    const wombat_module = b.addModule("wombat", .{
        .root_source_file = .{ .src_path = .{ .owner = b, .sub_path = "lib/wombat.zig" } },
        .target = target,
        .optimize = optimize,
    });

    // Wombat static library
    const lib = b.addLibrary(.{
        .name = "wombat",
        .root_module = wombat_module,
    });
    b.installArtifact(lib);

    // Unit tests
    const tests = b.addTest(.{
        .root_module = wombat_module,
    });
    const test_step = b.step("test", "Run unit tests");
    test_step.dependOn(&b.addRunArtifact(tests).step);

    // All tests step
    const all_tests_step = b.step("test-all", "Run all tests");
    all_tests_step.dependOn(test_step);

    // Documentation
    const docs = b.addInstallDirectory(.{
        .source_dir = lib.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });
    const docs_step = b.step("docs", "Generate documentation");
    docs_step.dependOn(&docs.step);
}
