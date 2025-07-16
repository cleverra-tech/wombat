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

    // Recovery tests
    const recovery_tests = [_][]const u8{
        "tests/minimal_recovery_test.zig",
        "tests/simple_recovery_test.zig",
        "tests/crash_recovery_test.zig",
        "tests/simple_comprehensive_test.zig",
        "tests/comprehensive_test.zig",
        "tests/options_test.zig",
    };

    const recovery_test_step = b.step("test-recovery", "Run recovery tests");
    for (recovery_tests) |test_file| {
        const recovery_test = b.addTest(.{
            .root_module = b.createModule(.{
                .root_source_file = .{ .src_path = .{ .owner = b, .sub_path = test_file } },
                .target = target,
                .optimize = optimize,
            }),
        });
        recovery_test.root_module.addImport("wombat", wombat_module);
        recovery_test_step.dependOn(&b.addRunArtifact(recovery_test).step);
    }

    // All tests step
    const all_tests_step = b.step("test-all", "Run all tests");
    all_tests_step.dependOn(test_step);
    all_tests_step.dependOn(recovery_test_step);

    // Add benchmarks
    const benchmarks = b.addExecutable(.{
        .name = "benchmarks",
        .root_module = b.createModule(.{
            .root_source_file = .{ .src_path = .{ .owner = b, .sub_path = "benchmarks/benchmark.zig" } },
            .target = target,
            .optimize = optimize,
        }),
    });
    benchmarks.root_module.addImport("wombat", wombat_module);
    b.installArtifact(benchmarks);

    // Add benchmark run step
    const benchmark_run = b.addRunArtifact(benchmarks);
    const benchmark_step = b.step("benchmark", "Run performance benchmarks");
    benchmark_step.dependOn(&benchmark_run.step);

    // Documentation
    const docs = b.addInstallDirectory(.{
        .source_dir = lib.getEmittedDocs(),
        .install_dir = .prefix,
        .install_subdir = "docs",
    });
    const docs_step = b.step("docs", "Generate documentation");
    docs_step.dependOn(&docs.step);
}
