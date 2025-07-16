const std = @import("std");
const testing = std.testing;
const CompressionType = @import("../lib/wombat/core/options.zig").CompressionType;
const Compressor = @import("../lib/wombat/compression/compressor.zig").Compressor;
const CompressionError = @import("../lib/wombat/compression/compressor.zig").CompressionError;
const CompressionStats = @import("../lib/wombat/compression/compressor.zig").CompressionStats;
const CompressionUtils = @import("../lib/wombat/compression/compressor.zig").CompressionUtils;

test "Compressor basic functionality" {
    const allocator = testing.allocator;

    // Test all compression types
    const compression_types = [_]CompressionType{ .none, .zlib };

    for (compression_types) |comp_type| {
        var compressor = Compressor.init(allocator, comp_type);

        const test_data = "Hello, World! This is a test string for compression.";
        const max_size = compressor.maxCompressedSize(test_data.len);

        var compressed_buffer = try allocator.alloc(u8, max_size);
        defer allocator.free(compressed_buffer);

        var decompressed_buffer = try allocator.alloc(u8, test_data.len);
        defer allocator.free(decompressed_buffer);

        // Test compression
        const compressed_size = try compressor.compress(test_data, compressed_buffer);
        try testing.expect(compressed_size <= max_size);

        // Test decompression
        const decompressed_size = try compressor.decompress(compressed_buffer[0..compressed_size], decompressed_buffer);
        try testing.expect(decompressed_size == test_data.len);
        try testing.expectEqualSlices(u8, test_data, decompressed_buffer[0..decompressed_size]);
    }
}

test "Compressor with empty data" {
    const allocator = testing.allocator;
    var compressor = Compressor.init(allocator, .zlib);

    const empty_data = "";
    var buffer = try allocator.alloc(u8, 64);
    defer allocator.free(buffer);

    // Empty data should return 0 size
    const compressed_size = try compressor.compress(empty_data, buffer);
    try testing.expect(compressed_size == 0);

    const decompressed_size = try compressor.decompress(buffer[0..compressed_size], buffer);
    try testing.expect(decompressed_size == 0);
}

test "Compressor alloc functions" {
    const allocator = testing.allocator;
    var compressor = Compressor.init(allocator, .zlib);

    const test_data = "Hello, compression world! This is a longer string to test allocation.";

    // Test compressAlloc
    const compressed = try compressor.compressAlloc(test_data);
    defer allocator.free(compressed);

    // Test decompressAlloc
    const decompressed = try compressor.decompressAlloc(compressed, test_data.len);
    defer allocator.free(decompressed);

    try testing.expectEqualSlices(u8, test_data, decompressed);
}

test "Compressor shouldCompress logic" {
    const allocator = testing.allocator;
    var compressor = Compressor.init(allocator, .zlib);

    // Very small data should not be compressed
    const small_data = "hi";
    try testing.expect(!compressor.shouldCompress(small_data));

    // Repetitive data should be compressed
    const repetitive_data = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    try testing.expect(compressor.shouldCompress(repetitive_data));

    // Test with no compression
    var no_compressor = Compressor.init(allocator, .none);
    try testing.expect(!no_compressor.shouldCompress(repetitive_data));
}

test "CompressionStats functionality" {
    const original_size: usize = 1000;
    const compressed_size: usize = 300;

    const stats = CompressionStats.init(original_size, compressed_size);

    try testing.expect(stats.original_size == original_size);
    try testing.expect(stats.compressed_size == compressed_size);
    try testing.expect(stats.compression_ratio == 0.3);
    try testing.expect(stats.compressionSavings() == 700);
    try testing.expect(stats.compressionPercentage() == 70.0);
}

test "CompressionUtils name functions" {
    try testing.expectEqualSlices(u8, "none", CompressionUtils.getCompressionName(.none));
    try testing.expectEqualSlices(u8, "zlib", CompressionUtils.getCompressionName(.zlib));

    try testing.expect(CompressionUtils.parseCompressionType("none") == .none);
    try testing.expect(CompressionUtils.parseCompressionType("zlib") == .zlib);
    try testing.expect(CompressionUtils.parseCompressionType("unknown") == null);
}

test "CompressionUtils efficiency calculation" {
    try testing.expect(CompressionUtils.calculateEfficiency(100, 50) == 0.5);
    try testing.expect(CompressionUtils.calculateEfficiency(100, 100) == 0.0);
    try testing.expect(CompressionUtils.calculateEfficiency(100, 120) == 0.0);
    try testing.expect(CompressionUtils.calculateEfficiency(0, 50) == 0.0);
}

test "Zlib compression/decompression" {
    const allocator = testing.allocator;
    var compressor = Compressor.init(allocator, .zlib);

    // Test data with repetitive patterns
    const test_data = "aaaaaabbbbbbccccccdddddd";
    const max_size = compressor.maxCompressedSize(test_data.len);

    var compressed_buffer = try allocator.alloc(u8, max_size);
    defer allocator.free(compressed_buffer);

    var decompressed_buffer = try allocator.alloc(u8, test_data.len);
    defer allocator.free(decompressed_buffer);

    // Compress
    const compressed_size = try compressor.compress(test_data, compressed_buffer);

    // Should achieve some compression with repetitive data
    try testing.expect(compressed_size < test_data.len);

    // Decompress
    const decompressed_size = try compressor.decompress(compressed_buffer[0..compressed_size], decompressed_buffer);
    try testing.expect(decompressed_size == test_data.len);
    try testing.expectEqualSlices(u8, test_data, decompressed_buffer[0..decompressed_size]);
}

test "Compression with various data sizes" {
    const allocator = testing.allocator;
    var compressor = Compressor.init(allocator, .zlib);

    // Test different data sizes
    const sizes = [_]usize{ 1, 10, 100, 1000, 10000 };

    for (sizes) |size| {
        const test_data = try allocator.alloc(u8, size);
        defer allocator.free(test_data);

        // Fill with some pattern
        for (test_data, 0..) |*byte, i| {
            byte.* = @intCast(i % 256);
        }

        const max_size = compressor.maxCompressedSize(size);
        var compressed_buffer = try allocator.alloc(u8, max_size);
        defer allocator.free(compressed_buffer);

        var decompressed_buffer = try allocator.alloc(u8, size);
        defer allocator.free(decompressed_buffer);

        // Test compression round-trip
        const compressed_size = try compressor.compress(test_data, compressed_buffer);
        const decompressed_size = try compressor.decompress(compressed_buffer[0..compressed_size], decompressed_buffer);

        try testing.expect(decompressed_size == size);
        try testing.expectEqualSlices(u8, test_data, decompressed_buffer[0..decompressed_size]);
    }
}

test "Compression type max size calculations" {
    const allocator = testing.allocator;
    const input_size: usize = 1000;

    var none_compressor = Compressor.init(allocator, .none);
    var zlib_compressor = Compressor.init(allocator, .zlib);

    const none_max = none_compressor.maxCompressedSize(input_size);
    const zlib_max = zlib_compressor.maxCompressedSize(input_size);

    try testing.expect(none_max == input_size);
    try testing.expect(zlib_max > input_size); // Includes overhead
}

test "isCompressed detection" {
    // Test with zlib header
    const zlib_data = [_]u8{ 0x78, 0x9C, 0x01, 0x00, 0x00 };
    try testing.expect(CompressionUtils.isCompressed(&zlib_data));

    // Test with uncompressed data
    const uncompressed = "Hello, World!";
    try testing.expect(!CompressionUtils.isCompressed(uncompressed));

    // Test with too small data
    const tiny_data = [_]u8{0x01};
    try testing.expect(!CompressionUtils.isCompressed(&tiny_data));
}

test "Compression round-trip with all types" {
    const allocator = testing.allocator;

    const test_cases = [_]struct {
        name: []const u8,
        data: []const u8,
        compression_type: CompressionType,
    }{
        .{ .name = "none_simple", .data = "Hello, World!", .compression_type = .none },
        .{ .name = "zlib_repetitive", .data = "aaaabbbbccccdddd", .compression_type = .zlib },
    };

    for (test_cases) |test_case| {
        var compressor = Compressor.init(allocator, test_case.compression_type);

        const compressed = try compressor.compressAlloc(test_case.data);
        defer allocator.free(compressed);

        const decompressed = try compressor.decompressAlloc(compressed, test_case.data.len);
        defer allocator.free(decompressed);

        try testing.expectEqualSlices(u8, test_case.data, decompressed);
    }
}

test "Entropy estimation" {
    const allocator = testing.allocator;
    var compressor = Compressor.init(allocator, .zlib);

    // High entropy data (random-like)
    const high_entropy = "abcdefghijklmnopqrstuvwxyz0123456789";
    try testing.expect(!compressor.shouldCompress(high_entropy));

    // Low entropy data (repetitive)
    const low_entropy = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    try testing.expect(compressor.shouldCompress(low_entropy));
}
