const std = @import("std");
const testing = std.testing;
const wombat = @import("wombat");

test "compression system works" {
    const allocator = testing.allocator;

    // Test all compression types
    const compression_types = [_]wombat.CompressionType{ .none, .zlib };

    for (compression_types) |comp_type| {
        var compressor = wombat.Compressor.init(allocator, comp_type);

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

pub fn main() !void {
    std.debug.print("Testing compression system...\n", .{});
    testing.refAllDecls(@This());
    std.debug.print("All tests passed!\n", .{});
}
