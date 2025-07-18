const std = @import("std");
const Allocator = std.mem.Allocator;
const CompressionType = @import("../core/options.zig").CompressionType;
const zlib = std.compress.zlib;

/// Compression errors
pub const CompressionError = error{
    BufferTooSmall,
    InvalidInput,
    CompressionFailed,
    DecompressionFailed,
    UnsupportedAlgorithm,
    OutOfMemory,
    StreamTooLong,
    InvalidData,
    EndOfStream,
    CorruptInput,
    DictRequired,
    StreamEnd,
    NeedDict,
    Errno,
    BufError,
    VersionError,
    DataError,
    MemError,
    Unknown,
};

/// Compression statistics for monitoring
pub const CompressionStats = struct {
    original_size: usize,
    compressed_size: usize,
    compression_ratio: f64,
    compression_time_ns: u64,
    decompression_time_ns: u64,

    pub fn init(original_size: usize, compressed_size: usize) CompressionStats {
        const ratio = if (original_size > 0) @as(f64, @floatFromInt(compressed_size)) / @as(f64, @floatFromInt(original_size)) else 1.0;
        return CompressionStats{
            .original_size = original_size,
            .compressed_size = compressed_size,
            .compression_ratio = ratio,
            .compression_time_ns = 0,
            .decompression_time_ns = 0,
        };
    }

    pub fn compressionSavings(self: *const CompressionStats) usize {
        return if (self.original_size > self.compressed_size) self.original_size - self.compressed_size else 0;
    }

    pub fn compressionPercentage(self: *const CompressionStats) f64 {
        return (1.0 - self.compression_ratio) * 100.0;
    }
};

/// Main compression interface using Zig's standard library
pub const Compressor = struct {
    compression_type: CompressionType,
    allocator: Allocator,

    const Self = @This();

    // Compression algorithm constants
    const ZLIB_OVERHEAD_RATIO = 1000; // 1/1000th of input size for zlib overhead
    const ZLIB_FIXED_OVERHEAD = 12; // Fixed zlib header/trailer overhead
    const ZLIB_EXTRA_BYTES = 2; // Additional safety margin

    // Size thresholds for compression decisions
    const MIN_COMPRESSION_SIZE = 64; // Minimum bytes to consider compression
    const MIN_ENTROPY_ESTIMATION_SIZE = 16; // Minimum bytes for entropy calculation
    const COMPRESSIBILITY_THRESHOLD = 0.1; // Minimum compressibility ratio to compress

    // Entropy calculation constants
    const MAX_ENTROPY_BITS = 8.0; // Maximum possible entropy (8 bits per byte)

    pub fn init(allocator: Allocator, compression_type: CompressionType) Self {
        return Self{
            .compression_type = compression_type,
            .allocator = allocator,
        };
    }

    /// Compress data into provided buffer
    pub fn compress(self: *const Self, src: []const u8, dst: []u8) CompressionError!usize {
        if (src.len == 0) return 0;

        return switch (self.compression_type) {
            .none => self.copyUncompressed(src, dst),
            .zlib => self.zlibCompress(src, dst),
        };
    }

    /// Decompress data into provided buffer
    pub fn decompress(self: *const Self, src: []const u8, dst: []u8) CompressionError!usize {
        if (src.len == 0) return 0;

        return switch (self.compression_type) {
            .none => self.copyUncompressed(src, dst),
            .zlib => self.zlibDecompress(src, dst),
        };
    }

    /// Calculate maximum possible compressed size
    pub fn maxCompressedSize(self: *const Self, src_size: usize) usize {
        return switch (self.compression_type) {
            .none => src_size,
            .zlib => src_size + (src_size / ZLIB_OVERHEAD_RATIO) + ZLIB_FIXED_OVERHEAD + ZLIB_EXTRA_BYTES,
        };
    }

    /// Compress data and return owned buffer
    pub fn compressAlloc(self: *const Self, src: []const u8) CompressionError![]u8 {
        const max_size = self.maxCompressedSize(src.len);
        const buffer = self.allocator.alloc(u8, max_size) catch return CompressionError.OutOfMemory;
        errdefer self.allocator.free(buffer);

        const compressed_size = try self.compress(src, buffer);
        return self.allocator.realloc(buffer, compressed_size) catch buffer[0..compressed_size];
    }

    /// Decompress data and return owned buffer (requires knowing original size)
    pub fn decompressAlloc(self: *const Self, src: []const u8, original_size: usize) CompressionError![]u8 {
        const buffer = self.allocator.alloc(u8, original_size) catch return CompressionError.OutOfMemory;
        errdefer self.allocator.free(buffer);

        const decompressed_size = try self.decompress(src, buffer);
        if (decompressed_size != original_size) {
            return CompressionError.DecompressionFailed;
        }

        return buffer;
    }

    /// Check if compression is beneficial for given data
    pub fn shouldCompress(self: *const Self, data: []const u8) bool {
        // Don't compress very small data
        if (data.len < MIN_COMPRESSION_SIZE) return false;

        // Don't compress if compression is disabled
        if (self.compression_type == .none) return false;

        // Check for highly repetitive data patterns that compress well
        return self.estimateCompressibility(data) > COMPRESSIBILITY_THRESHOLD;
    }

    /// Estimate compression ratio without actually compressing
    fn estimateCompressibility(self: *const Self, data: []const u8) f64 {
        if (data.len < MIN_ENTROPY_ESTIMATION_SIZE) return 0.0;

        // No compression means no compressibility benefit
        if (self.compression_type == .none) return 0.0;

        // Simple entropy estimation
        var byte_counts = [_]u32{0} ** 256;
        for (data) |byte| {
            byte_counts[byte] += 1;
        }

        var entropy: f64 = 0.0;
        const len_f = @as(f64, @floatFromInt(data.len));

        for (byte_counts) |count| {
            if (count > 0) {
                const p = @as(f64, @floatFromInt(count)) / len_f;
                entropy -= p * @log(p) / @log(2.0);
            }
        }

        // Normalize entropy to 0-1 range where 1 is most compressible
        var compressibility = @max(0.0, (MAX_ENTROPY_BITS - entropy) / MAX_ENTROPY_BITS);

        // Adjust based on compression algorithm characteristics
        compressibility *= switch (self.compression_type) {
            .none => 0.0, // No compression
            .zlib => 1.0, // Full entropy-based estimation for zlib
        };

        return compressibility;
    }

    /// No compression - just copy data
    fn copyUncompressed(self: *const Self, src: []const u8, dst: []u8) CompressionError!usize {
        _ = self;
        if (dst.len < src.len) return CompressionError.BufferTooSmall;
        @memcpy(dst[0..src.len], src);
        return src.len;
    }

    /// Zlib compression using Zig's standard library
    fn zlibCompress(self: *const Self, src: []const u8, dst: []u8) CompressionError!usize {
        _ = self;

        var src_stream = std.io.fixedBufferStream(src);
        var dst_stream = std.io.fixedBufferStream(dst);

        zlib.compress(src_stream.reader(), dst_stream.writer(), .{}) catch return CompressionError.CompressionFailed;

        return dst_stream.getWritten().len;
    }

    /// Zlib decompression using Zig's standard library
    fn zlibDecompress(self: *const Self, src: []const u8, dst: []u8) CompressionError!usize {
        _ = self;

        var src_stream = std.io.fixedBufferStream(src);
        var dst_stream = std.io.fixedBufferStream(dst);

        zlib.decompress(src_stream.reader(), dst_stream.writer()) catch return CompressionError.DecompressionFailed;

        return dst_stream.getWritten().len;
    }
};

/// Utility functions
pub const CompressionUtils = struct {
    /// Get compression type name as string
    pub fn getCompressionName(compression_type: CompressionType) []const u8 {
        return switch (compression_type) {
            .none => "none",
            .zlib => "zlib",
        };
    }

    /// Parse compression type from string
    pub fn parseCompressionType(name: []const u8) ?CompressionType {
        if (std.mem.eql(u8, name, "none")) return .none;
        if (std.mem.eql(u8, name, "zlib")) return .zlib;
        return null;
    }

    /// Check if data appears to be compressed
    pub fn isCompressed(data: []const u8) bool {
        if (data.len < 2) return false;

        // Check for zlib header
        if (data.len >= 2 and
            ((data[0] == 0x78 and data[1] == 0x9C) or // Default compression
                (data[0] == 0x78 and data[1] == 0x01) or // No compression
                (data[0] == 0x78 and data[1] == 0xDA)))
        { // Best compression
            return true;
        }

        return false;
    }

    /// Calculate compression efficiency
    pub fn calculateEfficiency(original_size: usize, compressed_size: usize) f64 {
        if (original_size == 0) return 0.0;
        const ratio = @as(f64, @floatFromInt(compressed_size)) / @as(f64, @floatFromInt(original_size));
        return @max(0.0, @min(1.0, 1.0 - ratio));
    }
};
