const std = @import("std");
const math = std.math;

pub const BloomFilter = struct {
    bits: []u8,
    bit_count: usize,
    hash_count: u8,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, expected_items: usize, false_positive_rate: f64) !Self {
        const bit_count = optimalBitCount(expected_items, false_positive_rate);
        const hash_count = optimalHashCount(bit_count, expected_items);

        const byte_count = (bit_count + 7) / 8;
        const bits = try allocator.alloc(u8, byte_count);
        @memset(bits, 0);

        return Self{
            .bits = bits,
            .bit_count = bit_count,
            .hash_count = @intCast(hash_count),
        };
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.bits);
    }

    pub fn add(self: *Self, key: []const u8) void {
        const hash1 = hash(key, 0);
        const hash2 = hash(key, hash1);

        for (0..self.hash_count) |i| {
            const bit_index = (hash1 +% @as(u64, @intCast(i)) *% hash2) % self.bit_count;
            self.setBit(bit_index);
        }
    }

    pub fn contains(self: *const Self, key: []const u8) bool {
        const hash1 = hash(key, 0);
        const hash2 = hash(key, hash1);

        for (0..self.hash_count) |i| {
            const bit_index = (hash1 +% @as(u64, @intCast(i)) *% hash2) % self.bit_count;
            if (!self.getBit(bit_index)) {
                return false;
            }
        }

        return true;
    }

    fn setBit(self: *Self, index: usize) void {
        const byte_index = index / 8;
        const bit_index = @as(u3, @intCast(index % 8));
        self.bits[byte_index] |= (@as(u8, 1) << bit_index);
    }

    fn getBit(self: *const Self, index: usize) bool {
        const byte_index = index / 8;
        const bit_index = @as(u3, @intCast(index % 8));
        return (self.bits[byte_index] & (@as(u8, 1) << bit_index)) != 0;
    }

    fn optimalBitCount(expected_items: usize, false_positive_rate: f64) usize {
        const ln2 = @log(2.0);
        const m = -1.0 * @as(f64, @floatFromInt(expected_items)) * @log(false_positive_rate) / (ln2 * ln2);
        return @intFromFloat(@max(1.0, m));
    }

    fn optimalHashCount(bit_count: usize, expected_items: usize) usize {
        const ln2 = @log(2.0);
        const k = @as(f64, @floatFromInt(bit_count)) / @as(f64, @floatFromInt(expected_items)) * ln2;
        return @intFromFloat(@max(1.0, @round(k)));
    }

    fn hash(key: []const u8, seed: u64) u64 {
        var h: u64 = seed;
        const m: u64 = 0xc6a4a7935bd1e995;
        const r: u6 = 47;

        h ^= @as(u64, @intCast(key.len)) *% m;

        var i: usize = 0;
        while (i + 8 <= key.len) : (i += 8) {
            var k = std.mem.readInt(u64, key[i .. i + 8][0..8], .little);
            k *%= m;
            k ^= k >> r;
            k *%= m;
            h ^= k;
            h *%= m;
        }

        if (key.len > i) {
            const remaining = key.len - i;
            var k: u64 = 0;
            for (0..remaining) |j| {
                k |= @as(u64, key[i + j]) << @intCast(j * 8);
            }
            h ^= k;
            h *%= m;
        }

        h ^= h >> r;
        h *%= m;
        h ^= h >> r;

        return h;
    }

    pub fn serialize(self: *const Self, buf: []u8) !usize {
        if (buf.len < self.serializedSize()) {
            return error.BufferTooSmall;
        }

        var offset: usize = 0;

        std.mem.writeInt(u64, buf[offset .. offset + 8][0..8], self.bit_count, .little);
        offset += 8;

        buf[offset] = self.hash_count;
        offset += 1;

        @memcpy(buf[offset .. offset + self.bits.len], self.bits);
        offset += self.bits.len;

        return offset;
    }

    pub fn deserialize(buf: []const u8, allocator: std.mem.Allocator) !Self {
        if (buf.len < 9) {
            return error.BufferTooSmall;
        }

        var offset: usize = 0;

        const bit_count = std.mem.readInt(u64, buf[offset .. offset + 8][0..8], .little);
        offset += 8;

        const hash_count = buf[offset];
        offset += 1;

        const byte_count = (bit_count + 7) / 8;
        if (buf.len < offset + byte_count) {
            return error.BufferTooSmall;
        }

        const bits = try allocator.dupe(u8, buf[offset .. offset + byte_count]);

        return Self{
            .bits = bits,
            .bit_count = bit_count,
            .hash_count = hash_count,
        };
    }

    pub fn serializedSize(self: *const Self) usize {
        return 8 + 1 + self.bits.len;
    }
};
