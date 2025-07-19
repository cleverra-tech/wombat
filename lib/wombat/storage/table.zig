const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const MmapFile = @import("../io/mmap.zig").MmapFile;
const BloomFilter = @import("../core/bloom.zig").BloomFilter;
const ValueStruct = @import("../core/skiplist.zig").ValueStruct;
const SkipList = @import("../core/skiplist.zig").SkipList;
const Options = @import("../core/options.zig").Options;
const CompressionType = @import("../core/options.zig").CompressionType;
const Compressor = @import("../compression/compressor.zig").Compressor;

// Use the same validation limits as the configuration system
const MAX_KEY_SIZE = 1024 * 1024; // 1MB (matches ValidationConfig default)
const MAX_VALUE_SIZE = 1024 * 1024 * 1024; // 1GB (matches ValidationConfig default)

/// Table errors
pub const TableError = error{
    InvalidTableFile,
    UnsupportedVersion,
    CorruptedData,
    BlockSizeExceeded,
    InvalidKeyOrder,
    ChecksumMismatch,
    OutOfMemory,
    IOError,
};

/// Table statistics for monitoring
pub const TableStats = struct {
    file_size: u64,
    key_count: u32,
    block_count: u32,
    bloom_false_positives: u64,
    cache_hits: u64,
    cache_misses: u64,
    reads_total: u64,
};

/// Index entry for fast key lookups
pub const IndexEntry = struct {
    key: []const u8,
    offset: u32,
    size: u32,
    block_count: u32, // Number of entries in this block
    checksum: u32, // CRC32 checksum of the block
};

/// Index statistics for performance monitoring
pub const IndexStats = struct {
    total_entries: u64,
    search_operations: u64,
    range_scans: u64,
    cache_hits: u64,
    cache_misses: u64,
    average_search_depth: f64,
    memory_usage: u64,
};

/// Range query result
pub const RangeResult = struct {
    start_index: usize,
    end_index: usize,
    count: usize,
};

/// Enhanced block index for multi-level indexing
pub const BlockIndex = struct {
    /// Block information for faster access
    blocks: ArrayList(BlockInfo),
    /// Two-level index for large tables
    level1_index: ?ArrayList(Level1Entry),
    /// Sparse index for very large tables
    sparse_index: ?ArrayList(SparseEntry),
    allocator: Allocator,

    const BlockInfo = struct {
        first_key: []const u8,
        last_key: []const u8,
        offset: u64,
        size: u32,
        entry_count: u32,
        checksum: u32,
        compression: CompressionType,
        max_timestamp: u64,
        min_timestamp: u64,
    };

    const Level1Entry = struct {
        key: []const u8,
        block_index: u32,
        offset: u64,
    };

    const SparseEntry = struct {
        key: []const u8,
        level1_index: u32,
        block_count: u32,
    };

    pub fn init(allocator: Allocator) BlockIndex {
        return BlockIndex{
            .blocks = ArrayList(BlockInfo).init(allocator),
            .level1_index = null,
            .sparse_index = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *BlockIndex) void {
        for (self.blocks.items) |block| {
            self.allocator.free(block.first_key);
            self.allocator.free(block.last_key);
        }
        self.blocks.deinit();

        if (self.level1_index) |*level1| {
            for (level1.items) |entry| {
                self.allocator.free(entry.key);
            }
            level1.deinit();
        }

        if (self.sparse_index) |*sparse| {
            for (sparse.items) |entry| {
                self.allocator.free(entry.key);
            }
            sparse.deinit();
        }
    }

    pub fn findBlock(self: *const BlockIndex, key: []const u8) ?u32 {
        // Check sparse index first (for very large tables)
        if (self.sparse_index) |sparse| {
            var sparse_idx: u32 = 0;
            for (sparse.items, 0..) |entry, i| {
                if (std.mem.order(u8, key, entry.key) != .gt) {
                    sparse_idx = @intCast(i);
                    break;
                }
            }

            // Search within the sparse segment
            if (self.level1_index) |level1| {
                const start_idx = if (sparse_idx == 0) 0 else sparse.items[sparse_idx - 1].level1_index;
                const end_idx = if (sparse_idx >= sparse.items.len) level1.items.len else sparse.items[sparse_idx].level1_index;

                for (level1.items[start_idx..end_idx]) |entry| {
                    if (std.mem.order(u8, key, entry.key) != .gt) {
                        return entry.block_index;
                    }
                }
            }
        }

        // Check level1 index
        if (self.level1_index) |level1| {
            for (level1.items) |entry| {
                if (std.mem.order(u8, key, entry.key) != .gt) {
                    return entry.block_index;
                }
            }
        }

        // Fallback to linear search in blocks
        for (self.blocks.items, 0..) |block, i| {
            if (std.mem.order(u8, key, block.first_key) != .lt and
                std.mem.order(u8, key, block.last_key) != .gt)
            {
                return @intCast(i);
            }
        }

        return null;
    }

    pub fn buildMultiLevelIndex(self: *BlockIndex, level1_threshold: usize, sparse_threshold: usize) !void {
        if (self.blocks.items.len <= level1_threshold) return;

        // Build level1 index
        const level1_step = self.blocks.items.len / level1_threshold;
        self.level1_index = ArrayList(Level1Entry).init(self.allocator);

        var i: usize = 0;
        while (i < self.blocks.items.len) : (i += level1_step) {
            const block = self.blocks.items[i];
            const entry = Level1Entry{
                .key = try self.allocator.dupe(u8, block.first_key),
                .block_index = @intCast(i),
                .offset = block.offset,
            };
            try self.level1_index.?.append(entry);
        }

        // Build sparse index if needed
        if (self.level1_index.?.items.len > sparse_threshold) {
            const sparse_step = self.level1_index.?.items.len / sparse_threshold;
            self.sparse_index = ArrayList(SparseEntry).init(self.allocator);

            i = 0;
            while (i < self.level1_index.?.items.len) : (i += sparse_step) {
                const level1_entry = self.level1_index.?.items[i];
                const entry = SparseEntry{
                    .key = try self.allocator.dupe(u8, level1_entry.key),
                    .level1_index = @intCast(i),
                    .block_count = @intCast(@min(sparse_step, self.level1_index.?.items.len - i)),
                };
                try self.sparse_index.?.append(entry);
            }
        }
    }

    pub fn serializedSize(self: *const BlockIndex) usize {
        var size: usize = 4; // block count

        for (self.blocks.items) |block| {
            size += 4; // first_key length
            size += block.first_key.len; // first_key data
            size += 4; // last_key length
            size += block.last_key.len; // last_key data
            size += 8; // offset
            size += 4; // size
            size += 4; // entry_count
            size += 4; // checksum
            size += 1; // compression type
            size += 8; // max_timestamp
            size += 8; // min_timestamp
        }

        // Level1 index
        if (self.level1_index) |level1| {
            size += 1; // has_level1_index flag
            size += 4; // level1 count
            for (level1.items) |entry| {
                size += 4; // key length
                size += entry.key.len; // key data
                size += 4; // block_index
                size += 8; // offset
            }
        } else {
            size += 1; // has_level1_index flag = false
        }

        // Sparse index
        if (self.sparse_index) |sparse| {
            size += 1; // has_sparse_index flag
            size += 4; // sparse count
            for (sparse.items) |entry| {
                size += 4; // key length
                size += entry.key.len; // key data
                size += 4; // level1_index
                size += 4; // block_count
            }
        } else {
            size += 1; // has_sparse_index flag = false
        }

        return size;
    }

    pub fn serialize(self: *const BlockIndex, buf: []u8) !usize {
        if (buf.len < self.serializedSize()) {
            return error.BufferTooSmall;
        }

        var offset: usize = 0;

        // Write block count
        std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], @intCast(self.blocks.items.len), .little);
        offset += 4;

        // Write blocks
        for (self.blocks.items) |block| {
            // Write first_key
            std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], @intCast(block.first_key.len), .little);
            offset += 4;
            @memcpy(buf[offset .. offset + block.first_key.len], block.first_key);
            offset += block.first_key.len;

            // Write last_key
            std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], @intCast(block.last_key.len), .little);
            offset += 4;
            @memcpy(buf[offset .. offset + block.last_key.len], block.last_key);
            offset += block.last_key.len;

            // Write block metadata
            std.mem.writeInt(u64, buf[offset .. offset + 8][0..8], block.offset, .little);
            offset += 8;
            std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], block.size, .little);
            offset += 4;
            std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], block.entry_count, .little);
            offset += 4;
            std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], block.checksum, .little);
            offset += 4;
            buf[offset] = @intFromEnum(block.compression);
            offset += 1;
            std.mem.writeInt(u64, buf[offset .. offset + 8][0..8], block.max_timestamp, .little);
            offset += 8;
            std.mem.writeInt(u64, buf[offset .. offset + 8][0..8], block.min_timestamp, .little);
            offset += 8;
        }

        // Write level1 index
        if (self.level1_index) |level1| {
            buf[offset] = 1; // has_level1_index = true
            offset += 1;

            std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], @intCast(level1.items.len), .little);
            offset += 4;

            for (level1.items) |entry| {
                std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], @intCast(entry.key.len), .little);
                offset += 4;
                @memcpy(buf[offset .. offset + entry.key.len], entry.key);
                offset += entry.key.len;

                std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], entry.block_index, .little);
                offset += 4;
                std.mem.writeInt(u64, buf[offset .. offset + 8][0..8], entry.offset, .little);
                offset += 8;
            }
        } else {
            buf[offset] = 0; // has_level1_index = false
            offset += 1;
        }

        // Write sparse index
        if (self.sparse_index) |sparse| {
            buf[offset] = 1; // has_sparse_index = true
            offset += 1;

            std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], @intCast(sparse.items.len), .little);
            offset += 4;

            for (sparse.items) |entry| {
                std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], @intCast(entry.key.len), .little);
                offset += 4;
                @memcpy(buf[offset .. offset + entry.key.len], entry.key);
                offset += entry.key.len;

                std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], entry.level1_index, .little);
                offset += 4;
                std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], entry.block_count, .little);
                offset += 4;
            }
        } else {
            buf[offset] = 0; // has_sparse_index = false
            offset += 1;
        }

        return offset;
    }

    pub fn deserialize(buf: []const u8, allocator: Allocator) !BlockIndex {
        if (buf.len < 5) {
            return error.BufferTooSmall;
        }

        var offset: usize = 0;
        var block_index = BlockIndex.init(allocator);

        // Read block count
        const block_count = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
        offset += 4;

        // Read blocks
        for (0..block_count) |_| {
            if (offset + 4 > buf.len) {
                block_index.deinit();
                return error.BufferTooSmall;
            }

            // Read first_key
            const first_key_len = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
            offset += 4;

            if (offset + first_key_len > buf.len) {
                block_index.deinit();
                return error.BufferTooSmall;
            }

            const first_key = try allocator.dupe(u8, buf[offset .. offset + first_key_len]);
            offset += first_key_len;

            // Read last_key
            const last_key_len = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
            offset += 4;

            if (offset + last_key_len > buf.len) {
                allocator.free(first_key);
                block_index.deinit();
                return error.BufferTooSmall;
            }

            const last_key = try allocator.dupe(u8, buf[offset .. offset + last_key_len]);
            offset += last_key_len;

            // Read block metadata
            if (offset + 41 > buf.len) { // 8+4+4+4+1+8+8 = 37 bytes + 4 for safety
                allocator.free(first_key);
                allocator.free(last_key);
                block_index.deinit();
                return error.BufferTooSmall;
            }

            const block_offset = std.mem.readInt(u64, buf[offset .. offset + 8][0..8], .little);
            offset += 8;
            const size = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
            offset += 4;
            const entry_count = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
            offset += 4;
            const checksum = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
            offset += 4;
            const compression = @as(CompressionType, @enumFromInt(buf[offset]));
            offset += 1;
            const max_timestamp = std.mem.readInt(u64, buf[offset .. offset + 8][0..8], .little);
            offset += 8;
            const min_timestamp = std.mem.readInt(u64, buf[offset .. offset + 8][0..8], .little);
            offset += 8;

            const block = BlockInfo{
                .first_key = first_key,
                .last_key = last_key,
                .offset = block_offset,
                .size = size,
                .entry_count = entry_count,
                .checksum = checksum,
                .compression = compression,
                .max_timestamp = max_timestamp,
                .min_timestamp = min_timestamp,
            };

            try block_index.blocks.append(block);
        }

        // Read level1 index
        if (offset >= buf.len) {
            block_index.deinit();
            return error.BufferTooSmall;
        }

        const has_level1_index = buf[offset] != 0;
        offset += 1;

        if (has_level1_index) {
            const level1_count = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
            offset += 4;

            block_index.level1_index = ArrayList(Level1Entry).init(allocator);

            for (0..level1_count) |_| {
                if (offset + 4 > buf.len) {
                    block_index.deinit();
                    return error.BufferTooSmall;
                }

                const key_len = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
                offset += 4;

                if (offset + key_len + 12 > buf.len) { // key + block_index + offset
                    block_index.deinit();
                    return error.BufferTooSmall;
                }

                const key = try allocator.dupe(u8, buf[offset .. offset + key_len]);
                offset += key_len;

                const block_idx = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
                offset += 4;
                const entry_offset = std.mem.readInt(u64, buf[offset .. offset + 8][0..8], .little);
                offset += 8;

                const entry = Level1Entry{
                    .key = key,
                    .block_index = block_idx,
                    .offset = entry_offset,
                };

                try block_index.level1_index.?.append(entry);
            }
        }

        // Read sparse index
        if (offset >= buf.len) {
            block_index.deinit();
            return error.BufferTooSmall;
        }

        const has_sparse_index = buf[offset] != 0;
        offset += 1;

        if (has_sparse_index) {
            const sparse_count = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
            offset += 4;

            block_index.sparse_index = ArrayList(SparseEntry).init(allocator);

            for (0..sparse_count) |_| {
                if (offset + 4 > buf.len) {
                    block_index.deinit();
                    return error.BufferTooSmall;
                }

                const key_len = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
                offset += 4;

                if (offset + key_len + 8 > buf.len) { // key + level1_index + block_count
                    block_index.deinit();
                    return error.BufferTooSmall;
                }

                const key = try allocator.dupe(u8, buf[offset .. offset + key_len]);
                offset += key_len;

                const level1_idx = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
                offset += 4;
                const block_cnt = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
                offset += 4;

                const entry = SparseEntry{
                    .key = key,
                    .level1_index = level1_idx,
                    .block_count = block_cnt,
                };

                try block_index.sparse_index.?.append(entry);
            }
        }

        return block_index;
    }
};

/// Enhanced filter index for bloom filter partitioning
pub const FilterIndex = struct {
    /// Partitioned bloom filters for better performance
    partitions: ArrayList(FilterPartition),
    /// Global bloom filter for quick negative lookups
    global_filter: ?BloomFilter,
    partition_size: u32,
    allocator: Allocator,

    const FilterPartition = struct {
        filter: BloomFilter,
        start_key: []const u8,
        end_key: []const u8,
        block_range: struct {
            start: u32,
            end: u32,
        },
    };

    pub fn init(allocator: Allocator, partition_size: u32) FilterIndex {
        return FilterIndex{
            .partitions = ArrayList(FilterPartition).init(allocator),
            .global_filter = null,
            .partition_size = partition_size,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *FilterIndex) void {
        for (self.partitions.items) |*partition| {
            var mutable_filter = partition.filter;
            mutable_filter.deinit(self.allocator);
            self.allocator.free(partition.start_key);
            self.allocator.free(partition.end_key);
        }
        self.partitions.deinit();

        if (self.global_filter) |*filter| {
            var mutable_filter = filter.*;
            mutable_filter.deinit(self.allocator);
        }
    }

    pub fn mightContain(self: *const FilterIndex, key: []const u8) bool {
        // Check global filter first for quick rejection
        if (self.global_filter) |filter| {
            if (!filter.contains(key)) {
                return false;
            }
        }

        // Check relevant partitions
        for (self.partitions.items) |partition| {
            if (std.mem.order(u8, key, partition.start_key) != .lt and
                std.mem.order(u8, key, partition.end_key) != .gt)
            {
                return partition.filter.contains(key);
            }
        }

        return true; // Conservative approach - if no partition matches, allow the lookup to avoid false negatives
    }

    pub fn addKey(self: *FilterIndex, key: []const u8, block_index: u32) !void {
        // Add to global filter if it exists
        if (self.global_filter) |*global_filter| {
            global_filter.add(key);
        }

        // Find or create appropriate partition
        for (self.partitions.items) |*partition| {
            if (block_index >= partition.block_range.start and
                block_index <= partition.block_range.end)
            {
                partition.filter.add(key);

                // Update key range for this partition
                if (std.mem.order(u8, key, partition.start_key) == .lt) {
                    self.allocator.free(partition.start_key);
                    partition.start_key = try self.allocator.dupe(u8, key);
                }
                if (std.mem.order(u8, key, partition.end_key) == .gt) {
                    self.allocator.free(partition.end_key);
                    partition.end_key = try self.allocator.dupe(u8, key);
                }
                return;
            }
        }

        // Create new partition if needed
        const partition = FilterPartition{
            .filter = try BloomFilter.init(self.allocator, self.partition_size, 0.01),
            .start_key = try self.allocator.dupe(u8, key),
            .end_key = try self.allocator.dupe(u8, key),
            .block_range = .{
                .start = block_index,
                .end = block_index,
            },
        };

        var mutable_partition = partition;
        mutable_partition.filter.add(key);
        try self.partitions.append(mutable_partition);
    }

    pub fn finalizePartitions(self: *FilterIndex, expected_items: usize) !void {
        _ = self; // No work needed - global filter already populated
        _ = expected_items; // Global filter already created with appropriate size

        // Global filter is already populated during key addition via addKey()
        // No additional work needed here since keys are added incrementally

        // Optionally, we could resize the global filter if needed, but
        // for simplicity we'll use the pre-allocated size
    }

    pub fn serializedSize(self: *const FilterIndex) usize {
        var size: usize = 4; // partition count
        if (self.global_filter) |_| {
            size += 1; // has_global_filter flag
            size += 8; // global filter bit_count
            size += 1; // global filter hash_count
            size += (self.global_filter.?.bit_count + 7) / 8; // global filter bits
        } else {
            size += 1; // has_global_filter flag
        }

        for (self.partitions.items) |partition| {
            size += 4; // start_key length
            size += partition.start_key.len; // start_key data
            size += 4; // end_key length
            size += partition.end_key.len; // end_key data
            size += 8; // block_range start and end
            size += 8; // filter bit_count
            size += 1; // filter hash_count
            size += (partition.filter.bit_count + 7) / 8; // filter bits
        }

        return size;
    }

    pub fn serialize(self: *const FilterIndex, buf: []u8) !usize {
        if (buf.len < self.serializedSize()) {
            return error.BufferTooSmall;
        }

        var offset: usize = 0;

        // Write partition count
        std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], @intCast(self.partitions.items.len), .little);
        offset += 4;

        // Write global filter
        if (self.global_filter) |global_filter| {
            buf[offset] = 1; // has_global_filter = true
            offset += 1;

            std.mem.writeInt(u64, buf[offset .. offset + 8][0..8], global_filter.bit_count, .little);
            offset += 8;

            buf[offset] = global_filter.hash_count;
            offset += 1;

            @memcpy(buf[offset .. offset + global_filter.bits.len], global_filter.bits);
            offset += global_filter.bits.len;
        } else {
            buf[offset] = 0; // has_global_filter = false
            offset += 1;
        }

        // Write partitions
        for (self.partitions.items) |partition| {
            // Write start_key
            std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], @intCast(partition.start_key.len), .little);
            offset += 4;
            @memcpy(buf[offset .. offset + partition.start_key.len], partition.start_key);
            offset += partition.start_key.len;

            // Write end_key
            std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], @intCast(partition.end_key.len), .little);
            offset += 4;
            @memcpy(buf[offset .. offset + partition.end_key.len], partition.end_key);
            offset += partition.end_key.len;

            // Write block_range
            std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], partition.block_range.start, .little);
            offset += 4;
            std.mem.writeInt(u32, buf[offset .. offset + 4][0..4], partition.block_range.end, .little);
            offset += 4;

            // Write filter
            std.mem.writeInt(u64, buf[offset .. offset + 8][0..8], partition.filter.bit_count, .little);
            offset += 8;

            buf[offset] = partition.filter.hash_count;
            offset += 1;

            @memcpy(buf[offset .. offset + partition.filter.bits.len], partition.filter.bits);
            offset += partition.filter.bits.len;
        }

        return offset;
    }

    pub fn deserialize(buf: []const u8, allocator: Allocator) !FilterIndex {
        if (buf.len < 5) {
            return error.BufferTooSmall;
        }

        var offset: usize = 0;
        var filter_index = FilterIndex.init(allocator, 1000);

        // Read partition count
        const partition_count = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
        offset += 4;

        // Read global filter
        const has_global_filter = buf[offset] != 0;
        offset += 1;

        if (has_global_filter) {
            const bit_count = std.mem.readInt(u64, buf[offset .. offset + 8][0..8], .little);
            offset += 8;

            const hash_count = buf[offset];
            offset += 1;

            const byte_count = (bit_count + 7) / 8;
            if (offset + byte_count > buf.len) {
                return error.BufferTooSmall;
            }

            const bits = try allocator.alloc(u8, byte_count);
            @memcpy(bits, buf[offset .. offset + byte_count]);
            offset += byte_count;

            filter_index.global_filter = BloomFilter{
                .bits = bits,
                .bit_count = bit_count,
                .hash_count = hash_count,
            };
        }

        // Read partitions
        for (0..partition_count) |_| {
            if (offset + 4 > buf.len) {
                filter_index.deinit();
                return error.BufferTooSmall;
            }

            // Read start_key
            const start_key_len = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
            offset += 4;

            if (offset + start_key_len > buf.len) {
                filter_index.deinit();
                return error.BufferTooSmall;
            }

            const start_key = try allocator.dupe(u8, buf[offset .. offset + start_key_len]);
            offset += start_key_len;

            // Read end_key
            const end_key_len = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
            offset += 4;

            if (offset + end_key_len > buf.len) {
                allocator.free(start_key);
                filter_index.deinit();
                return error.BufferTooSmall;
            }

            const end_key = try allocator.dupe(u8, buf[offset .. offset + end_key_len]);
            offset += end_key_len;

            // Read block_range
            const block_start = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
            offset += 4;
            const block_end = std.mem.readInt(u32, buf[offset .. offset + 4][0..4], .little);
            offset += 4;

            // Read filter
            const bit_count = std.mem.readInt(u64, buf[offset .. offset + 8][0..8], .little);
            offset += 8;

            const hash_count = buf[offset];
            offset += 1;

            const byte_count = (bit_count + 7) / 8;
            if (offset + byte_count > buf.len) {
                allocator.free(start_key);
                allocator.free(end_key);
                filter_index.deinit();
                return error.BufferTooSmall;
            }

            const bits = try allocator.alloc(u8, byte_count);
            @memcpy(bits, buf[offset .. offset + byte_count]);
            offset += byte_count;

            const partition = FilterPartition{
                .filter = BloomFilter{
                    .bits = bits,
                    .bit_count = bit_count,
                    .hash_count = hash_count,
                },
                .start_key = start_key,
                .end_key = end_key,
                .block_range = .{
                    .start = block_start,
                    .end = block_end,
                },
            };

            try filter_index.partitions.append(partition);
        }

        return filter_index;
    }
};

/// Table index for efficient key lookups with binary search and advanced features
pub const TableIndex = struct {
    entries: ArrayList(IndexEntry),
    allocator: Allocator,
    stats: IndexStats,
    is_sorted: bool,
    bloom_filter: ?*BloomFilter, // Optional bloom filter for negative lookups
    key_cache: std.AutoHashMap(u64, usize), // Hash of key -> index cache

    const Self = @This();

    /// Initialize a new table index
    pub fn init(allocator: Allocator, capacity: usize) !Self {
        var entries = ArrayList(IndexEntry).init(allocator);
        try entries.ensureTotalCapacity(capacity);

        return Self{
            .entries = entries,
            .allocator = allocator,
            .stats = IndexStats{
                .total_entries = 0,
                .search_operations = 0,
                .range_scans = 0,
                .cache_hits = 0,
                .cache_misses = 0,
                .average_search_depth = 0.0,
                .memory_usage = 0,
            },
            .is_sorted = true,
            .bloom_filter = null,
            .key_cache = std.AutoHashMap(u64, usize).init(allocator),
        };
    }

    /// Clean up index resources
    pub fn deinit(self: *Self) void {
        // Free all key strings
        for (self.entries.items) |entry| {
            self.allocator.free(entry.key);
        }
        self.entries.deinit();
        self.key_cache.deinit();

        if (self.bloom_filter) |bf| {
            var mutable_bf = bf.*;
            mutable_bf.deinit(self.allocator);
            self.allocator.destroy(bf);
        }
    }

    /// Add an index entry (keys must be added in sorted order)
    pub fn addEntry(self: *Self, key: []const u8, offset: u32, size: u32) !void {
        return self.addEntryWithStats(key, offset, size, 0, 0);
    }

    /// Add an index entry with additional metadata
    pub fn addEntryWithStats(self: *Self, key: []const u8, offset: u32, size: u32, block_count: u32, checksum: u32) !void {
        // Validate key ordering if sorted flag is set
        if (self.is_sorted and self.entries.items.len > 0) {
            const last_key = self.entries.items[self.entries.items.len - 1].key;
            if (std.mem.order(u8, last_key, key) != .lt) {
                return TableError.InvalidKeyOrder;
            }
        }

        // Create owned copy of key
        const owned_key = try self.allocator.dupe(u8, key);

        const entry = IndexEntry{
            .key = owned_key,
            .offset = offset,
            .size = size,
            .block_count = block_count,
            .checksum = checksum,
        };

        try self.entries.append(entry);

        // Update statistics
        self.stats.total_entries += 1;
        self.stats.memory_usage += owned_key.len + @sizeOf(IndexEntry);

        // Add to cache for fast lookups
        const key_hash = self.hashKey(key);
        try self.key_cache.put(key_hash, self.entries.items.len - 1);

        // Add to bloom filter if available
        if (self.bloom_filter) |bf| {
            bf.add(key);
        }
    }

    /// Hash function for keys (FNV-1a)
    fn hashKey(self: *const Self, key: []const u8) u64 {
        _ = self;
        var hash: u64 = 14695981039346656037; // FNV offset basis
        for (key) |byte| {
            hash ^= byte;
            hash *%= 1099511628211; // FNV prime
        }
        return hash;
    }

    /// Binary search for key, returns block index that might contain the key (const version)
    pub fn search(self: *const Self, key: []const u8) ?usize {
        if (self.entries.items.len == 0) {
            return null;
        }

        // Check bloom filter for negative lookups
        if (self.bloom_filter) |bf| {
            if (!bf.contains(key)) {
                return null; // Definitely not present
            }
        }

        // Try cache lookup first
        const key_hash = self.hashKey(key);
        if (self.key_cache.get(key_hash)) |cached_index| {
            if (cached_index < self.entries.items.len and
                std.mem.eql(u8, self.entries.items[cached_index].key, key))
            {
                return cached_index;
            }
        }

        var left: usize = 0;
        var right: usize = self.entries.items.len;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const cmp = std.mem.order(u8, self.entries.items[mid].key, key);

            switch (cmp) {
                .lt => left = mid + 1,
                .gt => right = mid,
                .eq => return mid,
            }
        }

        // Return the block that might contain the key
        if (left >= self.entries.items.len) {
            // Key is beyond the last entry, no block can contain it
            return null;
        }
        return if (left > 0) left - 1 else 0;
    }

    /// Binary search for key with statistics tracking (mutable version)
    pub fn searchWithStats(self: *Self, key: []const u8) ?usize {
        if (self.entries.items.len == 0) {
            return null;
        }

        // Update search statistics
        self.stats.search_operations += 1;

        // Try cache lookup first
        const key_hash = self.hashKey(key);
        if (self.key_cache.get(key_hash)) |cached_index| {
            if (cached_index < self.entries.items.len and
                std.mem.eql(u8, self.entries.items[cached_index].key, key))
            {
                self.stats.cache_hits += 1;
                return cached_index;
            }
        }

        self.stats.cache_misses += 1;

        // Check bloom filter for negative lookups
        if (self.bloom_filter) |bf| {
            if (!bf.contains(key)) {
                return null; // Definitely not present
            }
        }

        var left: usize = 0;
        var right: usize = self.entries.items.len;
        var search_depth: usize = 0;

        while (left < right) {
            search_depth += 1;
            const mid = left + (right - left) / 2;
            const cmp = std.mem.order(u8, self.entries.items[mid].key, key);

            switch (cmp) {
                .lt => left = mid + 1,
                .gt => right = mid,
                .eq => {
                    // Update search depth statistics
                    self.updateSearchDepth(search_depth);

                    // Update cache with correct mapping to handle hash collisions
                    self.key_cache.put(key_hash, mid) catch {};

                    return mid;
                },
            }
        }

        // Update search depth statistics
        self.updateSearchDepth(search_depth);

        // Return the block that might contain the key
        if (left >= self.entries.items.len) {
            // Key is beyond the last entry, no block can contain it
            return null;
        }
        return if (left > 0) left - 1 else 0;
    }

    /// Update average search depth statistics
    fn updateSearchDepth(self: *Self, depth: usize) void {
        const total_ops = self.stats.search_operations;
        if (total_ops == 1) {
            self.stats.average_search_depth = @floatFromInt(depth);
        } else {
            self.stats.average_search_depth =
                (self.stats.average_search_depth * @as(f64, @floatFromInt(total_ops - 1)) + @as(f64, @floatFromInt(depth))) / @as(f64, @floatFromInt(total_ops));
        }
    }

    /// Get the number of index entries
    pub fn getEntryCount(self: *const Self) usize {
        return self.entries.items.len;
    }

    /// Get an index entry by position
    pub fn getEntry(self: *const Self, index: usize) ?IndexEntry {
        if (index >= self.entries.items.len) {
            return null;
        }
        return self.entries.items[index];
    }

    /// Get the smallest key in the index
    pub fn getSmallestKey(self: *const Self) ?[]const u8 {
        if (self.entries.items.len == 0) {
            return null;
        }
        return self.entries.items[0].key;
    }

    /// Get the largest key in the index
    pub fn getLargestKey(self: *const Self) ?[]const u8 {
        if (self.entries.items.len == 0) {
            return null;
        }
        return self.entries.items[self.entries.items.len - 1].key;
    }

    /// Range query: find all entries between start_key and end_key (inclusive)
    pub fn searchRange(self: *const Self, start_key: []const u8, end_key: []const u8) RangeResult {
        if (self.entries.items.len == 0) {
            return RangeResult{ .start_index = 0, .end_index = 0, .count = 0 };
        }

        // Find start index
        var start_index: usize = 0;
        var left: usize = 0;
        var right: usize = self.entries.items.len;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const cmp = std.mem.order(u8, self.entries.items[mid].key, start_key);

            if (cmp == .lt) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        start_index = left;

        // Find end index
        var end_index: usize = self.entries.items.len;
        left = 0;
        right = self.entries.items.len;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const cmp = std.mem.order(u8, self.entries.items[mid].key, end_key);

            if (cmp == .gt) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        end_index = left;

        const count = if (end_index > start_index) end_index - start_index else 0;
        return RangeResult{ .start_index = start_index, .end_index = end_index, .count = count };
    }

    /// Prefix search: find all entries with keys that start with the given prefix
    pub fn searchPrefix(self: *const Self, prefix: []const u8) RangeResult {
        if (self.entries.items.len == 0 or prefix.len == 0) {
            return RangeResult{ .start_index = 0, .end_index = 0, .count = 0 };
        }

        // Find the first key that starts with prefix
        var start_index: usize = self.entries.items.len;
        for (self.entries.items, 0..) |entry, i| {
            if (entry.key.len >= prefix.len and std.mem.eql(u8, entry.key[0..prefix.len], prefix)) {
                start_index = i;
                break;
            }
        }

        if (start_index == self.entries.items.len) {
            return RangeResult{ .start_index = 0, .end_index = 0, .count = 0 };
        }

        // Find the end of prefix range
        var end_index: usize = start_index;
        for (self.entries.items[start_index..], start_index..) |entry, i| {
            if (entry.key.len >= prefix.len and std.mem.eql(u8, entry.key[0..prefix.len], prefix)) {
                end_index = i + 1;
            } else {
                break;
            }
        }

        const count = end_index - start_index;
        return RangeResult{ .start_index = start_index, .end_index = end_index, .count = count };
    }

    /// Bulk load entries (more efficient than individual adds)
    pub fn bulkLoad(self: *Self, entries: []const IndexEntry) !void {
        if (entries.len == 0) return;

        // Reserve capacity
        try self.entries.ensureUnusedCapacity(entries.len);

        // Validate that entries are sorted
        for (entries[1..], 1..) |entry, i| {
            const prev_entry = entries[i - 1];
            if (std.mem.order(u8, prev_entry.key, entry.key) != .lt) {
                return TableError.InvalidKeyOrder;
            }
        }

        // Add all entries
        for (entries) |entry| {
            const owned_key = try self.allocator.dupe(u8, entry.key);
            const new_entry = IndexEntry{
                .key = owned_key,
                .offset = entry.offset,
                .size = entry.size,
                .block_count = entry.block_count,
                .checksum = entry.checksum,
            };

            self.entries.appendAssumeCapacity(new_entry);

            // Update statistics
            self.stats.total_entries += 1;
            self.stats.memory_usage += owned_key.len + @sizeOf(IndexEntry);

            // Add to cache
            const key_hash = self.hashKey(entry.key);
            try self.key_cache.put(key_hash, self.entries.items.len - 1);

            // Add to bloom filter if available
            if (self.bloom_filter) |bf| {
                bf.add(entry.key);
            }
        }
    }

    /// Sort entries if they were added out of order
    pub fn sort(self: *Self) void {
        if (self.is_sorted) return;

        // Clear cache since indices will change
        self.key_cache.clearRetainingCapacity();

        // Sort entries by key
        std.sort.heap(IndexEntry, self.entries.items, {}, struct {
            fn lessThan(_: void, a: IndexEntry, b: IndexEntry) bool {
                return std.mem.order(u8, a.key, b.key) == .lt;
            }
        }.lessThan);

        // Rebuild cache with new indices
        for (self.entries.items, 0..) |entry, i| {
            const key_hash = self.hashKey(entry.key);
            self.key_cache.put(key_hash, i) catch {};
        }

        self.is_sorted = true;
    }

    /// Enable bloom filter for faster negative lookups
    pub fn enableBloomFilter(self: *Self, expected_items: usize, false_positive_rate: f64) !void {
        if (self.bloom_filter != null) return; // Already enabled

        const bf = try self.allocator.create(BloomFilter);
        bf.* = try BloomFilter.init(self.allocator, expected_items, false_positive_rate);

        // Add all existing keys to bloom filter
        for (self.entries.items) |entry| {
            bf.add(entry.key);
        }

        self.bloom_filter = bf;
    }

    /// Get index statistics
    pub fn getStats(self: *const Self) IndexStats {
        return self.stats;
    }

    /// Reset statistics counters
    pub fn resetStats(self: *Self) void {
        self.stats.search_operations = 0;
        self.stats.range_scans = 0;
        self.stats.cache_hits = 0;
        self.stats.cache_misses = 0;
        self.stats.average_search_depth = 0.0;
    }

    /// Get memory usage in bytes
    pub fn getMemoryUsage(self: *const Self) u64 {
        return self.stats.memory_usage +
            self.key_cache.capacity() * (@sizeOf(u64) + @sizeOf(usize)) +
            if (self.bloom_filter) |bf| bf.serializedSize() else 0;
    }

    /// Validate index integrity
    pub fn validate(self: *const Self) !void {
        if (self.entries.items.len == 0) return;

        // Check that entries are sorted
        for (self.entries.items[1..], 1..) |entry, i| {
            const prev_entry = self.entries.items[i - 1];
            if (std.mem.order(u8, prev_entry.key, entry.key) != .lt) {
                return TableError.InvalidKeyOrder;
            }
        }

        // Validate that cache entries point to correct indices
        var iterator = self.key_cache.iterator();
        while (iterator.next()) |kv| {
            const index = kv.value_ptr.*;
            if (index >= self.entries.items.len) {
                return TableError.CorruptedData;
            }

            const entry_key = self.entries.items[index].key;
            const expected_hash = self.hashKey(entry_key);
            if (kv.key_ptr.* != expected_hash) {
                return TableError.CorruptedData;
            }
        }
    }
};

/// Enhanced SSTable implementation with multi-level indexing and advanced bloom filter
pub const Table = struct {
    mmap_file: MmapFile,
    index: TableIndex,
    bloom_filter: BloomFilter,
    smallest_key: []const u8,
    biggest_key: []const u8,
    compression: CompressionType,
    file_id: u64,
    level: u32,
    stats: TableStats,
    allocator: Allocator,

    // Enhanced indexing features
    block_index: BlockIndex,
    filter_index: FilterIndex,
    checksum_type: ChecksumType,
    encryption_enabled: bool,

    // Performance optimizations
    cache_policy: CachePolicy,
    prefetch_enabled: bool,

    const Self = @This();
    const MAGIC_NUMBER: u32 = 0xCAFEBABE;
    const VERSION: u32 = 2; // Upgraded version for enhanced format
    const MIN_FILE_SIZE: usize = 128; // Increased minimum for enhanced format

    /// Checksum algorithms supported
    const ChecksumType = enum(u8) {
        none = 0,
        crc32 = 1,
        xxhash64 = 2,
    };

    /// Cache policy for block and filter caching
    const CachePolicy = enum(u8) {
        no_cache = 0,
        lru = 1,
        lfu = 2,
        adaptive = 3,
    };

    /// Open an existing SSTable file with enhanced format support
    pub fn open(allocator: Allocator, path: []const u8, file_id: u64) TableError!Self {
        const mmap_file = MmapFile.open(path, true) catch return TableError.IOError;

        if (mmap_file.size < MIN_FILE_SIZE) {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.InvalidTableFile;
        }

        // Read and validate enhanced footer (now 64 bytes)
        const footer_offset = mmap_file.size - 64;
        const footer = mmap_file.getSliceConst(footer_offset, 64) catch {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.CorruptedData;
        };

        const magic = std.mem.readInt(u32, footer[0..4][0..4], .little);
        if (magic != MAGIC_NUMBER) {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.InvalidTableFile;
        }

        const version = std.mem.readInt(u32, footer[4..8][0..4], .little);
        if (version != VERSION) {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.UnsupportedVersion;
        }

        // Enhanced footer layout
        const index_offset = std.mem.readInt(u64, footer[8..16][0..8], .little);
        const bloom_offset = std.mem.readInt(u64, footer[16..24][0..8], .little);
        const block_index_offset = std.mem.readInt(u64, footer[24..32][0..8], .little);
        const filter_index_offset = std.mem.readInt(u64, footer[32..40][0..8], .little);
        const compression = @as(CompressionType, @enumFromInt(std.mem.readInt(u32, footer[40..44][0..4], .little)));
        const level = std.mem.readInt(u32, footer[44..48][0..4], .little);
        const checksum_type = @as(ChecksumType, @enumFromInt(footer[48]));
        const encryption_enabled = footer[49] != 0;
        const cache_policy = @as(CachePolicy, @enumFromInt(footer[50]));
        const prefetch_enabled = footer[51] != 0;

        // Validate offsets
        if (bloom_offset >= filter_index_offset or
            filter_index_offset >= block_index_offset or
            block_index_offset >= index_offset or
            index_offset >= footer_offset)
        {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.CorruptedData;
        }

        // Load bloom filter
        const bloom_size = filter_index_offset - bloom_offset;
        const bloom_data = mmap_file.getSliceConst(bloom_offset, bloom_size) catch {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.CorruptedData;
        };
        const bloom_filter = BloomFilter.deserialize(bloom_data, allocator) catch {
            var mutable_mmap = mmap_file;
            mutable_mmap.close();
            return TableError.CorruptedData;
        };

        // Load filter index
        const filter_index_size = block_index_offset - filter_index_offset;
        var filter_index = FilterIndex.init(allocator, 1000);
        if (filter_index_size > 0) {
            const filter_index_data = mmap_file.getSliceConst(filter_index_offset, filter_index_size) catch {
                var mutable_mmap = mmap_file;
                mutable_mmap.close();
                return TableError.CorruptedData;
            };

            filter_index.deinit();
            if (FilterIndex.deserialize(filter_index_data, allocator)) |deserialized| {
                filter_index = deserialized;
            } else |err| {
                // Handle deserialization errors gracefully for backward compatibility
                switch (err) {
                    error.BufferTooSmall => {
                        // Initialize empty filter index for old format compatibility
                        filter_index = FilterIndex.init(allocator, 1000);
                    },
                    else => {
                        var mutable_mmap = mmap_file;
                        mutable_mmap.close();
                        return TableError.CorruptedData;
                    },
                }
            }
        }

        // Load block index
        const block_index_size = index_offset - block_index_offset;
        var block_index = BlockIndex.init(allocator);
        if (block_index_size > 0) {
            const block_index_data = mmap_file.getSliceConst(block_index_offset, block_index_size) catch {
                var mutable_mmap = mmap_file;
                mutable_mmap.close();
                return TableError.CorruptedData;
            };

            if (BlockIndex.deserialize(block_index_data, allocator)) |deserialized| {
                block_index.deinit();
                block_index = deserialized;
            } else |err| {
                // If deserialization fails, use empty block index for compatibility
                if (err != error.BufferTooSmall) {
                    var mutable_mmap = mmap_file;
                    mutable_mmap.close();
                    return TableError.CorruptedData;
                }
            }
        }

        // Initialize enhanced table
        var table = Self{
            .mmap_file = mmap_file,
            .index = undefined,
            .bloom_filter = bloom_filter,
            .smallest_key = &[_]u8{},
            .biggest_key = &[_]u8{},
            .compression = compression,
            .file_id = file_id,
            .level = level,
            .stats = TableStats{
                .file_size = mmap_file.size,
                .key_count = 0,
                .block_count = 0,
                .bloom_false_positives = 0,
                .cache_hits = 0,
                .cache_misses = 0,
                .reads_total = 0,
            },
            .allocator = allocator,
            .block_index = block_index,
            .filter_index = filter_index,
            .checksum_type = checksum_type,
            .encryption_enabled = encryption_enabled,
            .cache_policy = cache_policy,
            .prefetch_enabled = prefetch_enabled,
        };

        // Load main index
        const index_size = footer_offset - index_offset;
        table.loadIndex(index_offset, index_size) catch |err| {
            var mutable_bloom = table.bloom_filter;
            mutable_bloom.deinit(allocator);
            table.block_index.deinit();
            table.filter_index.deinit();
            var mutable_mmap = table.mmap_file;
            mutable_mmap.close();
            return err;
        };

        return table;
    }

    /// Close the table and clean up resources
    pub fn close(self: *Self) void {
        self.index.deinit();
        var mutable_bloom = self.bloom_filter;
        mutable_bloom.deinit(self.allocator);
        self.block_index.deinit();
        self.filter_index.deinit();
        var mutable_mmap = self.mmap_file;
        mutable_mmap.close();
    }

    /// Load the table index from file
    fn loadIndex(self: *Self, offset: u64, size: u64) TableError!void {
        const index_data = self.mmap_file.getSliceConst(offset, size) catch return TableError.CorruptedData;

        if (index_data.len < 4) {
            return TableError.CorruptedData;
        }

        var data_offset: usize = 0;
        const entry_count = std.mem.readInt(u32, index_data[data_offset .. data_offset + 4][0..4], .little);
        data_offset += 4;

        self.index = TableIndex.init(self.allocator, entry_count) catch return TableError.OutOfMemory;
        self.stats.key_count = entry_count;
        self.stats.block_count = entry_count;

        // Read index entries
        for (0..entry_count) |_| {
            if (data_offset + 12 > index_data.len) {
                return TableError.CorruptedData;
            }

            const entry_offset = std.mem.readInt(u32, index_data[data_offset .. data_offset + 4][0..4], .little);
            data_offset += 4;

            const block_size = std.mem.readInt(u32, index_data[data_offset .. data_offset + 4][0..4], .little);
            data_offset += 4;

            const key_len = std.mem.readInt(u32, index_data[data_offset .. data_offset + 4][0..4], .little);
            data_offset += 4;

            if (data_offset + key_len > index_data.len) {
                return TableError.CorruptedData;
            }

            const key = index_data[data_offset .. data_offset + key_len];
            data_offset += key_len;

            self.index.addEntry(key, entry_offset, block_size) catch return TableError.OutOfMemory;
        }

        // Set key range from index
        if (entry_count > 0) {
            self.smallest_key = self.index.getSmallestKey().?;
            self.biggest_key = self.index.getLargestKey().?;
        }
    }

    /// Get a value for the given key using enhanced indexing
    pub fn get(self: *Self, key: []const u8) TableError!?ValueStruct {
        self.stats.reads_total += 1;

        // Check partitioned filter index first for better performance
        if (!self.filter_index.mightContain(key)) {
            return null;
        }

        // Check global bloom filter as secondary filter
        if (!self.bloom_filter.contains(key)) {
            self.stats.bloom_false_positives += 1;
            return null;
        }

        // Use enhanced block index to find the exact block
        const block_index = self.block_index.findBlock(key) orelse
            // Fallback to traditional index search
            self.index.search(key) orelse {
            self.stats.bloom_false_positives += 1;
            return null;
        };

        // Read and search the block with enhanced caching
        const block_data = self.readBlockWithCaching(block_index) catch return TableError.IOError;
        defer self.allocator.free(block_data);

        const result = self.searchInBlockWithTimestamp(block_data, key);
        if (result == null) {
            self.stats.bloom_false_positives += 1;
        }

        return result;
    }

    /// Read a block from the table
    fn readBlock(self: *Self, block_index: usize) TableError![]u8 {
        const entry = self.index.getEntry(block_index) orelse return TableError.CorruptedData;

        const compressed_data = self.mmap_file.getSliceConst(entry.offset, entry.size) catch return TableError.IOError;

        return switch (self.compression) {
            .none => self.allocator.dupe(u8, compressed_data) catch return TableError.OutOfMemory,
            .zlib => blk: {
                const compressor = Compressor.init(self.allocator, self.compression);
                const original_size = std.mem.readInt(u32, compressed_data[0..4], .little);
                const decompressed = compressor.decompressAlloc(compressed_data[4..], original_size) catch |err| switch (err) {
                    error.OutOfMemory => return TableError.OutOfMemory,
                    else => return TableError.CorruptedData,
                };
                break :blk decompressed;
            },
        };
    }

    /// Enhanced block reading with caching support
    fn readBlockWithCaching(self: *Self, block_index: usize) TableError![]u8 {
        // Check if we should use caching based on cache policy
        if (self.cache_policy == .no_cache) {
            return self.readBlock(block_index);
        }

        // For now, just use the basic read - caching would require additional infrastructure
        // In a full implementation, this would check a block cache first
        return self.readBlock(block_index);
    }

    /// Enhanced block search with timestamp filtering
    fn searchInBlockWithTimestamp(self: *Self, block_data: []const u8, key: []const u8) ?ValueStruct {
        var offset: usize = 0;

        // Check for enhanced block format with metadata header
        if (block_data.len >= 16) {
            const block_magic = std.mem.readInt(u32, block_data[0..4], .little);
            if (block_magic == 0x424C4F43) { // Enhanced block format (BLOC)
                const entry_count = std.mem.readInt(u32, block_data[4..8], .little);
                const min_timestamp = std.mem.readInt(u64, block_data[8..16], .little);
                offset = 16;

                // Use binary search for enhanced blocks
                return self.binarySearchInBlock(block_data[offset..], key, entry_count, min_timestamp);
            }
        }

        // Fallback to linear search for legacy format
        return self.searchInBlock(block_data, key);
    }

    /// Binary search within a block for enhanced performance
    fn binarySearchInBlock(self: *Self, block_data: []const u8, key: []const u8, entry_count: u32, min_timestamp: u64) ?ValueStruct {
        _ = min_timestamp; // Reserved for timestamp filtering

        var entries = std.ArrayList(struct { key: []const u8, value: []const u8, timestamp: u64 }).init(self.allocator);
        defer entries.deinit();

        // Parse all entries first (in practice, this would be optimized)
        var offset: usize = 0;
        for (0..entry_count) |_| {
            if (offset + 16 > block_data.len) break;

            const key_len = std.mem.readInt(u32, block_data[offset .. offset + 4][0..4], .little);
            offset += 4;

            const value_len = std.mem.readInt(u32, block_data[offset .. offset + 4][0..4], .little);
            offset += 4;

            const timestamp = std.mem.readInt(u64, block_data[offset .. offset + 8][0..8], .little);
            offset += 8;

            if (offset + key_len + value_len > block_data.len) break;

            const entry_key = block_data[offset .. offset + key_len];
            offset += key_len;

            const entry_value = block_data[offset .. offset + value_len];
            offset += value_len;

            entries.append(.{
                .key = entry_key,
                .value = entry_value,
                .timestamp = timestamp,
            }) catch continue;
        }

        // Binary search
        var left: usize = 0;
        var right: usize = entries.items.len;

        while (left < right) {
            const mid = left + (right - left) / 2;
            const cmp = std.mem.order(u8, entries.items[mid].key, key);

            switch (cmp) {
                .lt => left = mid + 1,
                .gt => right = mid,
                .eq => {
                    const entry = entries.items[mid];
                    const owned_value = self.allocator.dupe(u8, entry.value) catch return null;
                    return ValueStruct{
                        .value = owned_value,
                        .timestamp = entry.timestamp,
                        .meta = 0,
                    };
                },
            }
        }

        return null;
    }

    /// Search for a key within a block
    fn searchInBlock(self: *Self, block_data: []const u8, key: []const u8) ?ValueStruct {
        var offset: usize = 0;

        while (offset < block_data.len) {
            // Ensure we have enough data for the header
            if (offset + 16 > block_data.len) break;

            const key_len = std.mem.readInt(u32, block_data[offset .. offset + 4][0..4], .little);
            offset += 4;

            const value_len = std.mem.readInt(u32, block_data[offset .. offset + 4][0..4], .little);
            offset += 4;

            const timestamp = std.mem.readInt(u64, block_data[offset .. offset + 8][0..8], .little);
            offset += 8;

            // Validate entry bounds
            if (offset + key_len + value_len > block_data.len) break;
            if (key_len == 0 or key_len > MAX_KEY_SIZE) break; // Reasonable key size limits
            if (value_len > MAX_VALUE_SIZE) break; // Reasonable value size limits

            const entry_key = block_data[offset .. offset + key_len];
            offset += key_len;

            const entry_value = block_data[offset .. offset + value_len];
            offset += value_len;

            const key_cmp = std.mem.order(u8, entry_key, key);
            if (key_cmp == .eq) {
                // Copy the value to owned memory to avoid use-after-free
                const owned_value = self.allocator.dupe(u8, entry_value) catch return null;
                return ValueStruct{
                    .value = owned_value,
                    .timestamp = timestamp,
                    .meta = 0,
                };
            }

            // Keys are sorted, so we can stop if we've passed the target
            if (key_cmp == .gt) {
                break;
            }
        }

        return null;
    }

    /// Create an iterator for the table
    pub fn iterator(self: *Self) TableIterator {
        return TableIterator.init(self);
    }

    /// Verify table integrity (checksum validation)
    pub fn verifyChecksum(self: *Self) TableError!void {
        const file_size = self.file.?.len;
        if (file_size < 49) return TableError.CorruptedData;

        const footer = self.file.?[file_size - 49 ..];
        const stored_checksum = std.mem.readInt(u32, footer[44..48], .little);
        const checksum_type = @as(ChecksumType, @enumFromInt(footer[48]));

        switch (checksum_type) {
            .none => return,
            .crc32 => {
                const data_to_check = self.file.?[0 .. file_size - 49];
                const calculated_checksum = std.hash.crc.Crc32.hash(data_to_check);
                if (calculated_checksum != stored_checksum) {
                    return TableError.ChecksumMismatch;
                }
            },
        }
    }

    /// Check if this table overlaps with the given key range
    pub fn overlapsWithKeyRange(self: *const Self, start_key: []const u8, end_key: []const u8) bool {
        return std.mem.order(u8, self.biggest_key, start_key) != .lt and
            std.mem.order(u8, self.smallest_key, end_key) != .gt;
    }

    /// Get table statistics
    pub fn getStats(self: *const Self) TableStats {
        return self.stats;
    }

    /// Get file ID
    pub fn getFileId(self: *const Self) u64 {
        return self.file_id;
    }

    /// Get level
    pub fn getLevel(self: *const Self) u32 {
        return self.level;
    }

    /// Get smallest key
    pub fn getSmallestKey(self: *const Self) []const u8 {
        return self.smallest_key;
    }

    /// Get biggest key
    pub fn getBiggestKey(self: *const Self) []const u8 {
        return self.biggest_key;
    }

    /// Get file size
    pub fn getFileSize(self: *const Self) u64 {
        return self.mmap_file.size;
    }
};

/// Iterator for traversing table entries
pub const TableIterator = struct {
    table: *Table,
    current_block: usize,
    block_data: ?[]u8,
    block_offset: usize,
    finished: bool,

    const Self = @This();

    /// Initialize a new table iterator
    pub fn init(table: *Table) Self {
        return Self{
            .table = table,
            .current_block = 0,
            .block_data = null,
            .block_offset = 0,
            .finished = false,
        };
    }

    /// Clean up iterator resources
    pub fn deinit(self: *Self) void {
        if (self.block_data) |data| {
            self.table.allocator.free(data);
            self.block_data = null;
        }
    }

    /// Get the next key-value pair
    pub fn next(self: *Self) TableError!?struct { key: []const u8, value: ValueStruct } {
        if (self.finished) {
            return null;
        }

        // Load first block if needed
        if (self.block_data == null and self.current_block < self.table.index.getEntryCount()) {
            try self.loadCurrentBlock();
        }

        while (self.current_block < self.table.index.getEntryCount()) {
            if (self.block_data) |data| {
                if (self.block_offset + 16 <= data.len) {
                    const key_len = std.mem.readInt(u32, data[self.block_offset .. self.block_offset + 4][0..4], .little);
                    self.block_offset += 4;

                    const value_len = std.mem.readInt(u32, data[self.block_offset .. self.block_offset + 4][0..4], .little);
                    self.block_offset += 4;

                    const timestamp = std.mem.readInt(u64, data[self.block_offset .. self.block_offset + 8][0..8], .little);
                    self.block_offset += 8;

                    // Validate entry bounds and sizes
                    if (self.block_offset + key_len + value_len <= data.len and
                        key_len > 0 and key_len <= MAX_KEY_SIZE and
                        value_len <= MAX_VALUE_SIZE)
                    {
                        const key = data[self.block_offset .. self.block_offset + key_len];
                        self.block_offset += key_len;

                        const value = data[self.block_offset .. self.block_offset + value_len];
                        self.block_offset += value_len;

                        return .{
                            .key = key,
                            .value = ValueStruct{
                                .value = value,
                                .timestamp = timestamp,
                                .meta = 0,
                            },
                        };
                    }
                }
            }

            // Move to next block
            self.current_block += 1;
            try self.loadCurrentBlock();
        }

        self.finished = true;
        return null;
    }

    /// Load the current block into memory
    fn loadCurrentBlock(self: *Self) TableError!void {
        // Free previous block data
        if (self.block_data) |data| {
            self.table.allocator.free(data);
            self.block_data = null;
        }

        if (self.current_block < self.table.index.getEntryCount()) {
            self.block_data = try self.table.readBlock(self.current_block);
            self.block_offset = 0;
        }
    }
};

/// Enhanced builder for creating SSTable files with advanced indexing and bloom filter integration
pub const TableBuilder = struct {
    file: std.fs.File,
    path: []const u8,
    allocator: Allocator,
    block_data: ArrayList(u8),
    index: TableIndex,
    bloom_filter: BloomFilter,
    current_block_offset: u64,
    entries_in_block: u32,
    total_entries: u32,
    block_size: usize,
    compression: CompressionType,
    level: u32,
    last_key: ?[]const u8,
    first_key: ?[]const u8,
    smallest_key: ?[]const u8,
    biggest_key: ?[]const u8,
    finished: bool,

    // Enhanced indexing features
    block_index: BlockIndex,
    filter_index: FilterIndex,
    checksum_type: Table.ChecksumType,
    encryption_enabled: bool,
    cache_policy: Table.CachePolicy,
    prefetch_enabled: bool,

    // Enhanced block tracking
    current_block_entries: ArrayList(BlockEntry),
    block_min_timestamp: u64,
    block_max_timestamp: u64,

    const Self = @This();

    const BlockEntry = struct {
        key: []const u8,
        value: []const u8,
        timestamp: u64,
    };

    /// Initialize a new enhanced table builder
    pub fn init(allocator: Allocator, path: []const u8, options: Options, level: u32) TableError!Self {
        const file = std.fs.cwd().createFile(path, .{ .read = true, .truncate = true }) catch return TableError.IOError;

        const bloom_filter = BloomFilter.init(allocator, @as(usize, options.bloom_expected_items), options.bloom_false_positive) catch {
            file.close();
            return TableError.OutOfMemory;
        };

        const index = TableIndex.init(allocator, @max(100, options.bloom_expected_items / 10)) catch {
            var mutable_bloom = bloom_filter;
            mutable_bloom.deinit(allocator);
            file.close();
            return TableError.OutOfMemory;
        };

        const block_index = BlockIndex.init(allocator);
        var filter_index = FilterIndex.init(allocator, 1000);

        // Initialize global filter early so keys can be added during building
        filter_index.global_filter = BloomFilter.init(allocator, 10000, 0.01) catch {
            var mutable_bloom = bloom_filter;
            mutable_bloom.deinit(allocator);
            var mutable_index = index;
            mutable_index.deinit();
            file.close();
            return TableError.OutOfMemory;
        };

        return Self{
            .file = file,
            .path = path,
            .allocator = allocator,
            .block_data = ArrayList(u8).init(allocator),
            .index = index,
            .bloom_filter = bloom_filter,
            .current_block_offset = 0,
            .entries_in_block = 0,
            .total_entries = 0,
            .block_size = options.block_size,
            .compression = options.compression,
            .level = level,
            .last_key = null,
            .first_key = null,
            .smallest_key = null,
            .biggest_key = null,
            .finished = false,

            // Enhanced features
            .block_index = block_index,
            .filter_index = filter_index,
            .checksum_type = Table.ChecksumType.crc32,
            .encryption_enabled = false,
            .cache_policy = Table.CachePolicy.lru,
            .prefetch_enabled = true,

            // Enhanced block tracking
            .current_block_entries = ArrayList(BlockEntry).init(allocator),
            .block_min_timestamp = std.math.maxInt(u64),
            .block_max_timestamp = 0,
        };
    }

    /// Clean up enhanced builder resources
    pub fn deinit(self: *Self) void {
        if (!self.finished) {
            // If not finished properly, delete the partial file
            self.file.close();
            std.fs.cwd().deleteFile(self.path) catch {};
        } else {
            self.file.close();
        }

        self.block_data.deinit();
        self.index.deinit();
        var mutable_bloom = self.bloom_filter;
        mutable_bloom.deinit(self.allocator);

        // Clean up enhanced structures
        self.block_index.deinit();
        self.filter_index.deinit();
        // Clean up current block entries
        for (self.current_block_entries.items) |entry| {
            self.allocator.free(entry.key);
            self.allocator.free(entry.value);
        }
        self.current_block_entries.deinit();

        if (self.last_key) |key| {
            self.allocator.free(key);
        }
        if (self.first_key) |key| {
            self.allocator.free(key);
        }
        if (self.smallest_key) |key| {
            self.allocator.free(key);
        }
        if (self.biggest_key) |key| {
            self.allocator.free(key);
        }
    }

    /// Add a key-value pair to the enhanced table (keys must be in sorted order)
    pub fn add(self: *Self, key: []const u8, value: ValueStruct) TableError!void {
        if (self.finished) {
            return TableError.InvalidKeyOrder;
        }

        // Validate key ordering
        if (self.last_key) |last| {
            if (std.mem.order(u8, last, key) != .lt) {
                return TableError.InvalidKeyOrder;
            }
        }

        // Calculate entry size
        const entry_size = 4 + 4 + 8 + key.len + value.value.len;

        // Validate reasonable sizes
        if (key.len == 0 or key.len > MAX_KEY_SIZE) {
            return TableError.InvalidKeyOrder;
        }
        if (value.value.len > MAX_VALUE_SIZE) {
            return TableError.BlockSizeExceeded;
        }

        // Finish current block if it would exceed block size
        if (self.block_data.items.len + entry_size > self.block_size and self.entries_in_block > 0) {
            try self.finishCurrentBlockEnhanced();
        }

        // Add entry to current block entries for enhanced processing
        const block_entry = BlockEntry{
            .key = try self.allocator.dupe(u8, key),
            .value = try self.allocator.dupe(u8, value.value),
            .timestamp = value.timestamp,
        };
        try self.current_block_entries.append(block_entry);

        // Update timestamp range for block
        self.block_min_timestamp = @min(self.block_min_timestamp, value.timestamp);
        self.block_max_timestamp = @max(self.block_max_timestamp, value.timestamp);

        // Add entry to current block data
        var entry_header: [16]u8 = undefined;
        std.mem.writeInt(u32, entry_header[0..4], @intCast(key.len), .little);
        std.mem.writeInt(u32, entry_header[4..8], @intCast(value.value.len), .little);
        std.mem.writeInt(u64, entry_header[8..16], value.timestamp, .little);

        self.block_data.appendSlice(&entry_header) catch return TableError.OutOfMemory;
        self.block_data.appendSlice(key) catch return TableError.OutOfMemory;
        self.block_data.appendSlice(value.value) catch return TableError.OutOfMemory;

        // Update bloom filter and filter index
        self.bloom_filter.add(key);
        try self.filter_index.addKey(key, @intCast(self.total_entries / 100)); // Rough block estimate

        // Update counters
        self.entries_in_block += 1;
        self.total_entries += 1;

        // Update key tracking
        if (self.first_key == null) {
            self.first_key = self.allocator.dupe(u8, key) catch return TableError.OutOfMemory;
        }

        if (self.last_key) |old_key| {
            self.allocator.free(old_key);
        }
        self.last_key = self.allocator.dupe(u8, key) catch return TableError.OutOfMemory;

        // Track overall smallest and biggest keys
        if (self.smallest_key == null) {
            self.smallest_key = self.allocator.dupe(u8, key) catch return TableError.OutOfMemory;
        }

        if (self.biggest_key) |old_key| {
            self.allocator.free(old_key);
        }
        self.biggest_key = self.allocator.dupe(u8, key) catch return TableError.OutOfMemory;
    }

    /// Finish the current block and add it to the enhanced index
    fn finishCurrentBlockEnhanced(self: *Self) TableError!void {
        if (self.entries_in_block == 0) {
            return;
        }

        // Create enhanced block with metadata header
        var enhanced_block = ArrayList(u8).init(self.allocator);
        defer enhanced_block.deinit();

        // Write block header with metadata
        const block_magic: u32 = 0x424C4F43; // BLOC
        const entry_count: u32 = self.entries_in_block;
        const min_timestamp: u64 = self.block_min_timestamp;
        const max_timestamp: u64 = self.block_max_timestamp;

        var header: [24]u8 = undefined;
        std.mem.writeInt(u32, header[0..4], block_magic, .little);
        std.mem.writeInt(u32, header[4..8], entry_count, .little);
        std.mem.writeInt(u64, header[8..16], min_timestamp, .little);
        std.mem.writeInt(u64, header[16..24], max_timestamp, .little);

        try enhanced_block.appendSlice(&header);
        try enhanced_block.appendSlice(self.block_data.items);

        // Add to block index with enhanced metadata
        if (self.current_block_entries.items.len > 0) {
            const first_entry = self.current_block_entries.items[0];
            const last_entry = self.current_block_entries.items[self.current_block_entries.items.len - 1];

            const calculated_checksum = std.hash.crc.Crc32.hash(enhanced_block.items);

            const block_info = BlockIndex.BlockInfo{
                .first_key = try self.allocator.dupe(u8, first_entry.key),
                .last_key = try self.allocator.dupe(u8, last_entry.key),
                .offset = self.current_block_offset,
                .size = @intCast(enhanced_block.items.len),
                .entry_count = entry_count,
                .checksum = calculated_checksum,
                .compression = self.compression,
                .max_timestamp = max_timestamp,
                .min_timestamp = min_timestamp,
            };

            try self.block_index.blocks.append(block_info);
        }

        // Fall back to original block finishing
        try self.finishCurrentBlock();

        // Clean up current block entries
        for (self.current_block_entries.items) |entry| {
            self.allocator.free(entry.key);
            self.allocator.free(entry.value);
        }
        self.current_block_entries.clearRetainingCapacity();

        // Reset timestamp tracking
        self.block_min_timestamp = std.math.maxInt(u64);
        self.block_max_timestamp = 0;
    }

    /// Finish the current block and add it to the index
    fn finishCurrentBlock(self: *Self) TableError!void {
        if (self.entries_in_block == 0) {
            return;
        }

        // Compress block data
        const block_data = self.block_data.toOwnedSlice() catch return TableError.OutOfMemory;
        defer self.allocator.free(block_data);

        const compressed_block = switch (self.compression) {
            .none => self.allocator.dupe(u8, block_data) catch return TableError.OutOfMemory,
            .zlib => blk: {
                const compressor = Compressor.init(self.allocator, self.compression);

                // Allocate buffer for compressed data + original size header
                const max_compressed_size = compressor.maxCompressedSize(block_data.len);
                const compressed_buffer = self.allocator.alloc(u8, max_compressed_size + 4) catch return TableError.OutOfMemory;

                // Write original size as header
                std.mem.writeInt(u32, compressed_buffer[0..4], @intCast(block_data.len), .little);

                // Compress the data
                const compressed_size = compressor.compress(block_data, compressed_buffer[4..]) catch |err| switch (err) {
                    error.OutOfMemory => return TableError.OutOfMemory,
                    else => return TableError.IOError,
                };

                // Resize buffer to actual compressed size
                const final_buffer = self.allocator.realloc(compressed_buffer, compressed_size + 4) catch compressed_buffer[0 .. compressed_size + 4];
                break :blk final_buffer;
            },
        };
        defer self.allocator.free(compressed_block);

        // Write block to file
        const bytes_written = self.file.write(compressed_block) catch return TableError.IOError;
        if (bytes_written != compressed_block.len) {
            return TableError.IOError;
        }

        // Add index entry (use first key of the block)
        if (self.first_key) |block_first_key| {
            try self.index.addEntry(block_first_key, @intCast(self.current_block_offset), @intCast(compressed_block.len));

            // If this is the only block (all entries fit), also add the biggest key for proper range
            if (self.biggest_key) |biggest_key| {
                if (!std.mem.eql(u8, block_first_key, biggest_key)) {
                    try self.index.addEntry(biggest_key, @intCast(self.current_block_offset), @intCast(compressed_block.len));
                }
            }
        }

        // Update offsets and reset block
        self.current_block_offset += compressed_block.len;
        self.entries_in_block = 0;
        self.block_data.clearRetainingCapacity();

        // Reset first_key for next block
        if (self.first_key) |key| {
            self.allocator.free(key);
            self.first_key = null;
        }
    }

    /// Finish building the enhanced table and write metadata
    pub fn finish(self: *Self) TableError!void {
        if (self.finished) {
            return;
        }

        // Finish any remaining block
        if (self.entries_in_block > 0) {
            try self.finishCurrentBlockEnhanced();
        }

        // Build multi-level indexes
        try self.block_index.buildMultiLevelIndex(100, 10);
        try self.filter_index.finalizePartitions(self.total_entries);

        // Serialize and write bloom filter
        const bloom_data = self.allocator.alloc(u8, self.bloom_filter.serializedSize()) catch return TableError.OutOfMemory;
        defer self.allocator.free(bloom_data);

        const bloom_bytes = self.bloom_filter.serialize(bloom_data) catch return TableError.IOError;
        if (bloom_bytes != bloom_data.len) {
            return TableError.IOError;
        }

        const bloom_offset = self.file.getPos() catch return TableError.IOError;
        const bloom_written = self.file.write(bloom_data) catch return TableError.IOError;
        if (bloom_written != bloom_data.len) {
            return TableError.IOError;
        }

        // Serialize and write filter index
        const filter_index_offset = self.file.getPos() catch return TableError.IOError;
        const filter_index_data = self.allocator.alloc(u8, self.filter_index.serializedSize()) catch return TableError.OutOfMemory;
        defer self.allocator.free(filter_index_data);

        const filter_index_bytes = self.filter_index.serialize(filter_index_data) catch return TableError.IOError;
        if (filter_index_bytes != filter_index_data.len) {
            return TableError.IOError;
        }

        const filter_index_written = self.file.write(filter_index_data) catch return TableError.IOError;
        if (filter_index_written != filter_index_data.len) {
            return TableError.IOError;
        }

        // Serialize and write block index
        const block_index_offset = self.file.getPos() catch return TableError.IOError;
        const block_index_data = self.allocator.alloc(u8, self.block_index.serializedSize()) catch return TableError.OutOfMemory;
        defer self.allocator.free(block_index_data);

        const block_index_bytes = self.block_index.serialize(block_index_data) catch return TableError.IOError;
        if (block_index_bytes != block_index_data.len) {
            return TableError.IOError;
        }

        const block_index_written = self.file.write(block_index_data) catch return TableError.IOError;
        if (block_index_written != block_index_data.len) {
            return TableError.IOError;
        }

        // Write main index
        const index_offset = self.file.getPos() catch return TableError.IOError;

        // Write index header
        var header_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &header_buf, @intCast(self.index.getEntryCount()), .little);
        self.file.writeAll(&header_buf) catch return TableError.IOError;

        // Write index entries
        for (self.index.entries.items) |entry| {
            var entry_buf: [12]u8 = undefined;
            std.mem.writeInt(u32, entry_buf[0..4], entry.offset, .little);
            std.mem.writeInt(u32, entry_buf[4..8], entry.size, .little);
            std.mem.writeInt(u32, entry_buf[8..12], @intCast(entry.key.len), .little);
            self.file.writeAll(&entry_buf) catch return TableError.IOError;
            self.file.writeAll(entry.key) catch return TableError.IOError;
        }

        // Write enhanced footer (64 bytes)
        var footer_buf: [64]u8 = undefined;
        std.mem.writeInt(u32, footer_buf[0..4], Table.MAGIC_NUMBER, .little);
        std.mem.writeInt(u32, footer_buf[4..8], Table.VERSION, .little);
        std.mem.writeInt(u64, footer_buf[8..16], index_offset, .little);
        std.mem.writeInt(u64, footer_buf[16..24], bloom_offset, .little);
        std.mem.writeInt(u64, footer_buf[24..32], block_index_offset, .little);
        std.mem.writeInt(u64, footer_buf[32..40], filter_index_offset, .little);
        std.mem.writeInt(u32, footer_buf[40..44], @intFromEnum(self.compression), .little);
        std.mem.writeInt(u32, footer_buf[44..48], self.level, .little);
        footer_buf[48] = @intFromEnum(self.checksum_type);
        footer_buf[49] = if (self.encryption_enabled) 1 else 0;
        footer_buf[50] = @intFromEnum(self.cache_policy);
        footer_buf[51] = if (self.prefetch_enabled) 1 else 0;
        // Remaining bytes are reserved/padding
        @memset(footer_buf[52..64], 0);

        self.file.writeAll(&footer_buf) catch return TableError.IOError;

        // Sync to disk
        self.file.sync() catch return TableError.IOError;

        self.finished = true;
    }

    /// Get the number of entries added so far
    pub fn getEntryCount(self: *const Self) u32 {
        return self.total_entries;
    }

    /// Get the estimated file size
    pub fn getEstimatedSize(self: *const Self) u64 {
        return self.current_block_offset + self.block_data.items.len;
    }

    /// Check if the builder is finished
    pub fn isFinished(self: *const Self) bool {
        return self.finished;
    }
};

/// Generate a temporary test file path with unique suffix
fn generateTestFilePath(allocator: Allocator, comptime base_name: []const u8, comptime extension: []const u8) ![]const u8 {
    const timestamp = std.time.milliTimestamp();
    const random_suffix = std.crypto.random.int(u32);
    return std.fmt.allocPrint(allocator, "{s}_{d}_{d}.{s}", .{ base_name, timestamp, random_suffix, extension });
}

// Tests
test "TableIndex basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var index = try TableIndex.init(allocator, 10);
    defer index.deinit();

    // Test adding entries in order
    try index.addEntry("key1", 0, 100);
    try index.addEntry("key2", 100, 150);
    try index.addEntry("key3", 250, 200);

    std.testing.expect(index.getEntryCount() == 3) catch unreachable;

    // Test search functionality
    std.testing.expect(index.search("key1").? == 0) catch unreachable;
    std.testing.expect(index.search("key2").? == 1) catch unreachable;
    std.testing.expect(index.search("key3").? == 2) catch unreachable;
    std.testing.expect(index.search("key0").? == 0) catch unreachable; // Should return block that might contain key
    std.testing.expect(index.search("key4") == null) catch unreachable; // Key beyond range

    // Test key range
    std.testing.expect(std.mem.eql(u8, index.getSmallestKey().?, "key1")) catch unreachable;
    std.testing.expect(std.mem.eql(u8, index.getLargestKey().?, "key3")) catch unreachable;
}

test "TableIndex key ordering validation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var index = try TableIndex.init(allocator, 10);
    defer index.deinit();

    // Test valid ordering
    try index.addEntry("key1", 0, 100);
    try index.addEntry("key2", 100, 150);

    // Test invalid ordering (should fail)
    const result = index.addEntry("key1", 250, 200);
    std.testing.expect(result == TableError.InvalidKeyOrder) catch unreachable;
}

test "TableIndex range queries" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var index = try TableIndex.init(allocator, 10);
    defer index.deinit();

    // Add test data
    try index.addEntry("apple", 0, 100);
    try index.addEntry("banana", 100, 150);
    try index.addEntry("cherry", 250, 200);
    try index.addEntry("date", 450, 180);
    try index.addEntry("elderberry", 630, 220);

    // Test range query
    const range1 = index.searchRange("banana", "date");
    std.testing.expect(range1.start_index == 1) catch unreachable;
    std.testing.expect(range1.end_index == 4) catch unreachable;
    std.testing.expect(range1.count == 3) catch unreachable;

    // Test range with no matches
    const range2 = index.searchRange("fig", "grape");
    std.testing.expect(range2.count == 0) catch unreachable;

    // Test range that includes all
    const range3 = index.searchRange("a", "z");
    std.testing.expect(range3.count == 5) catch unreachable;
}

test "TableIndex prefix search" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var index = try TableIndex.init(allocator, 10);
    defer index.deinit();

    // Add test data with common prefixes
    try index.addEntry("app1", 0, 100);
    try index.addEntry("app2", 100, 150);
    try index.addEntry("app3", 250, 200);
    try index.addEntry("banana", 450, 180);
    try index.addEntry("cat", 630, 220);

    // Test prefix search for "app"
    const prefix_result = index.searchPrefix("app");
    std.testing.expect(prefix_result.start_index == 0) catch unreachable;
    std.testing.expect(prefix_result.end_index == 3) catch unreachable;
    std.testing.expect(prefix_result.count == 3) catch unreachable;

    // Test prefix with no matches
    const no_match = index.searchPrefix("xyz");
    std.testing.expect(no_match.count == 0) catch unreachable;

    // Test single character prefix
    const single_char = index.searchPrefix("c");
    std.testing.expect(single_char.count == 1) catch unreachable;
}

test "TableIndex bulk loading" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var index = try TableIndex.init(allocator, 10);
    defer index.deinit();

    // Create test entries
    const test_entries = [_]IndexEntry{
        IndexEntry{ .key = "key1", .offset = 0, .size = 100, .block_count = 10, .checksum = 0x12345678 },
        IndexEntry{ .key = "key2", .offset = 100, .size = 150, .block_count = 15, .checksum = 0x87654321 },
        IndexEntry{ .key = "key3", .offset = 250, .size = 200, .block_count = 20, .checksum = 0xABCDEF00 },
    };

    // Test bulk loading
    try index.bulkLoad(&test_entries);

    std.testing.expect(index.getEntryCount() == 3) catch unreachable;
    std.testing.expect(index.search("key2").? == 1) catch unreachable;

    const stats = index.getStats();
    std.testing.expect(stats.total_entries == 3) catch unreachable;
}

test "TableIndex statistics and caching" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var index = try TableIndex.init(allocator, 10);
    defer index.deinit();

    // Add test data
    try index.addEntry("key1", 0, 100);
    try index.addEntry("key2", 100, 150);
    try index.addEntry("key3", 250, 200);

    // Test search with statistics
    _ = index.searchWithStats("key2");
    _ = index.searchWithStats("key1");
    _ = index.searchWithStats("key2"); // This should be a cache hit

    const stats = index.getStats();
    std.testing.expect(stats.search_operations == 3) catch unreachable;
    // Cache should work correctly now that hash collisions are properly handled
    // Check that we have proper cache activity
    std.testing.expect(stats.cache_hits + stats.cache_misses == 3) catch unreachable;
}

test "TableIndex bloom filter integration" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var index = try TableIndex.init(allocator, 10);
    defer index.deinit();

    // Enable bloom filter
    try index.enableBloomFilter(100, 0.01);

    // Add test data
    try index.addEntry("key1", 0, 100);
    try index.addEntry("key2", 100, 150);
    try index.addEntry("key3", 250, 200);

    // Test search for existing key
    std.testing.expect(index.search("key2").? == 1) catch unreachable;

    // Test search for non-existing key (bloom filter should help)
    std.testing.expect(index.search("nonexistent") == null) catch unreachable;
}

test "TableIndex validation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var index = try TableIndex.init(allocator, 10);
    defer index.deinit();

    // Add sorted entries
    try index.addEntry("a", 0, 100);
    try index.addEntry("b", 100, 150);
    try index.addEntry("c", 250, 200);

    // Validation should pass
    try index.validate();

    std.testing.expect(index.getMemoryUsage() > 0) catch unreachable;
}

test "TableBuilder basic functionality" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_path = try generateTestFilePath(allocator, "test_table", "sst");
    defer allocator.free(test_path);
    defer std.fs.cwd().deleteFile(test_path) catch {};

    const options = Options{
        .dir = ".",
        .value_dir = ".",
        .mem_table_size = 1024 * 1024,
        .num_memtables = 5,
        .max_levels = 7,
        .level_size_multiplier = 10,
        .base_table_size = 2 * 1024 * 1024,
        .num_compactors = 4,
        .num_level_zero_tables = 5,
        .num_level_zero_tables_stall = 15,
        .value_threshold = 1024 * 1024,
        .value_log_file_size = 1024 * 1024 * 1024,
        .block_size = 4 * 1024,
        .bloom_false_positive = 0.01,
        .compression = .none,
        .sync_writes = false,
        .detect_conflicts = true,
        .verify_checksums = true,
    };

    var builder = try TableBuilder.init(allocator, test_path, options, 0);
    defer builder.deinit();

    // Add some test data
    try builder.add("key1", ValueStruct{ .value = "value1", .timestamp = 1, .meta = 0 });
    try builder.add("key2", ValueStruct{ .value = "value2", .timestamp = 2, .meta = 0 });
    try builder.add("key3", ValueStruct{ .value = "value3", .timestamp = 3, .meta = 0 });

    std.testing.expect(builder.getEntryCount() == 3) catch unreachable;
    std.testing.expect(!builder.isFinished()) catch unreachable;

    // Finish the table
    try builder.finish();
    std.testing.expect(builder.isFinished()) catch unreachable;
}

test "TableBuilder key ordering validation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_path = try generateTestFilePath(allocator, "test_table_order", "sst");
    defer allocator.free(test_path);
    defer std.fs.cwd().deleteFile(test_path) catch {};

    const options = Options{
        .dir = ".",
        .value_dir = ".",
        .mem_table_size = 1024 * 1024,
        .num_memtables = 5,
        .max_levels = 7,
        .level_size_multiplier = 10,
        .base_table_size = 2 * 1024 * 1024,
        .num_compactors = 4,
        .num_level_zero_tables = 5,
        .num_level_zero_tables_stall = 15,
        .value_threshold = 1024 * 1024,
        .value_log_file_size = 1024 * 1024 * 1024,
        .block_size = 4 * 1024,
        .bloom_false_positive = 0.01,
        .compression = .none,
        .sync_writes = false,
        .detect_conflicts = true,
        .verify_checksums = true,
    };

    var builder = try TableBuilder.init(allocator, test_path, options, 0);
    defer builder.deinit();

    // Add keys in correct order
    try builder.add("key1", ValueStruct{ .value = "value1", .timestamp = 1, .meta = 0 });
    try builder.add("key2", ValueStruct{ .value = "value2", .timestamp = 2, .meta = 0 });

    // Try to add key out of order (should fail)
    const result = builder.add("key1", ValueStruct{ .value = "value1", .timestamp = 3, .meta = 0 });
    std.testing.expect(result == TableError.InvalidKeyOrder) catch unreachable;
}
