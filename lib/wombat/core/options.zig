const std = @import("std");

pub const CompressionType = enum {
    none,
    zlib,
};

pub const CompactionStrategy = enum {
    level,
    size_tiered,
    universal,
};

pub const Options = struct {
    dir: []const u8,
    value_dir: []const u8,

    mem_table_size: usize = 64 * 1024 * 1024,
    num_memtables: u32 = 5,

    max_levels: u32 = 7,
    level_size_multiplier: u32 = 10,
    base_table_size: usize = 2 * 1024 * 1024,

    num_compactors: u32 = 4,
    num_level_zero_tables: u32 = 5,
    num_level_zero_tables_stall: u32 = 15,

    value_threshold: usize = 1024 * 1024,
    value_log_file_size: usize = 1024 * 1024 * 1024,

    block_size: usize = 4 * 1024,
    bloom_false_positive: f64 = 0.01,
    compression: CompressionType = .zlib,
    compaction_strategy: CompactionStrategy = .level,

    sync_writes: bool = false,
    detect_conflicts: bool = true,
    verify_checksums: bool = true,

    const Self = @This();

    pub fn validate(comptime self: Self) void {
        if (self.mem_table_size == 0) @compileError("mem_table_size must be > 0");
        if (self.max_levels == 0) @compileError("max_levels must be > 0");
        if (self.block_size == 0) @compileError("block_size must be > 0");
        if (self.num_memtables == 0) @compileError("num_memtables must be > 0");
        if (self.level_size_multiplier == 0) @compileError("level_size_multiplier must be > 0");
        if (self.base_table_size == 0) @compileError("base_table_size must be > 0");
        if (self.value_log_file_size == 0) @compileError("value_log_file_size must be > 0");
        if (self.bloom_false_positive <= 0.0 or self.bloom_false_positive >= 1.0) {
            @compileError("bloom_false_positive must be between 0.0 and 1.0");
        }
    }

    pub fn default(dir: []const u8) Self {
        return Self{
            .dir = dir,
            .value_dir = dir,
        };
    }

    pub fn withValueDir(self: Self, value_dir: []const u8) Self {
        var opts = self;
        opts.value_dir = value_dir;
        return opts;
    }

    pub fn withMemTableSize(self: Self, size: usize) Self {
        var opts = self;
        opts.mem_table_size = size;
        return opts;
    }

    pub fn withCompression(self: Self, compression: CompressionType) Self {
        var opts = self;
        opts.compression = compression;
        return opts;
    }

    pub fn withSyncWrites(self: Self, sync: bool) Self {
        var opts = self;
        opts.sync_writes = sync;
        return opts;
    }

    pub fn tableSizeForLevel(self: *const Self, level: u32) usize {
        if (level == 0) return self.base_table_size;

        var size = self.base_table_size;
        for (0..level) |_| {
            // Use checked multiplication to avoid overflow
            const new_size = std.math.mul(usize, size, self.level_size_multiplier) catch {
                // Return a very large value to indicate max size
                return std.math.maxInt(usize);
            };
            size = new_size;
        }
        return size;
    }

    pub fn maxTablesForLevel(self: *const Self, level: u32) u32 {
        if (level == 0) return self.num_level_zero_tables;
        return @intCast(self.level_size_multiplier);
    }
};
