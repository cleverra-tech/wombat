const std = @import("std");
const ArrayList = std.ArrayList;
const Allocator = std.mem.Allocator;
const Options = @import("../core/options.zig").Options;
const Table = @import("table.zig").Table;
const TableInfo = @import("manifest.zig").TableInfo;

/// Compaction strategy types
pub const CompactionStrategy = enum {
    /// Level-based compaction (default)
    level,
    /// Size-tiered compaction
    size_tiered,
    /// Universal compaction
    universal,
};

/// Compaction job information
pub const CompactionJob = struct {
    /// Source level
    level: u32,
    /// Tables to compact from source level
    inputs: []TableInfo,
    /// Target level for output
    target_level: u32,
    /// Estimated output size
    estimated_size: usize,
    /// Priority score (higher = more urgent)
    priority: f64,
    /// Compaction reason
    reason: CompactionReason,

    pub fn deinit(self: *CompactionJob, allocator: Allocator) void {
        allocator.free(self.inputs);
    }
};

/// Reasons for compaction
pub const CompactionReason = enum {
    /// Level has too many files
    level_full,
    /// Level size exceeds threshold
    size_threshold,
    /// Manual compaction requested
    manual,
    /// Tombstone cleanup
    tombstone_cleanup,
    /// File age-based compaction
    age_based,
    /// Read amplification reduction
    read_amplification,
};

/// Level statistics for compaction decisions
pub const LevelStats = struct {
    level: u32,
    file_count: u32,
    total_size: usize,
    avg_file_size: usize,
    max_file_size: usize,
    min_file_size: usize,
    read_amplification: f64,
    tombstone_ratio: f64,
    age_score: f64,
};

/// Compaction picker that decides which files to compact
pub const CompactionPicker = struct {
    options: *const Options,
    strategy: CompactionStrategy,
    level_stats: []LevelStats,
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator, options: *const Options) !Self {
        const level_stats = try allocator.alloc(LevelStats, options.max_levels);

        // Initialize level stats
        for (level_stats, 0..) |*stats, i| {
            stats.* = LevelStats{
                .level = @intCast(i),
                .file_count = 0,
                .total_size = 0,
                .avg_file_size = 0,
                .max_file_size = 0,
                .min_file_size = std.math.maxInt(usize),
                .read_amplification = 1.0,
                .tombstone_ratio = 0.0,
                .age_score = 0.0,
            };
        }

        return Self{
            .options = options,
            .strategy = .level,
            .level_stats = level_stats,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.level_stats);
    }

    /// Update level statistics
    pub fn updateLevelStats(self: *Self, level: u32, tables: []const TableInfo) void {
        if (level >= self.level_stats.len) return;

        var stats = &self.level_stats[level];
        stats.file_count = @intCast(tables.len);
        stats.total_size = 0;
        stats.max_file_size = 0;
        stats.min_file_size = if (tables.len > 0) std.math.maxInt(usize) else 0;

        var total_age: u64 = 0;
        const current_time = @as(u64, @intCast(std.time.milliTimestamp()));

        for (tables) |table| {
            stats.total_size += table.size;
            stats.max_file_size = @max(stats.max_file_size, table.size);
            stats.min_file_size = @min(stats.min_file_size, table.size);

            // Calculate age score
            const age = current_time - table.created_at;
            total_age += age;
        }

        if (tables.len > 0) {
            stats.avg_file_size = stats.total_size / tables.len;
            stats.age_score = @as(f64, @floatFromInt(total_age)) / @as(f64, @floatFromInt(tables.len));
        } else {
            stats.avg_file_size = 0;
            stats.age_score = 0.0;
            stats.min_file_size = 0;
        }

        // Calculate read amplification (simplified)
        stats.read_amplification = @as(f64, @floatFromInt(stats.file_count));
    }

    /// Pick the best compaction job
    pub fn pickCompaction(self: *Self, level_tables: []const []const TableInfo) ?CompactionJob {
        return switch (self.strategy) {
            .level => self.pickLevelCompaction(level_tables),
            .size_tiered => self.pickSizeTieredCompaction(level_tables),
            .universal => self.pickUniversalCompaction(level_tables),
        };
    }

    /// Level-based compaction strategy
    fn pickLevelCompaction(self: *Self, level_tables: []const []const TableInfo) ?CompactionJob {
        var best_job: ?CompactionJob = null;
        var best_priority: f64 = 0.0;

        for (level_tables, 0..) |tables, level_idx| {
            const level = @as(u32, @intCast(level_idx));
            if (level >= self.options.max_levels - 1) continue;

            self.updateLevelStats(level, tables);

            // Check if level needs compaction
            if (self.levelNeedsCompaction(level)) {
                if (self.createLevelCompactionJob(level, tables)) |job| {
                    if (job.priority > best_priority) {
                        if (best_job) |*old_job| {
                            old_job.deinit(self.allocator);
                        }
                        best_job = job;
                        best_priority = job.priority;
                    } else {
                        var mutable_job = job;
                        mutable_job.deinit(self.allocator);
                    }
                }
            }
        }

        return best_job;
    }

    /// Size-tiered compaction strategy
    fn pickSizeTieredCompaction(self: *Self, level_tables: []const []const TableInfo) ?CompactionJob {
        // For size-tiered, we look for files of similar size to compact together
        for (level_tables, 0..) |tables, level_idx| {
            const level = @as(u32, @intCast(level_idx));
            if (tables.len < 4) continue; // Need at least 4 files

            // Sort tables by size
            var sorted_tables = self.allocator.alloc(TableInfo, tables.len) catch continue;
            defer self.allocator.free(sorted_tables);

            @memcpy(sorted_tables, tables);
            std.sort.heap(TableInfo, sorted_tables, {}, struct {
                fn lessThan(context: void, a: TableInfo, b: TableInfo) bool {
                    _ = context;
                    return a.size < b.size;
                }
            }.lessThan);

            // Find groups of similar-sized files
            var group_start: usize = 0;
            while (group_start < sorted_tables.len) {
                var group_end = group_start + 1;
                const base_size = sorted_tables[group_start].size;

                while (group_end < sorted_tables.len) {
                    const size_ratio = @as(f64, @floatFromInt(sorted_tables[group_end].size)) / @as(f64, @floatFromInt(base_size));
                    if (size_ratio > 2.0) break; // Size difference too large
                    group_end += 1;
                }

                if (group_end - group_start >= 4) {
                    // Found a group worth compacting
                    const inputs = self.allocator.alloc(TableInfo, group_end - group_start) catch continue;
                    @memcpy(inputs, sorted_tables[group_start..group_end]);

                    var total_size: usize = 0;
                    for (inputs) |table| {
                        total_size += table.size;
                    }

                    return CompactionJob{
                        .level = level,
                        .inputs = inputs,
                        .target_level = level,
                        .estimated_size = total_size,
                        .priority = @as(f64, @floatFromInt(inputs.len)) * 10.0,
                        .reason = .size_threshold,
                    };
                }

                group_start = group_end;
            }
        }

        return null;
    }

    /// Universal compaction strategy
    fn pickUniversalCompaction(self: *Self, level_tables: []const []const TableInfo) ?CompactionJob {
        // Universal compaction focuses on level 0 and compacts all levels together
        if (level_tables.len == 0) return null;

        const level0_tables = level_tables[0];
        if (level0_tables.len < self.options.num_level_zero_tables) return null;

        // Compact all level 0 files together
        const inputs = self.allocator.alloc(TableInfo, level0_tables.len) catch return null;
        @memcpy(inputs, level0_tables);

        var total_size: usize = 0;
        for (inputs) |table| {
            total_size += table.size;
        }

        return CompactionJob{
            .level = 0,
            .inputs = inputs,
            .target_level = 1,
            .estimated_size = total_size,
            .priority = @as(f64, @floatFromInt(inputs.len)) * 20.0,
            .reason = .level_full,
        };
    }

    /// Check if a level needs compaction
    fn levelNeedsCompaction(self: *Self, level: u32) bool {
        if (level >= self.level_stats.len) return false;

        const stats = &self.level_stats[level];

        // Check file count threshold
        if (level == 0) {
            return stats.file_count >= self.options.num_level_zero_tables;
        }

        // Check size threshold for other levels
        const max_size = self.options.tableSizeForLevel(level);
        if (stats.total_size >= max_size) return true;

        // Check read amplification
        if (stats.read_amplification > 10.0) return true;

        // Check tombstone ratio
        if (stats.tombstone_ratio > 0.3) return true;

        return false;
    }

    /// Create a compaction job for a level
    fn createLevelCompactionJob(self: *Self, level: u32, tables: []const TableInfo) ?CompactionJob {
        if (tables.len == 0) return null;

        const stats = &self.level_stats[level];

        // For level 0, compact all files
        if (level == 0) {
            const inputs = self.allocator.alloc(TableInfo, tables.len) catch return null;
            @memcpy(inputs, tables);

            return CompactionJob{
                .level = level,
                .inputs = inputs,
                .target_level = 1,
                .estimated_size = stats.total_size,
                .priority = @as(f64, @floatFromInt(stats.file_count)) * 10.0,
                .reason = .level_full,
            };
        }

        // For other levels, select files intelligently
        var selected_tables = ArrayList(TableInfo).init(self.allocator);
        defer selected_tables.deinit();

        var selected_size: usize = 0;
        const max_compaction_size = self.options.tableSizeForLevel(level + 1);

        // Select files based on various criteria
        for (tables) |table| {
            if (selected_size + table.size <= max_compaction_size) {
                selected_tables.append(table) catch continue;
                selected_size += table.size;
            }
        }

        if (selected_tables.items.len == 0) return null;

        const inputs = self.allocator.alloc(TableInfo, selected_tables.items.len) catch return null;
        @memcpy(inputs, selected_tables.items);

        // Calculate priority based on multiple factors
        var priority: f64 = 0.0;

        // Size factor
        priority += @as(f64, @floatFromInt(selected_size)) / @as(f64, @floatFromInt(max_compaction_size)) * 50.0;

        // File count factor
        priority += @as(f64, @floatFromInt(inputs.len)) * 5.0;

        // Read amplification factor
        priority += stats.read_amplification * 2.0;

        // Age factor
        priority += stats.age_score / 1000000.0; // Convert milliseconds to reasonable scale

        var reason = CompactionReason.size_threshold;
        if (stats.file_count >= self.options.maxTablesForLevel(level)) {
            reason = .level_full;
            priority += 100.0; // High priority for full levels
        } else if (stats.tombstone_ratio > 0.2) {
            reason = .tombstone_cleanup;
            priority += 30.0;
        } else if (stats.read_amplification > 5.0) {
            reason = .read_amplification;
            priority += 20.0;
        }

        return CompactionJob{
            .level = level,
            .inputs = inputs,
            .target_level = level + 1,
            .estimated_size = selected_size,
            .priority = priority,
            .reason = reason,
        };
    }

    /// Set compaction strategy
    pub fn setStrategy(self: *Self, strategy: CompactionStrategy) void {
        self.strategy = strategy;
    }

    /// Get current strategy
    pub fn getStrategy(self: *const Self) CompactionStrategy {
        return self.strategy;
    }

    /// Get level statistics
    pub fn getLevelStats(self: *const Self, level: u32) ?LevelStats {
        if (level >= self.level_stats.len) return null;
        return self.level_stats[level];
    }

    /// Check if any level needs compaction
    pub fn needsCompaction(self: *Self, level_tables: []const []const TableInfo) bool {
        for (level_tables, 0..) |tables, level_idx| {
            const level = @as(u32, @intCast(level_idx));
            self.updateLevelStats(level, tables);
            if (self.levelNeedsCompaction(level)) return true;
        }
        return false;
    }

    /// Estimate compaction time using dynamic factors
    pub fn estimateCompactionTime(self: *const Self, job: *const CompactionJob) u64 {
        // Base throughput varies by level (lower levels are faster due to less seek overhead)
        var base_throughput: f64 = switch (job.level) {
            0 => 20.0, // Level 0: Fast, mostly sequential
            1 => 15.0, // Level 1: Still relatively fast
            2, 3 => 12.0, // Mid levels: Moderate speed
            else => 8.0, // Deep levels: Slower due to more random I/O
        };

        // Strategy affects throughput
        base_throughput *= switch (self.strategy) {
            .level => 1.0, // Baseline
            .size_tiered => 0.9, // Slightly slower due to more complex merging
            .universal => 0.8, // Slowest due to more comprehensive compaction
        };

        // Reason affects complexity
        const reason_multiplier: f64 = switch (job.reason) {
            .level_full, .size_threshold => 1.0, // Standard compaction
            .manual => 1.1, // Slightly slower due to comprehensive processing
            .tombstone_cleanup => 0.9, // Faster since mostly deletion
            .age_based => 1.0, // Standard speed
            .read_amplification => 1.2, // Slower due to more complex optimization
        };

        // File count overhead (more files = more overhead for merging)
        const file_overhead: f64 = 1.0 + (@as(f64, @floatFromInt(job.inputs.len)) - 1.0) * 0.05;

        // Compression overhead if enabled
        const compression_overhead: f64 = switch (self.options.compression) {
            .none => 1.0,
            .zlib => 1.3, // 30% overhead for compression
        };

        // Calculate final throughput in MB/s
        const final_throughput = base_throughput * reason_multiplier / file_overhead / compression_overhead;

        // Convert to bytes per second
        const bytes_per_second = final_throughput * 1024.0 * 1024.0;

        // Calculate estimated time in seconds
        const estimated_seconds = @as(f64, @floatFromInt(job.estimated_size)) / bytes_per_second;

        // Return at least 1 second, convert to u64
        return @max(1, @as(u64, @intFromFloat(@ceil(estimated_seconds))));
    }

    /// Get compaction priority explanation
    pub fn explainPriority(self: *const Self, job: *const CompactionJob, buffer: []u8) []const u8 {
        _ = self;
        return std.fmt.bufPrint(buffer, "Level {d}: {s}, {} files, {:.1f} MB, priority {:.1f}", .{
            job.level,
            @tagName(job.reason),
            job.inputs.len,
            @as(f64, @floatFromInt(job.estimated_size)) / (1024.0 * 1024.0),
            job.priority,
        }) catch "Priority explanation unavailable";
    }
};

/// Compaction statistics
pub const CompactionStats = struct {
    total_compactions: u64,
    total_bytes_compacted: u64,
    total_time_spent: u64,
    avg_compaction_time: f64,
    level_compactions: [8]u64,

    pub fn init() CompactionStats {
        return CompactionStats{
            .total_compactions = 0,
            .total_bytes_compacted = 0,
            .total_time_spent = 0,
            .avg_compaction_time = 0.0,
            .level_compactions = [_]u64{0} ** 8,
        };
    }

    pub fn recordCompaction(self: *CompactionStats, level: u32, bytes: u64, time: u64) void {
        self.total_compactions += 1;
        self.total_bytes_compacted += bytes;
        self.total_time_spent += time;

        if (level < self.level_compactions.len) {
            self.level_compactions[level] += 1;
        }

        if (self.total_compactions > 0) {
            self.avg_compaction_time = @as(f64, @floatFromInt(self.total_time_spent)) / @as(f64, @floatFromInt(self.total_compactions));
        }
    }

    pub fn getCompactionRate(self: *const CompactionStats) f64 {
        if (self.total_time_spent == 0) return 0.0;
        return @as(f64, @floatFromInt(self.total_bytes_compacted)) / @as(f64, @floatFromInt(self.total_time_spent));
    }
};
