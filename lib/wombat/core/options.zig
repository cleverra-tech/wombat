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

pub const ChecksumType = enum {
    none,
    crc32,
    xxhash,
};

pub const ConflictDetectionMode = enum {
    none,
    basic,
    advanced,
};

pub const LogLevel = enum {
    debug,
    info,
    warn,
    err,
};

/// Configuration for transaction system
pub const TransactionConfig = struct {
    max_read_keys: u32 = 10000,
    max_write_keys: u32 = 1000,
    detect_conflicts: bool = true,
    conflict_detection_mode: ConflictDetectionMode = .basic,
    max_pending_writes: u32 = 1000,
    gc_watermark_ratio: f64 = 0.1,
    txn_timeout_ms: u32 = 30000,
};

/// Configuration for retry and error handling
pub const RetryConfig = struct {
    max_retries: u32 = 3,
    initial_delay_ms: u32 = 100,
    max_delay_ms: u32 = 5000,
    backoff_multiplier: f64 = 2.0,
    jitter_ratio: f64 = 0.1,
    enable_circuit_breaker: bool = true,
    circuit_breaker_threshold: u32 = 10,
    circuit_breaker_timeout_ms: u32 = 60000,
};

/// Configuration for I/O throttling and performance
pub const IOConfig = struct {
    compaction_throttle_bytes_per_sec: u64 = 10 * 1024 * 1024, // 10MB/s
    write_throttle_bytes_per_sec: u64 = 50 * 1024 * 1024, // 50MB/s
    read_ahead_size: usize = 64 * 1024, // 64KB
    write_buffer_size: usize = 1024 * 1024, // 1MB
    sync_frequency_ms: u32 = 1000,
    use_mmap: bool = true,
    mmap_populate: bool = false,
    direct_io: bool = false,
};

/// Configuration for caching
pub const CacheConfig = struct {
    block_cache_size: usize = 256 * 1024 * 1024, // 256MB
    table_cache_size: usize = 64 * 1024 * 1024, // 64MB
    filter_cache_size: usize = 32 * 1024 * 1024, // 32MB
    enable_cache_compression: bool = true,
    cache_eviction_policy: enum { lru, lfu, arc } = .lru,
    cache_shards: u32 = 16,
};

/// Configuration for monitoring and metrics
pub const MonitoringConfig = struct {
    enable_metrics: bool = true,
    metrics_interval_ms: u32 = 10000,
    max_error_history: u32 = 1000,
    log_level: LogLevel = .info,
    enable_slow_query_log: bool = true,
    slow_query_threshold_ms: u32 = 1000,
    enable_operation_tracing: bool = false,
};

/// Configuration for recovery and backup
pub const RecoveryConfig = struct {
    enable_wal: bool = true,
    wal_file_size: usize = 64 * 1024 * 1024, // 64MB
    wal_sync_writes: bool = true,
    checkpoint_interval_ms: u32 = 300000, // 5 minutes
    manifest_rewrite_threshold: u32 = 1000,
    crash_recovery_timeout_ms: u32 = 30000,
    backup_retention_days: u32 = 7,
};

/// Configuration for encryption
pub const EncryptionConfig = struct {
    enable_encryption: bool = false,
    encryption_key: ?[]const u8 = null,
    key_rotation_interval_days: u32 = 30,
    cipher_mode: enum { aes_gcm, chacha20_poly1305 } = .aes_gcm,
    compress_before_encrypt: bool = true,
};

/// Configuration for validation limits
pub const ValidationConfig = struct {
    max_key_size: usize = 1024 * 1024, // 1MB
    max_value_size: usize = 1024 * 1024 * 1024, // 1GB
    max_batch_size: u32 = 1000,
    max_batch_bytes: usize = 10 * 1024 * 1024, // 10MB
    max_open_files: u32 = 1000,
    max_file_size: usize = 2 * 1024 * 1024 * 1024, // 2GB
};

pub const Options = struct {
    // Directory settings
    dir: []const u8,
    value_dir: []const u8,

    // Core LSM tree settings
    mem_table_size: usize = 64 * 1024 * 1024,
    num_memtables: u32 = 5,
    max_levels: u32 = 7,
    level_size_multiplier: u32 = 10,
    base_table_size: usize = 2 * 1024 * 1024,

    // Compaction settings
    num_compactors: u32 = 4,
    num_level_zero_tables: u32 = 5,
    num_level_zero_tables_stall: u32 = 15,
    compaction_strategy: CompactionStrategy = .level,
    compaction_priority_threshold: f64 = 0.8,
    compaction_delete_ratio: f64 = 0.5,

    // Value log settings
    value_threshold: usize = 1024 * 1024,
    value_log_file_size: usize = 1024 * 1024 * 1024,
    value_log_gc_threshold: f64 = 0.7,
    value_log_max_entries: u32 = 1000000,

    // Performance settings
    block_size: usize = 4 * 1024,
    bloom_false_positive: f64 = 0.01,
    bloom_expected_items: u32 = 10000,
    compression: CompressionType = .zlib,
    compression_level: i32 = 6,
    
    // Checksum and verification
    checksum_type: ChecksumType = .crc32,
    verify_checksums: bool = true,
    verify_checksums_on_read: bool = false,
    
    // Basic safety settings
    sync_writes: bool = false,
    detect_conflicts: bool = true,

    // Advanced configuration sections
    transaction_config: TransactionConfig = .{},
    retry_config: RetryConfig = .{},
    io_config: IOConfig = .{},
    cache_config: CacheConfig = .{},
    monitoring_config: MonitoringConfig = .{},
    recovery_config: RecoveryConfig = .{},
    encryption_config: EncryptionConfig = .{},
    validation_config: ValidationConfig = .{},

    const Self = @This();

    /// Compile-time validation for configuration
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
        if (self.compression_level < 0 or self.compression_level > 9) {
            @compileError("compression_level must be between 0 and 9");
        }
        if (self.value_log_gc_threshold <= 0.0 or self.value_log_gc_threshold >= 1.0) {
            @compileError("value_log_gc_threshold must be between 0.0 and 1.0");
        }
        if (self.compaction_priority_threshold <= 0.0 or self.compaction_priority_threshold >= 1.0) {
            @compileError("compaction_priority_threshold must be between 0.0 and 1.0");
        }
    }

    /// Runtime validation for configuration
    pub fn validateRuntime(self: *const Self) !void {
        if (self.mem_table_size == 0) return error.InvalidMemTableSize;
        if (self.max_levels == 0) return error.InvalidMaxLevels;
        if (self.block_size == 0) return error.InvalidBlockSize;
        if (self.num_memtables == 0) return error.InvalidNumMemTables;
        if (self.level_size_multiplier == 0) return error.InvalidLevelSizeMultiplier;
        if (self.base_table_size == 0) return error.InvalidBaseTableSize;
        if (self.value_log_file_size == 0) return error.InvalidValueLogFileSize;
        if (self.bloom_false_positive <= 0.0 or self.bloom_false_positive >= 1.0) {
            return error.InvalidBloomFalsePositive;
        }
        if (self.compression_level < 0 or self.compression_level > 9) {
            return error.InvalidCompressionLevel;
        }
        if (self.value_log_gc_threshold <= 0.0 or self.value_log_gc_threshold >= 1.0) {
            return error.InvalidValueLogGCThreshold;
        }
        if (self.compaction_priority_threshold <= 0.0 or self.compaction_priority_threshold >= 1.0) {
            return error.InvalidCompactionPriorityThreshold;
        }
        if (self.validation_config.max_key_size == 0) return error.InvalidMaxKeySize;
        if (self.validation_config.max_value_size == 0) return error.InvalidMaxValueSize;
        if (self.validation_config.max_batch_size == 0) return error.InvalidMaxBatchSize;
        if (self.validation_config.max_batch_bytes == 0) return error.InvalidMaxBatchBytes;
        if (self.validation_config.max_open_files == 0) return error.InvalidMaxOpenFiles;
        if (self.validation_config.max_file_size == 0) return error.InvalidMaxFileSize;
        
        // Validate transaction config
        if (self.transaction_config.max_read_keys == 0) return error.InvalidMaxReadKeys;
        if (self.transaction_config.max_write_keys == 0) return error.InvalidMaxWriteKeys;
        if (self.transaction_config.gc_watermark_ratio <= 0.0 or self.transaction_config.gc_watermark_ratio >= 1.0) {
            return error.InvalidGCWatermarkRatio;
        }
        
        // Validate retry config
        if (self.retry_config.max_retries == 0) return error.InvalidMaxRetries;
        if (self.retry_config.initial_delay_ms == 0) return error.InvalidInitialDelay;
        if (self.retry_config.max_delay_ms == 0) return error.InvalidMaxDelay;
        if (self.retry_config.backoff_multiplier <= 1.0) return error.InvalidBackoffMultiplier;
        if (self.retry_config.jitter_ratio < 0.0 or self.retry_config.jitter_ratio > 1.0) {
            return error.InvalidJitterRatio;
        }
        
        // Validate I/O config
        if (self.io_config.compaction_throttle_bytes_per_sec == 0) return error.InvalidCompactionThrottle;
        if (self.io_config.write_throttle_bytes_per_sec == 0) return error.InvalidWriteThrottle;
        if (self.io_config.read_ahead_size == 0) return error.InvalidReadAheadSize;
        if (self.io_config.write_buffer_size == 0) return error.InvalidWriteBufferSize;
        if (self.io_config.sync_frequency_ms == 0) return error.InvalidSyncFrequency;
        
        // Validate cache config
        if (self.cache_config.block_cache_size == 0) return error.InvalidBlockCacheSize;
        if (self.cache_config.table_cache_size == 0) return error.InvalidTableCacheSize;
        if (self.cache_config.filter_cache_size == 0) return error.InvalidFilterCacheSize;
        if (self.cache_config.cache_shards == 0) return error.InvalidCacheShards;
        
        // Validate monitoring config
        if (self.monitoring_config.metrics_interval_ms == 0) return error.InvalidMetricsInterval;
        if (self.monitoring_config.max_error_history == 0) return error.InvalidMaxErrorHistory;
        if (self.monitoring_config.slow_query_threshold_ms == 0) return error.InvalidSlowQueryThreshold;
        
        // Validate recovery config
        if (self.recovery_config.wal_file_size == 0) return error.InvalidWALFileSize;
        if (self.recovery_config.checkpoint_interval_ms == 0) return error.InvalidCheckpointInterval;
        if (self.recovery_config.manifest_rewrite_threshold == 0) return error.InvalidManifestRewriteThreshold;
        if (self.recovery_config.crash_recovery_timeout_ms == 0) return error.InvalidCrashRecoveryTimeout;
        if (self.recovery_config.backup_retention_days == 0) return error.InvalidBackupRetentionDays;
        
        // Validate encryption config
        if (self.encryption_config.enable_encryption and self.encryption_config.encryption_key == null) {
            return error.MissingEncryptionKey;
        }
        if (self.encryption_config.key_rotation_interval_days == 0) return error.InvalidKeyRotationInterval;
    }

    /// Create default configuration
    pub fn default(dir: []const u8) Self {
        return Self{
            .dir = dir,
            .value_dir = dir,
        };
    }

    /// Create configuration optimized for development
    pub fn development(dir: []const u8) Self {
        var opts = Self.default(dir);
        opts.mem_table_size = 1 * 1024 * 1024; // 1MB
        opts.value_log_file_size = 10 * 1024 * 1024; // 10MB
        opts.num_compactors = 1;
        opts.sync_writes = false;
        opts.compression = .none;
        opts.cache_config.block_cache_size = 16 * 1024 * 1024; // 16MB
        opts.monitoring_config.enable_metrics = true;
        opts.monitoring_config.log_level = .debug;
        opts.recovery_config.wal_file_size = 1 * 1024 * 1024; // 1MB
        opts.io_config.compaction_throttle_bytes_per_sec = 1024 * 1024; // 1MB/s
        return opts;
    }

    /// Create configuration optimized for production
    pub fn production(dir: []const u8) Self {
        var opts = Self.default(dir);
        opts.mem_table_size = 128 * 1024 * 1024; // 128MB
        opts.value_log_file_size = 2 * 1024 * 1024 * 1024; // 2GB
        opts.num_compactors = 8;
        opts.sync_writes = true;
        opts.compression = .zlib;
        opts.compression_level = 6;
        opts.cache_config.block_cache_size = 512 * 1024 * 1024; // 512MB
        opts.monitoring_config.enable_metrics = true;
        opts.monitoring_config.log_level = .info;
        opts.recovery_config.wal_sync_writes = true;
        opts.encryption_config.enable_encryption = false; // Must be explicitly enabled
        opts.io_config.compaction_throttle_bytes_per_sec = 50 * 1024 * 1024; // 50MB/s
        return opts;
    }

    /// Create configuration optimized for high performance
    pub fn highPerformance(dir: []const u8) Self {
        var opts = Self.default(dir);
        opts.mem_table_size = 256 * 1024 * 1024; // 256MB
        opts.value_log_file_size = 4 * 1024 * 1024 * 1024; // 4GB
        opts.num_compactors = 16;
        opts.sync_writes = false;
        opts.compression = .zlib;
        opts.cache_config.block_cache_size = 1024 * 1024 * 1024; // 1GB
        opts.cache_config.table_cache_size = 256 * 1024 * 1024; // 256MB
        opts.monitoring_config.enable_metrics = true;
        opts.monitoring_config.log_level = .warn;
        opts.recovery_config.wal_sync_writes = false;
        opts.io_config.compaction_throttle_bytes_per_sec = 200 * 1024 * 1024; // 200MB/s
        opts.io_config.write_throttle_bytes_per_sec = 200 * 1024 * 1024; // 200MB/s
        opts.io_config.use_mmap = true;
        opts.io_config.direct_io = true;
        return opts;
    }

    /// Create configuration optimized for memory usage
    pub fn memoryOptimized(dir: []const u8) Self {
        var opts = Self.default(dir);
        opts.mem_table_size = 16 * 1024 * 1024; // 16MB
        opts.value_log_file_size = 256 * 1024 * 1024; // 256MB
        opts.num_compactors = 2;
        opts.sync_writes = true;
        opts.compression = .zlib;
        opts.compression_level = 9;
        opts.cache_config.block_cache_size = 32 * 1024 * 1024; // 32MB
        opts.cache_config.table_cache_size = 16 * 1024 * 1024; // 16MB
        opts.cache_config.filter_cache_size = 8 * 1024 * 1024; // 8MB
        opts.cache_config.enable_cache_compression = true;
        opts.monitoring_config.enable_metrics = false;
        opts.io_config.compaction_throttle_bytes_per_sec = 5 * 1024 * 1024; // 5MB/s
        return opts;
    }

    // Builder pattern methods for core settings
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

    pub fn withCompressionLevel(self: Self, level: i32) Self {
        var opts = self;
        opts.compression_level = level;
        return opts;
    }

    pub fn withSyncWrites(self: Self, sync: bool) Self {
        var opts = self;
        opts.sync_writes = sync;
        return opts;
    }

    pub fn withCompactionStrategy(self: Self, strategy: CompactionStrategy) Self {
        var opts = self;
        opts.compaction_strategy = strategy;
        return opts;
    }

    pub fn withMaxLevels(self: Self, levels: u32) Self {
        var opts = self;
        opts.max_levels = levels;
        return opts;
    }

    pub fn withNumCompactors(self: Self, compactors: u32) Self {
        var opts = self;
        opts.num_compactors = compactors;
        return opts;
    }

    pub fn withBlockSize(self: Self, size: usize) Self {
        var opts = self;
        opts.block_size = size;
        return opts;
    }

    pub fn withValueThreshold(self: Self, threshold: usize) Self {
        var opts = self;
        opts.value_threshold = threshold;
        return opts;
    }

    pub fn withValueLogFileSize(self: Self, size: usize) Self {
        var opts = self;
        opts.value_log_file_size = size;
        return opts;
    }

    pub fn withBloomFilter(self: Self, false_positive: f64, expected_items: u32) Self {
        var opts = self;
        opts.bloom_false_positive = false_positive;
        opts.bloom_expected_items = expected_items;
        return opts;
    }

    pub fn withChecksumType(self: Self, checksum_type: ChecksumType) Self {
        var opts = self;
        opts.checksum_type = checksum_type;
        return opts;
    }

    // Builder pattern methods for advanced configuration
    pub fn withTransactionConfig(self: Self, config: TransactionConfig) Self {
        var opts = self;
        opts.transaction_config = config;
        return opts;
    }

    pub fn withRetryConfig(self: Self, config: RetryConfig) Self {
        var opts = self;
        opts.retry_config = config;
        return opts;
    }

    pub fn withIOConfig(self: Self, config: IOConfig) Self {
        var opts = self;
        opts.io_config = config;
        return opts;
    }

    pub fn withCacheConfig(self: Self, config: CacheConfig) Self {
        var opts = self;
        opts.cache_config = config;
        return opts;
    }

    pub fn withMonitoringConfig(self: Self, config: MonitoringConfig) Self {
        var opts = self;
        opts.monitoring_config = config;
        return opts;
    }

    pub fn withRecoveryConfig(self: Self, config: RecoveryConfig) Self {
        var opts = self;
        opts.recovery_config = config;
        return opts;
    }

    pub fn withEncryptionConfig(self: Self, config: EncryptionConfig) Self {
        var opts = self;
        opts.encryption_config = config;
        return opts;
    }

    pub fn withValidationConfig(self: Self, config: ValidationConfig) Self {
        var opts = self;
        opts.validation_config = config;
        return opts;
    }

    // Convenience methods for common settings
    pub fn withMaxTransactionKeys(self: Self, read_keys: u32, write_keys: u32) Self {
        var opts = self;
        opts.transaction_config.max_read_keys = read_keys;
        opts.transaction_config.max_write_keys = write_keys;
        return opts;
    }

    pub fn withIOThrottling(self: Self, compaction_bps: u64, write_bps: u64) Self {
        var opts = self;
        opts.io_config.compaction_throttle_bytes_per_sec = compaction_bps;
        opts.io_config.write_throttle_bytes_per_sec = write_bps;
        return opts;
    }

    pub fn withCacheSizes(self: Self, block_cache: usize, table_cache: usize, filter_cache: usize) Self {
        var opts = self;
        opts.cache_config.block_cache_size = block_cache;
        opts.cache_config.table_cache_size = table_cache;
        opts.cache_config.filter_cache_size = filter_cache;
        return opts;
    }

    pub fn withEncryption(self: Self, enable: bool, key: ?[]const u8) Self {
        var opts = self;
        opts.encryption_config.enable_encryption = enable;
        opts.encryption_config.encryption_key = key;
        return opts;
    }

    pub fn withLogLevel(self: Self, level: LogLevel) Self {
        var opts = self;
        opts.monitoring_config.log_level = level;
        return opts;
    }

    pub fn withValidationLimits(self: Self, max_key_size: usize, max_value_size: usize, max_batch_size: u32) Self {
        var opts = self;
        opts.validation_config.max_key_size = max_key_size;
        opts.validation_config.max_value_size = max_value_size;
        opts.validation_config.max_batch_size = max_batch_size;
        return opts;
    }

    // Utility methods for configuration calculations
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

    /// Calculate total memory usage for configured cache sizes
    pub fn calculateCacheMemoryUsage(self: *const Self) usize {
        return self.cache_config.block_cache_size + 
               self.cache_config.table_cache_size + 
               self.cache_config.filter_cache_size;
    }

    /// Calculate total memory usage for configured memtables
    pub fn calculateMemTableMemoryUsage(self: *const Self) usize {
        return self.mem_table_size * self.num_memtables;
    }

    /// Calculate estimated total memory usage
    pub fn calculateEstimatedMemoryUsage(self: *const Self) usize {
        return self.calculateCacheMemoryUsage() + 
               self.calculateMemTableMemoryUsage() + 
               self.io_config.write_buffer_size +
               self.io_config.read_ahead_size;
    }

    /// Get optimal bloom filter size for current configuration
    pub fn getOptimalBloomFilterSize(self: *const Self) usize {
        // Calculate based on expected items and false positive rate
        // Size = -n * ln(p) / (ln(2)^2) where n=items, p=false_positive
        const items = @as(f64, @floatFromInt(self.bloom_expected_items));
        const ln_p = std.math.log(f64, std.math.e, self.bloom_false_positive);
        const ln_2 = std.math.log(f64, std.math.e, 2.0);
        const size = -items * ln_p / (ln_2 * ln_2);
        return @intFromFloat(size / 8.0); // Convert bits to bytes
    }

    /// Check if the configuration is suitable for high concurrency
    pub fn isSuitableForHighConcurrency(self: *const Self) bool {
        return self.num_compactors >= 4 and 
               self.cache_config.cache_shards >= 8 and
               self.transaction_config.max_read_keys >= 1000 and
               self.transaction_config.max_write_keys >= 100;
    }

    /// Check if the configuration is suitable for large datasets
    pub fn isSuitableForLargeDatasets(self: *const Self) bool {
        return self.mem_table_size >= 64 * 1024 * 1024 and // 64MB
               self.value_log_file_size >= 1024 * 1024 * 1024 and // 1GB
               self.cache_config.block_cache_size >= 256 * 1024 * 1024 and // 256MB
               self.max_levels >= 5;
    }

    /// Get a summary of the configuration for logging/debugging
    pub fn getSummary(self: *const Self) ConfigSummary {
        return ConfigSummary{
            .estimated_memory_usage = self.calculateEstimatedMemoryUsage(),
            .cache_memory_usage = self.calculateCacheMemoryUsage(),
            .memtable_memory_usage = self.calculateMemTableMemoryUsage(),
            .bloom_filter_size = self.getOptimalBloomFilterSize(),
            .suitable_for_high_concurrency = self.isSuitableForHighConcurrency(),
            .suitable_for_large_datasets = self.isSuitableForLargeDatasets(),
            .max_theoretical_level_size = self.tableSizeForLevel(self.max_levels - 1),
            .compression_enabled = self.compression != .none,
            .encryption_enabled = self.encryption_config.enable_encryption,
            .monitoring_enabled = self.monitoring_config.enable_metrics,
            .wal_enabled = self.recovery_config.enable_wal,
            .sync_writes_enabled = self.sync_writes,
        };
    }

    /// Validate configuration compatibility
    pub fn validateCompatibility(self: *const Self) !void {
        // Check if compression and encryption are compatible
        if (self.encryption_config.enable_encryption and 
            self.encryption_config.compress_before_encrypt and 
            self.compression == .none) {
            return error.IncompatibleCompressionEncryption;
        }
        
        // Check if cache sizes are reasonable relative to memory
        const cache_usage = self.calculateCacheMemoryUsage();
        const memtable_usage = self.calculateMemTableMemoryUsage();
        const total_usage = cache_usage + memtable_usage;
        
        if (total_usage > 8 * 1024 * 1024 * 1024) { // 8GB warning threshold
            return error.ExcessiveMemoryUsage;
        }
        
        // Check if I/O throttling is reasonable
        if (self.io_config.compaction_throttle_bytes_per_sec > 1024 * 1024 * 1024) { // 1GB/s
            return error.ExcessiveIOThrottling;
        }
        
        // Check if value log settings are compatible
        if (self.value_threshold > self.value_log_file_size / 10) {
            return error.IncompatibleValueLogSettings;
        }
        
        // Check if transaction limits are reasonable
        if (self.transaction_config.max_read_keys > 1000000) { // 1M keys
            return error.ExcessiveTransactionLimits;
        }
        
        // Check if retry settings are reasonable
        if (self.retry_config.max_delay_ms > 300000) { // 5 minutes
            return error.ExcessiveRetryDelay;
        }
        
        // Check if monitoring settings are compatible
        if (self.monitoring_config.enable_metrics and 
            self.monitoring_config.metrics_interval_ms < 100) {
            return error.ExcessiveMonitoringFrequency;
        }
    }
};

/// Summary of configuration settings for debugging and monitoring
pub const ConfigSummary = struct {
    estimated_memory_usage: usize,
    cache_memory_usage: usize,
    memtable_memory_usage: usize,
    bloom_filter_size: usize,
    suitable_for_high_concurrency: bool,
    suitable_for_large_datasets: bool,
    max_theoretical_level_size: usize,
    compression_enabled: bool,
    encryption_enabled: bool,
    monitoring_enabled: bool,
    wal_enabled: bool,
    sync_writes_enabled: bool,
};
