const std = @import("std");
const testing = std.testing;
const wombat = @import("wombat");
const Options = wombat.Options;

test "Options default configuration" {
    const opts = Options.default("/tmp/test_db");
    
    try testing.expect(std.mem.eql(u8, opts.dir, "/tmp/test_db"));
    try testing.expect(std.mem.eql(u8, opts.value_dir, "/tmp/test_db"));
    try testing.expect(opts.mem_table_size == 64 * 1024 * 1024);
    try testing.expect(opts.compression == .zlib);
    try testing.expect(opts.sync_writes == false);
    try testing.expect(opts.detect_conflicts == true);
    
    // Test validation
    try opts.validateRuntime();
    
    std.debug.print("Default options test passed\n", .{});
}

test "Options development preset" {
    const opts = Options.development("/tmp/test_dev");
    
    try testing.expect(opts.mem_table_size == 1 * 1024 * 1024); // 1MB
    try testing.expect(opts.compression == .none);
    try testing.expect(opts.num_compactors == 1);
    try testing.expect(opts.monitoring_config.log_level == .debug);
    try testing.expect(opts.cache_config.block_cache_size == 16 * 1024 * 1024); // 16MB
    
    try opts.validateRuntime();
    
    std.debug.print("Development preset test passed\n", .{});
}

test "Options production preset" {
    const opts = Options.production("/tmp/test_prod");
    
    try testing.expect(opts.mem_table_size == 128 * 1024 * 1024); // 128MB
    try testing.expect(opts.compression == .zlib);
    try testing.expect(opts.num_compactors == 8);
    try testing.expect(opts.sync_writes == true);
    try testing.expect(opts.monitoring_config.log_level == .info);
    try testing.expect(opts.cache_config.block_cache_size == 512 * 1024 * 1024); // 512MB
    
    try opts.validateRuntime();
    
    std.debug.print("Production preset test passed\n", .{});
}

test "Options high performance preset" {
    const opts = Options.highPerformance("/tmp/test_perf");
    
    try testing.expect(opts.mem_table_size == 256 * 1024 * 1024); // 256MB
    try testing.expect(opts.num_compactors == 16);
    try testing.expect(opts.sync_writes == false);
    try testing.expect(opts.io_config.direct_io == true);
    try testing.expect(opts.cache_config.block_cache_size == 1024 * 1024 * 1024); // 1GB
    
    try opts.validateRuntime();
    
    std.debug.print("High performance preset test passed\n", .{});
}

test "Options memory optimized preset" {
    const opts = Options.memoryOptimized("/tmp/test_mem");
    
    try testing.expect(opts.mem_table_size == 16 * 1024 * 1024); // 16MB
    try testing.expect(opts.compression == .zlib);
    try testing.expect(opts.compression_level == 9);
    try testing.expect(opts.cache_config.block_cache_size == 32 * 1024 * 1024); // 32MB
    try testing.expect(opts.cache_config.enable_cache_compression == true);
    
    try opts.validateRuntime();
    
    std.debug.print("Memory optimized preset test passed\n", .{});
}

test "Options builder pattern" {
    const opts = Options.default("/tmp/test_builder")
        .withMemTableSize(32 * 1024 * 1024)
        .withCompression(.zlib)
        .withCompressionLevel(3)
        .withSyncWrites(true)
        .withNumCompactors(4)
        .withLogLevel(.warn)
        .withMaxTransactionKeys(5000, 500)
        .withIOThrottling(20 * 1024 * 1024, 100 * 1024 * 1024)
        .withCacheSizes(128 * 1024 * 1024, 32 * 1024 * 1024, 16 * 1024 * 1024)
        .withValidationLimits(512 * 1024, 512 * 1024 * 1024, 500);
    
    try testing.expect(opts.mem_table_size == 32 * 1024 * 1024);
    try testing.expect(opts.compression == .zlib);
    try testing.expect(opts.compression_level == 3);
    try testing.expect(opts.sync_writes == true);
    try testing.expect(opts.num_compactors == 4);
    try testing.expect(opts.monitoring_config.log_level == .warn);
    try testing.expect(opts.transaction_config.max_read_keys == 5000);
    try testing.expect(opts.transaction_config.max_write_keys == 500);
    try testing.expect(opts.io_config.compaction_throttle_bytes_per_sec == 20 * 1024 * 1024);
    try testing.expect(opts.io_config.write_throttle_bytes_per_sec == 100 * 1024 * 1024);
    try testing.expect(opts.cache_config.block_cache_size == 128 * 1024 * 1024);
    try testing.expect(opts.cache_config.table_cache_size == 32 * 1024 * 1024);
    try testing.expect(opts.cache_config.filter_cache_size == 16 * 1024 * 1024);
    try testing.expect(opts.validation_config.max_key_size == 512 * 1024);
    try testing.expect(opts.validation_config.max_value_size == 512 * 1024 * 1024);
    try testing.expect(opts.validation_config.max_batch_size == 500);
    
    try opts.validateRuntime();
    
    std.debug.print("Builder pattern test passed\n", .{});
}

test "Options utility methods" {
    const opts = Options.production("/tmp/test_util");
    
    // Test memory usage calculations
    const cache_memory = opts.calculateCacheMemoryUsage();
    const memtable_memory = opts.calculateMemTableMemoryUsage();
    const total_memory = opts.calculateEstimatedMemoryUsage();
    
    try testing.expect(cache_memory > 0);
    try testing.expect(memtable_memory > 0);
    try testing.expect(total_memory > cache_memory);
    try testing.expect(total_memory > memtable_memory);
    
    // Test bloom filter size calculation
    const bloom_size = opts.getOptimalBloomFilterSize();
    try testing.expect(bloom_size > 0);
    
    // Test suitability checks
    try testing.expect(opts.isSuitableForHighConcurrency());
    try testing.expect(opts.isSuitableForLargeDatasets());
    
    // Test level calculations
    const level_0_size = opts.tableSizeForLevel(0);
    const level_1_size = opts.tableSizeForLevel(1);
    try testing.expect(level_0_size == opts.base_table_size);
    try testing.expect(level_1_size == opts.base_table_size * opts.level_size_multiplier);
    
    // Test configuration summary
    const summary = opts.getSummary();
    try testing.expect(summary.compression_enabled);
    try testing.expect(summary.monitoring_enabled);
    try testing.expect(summary.wal_enabled);
    try testing.expect(summary.sync_writes_enabled);
    
    // Test compatibility validation
    try opts.validateCompatibility();
    
    std.debug.print("Utility methods test passed\n", .{});
}

test "Options validation errors" {
    var opts = Options.default("/tmp/test_validation");
    
    // Test invalid mem_table_size
    opts.mem_table_size = 0;
    try testing.expectError(error.InvalidMemTableSize, opts.validateRuntime());
    
    opts = Options.default("/tmp/test_validation");
    
    // Test invalid bloom false positive
    opts.bloom_false_positive = 1.5;
    try testing.expectError(error.InvalidBloomFalsePositive, opts.validateRuntime());
    
    opts = Options.default("/tmp/test_validation");
    
    // Test invalid compression level
    opts.compression_level = 15;
    try testing.expectError(error.InvalidCompressionLevel, opts.validateRuntime());
    
    opts = Options.default("/tmp/test_validation");
    
    // Test missing encryption key
    opts.encryption_config.enable_encryption = true;
    opts.encryption_config.encryption_key = null;
    try testing.expectError(error.MissingEncryptionKey, opts.validateRuntime());
    
    std.debug.print("Validation errors test passed\n", .{});
}

test "Options compatibility validation" {
    var opts = Options.default("/tmp/test_compat");
    
    // Test excessive memory usage
    opts.mem_table_size = 8 * 1024 * 1024 * 1024; // 8GB
    opts.cache_config.block_cache_size = 2 * 1024 * 1024 * 1024; // 2GB
    try testing.expectError(error.ExcessiveMemoryUsage, opts.validateCompatibility());
    
    opts = Options.default("/tmp/test_compat");
    
    // Test incompatible value log settings
    opts.value_threshold = opts.value_log_file_size; // Threshold equal to file size
    try testing.expectError(error.IncompatibleValueLogSettings, opts.validateCompatibility());
    
    std.debug.print("Compatibility validation test passed\n", .{});
}

/// Helper function to clean up test directories
fn cleanupTestDir(dir_path: []const u8) void {
    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch return;
    defer dir.close();
    var iterator = dir.iterate();
    while (iterator.next() catch null) |entry| {
        if (entry.kind == .file) {
            var path_buf: [512]u8 = undefined;
            const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir_path, entry.name }) catch continue;
            std.fs.cwd().deleteFile(path) catch {};
        }
    }
    std.fs.cwd().deleteDir(dir_path) catch {};
}