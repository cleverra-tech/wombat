const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const HashMap = std.HashMap;
const atomic = std.atomic;

/// Enhanced error context for debugging and monitoring
pub const ErrorContext = struct {
    operation: []const u8,
    file_path: ?[]const u8,
    key: ?[]const u8,
    timestamp: u64,
    additional_info: ?[]const u8,

    const Self = @This();

    pub fn init(operation: []const u8) Self {
        return Self{
            .operation = operation,
            .file_path = null,
            .key = null,
            .timestamp = @intCast(std.time.milliTimestamp()),
            .additional_info = null,
        };
    }

    pub fn withFilePath(self: Self, file_path: []const u8) Self {
        var result = self;
        result.file_path = file_path;
        return result;
    }

    pub fn withKey(self: Self, key: []const u8) Self {
        var result = self;
        result.key = key;
        return result;
    }

    pub fn withInfo(self: Self, info: []const u8) Self {
        var result = self;
        result.additional_info = info;
        return result;
    }

    pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;

        try writer.print("ErrorContext{{ operation: '{s}', timestamp: {any}", .{ self.operation, self.timestamp });

        if (self.file_path) |path| {
            try writer.print(", file_path: '{s}'", .{path});
        }

        if (self.key) |key| {
            try writer.print(", key: '{s}'", .{key});
        }

        if (self.additional_info) |info| {
            try writer.print(", info: '{s}'", .{info});
        }

        try writer.print(" }}");
    }
};

/// Enhanced database errors with context
pub const DBError = error{
    DatabaseClosed,
    InvalidOptions,
    CorruptedData,
    OutOfMemory,
    IOError,
    TransactionError,
    InvalidKey,
    KeyTooLarge,
    ValueTooLarge,
    ConfigurationError,
    AuthenticationError,
    PermissionDenied,
    DiskSpaceExhausted,
    NetworkError,
    TimeoutError,
    ResourceExhausted,
    InvalidState,
    VersionMismatch,
    LockTimeout,
    DeadlockDetected,
    MaxRetriesExceeded,
};

/// Value log specific errors
pub const VLogError = error{
    FileFull,
    WrongFile,
    InvalidPointer,
    CorruptedEntry,
    ChecksumMismatch,
    CompressionFailed,
    DecompressionFailed,
    FileNotFound,
    InvalidFileFormat,
    OutOfMemory,
    IOError,
    DiskSpaceExhausted,
    WriteTimeout,
    ReadTimeout,
    ConcurrentModification,
    InvalidOffset,
    InvalidSize,
    TruncatedFile,
    PermissionDenied,
    // Additional compression errors
    EndOfStream,
    Unknown,
    BufferTooSmall,
    InvalidInput,
    UnsupportedAlgorithm,
    StreamTooLong,
    InvalidData,
    CorruptInput,
    DictRequired,
    StreamEnd,
    NeedDict,
    Errno,
    BufError,
    VersionError,
    DataError,
    MemError,
    // Additional I/O errors
    OutOfBounds,
    InputOutput,
    SystemResources,
    IsDir,
    WouldBlock,
    AccessDenied,
    ProcessNotFound,
    Unexpected,
    SharingViolation,
    PathAlreadyExists,
    PipeBusy,
    NoDevice,
    NameTooLong,
    InvalidUtf8,
    InvalidWtf8,
    BadPathName,
    NetworkNotFound,
    AntivirusInterference,
    SymLinkLoop,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    FileTooBig,
    NoSpaceLeft,
    NotDir,
    DeviceBusy,
    FileLocksNotSupported,
    FileBusy,
    MemoryMappingNotSupported,
    LockedMemoryLimitExceeded,
    MappingAlreadyExists,
};

/// Manifest specific errors
pub const ManifestError = error{
    TableNotFound,
    FileNotFound,
    InvalidLevel,
    CorruptedManifest,
    VersionMismatch,
    InvalidTableId,
    DuplicateTableId,
    InvalidChecksum,
    OutOfMemory,
    IOError,
    ParseError,
    SerializationError,
    ConcurrentModification,
    OperationTimeout,
    InvalidOperation,
    ManifestLocked,
    BackupFailed,
    RestoreFailed,
};

/// Enhanced transaction errors with conflict details
pub const TxnError = error{
    TransactionConflict,
    TransactionAborted,
    TransactionReadOnly,
    KeyNotFound,
    OutOfMemory,
    InvalidOperation,
    TransactionExpired,
    DeadlockDetected,
    ValidationFailed,
    CommitConflict,
    ReadConflict,
    WriteConflict,
    VersionConflict,
    LockTimeout,
    ConcurrentModification,
    TransactionTooLarge,
    InvalidTransactionState,
    RollbackFailed,
};

/// Table specific errors
pub const TableError = error{
    InvalidTableFile,
    UnsupportedVersion,
    CorruptedData,
    BlockSizeExceeded,
    InvalidKeyOrder,
    ChecksumMismatch,
    OutOfMemory,
    IOError,
    IndexCorrupted,
    BloomFilterCorrupted,
    InvalidMetadata,
    ConcurrentAccess,
    TableLocked,
    TableNotFound,
    InvalidBlockFormat,
    DeserializationError,
    SerializationError,
    CacheEviction,
    InvalidCacheState,
};

/// Compaction specific errors
pub const CompactionError = error{
    OutOfMemory,
    IOError,
    CompactionAborted,
    CompactionConflict,
    CompactionTimeout,
    InvalidLevel,
    InsufficientSpace,
    ConcurrentCompaction,
    CompactionFailed,
    OutputSizeExceeded,
    TooManyFiles,
    InvalidCompactionJob,
    ResourceExhausted,
    ThrottlingActive,
    WorkerPoolExhausted,
    JobQueueFull,
    PriorityInversion,
    SchedulingFailed,
};

/// Conflict type for transaction errors
pub const ConflictType = enum {
    ReadWriteConflict,
    WriteWriteConflict,
    VersionConflict,
    KeyConflict,
    TimestampConflict,
    SerializationConflict,

    pub fn toString(self: ConflictType) []const u8 {
        return switch (self) {
            .ReadWriteConflict => "Read-Write conflict",
            .WriteWriteConflict => "Write-Write conflict",
            .VersionConflict => "Version conflict",
            .KeyConflict => "Key conflict",
            .TimestampConflict => "Timestamp conflict",
            .SerializationConflict => "Serialization conflict",
        };
    }
};

/// Transaction error context
pub const TxnErrorContext = struct {
    transaction_id: u64,
    conflicting_keys: [][]const u8,
    read_timestamp: u64,
    commit_timestamp: ?u64,
    conflict_type: ConflictType,
    operation: []const u8,

    const Self = @This();

    pub fn init(transaction_id: u64, read_timestamp: u64, operation: []const u8) Self {
        return Self{
            .transaction_id = transaction_id,
            .conflicting_keys = &[_][]const u8{},
            .read_timestamp = read_timestamp,
            .commit_timestamp = null,
            .conflict_type = .ReadWriteConflict,
            .operation = operation,
        };
    }

    pub fn withConflictingKeys(self: Self, keys: [][]const u8) Self {
        var result = self;
        result.conflicting_keys = keys;
        return result;
    }

    pub fn withCommitTimestamp(self: Self, timestamp: u64) Self {
        var result = self;
        result.commit_timestamp = timestamp;
        return result;
    }

    pub fn withConflictType(self: Self, conflict_type: ConflictType) Self {
        var result = self;
        result.conflict_type = conflict_type;
        return result;
    }

    pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;

        try writer.print("TxnErrorContext{{ id: {}, operation: '{s}', read_ts: {}, conflict: '{s}'", .{
            self.transaction_id,
            self.operation,
            self.read_timestamp,
            self.conflict_type.toString(),
        });

        if (self.commit_timestamp) |ts| {
            try writer.print(", commit_ts: {any}", .{ts});
        }

        if (self.conflicting_keys.len > 0) {
            try writer.print(", conflicting_keys: [");
            for (self.conflicting_keys, 0..) |key, i| {
                if (i > 0) try writer.print(", ");
                try writer.print("'{s}'", .{key});
            }
            try writer.print("]");
        }

        try writer.print(" }}");
    }
};

/// Retry configuration for transient errors
pub const RetryConfig = struct {
    max_retries: u32,
    initial_delay_ms: u64,
    max_delay_ms: u64,
    backoff_multiplier: f64,
    jitter_enabled: bool,

    const Self = @This();

    pub fn init() Self {
        return Self{
            .max_retries = 3,
            .initial_delay_ms = 10,
            .max_delay_ms = 1000,
            .backoff_multiplier = 2.0,
            .jitter_enabled = true,
        };
    }

    pub fn withMaxRetries(self: Self, max_retries: u32) Self {
        var result = self;
        result.max_retries = max_retries;
        return result;
    }

    pub fn withInitialDelay(self: Self, delay_ms: u64) Self {
        var result = self;
        result.initial_delay_ms = delay_ms;
        return result;
    }

    pub fn calculateDelay(self: Self, attempt: u32) u64 {
        const base_delay = @as(f64, @floatFromInt(self.initial_delay_ms)) * std.math.pow(f64, self.backoff_multiplier, @as(f64, @floatFromInt(attempt)));
        var delay = @as(u64, @intFromFloat(@min(base_delay, @as(f64, @floatFromInt(self.max_delay_ms)))));

        if (self.jitter_enabled) {
            // Add up to 10% jitter
            const jitter_range = delay / 10;
            const jitter = @as(u64, @intCast(std.crypto.random.int(u32))) % jitter_range;
            delay += jitter;
        }

        return delay;
    }
};

/// Error metrics for monitoring
pub const ErrorMetrics = struct {
    db_errors: HashMap(DBError, u64, std.hash_map.AutoContext(DBError), 80),
    vlog_errors: HashMap(VLogError, u64, std.hash_map.AutoContext(VLogError), 80),
    manifest_errors: HashMap(ManifestError, u64, std.hash_map.AutoContext(ManifestError), 80),
    transaction_errors: HashMap(TxnError, u64, std.hash_map.AutoContext(TxnError), 80),
    table_errors: HashMap(TableError, u64, std.hash_map.AutoContext(TableError), 80),
    compaction_errors: HashMap(CompactionError, u64, std.hash_map.AutoContext(CompactionError), 80),
    error_timestamps: ArrayList(u64),
    total_errors: atomic.Value(u64),
    allocator: Allocator,

    const Self = @This();

    pub fn init(allocator: Allocator) Self {
        return Self{
            .db_errors = HashMap(DBError, u64, std.hash_map.AutoContext(DBError), 80).init(allocator),
            .vlog_errors = HashMap(VLogError, u64, std.hash_map.AutoContext(VLogError), 80).init(allocator),
            .manifest_errors = HashMap(ManifestError, u64, std.hash_map.AutoContext(ManifestError), 80).init(allocator),
            .transaction_errors = HashMap(TxnError, u64, std.hash_map.AutoContext(TxnError), 80).init(allocator),
            .table_errors = HashMap(TableError, u64, std.hash_map.AutoContext(TableError), 80).init(allocator),
            .compaction_errors = HashMap(CompactionError, u64, std.hash_map.AutoContext(CompactionError), 80).init(allocator),
            .error_timestamps = ArrayList(u64).init(allocator),
            .total_errors = atomic.Value(u64).init(0),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.db_errors.deinit();
        self.vlog_errors.deinit();
        self.manifest_errors.deinit();
        self.transaction_errors.deinit();
        self.table_errors.deinit();
        self.compaction_errors.deinit();
        self.error_timestamps.deinit();
    }

    pub fn recordError(self: *Self, comptime ErrorType: type, error_value: ErrorType) void {
        const timestamp = @as(u64, @intCast(std.time.milliTimestamp()));

        switch (ErrorType) {
            DBError => {
                const current = self.db_errors.get(error_value) orelse 0;
                self.db_errors.put(error_value, current + 1) catch {};
            },
            VLogError => {
                const current = self.vlog_errors.get(error_value) orelse 0;
                self.vlog_errors.put(error_value, current + 1) catch {};
            },
            ManifestError => {
                const current = self.manifest_errors.get(error_value) orelse 0;
                self.manifest_errors.put(error_value, current + 1) catch {};
            },
            TxnError => {
                const current = self.transaction_errors.get(error_value) orelse 0;
                self.transaction_errors.put(error_value, current + 1) catch {};
            },
            TableError => {
                const current = self.table_errors.get(error_value) orelse 0;
                self.table_errors.put(error_value, current + 1) catch {};
            },
            CompactionError => {
                const current = self.compaction_errors.get(error_value) orelse 0;
                self.compaction_errors.put(error_value, current + 1) catch {};
            },
            else => {},
        }

        self.error_timestamps.append(timestamp) catch {};
        _ = self.total_errors.fetchAdd(1, .acq_rel);
    }

    pub fn getTotalErrors(self: *const Self) u64 {
        return self.total_errors.load(.acquire);
    }

    pub fn getErrorCount(self: *const Self, comptime ErrorType: type, error_value: ErrorType) u64 {
        return switch (ErrorType) {
            DBError => self.db_errors.get(error_value) orelse 0,
            VLogError => self.vlog_errors.get(error_value) orelse 0,
            ManifestError => self.manifest_errors.get(error_value) orelse 0,
            TxnError => self.transaction_errors.get(error_value) orelse 0,
            TableError => self.table_errors.get(error_value) orelse 0,
            CompactionError => self.compaction_errors.get(error_value) orelse 0,
            else => 0,
        };
    }

    pub fn getErrorRate(self: *const Self, window_ms: u64) f64 {
        const now = @as(u64, @intCast(std.time.milliTimestamp()));
        const cutoff = now - window_ms;

        var count: u64 = 0;
        for (self.error_timestamps.items) |timestamp| {
            if (timestamp >= cutoff) {
                count += 1;
            }
        }

        return @as(f64, @floatFromInt(count)) / @as(f64, @floatFromInt(window_ms)) * 1000.0; // errors per second
    }
};

/// Enhanced error result with context
pub fn ErrorResult(comptime T: type, comptime ErrorType: type) type {
    return struct {
        value: ?T,
        error_type: ?ErrorType,
        context: ?ErrorContext,

        const Self = @This();

        pub fn ok(value: T) Self {
            return Self{
                .value = value,
                .error_type = null,
                .context = null,
            };
        }

        pub fn err(error_type: ErrorType, context: ErrorContext) Self {
            return Self{
                .value = null,
                .error_type = error_type,
                .context = context,
            };
        }

        pub fn isOk(self: Self) bool {
            return self.value != null;
        }

        pub fn isErr(self: Self) bool {
            return self.error_type != null;
        }

        pub fn unwrap(self: Self) T {
            return self.value.?;
        }

        pub fn unwrapErr(self: Self) ErrorType {
            return self.error_type.?;
        }

        pub fn getContext(self: Self) ?ErrorContext {
            return self.context;
        }
    };
}

/// Retry mechanism for transient errors
pub fn retryWithBackoff(
    comptime T: type,
    comptime ErrorType: type,
    operation: anytype,
    config: RetryConfig,
    error_metrics: ?*ErrorMetrics,
) ErrorType!T {
    var attempt: u32 = 0;

    while (attempt <= config.max_retries) {
        const result = operation() catch |err| {
            // Record error metrics if available
            if (error_metrics) |metrics| {
                metrics.recordError(ErrorType, err);
            }

            // Check if error is retryable
            if (isRetryableError(ErrorType, err) and attempt < config.max_retries) {
                const delay = config.calculateDelay(attempt);
                std.time.sleep(delay * std.time.ns_per_ms);
                attempt += 1;
                continue;
            }

            return err;
        };

        return result;
    }

    unreachable;
}

/// Determine if an error is retryable
fn isRetryableError(comptime ErrorType: type, err: ErrorType) bool {
    return switch (ErrorType) {
        DBError => switch (err) {
            error.IOError, error.TimeoutError, error.NetworkError, error.ResourceExhausted => true,
            else => false,
        },
        VLogError => switch (err) {
            error.IOError, error.WriteTimeout, error.ReadTimeout, error.ConcurrentModification => true,
            else => false,
        },
        ManifestError => switch (err) {
            error.IOError, error.ConcurrentModification, error.OperationTimeout => true,
            else => false,
        },
        TxnError => switch (err) {
            error.TransactionConflict, error.LockTimeout, error.ConcurrentModification => true,
            else => false,
        },
        TableError => switch (err) {
            error.IOError, error.ConcurrentAccess, error.CacheEviction => true,
            else => false,
        },
        CompactionError => switch (err) {
            error.IOError, error.CompactionConflict, error.CompactionTimeout, error.ResourceExhausted => true,
            else => false,
        },
        else => false,
    };
}

/// Error logging utility
pub fn logError(comptime ErrorType: type, err: ErrorType, context: ?ErrorContext) void {
    const error_name = @errorName(err);

    if (context) |ctx| {
        std.log.err("{s}: {s} - {}", .{ @typeName(ErrorType), error_name, ctx });
    } else {
        std.log.err("{s}: {s}", .{ @typeName(ErrorType), error_name });
    }
}

/// Error chain for tracking error propagation
pub const ErrorChain = struct {
    errors: ArrayList(ErrorChainEntry),
    allocator: Allocator,

    const Self = @This();

    const ErrorChainEntry = struct {
        error_name: []const u8,
        context: ?ErrorContext,
        timestamp: u64,
    };

    pub fn init(allocator: Allocator) Self {
        return Self{
            .errors = ArrayList(ErrorChainEntry).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        self.errors.deinit();
    }

    pub fn addError(self: *Self, comptime ErrorType: type, err: ErrorType, context: ?ErrorContext) void {
        const entry = ErrorChainEntry{
            .error_name = @errorName(err),
            .context = context,
            .timestamp = @as(u64, @intCast(std.time.milliTimestamp())),
        };

        self.errors.append(entry) catch {};
    }

    pub fn format(self: Self, comptime fmt: []const u8, options: std.fmt.FormatOptions, writer: anytype) !void {
        _ = fmt;
        _ = options;

        try writer.print("ErrorChain[\n");
        for (self.errors.items, 0..) |entry, i| {
            try writer.print("  {any}: {s} at {any}", .{ i, entry.error_name, entry.timestamp });
            if (entry.context) |ctx| {
                try writer.print(" - {any}", .{ctx});
            }
            try writer.print("\n");
        }
        try writer.print("]");
    }
};
