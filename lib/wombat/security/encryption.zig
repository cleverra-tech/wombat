const std = @import("std");
const Allocator = std.mem.Allocator;
const crypto = std.crypto;
const Random = std.rand.Random;
const Options = @import("../core/options.zig");

/// Encryption errors
pub const EncryptionError = error{
    InvalidKey,
    InvalidIV,
    InvalidInput,
    EncryptionFailed,
    DecryptionFailed,
    KeyDerivationFailed,
    OutOfMemory,
    InvalidKeyLength,
    InvalidIVLength,
    AuthenticationFailed,
};

/// Re-export EncryptionConfig from options for easier access
pub const EncryptionConfig = Options.EncryptionConfig;

/// Encryption key material
pub const EncryptionKey = struct {
    /// AES-256 key (32 bytes)
    key: [32]u8,
    /// Salt used for key derivation (dynamically sized)
    salt: []u8,
    /// Allocator for salt memory management
    allocator: Allocator,

    const Self = @This();

    /// Generate a new random encryption key
    pub fn generate(random: Random, salt_length: usize, allocator: Allocator) EncryptionError!Self {
        var key: [32]u8 = undefined;
        const salt = allocator.alloc(u8, salt_length) catch return EncryptionError.OutOfMemory;
        errdefer allocator.free(salt);

        random.bytes(&key);
        random.bytes(salt);
        return Self{
            .key = key,
            .salt = salt,
            .allocator = allocator,
        };
    }

    /// Derive key from password using PBKDF2
    pub fn fromPassword(password: []const u8, salt: []const u8, iterations: u32, allocator: Allocator) EncryptionError!Self {
        var key: [32]u8 = undefined;

        crypto.pwhash.pbkdf2(&key, password, salt, iterations, crypto.hash.sha2.Sha256) catch {
            return EncryptionError.KeyDerivationFailed;
        };

        const salt_copy = allocator.dupe(u8, salt) catch return EncryptionError.OutOfMemory;
        return Self{
            .key = key,
            .salt = salt_copy,
            .allocator = allocator,
        };
    }

    /// Securely clear key material
    pub fn clear(self: *Self) void {
        crypto.utils.secureZero(&self.key);
        crypto.utils.secureZero(self.salt);
        self.allocator.free(self.salt);
    }
};

/// Authenticated encryption context
pub const Encryption = struct {
    config: EncryptionConfig,
    key: ?EncryptionKey,
    random: Random,

    const Self = @This();
    const AES = crypto.core.aes.Aes256;
    const GCM = crypto.aead.aes_gcm.Aes256Gcm;
    pub fn init(config: EncryptionConfig, random: Random) Self {
        return Self{
            .config = config,
            .key = null,
            .random = random,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.key) |*key| {
            key.clear();
        }
    }

    /// Set encryption key
    pub fn setKey(self: *Self, key: EncryptionKey) void {
        if (self.key) |*existing_key| {
            existing_key.clear();
        }
        self.key = key;
    }

    /// Generate and set a new random key
    pub fn generateKey(self: *Self, allocator: Allocator) EncryptionError!EncryptionKey {
        const key = try EncryptionKey.generate(self.random, self.config.salt_length, allocator);
        self.setKey(key);
        return key;
    }

    /// Set key from password
    pub fn setKeyFromPassword(self: *Self, password: []const u8, allocator: Allocator) EncryptionError!void {
        if (!self.config.enable_encryption) return;

        const salt = allocator.alloc(u8, self.config.salt_length) catch return EncryptionError.OutOfMemory;
        defer allocator.free(salt);
        self.random.bytes(salt);

        const key = try EncryptionKey.fromPassword(password, salt, self.config.key_derivation_iterations, allocator);
        self.setKey(key);
    }
    /// Generate random IV
    pub fn generateIV(self: *Self, allocator: Allocator) EncryptionError![]u8 {
        const iv = allocator.alloc(u8, self.config.iv_length) catch return EncryptionError.OutOfMemory;
        self.random.bytes(iv);
        return iv;
    }

    /// Encrypt data using AES-256-GCM
    pub fn encrypt(self: *Self, plaintext: []const u8, additional_data: []const u8, allocator: Allocator) EncryptionError![]u8 {
        if (!self.config.enable_encryption) {
            return allocator.dupe(u8, plaintext) catch return EncryptionError.OutOfMemory;
        }

        const key = self.key orelse return EncryptionError.InvalidKey;

        // Generate random IV
        const iv = try self.generateIV(allocator);
        defer allocator.free(iv);

        // Validate IV length for AES-GCM (must be 12 bytes for GCM)
        if (iv.len != 12) {
            return EncryptionError.InvalidIVLength;
        }

        // Allocate buffer for IV + ciphertext + tag
        const output_len = iv.len + plaintext.len + GCM.tag_length;
        const output = allocator.alloc(u8, output_len) catch return EncryptionError.OutOfMemory;
        errdefer allocator.free(output);

        // Copy IV to output
        @memcpy(output[0..iv.len], iv);

        // Encrypt
        const ciphertext = output[iv.len .. iv.len + plaintext.len];
        const tag = output[iv.len + plaintext.len ..];

        GCM.encrypt(ciphertext, tag, plaintext, additional_data, iv[0..12].*, key.key) catch {
            return EncryptionError.EncryptionFailed;
        };

        return output;
    }
    /// Decrypt data using AES-256-GCM
    pub fn decrypt(self: *Self, ciphertext: []const u8, additional_data: []const u8, allocator: Allocator) EncryptionError![]u8 {
        if (!self.config.enable_encryption) {
            return allocator.dupe(u8, ciphertext) catch return EncryptionError.OutOfMemory;
        }

        const key = self.key orelse return EncryptionError.InvalidKey;

        // Validate IV length for AES-GCM (must be 12 bytes for GCM)
        if (self.config.iv_length != 12) {
            return EncryptionError.InvalidIVLength;
        }

        // Minimum size is IV + tag
        if (ciphertext.len < self.config.iv_length + GCM.tag_length) {
            return EncryptionError.InvalidInput;
        }

        // Extract IV, ciphertext, and tag
        const iv = ciphertext[0..self.config.iv_length];
        const encrypted_data = ciphertext[self.config.iv_length .. ciphertext.len - GCM.tag_length];
        const tag = ciphertext[ciphertext.len - GCM.tag_length ..];

        // Allocate buffer for plaintext
        const plaintext = allocator.alloc(u8, encrypted_data.len) catch return EncryptionError.OutOfMemory;
        errdefer allocator.free(plaintext);
        // Decrypt and verify
        GCM.decrypt(plaintext, encrypted_data, tag.*, additional_data, iv[0..12].*, key.key) catch {
            return EncryptionError.DecryptionFailed;
        };

        return plaintext;
    }

    /// Encrypt data in place (for memory-mapped files)
    pub fn encryptInPlace(self: *Self, data: []u8, additional_data: []const u8, iv: []const u8) EncryptionError!void {
        if (!self.config.enable_encryption) return;

        const key = self.key orelse return EncryptionError.InvalidKey;

        // Validate IV length for AES-GCM (must be 12 bytes for GCM)
        if (iv.len != 12) {
            return EncryptionError.InvalidIVLength;
        }

        if (data.len < GCM.tag_length) {
            return EncryptionError.InvalidInput;
        }

        const plaintext_len = data.len - GCM.tag_length;
        const plaintext = data[0..plaintext_len];
        const tag = data[plaintext_len..];
        GCM.encrypt(plaintext, tag[0..GCM.tag_length], plaintext, additional_data, iv[0..12].*, key.key) catch {
            return EncryptionError.EncryptionFailed;
        };
    }

    /// Decrypt data in place (for memory-mapped files)
    pub fn decryptInPlace(self: *Self, data: []u8, additional_data: []const u8, iv: []const u8) EncryptionError!void {
        if (!self.config.enable_encryption) return;

        const key = self.key orelse return EncryptionError.InvalidKey;

        // Validate IV length for AES-GCM (must be 12 bytes for GCM)
        if (iv.len != 12) {
            return EncryptionError.InvalidIVLength;
        }

        if (data.len < GCM.tag_length) {
            return EncryptionError.InvalidInput;
        }

        const ciphertext_len = data.len - GCM.tag_length;
        const ciphertext = data[0..ciphertext_len];
        const tag = data[ciphertext_len..];
        GCM.decrypt(ciphertext, ciphertext, tag[0..GCM.tag_length].*, additional_data, iv[0..12].*, key.key) catch {
            return EncryptionError.DecryptionFailed;
        };
    }

    /// Check if encryption is enabled
    pub fn isEnabled(self: *const Self) bool {
        return self.config.enable_encryption and self.key != null;
    }

    /// Get encryption overhead (IV + tag)
    pub fn getOverhead(self: *const Self) usize {
        if (!self.config.enable_encryption) return 0;
        return self.config.iv_length + GCM.tag_length; // IV + tag
    }

    /// Calculate encrypted size
    pub fn getEncryptedSize(self: *const Self, plaintext_size: usize) usize {
        if (!self.config.enable_encryption) return plaintext_size;
        return plaintext_size + self.getOverhead();
    }

    /// Calculate decrypted size
    pub fn getDecryptedSize(self: *const Self, ciphertext_size: usize) usize {
        if (!self.config.enable_encryption) return ciphertext_size;
        const overhead = self.getOverhead();
        return if (ciphertext_size >= overhead) ciphertext_size - overhead else 0;
    }
};

/// Utility functions for encryption
pub const EncryptionUtils = struct {
    /// Securely compare two byte arrays
    pub fn secureEqual(a: []const u8, b: []const u8) bool {
        if (a.len != b.len) return false;
        return crypto.utils.timingSafeEql([*]const u8, a.ptr, b.ptr, a.len);
    }

    /// Generate cryptographically secure random bytes
    pub fn randomBytes(buffer: []u8) void {
        crypto.random.bytes(buffer);
    }

    /// Hash data using SHA-256
    pub fn hash(data: []const u8, output: *[32]u8) void {
        crypto.hash.sha2.Sha256.hash(data, output, .{});
    }

    /// Derive key from password with random salt
    pub fn deriveKeyWithRandomSalt(password: []const u8, iterations: u32, salt_length: usize, random: Random, allocator: Allocator) EncryptionError!EncryptionKey {
        const salt = allocator.alloc(u8, salt_length) catch return EncryptionError.OutOfMemory;
        defer allocator.free(salt);
        random.bytes(salt);
        return EncryptionKey.fromPassword(password, salt, iterations, allocator);
    }

    /// Check if data appears to be encrypted (basic heuristic)
    pub fn appearsEncrypted(data: []const u8) bool {
        if (data.len < 32) return false;
        // Check for high entropy (simple test)
        var byte_counts = [_]u32{0} ** 256;
        for (data[0..@min(data.len, 1024)]) |byte| {
            byte_counts[byte] += 1;
        }

        // Count non-zero bytes
        var non_zero_count: u32 = 0;
        for (byte_counts) |count| {
            if (count > 0) non_zero_count += 1;
        }

        // If we have good distribution of bytes, it's likely encrypted
        return non_zero_count > 200;
    }
};

/// File encryption helper
pub const FileEncryption = struct {
    encryption: *Encryption,
    allocator: Allocator,

    const Self = @This();
    pub fn init(encryption: *Encryption, allocator: Allocator) Self {
        return Self{
            .encryption = encryption,
            .allocator = allocator,
        };
    }

    /// Encrypt file data
    pub fn encryptFile(self: *Self, file_path: []const u8, data: []const u8) EncryptionError!void {
        if (!self.encryption.isEnabled()) {
            // Just write the data as-is
            std.fs.cwd().writeFile(file_path, data) catch return EncryptionError.EncryptionFailed;
            return;
        }

        const encrypted_data = try self.encryption.encrypt(data, file_path, self.allocator);
        defer self.allocator.free(encrypted_data);

        std.fs.cwd().writeFile(file_path, encrypted_data) catch return EncryptionError.EncryptionFailed;
    }
    /// Decrypt file data
    pub fn decryptFile(self: *Self, file_path: []const u8) EncryptionError![]u8 {
        const file_data = std.fs.cwd().readFileAlloc(self.allocator, file_path, std.math.maxInt(usize)) catch {
            return EncryptionError.DecryptionFailed;
        };
        defer self.allocator.free(file_data);

        if (!self.encryption.isEnabled()) {
            return self.allocator.dupe(u8, file_data) catch return EncryptionError.OutOfMemory;
        }

        return self.encryption.decrypt(file_data, file_path, self.allocator);
    }
};
