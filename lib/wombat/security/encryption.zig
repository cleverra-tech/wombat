const std = @import("std");
const Allocator = std.mem.Allocator;
const crypto = std.crypto;
const Random = std.rand.Random;

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

/// Encryption configuration
pub const EncryptionConfig = struct {
    /// Whether encryption is enabled
    enabled: bool = false,
    /// Key derivation iterations for PBKDF2
    key_derivation_iterations: u32 = 10000,
    /// Salt length for key derivation
    salt_length: usize = 16,

    pub fn default() EncryptionConfig {
        return EncryptionConfig{};
    }

    pub fn withEnabled(self: EncryptionConfig, enabled: bool) EncryptionConfig {
        var config = self;
        config.enabled = enabled;
        return config;
    }
};

/// Encryption key material
pub const EncryptionKey = struct {
    /// AES-256 key (32 bytes)
    key: [32]u8,
    /// Salt used for key derivation
    salt: [16]u8,

    const Self = @This();
    /// Generate a new random encryption key
    pub fn generate(random: Random) Self {
        var key: [32]u8 = undefined;
        var salt: [16]u8 = undefined;

        random.bytes(&key);
        random.bytes(&salt);
        return Self{
            .key = key,
            .salt = salt,
        };
    }

    /// Derive key from password using PBKDF2
    pub fn fromPassword(password: []const u8, salt: [16]u8, iterations: u32) EncryptionError!Self {
        var key: [32]u8 = undefined;

        crypto.pwhash.pbkdf2(&key, password, &salt, iterations, crypto.hash.sha2.Sha256) catch {
            return EncryptionError.KeyDerivationFailed;
        };
        return Self{
            .key = key,
            .salt = salt,
        };
    }

    /// Securely clear key material
    pub fn clear(self: *Self) void {
        crypto.utils.secureZero(&self.key);
        crypto.utils.secureZero(&self.salt);
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
    pub fn generateKey(self: *Self) EncryptionKey {
        const key = EncryptionKey.generate(self.random);
        self.setKey(key);
        return key;
    }

    /// Set key from password
    pub fn setKeyFromPassword(self: *Self, password: []const u8) EncryptionError!void {
        if (!self.config.enabled) return;

        var salt: [16]u8 = undefined;
        self.random.bytes(&salt);

        const key = try EncryptionKey.fromPassword(password, salt, self.config.key_derivation_iterations);
        self.setKey(key);
    }
    /// Generate random IV
    pub fn generateIV(self: *Self) [12]u8 {
        var iv: [12]u8 = undefined;
        self.random.bytes(&iv);
        return iv;
    }

    /// Encrypt data using AES-256-GCM
    pub fn encrypt(self: *Self, plaintext: []const u8, additional_data: []const u8, allocator: Allocator) EncryptionError![]u8 {
        if (!self.config.enabled) {
            return allocator.dupe(u8, plaintext) catch return EncryptionError.OutOfMemory;
        }

        const key = self.key orelse return EncryptionError.InvalidKey;

        // Generate random IV
        const iv = self.generateIV();
        // Allocate buffer for IV + ciphertext + tag
        const output_len = iv.len + plaintext.len + GCM.tag_length;
        const output = allocator.alloc(u8, output_len) catch return EncryptionError.OutOfMemory;
        errdefer allocator.free(output);

        // Copy IV to output
        @memcpy(output[0..iv.len], &iv);

        // Encrypt
        const ciphertext = output[iv.len .. iv.len + plaintext.len];
        const tag = output[iv.len + plaintext.len ..];

        GCM.encrypt(ciphertext, tag, plaintext, additional_data, iv, key.key) catch {
            return EncryptionError.EncryptionFailed;
        };

        return output;
    }
    /// Decrypt data using AES-256-GCM
    pub fn decrypt(self: *Self, ciphertext: []const u8, additional_data: []const u8, allocator: Allocator) EncryptionError![]u8 {
        if (!self.config.enabled) {
            return allocator.dupe(u8, ciphertext) catch return EncryptionError.OutOfMemory;
        }

        const key = self.key orelse return EncryptionError.InvalidKey;
        // Minimum size is IV + tag
        if (ciphertext.len < 12 + GCM.tag_length) {
            return EncryptionError.InvalidInput;
        }

        // Extract IV, ciphertext, and tag
        const iv = ciphertext[0..12];
        const encrypted_data = ciphertext[12 .. ciphertext.len - GCM.tag_length];
        const tag = ciphertext[ciphertext.len - GCM.tag_length ..];

        // Allocate buffer for plaintext
        const plaintext = allocator.alloc(u8, encrypted_data.len) catch return EncryptionError.OutOfMemory;
        errdefer allocator.free(plaintext);
        // Decrypt and verify
        GCM.decrypt(plaintext, encrypted_data, tag.*, additional_data, iv.*, key.key) catch {
            return EncryptionError.DecryptionFailed;
        };

        return plaintext;
    }

    /// Encrypt data in place (for memory-mapped files)
    pub fn encryptInPlace(self: *Self, data: []u8, additional_data: []const u8, iv: [12]u8) EncryptionError!void {
        if (!self.config.enabled) return;

        const key = self.key orelse return EncryptionError.InvalidKey;

        if (data.len < GCM.tag_length) {
            return EncryptionError.InvalidInput;
        }

        const plaintext_len = data.len - GCM.tag_length;
        const plaintext = data[0..plaintext_len];
        const tag = data[plaintext_len..];
        GCM.encrypt(plaintext, tag[0..GCM.tag_length], plaintext, additional_data, iv, key.key) catch {
            return EncryptionError.EncryptionFailed;
        };
    }

    /// Decrypt data in place (for memory-mapped files)
    pub fn decryptInPlace(self: *Self, data: []u8, additional_data: []const u8, iv: [12]u8) EncryptionError!void {
        if (!self.config.enabled) return;

        const key = self.key orelse return EncryptionError.InvalidKey;

        if (data.len < GCM.tag_length) {
            return EncryptionError.InvalidInput;
        }

        const ciphertext_len = data.len - GCM.tag_length;
        const ciphertext = data[0..ciphertext_len];
        const tag = data[ciphertext_len..];
        GCM.decrypt(ciphertext, ciphertext, tag[0..GCM.tag_length].*, additional_data, iv, key.key) catch {
            return EncryptionError.DecryptionFailed;
        };
    }

    /// Check if encryption is enabled
    pub fn isEnabled(self: *const Self) bool {
        return self.config.enabled and self.key != null;
    }

    /// Get encryption overhead (IV + tag)
    pub fn getOverhead(self: *const Self) usize {
        if (!self.config.enabled) return 0;
        return 12 + GCM.tag_length; // IV + tag
    }

    /// Calculate encrypted size
    pub fn getEncryptedSize(self: *const Self, plaintext_size: usize) usize {
        if (!self.config.enabled) return plaintext_size;
        return plaintext_size + self.getOverhead();
    }

    /// Calculate decrypted size
    pub fn getDecryptedSize(self: *const Self, ciphertext_size: usize) usize {
        if (!self.config.enabled) return ciphertext_size;
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
    pub fn deriveKeyWithRandomSalt(password: []const u8, iterations: u32, random: Random) EncryptionError!EncryptionKey {
        var salt: [16]u8 = undefined;
        random.bytes(&salt);
        return EncryptionKey.fromPassword(password, salt, iterations);
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
