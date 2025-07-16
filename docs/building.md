# Building from Source

This guide covers building Wombat from source code, including development setup, dependencies, and build system details.

## Prerequisites

### System Requirements

- **Zig 0.15+**: Download from [ziglang.org](https://ziglang.org/download/)
- **Git**: For version control
- **Make**: Optional, for convenience commands

### Operating System Support

- **Linux**: Primary development platform (Ubuntu, Debian, CentOS, etc.)
- **macOS**: Supported on Intel and Apple Silicon
- **Windows**: Supported with minor limitations

### Hardware Requirements

- **CPU**: x86_64 or ARM64
- **Memory**: 4GB+ for building, 8GB+ for development
- **Storage**: 1GB+ for source and build artifacts

## Quick Build

```bash
# Clone repository
git clone https://github.com/cleverra-tech/wombat.git
cd wombat

# Build library
zig build

# Run tests
zig build test
```

## Build System

Wombat uses Zig's built-in build system with a comprehensive `build.zig` file.

### Available Build Targets

```bash
# Library builds
zig build                    # Build library (default)
zig build --release=fast     # Release build
zig build --release=small    # Size-optimized build

# Testing
zig build test              # Run unit tests
zig build test-all          # Run all tests
zig build test-recovery     # Run recovery tests

# Benchmarks
zig build benchmark         # Run performance benchmarks

# Documentation
zig build docs             # Generate documentation
```

### Build Options

```bash
# Debug build with assertions
zig build -Doptimize=Debug

# Release builds
zig build -Doptimize=ReleaseFast    # Speed optimized
zig build -Doptimize=ReleaseSmall   # Size optimized
zig build -Doptimize=ReleaseSafe    # Safety optimized

# Cross-compilation
zig build -Dtarget=x86_64-linux-gnu
zig build -Dtarget=aarch64-linux-gnu
zig build -Dtarget=x86_64-windows-gnu
```

## Development Setup

### Editor Configuration

#### VS Code
Install the Zig extension and configure:

```json
{
    "zig.buildOnSave": true,
    "zig.checkForUpdate": false,
    "zig.enableSemanticTokens": true,
    "files.associations": {
        "*.zig": "zig"
    }
}
```

#### Vim/Neovim
Use the `ziglang/zig.vim` plugin:

```vim
Plug 'ziglang/zig.vim'
```

### Git Hooks

Set up pre-commit hooks for code quality:

```bash
# Install pre-commit hooks
cp scripts/pre-commit.sh .git/hooks/pre-commit
chmod +x .git/hooks/pre-commit
```

The pre-commit hook runs:
- Code formatting check
- Build verification
- Test execution
- Style checks

### Environment Variables

```bash
# Optional: Set Zig cache directory
export ZIG_CACHE_DIR=/tmp/zig-cache

# Optional: Set global cache directory
export ZIG_GLOBAL_CACHE_DIR=$HOME/.cache/zig
```

## Build Configuration

### build.zig Structure

```zig
const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Main library
    const lib = b.addStaticLibrary(.{
        .name = "wombat",
        .root_source_file = .{ .path = "lib/wombat.zig" },
        .target = target,
        .optimize = optimize,
    });

    // Tests
    const tests = b.addTest(.{
        .root_source_file = .{ .path = "lib/wombat.zig" },
        .target = target,
        .optimize = optimize,
    });

    // Benchmarks
    const benchmarks = b.addExecutable(.{
        .name = "benchmarks",
        .root_source_file = .{ .path = "benchmarks/benchmark.zig" },
        .target = target,
        .optimize = optimize,
    });
}
```

### Custom Build Options

Add custom build options in `build.zig`:

```zig
const enable_compression = b.option(bool, "compression", "Enable compression support") orelse true;
const enable_encryption = b.option(bool, "encryption", "Enable encryption support") orelse false;

if (enable_compression) {
    lib.defineCMacro("WOMBAT_COMPRESSION", "1");
}

if (enable_encryption) {
    lib.defineCMacro("WOMBAT_ENCRYPTION", "1");
}
```

Usage:
```bash
zig build -Dcompression=false
zig build -Dencryption=true
```

## Testing

### Test Structure

```
tests/
├── unit/                  # Unit tests
├── integration/           # Integration tests
├── recovery/             # Recovery tests
├── performance/          # Performance tests
└── benchmarks/           # Benchmark tests
```

### Running Tests

```bash
# All tests
zig build test

# Specific test file
zig build test -- --test-filter "skiplist"

# With coverage
zig build test -- --test-coverage

# Memory leak detection
zig build test -- --test-leak-check

# Verbose output
zig build test -- --verbose
```

### Test Configuration

```zig
// In test files
const testing = std.testing;
const expect = testing.expect;
const expectEqual = testing.expectEqual;

test "basic functionality" {
    // Test implementation
    try expect(condition);
}
```

## Debugging

### Debug Builds

```bash
# Debug build with symbols
zig build -Doptimize=Debug

# With debug output
zig build -Doptimize=Debug -Ddebug=true
```

### GDB Integration

```bash
# Compile with debug info
zig build -Doptimize=Debug

# Debug with GDB
gdb ./zig-out/bin/wombat
```

### Sanitizers

```bash
# Address sanitizer
zig build -Doptimize=Debug --summary all

# Undefined behavior sanitizer
zig build test -- --test-thread-count=1
```

## Performance Optimization

### Release Builds

```bash
# Fast release build
zig build -Doptimize=ReleaseFast

# Small release build
zig build -Doptimize=ReleaseSmall

# Safe release build (with safety checks)
zig build -Doptimize=ReleaseSafe
```

### Link-Time Optimization

```bash
# Enable LTO
zig build -Doptimize=ReleaseFast -Dtarget=native-native-lto
```

### CPU-Specific Optimizations

```bash
# Target specific CPU
zig build -Dtarget=native-native-baseline

# Target current CPU
zig build -Dtarget=native-native-native
```

## Cross-Compilation

### Common Targets

```bash
# Linux x86_64
zig build -Dtarget=x86_64-linux-gnu

# Linux ARM64
zig build -Dtarget=aarch64-linux-gnu

# Windows x86_64
zig build -Dtarget=x86_64-windows-gnu

# macOS x86_64
zig build -Dtarget=x86_64-macos-gnu

# macOS ARM64
zig build -Dtarget=aarch64-macos-gnu
```

### Cross-Compilation Setup

```bash
# List available targets
zig targets

# Build for specific target
zig build -Dtarget=x86_64-linux-musl -Doptimize=ReleaseFast
```

## Continuous Integration

### GitHub Actions

```yaml
name: CI

on: [push, pull_request]

jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        zig-version: [0.15.0]
        
    steps:
    - uses: actions/checkout@v3
    
    - name: Install Zig
      uses: goto-bus-stop/setup-zig@v2
      with:
        version: ${{ matrix.zig-version }}
    
    - name: Build
      run: zig build
    
    - name: Test
      run: zig build test
    
    - name: Benchmark
      run: zig build benchmark
```

### Docker Build

```dockerfile
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    curl \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Install Zig
RUN curl -L https://ziglang.org/download/0.15.0/zig-linux-x86_64-0.15.0.tar.xz | tar -xJ \
    && mv zig-linux-x86_64-0.15.0 /usr/local/zig \
    && ln -s /usr/local/zig/zig /usr/local/bin/zig

WORKDIR /app
COPY . .

RUN zig build
RUN zig build test

CMD ["zig", "build", "benchmark"]
```

## Static Analysis

### Code Quality

```bash
# Format code
zig fmt lib/ tests/ benchmarks/

# Check formatting
zig fmt --check lib/ tests/ benchmarks/

# AST check
zig ast-check lib/wombat.zig
```

### Memory Safety

```bash
# Memory leak detection
zig build test -- --test-leak-check

# Address sanitizer
zig build -Doptimize=Debug test
```

## Package Management

### Creating a Package

```bash
# Create package tarball
zig build --release=fast
tar -czf wombat-1.0.0.tar.gz zig-out/
```

### Using as Dependency

In your `build.zig`:

```zig
const wombat = b.dependency("wombat", .{
    .target = target,
    .optimize = optimize,
});

exe.root_module.addImport("wombat", wombat.module("wombat"));
```

## Troubleshooting

### Common Build Issues

#### Zig Version Mismatch
```bash
# Check Zig version
zig version

# Update Zig
curl -L https://ziglang.org/download/0.15.0/zig-linux-x86_64-0.15.0.tar.xz | tar -xJ
```

#### Cache Issues
```bash
# Clear cache
rm -rf .zig-cache/
rm -rf zig-out/
```

#### Permission Issues
```bash
# Fix permissions
chmod +x scripts/*.sh
```

### Build Performance

#### Parallel Builds
```bash
# Use all CPU cores
zig build -j$(nproc)

# Limit parallel jobs
zig build -j4
```

#### Cache Optimization
```bash
# Use shared cache
export ZIG_GLOBAL_CACHE_DIR=$HOME/.cache/zig
```

### Memory Usage

#### Reduce Memory Usage
```bash
# Smaller builds
zig build -Doptimize=ReleaseSmall

# Limit memory usage
ulimit -v 4194304  # 4GB limit
```

## Advanced Build Configuration

### Custom Allocators

```zig
// In build.zig
const use_custom_allocator = b.option(bool, "custom-allocator", "Use custom allocator") orelse false;

if (use_custom_allocator) {
    lib.defineCMacro("WOMBAT_CUSTOM_ALLOCATOR", "1");
}
```

### Feature Flags

```zig
const features = b.addOptions();
features.addOption(bool, "compression", enable_compression);
features.addOption(bool, "encryption", enable_encryption);

lib.root_module.addOptions("features", features);
```

### Custom Build Steps

```zig
const generate_docs = b.step("docs", "Generate documentation");
const docs_cmd = b.addSystemCommand(&[_][]const u8{
    "zig", "build-lib", "lib/wombat.zig", "-femit-docs"
});
generate_docs.dependOn(&docs_cmd.step);
```

This comprehensive build guide should help you successfully build and develop Wombat from source code across different platforms and configurations.