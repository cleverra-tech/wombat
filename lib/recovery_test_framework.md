# Crash Recovery and Persistence Test Framework

## Overview
This document describes the crash recovery and persistence test framework for the Wombat LSM-Tree database. The framework validates data durability, consistency, and recovery mechanisms after unexpected shutdowns.

## Test Categories

### 1. Basic Persistence Tests
- **Purpose**: Verify that data written to the database persists across restarts
- **Implementation**: `minimal_recovery_test.zig`
- **Status**: ✅ Database creation works, basic framework functional

### 2. WAL (Write-Ahead Log) Recovery Tests
- **Purpose**: Validate that uncommitted data can be recovered from WAL files
- **Key scenarios**:
  - Recovery after crash before sync
  - Partial write recovery
  - WAL file corruption handling
- **Status**: ⚠️ Requires WAL implementation enhancement

### 3. Transaction Recovery Tests
- **Purpose**: Ensure transaction atomicity across crashes
- **Key scenarios**:
  - Committed transactions survive crashes
  - Uncommitted transactions are properly rolled back
  - Concurrent transaction recovery
- **Status**: ⚠️ Memory corruption issues in transaction system

### 4. Manifest Consistency Tests
- **Purpose**: Verify that metadata remains consistent after crashes
- **Key scenarios**:
  - SST file registration consistency
  - Level metadata accuracy
  - Compaction state recovery
- **Status**: ✅ Framework ready, needs implementation

### 5. ValueLog Recovery Tests
- **Purpose**: Validate large value recovery from value log files
- **Key scenarios**:
  - Value log file consistency
  - Pointer validity after restart
  - Garbage collection state recovery
- **Status**: ✅ Framework ready, needs implementation

## Current Implementation Status

### Working Components
1. **Database Creation**: ✅ Successfully initializes all components
2. **Component Tests**: ✅ All individual components pass tests
3. **Statistics**: ✅ Proper statistics tracking
4. **Resource Management**: ✅ Proper cleanup and deinitialization

### Issues Identified
1. **Get/Set Operations**: ❌ Basic operations failing in tests
2. **Transaction System**: ❌ Memory corruption in watermark system
3. **Async Write System**: ❌ Channel-based writes may have issues
4. **Recovery Mechanism**: ❌ Data not persisting across restarts

## Recommended Fixes

### Priority 1: Core Operations
1. Fix get/set operations in the main database interface
2. Resolve async write channel issues
3. Implement proper WAL recovery in the `recover()` method

### Priority 2: Transaction System
1. Fix memory corruption in watermark system
2. Ensure proper transaction isolation
3. Implement transaction rollback mechanism

### Priority 3: Persistence Layer
1. Enhance manifest-based recovery
2. Implement proper SST file loading
3. Add ValueLog recovery mechanism

## Test Framework Structure

```
lib/
├── minimal_recovery_test.zig          # Basic functionality validation
├── crash_recovery_test.zig            # Comprehensive crash scenarios
├── simple_recovery_test.zig           # Focused persistence tests
└── recovery_test_framework.md         # This documentation
```

## Running Tests

```bash
# Basic functionality (currently working)
zig test lib/minimal_recovery_test.zig

# Full component tests (currently working)
zig test lib/wombat.zig

# Comprehensive recovery tests (needs fixes)
zig test lib/simple_recovery_test.zig
```

## Future Enhancements

1. **Fault Injection**: Add ability to simulate disk failures
2. **Concurrent Recovery**: Test recovery under concurrent load
3. **Performance Testing**: Measure recovery time for large datasets
4. **Checksum Validation**: Ensure data integrity after recovery
5. **Backup/Restore**: Add snapshot-based recovery mechanisms

## Conclusion

The crash recovery and persistence test framework is architecturally sound and the foundation is working. The main issues are in the core database operations and transaction system. Once these are resolved, the comprehensive recovery tests will provide robust validation of the database's durability guarantees.