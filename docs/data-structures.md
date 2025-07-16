# Data Structures

This document details the internal data structures and algorithms used in Wombat's implementation.

## Core Data Structures Overview

```mermaid
graph TB
    subgraph "Memory Management"
        A[Arena Allocator] --> B[Memory Pool]
        B --> C[Node Allocation]
    end
    
    subgraph "In-Memory Structures"
        D[SkipList] --> E[MemTable]
        F[Bloom Filter] --> G[SSTable Index]
    end
    
    subgraph "Persistent Structures"
        H[SSTable] --> I[Block Structure]
        J[Value Log] --> K[Log Entries]
        L[Manifest] --> M[Metadata]
    end
    
    A --> D
    A --> F
    H --> N[Compression Layer]
    J --> N
```

## Arena Allocator

The Arena allocator provides efficient memory management for frequently allocated data structures.

```mermaid
graph LR
    subgraph "Arena Structure"
        A[Arena Header] --> B[Current Offset]
        B --> C[Total Size]
        C --> D[Memory Block]
    end
    
    subgraph "Allocation Process"
        E[Alloc Request] --> F{Enough Space?}
        F -->|Yes| G[Bump Pointer]
        F -->|No| H[Allocate New Block]
        G --> I[Return Pointer]
        H --> G
    end
```

### Implementation Details

```zig
pub const Arena = struct {
    blocks: std.ArrayList([]u8),
    current_block: usize,
    current_offset: usize,
    block_size: usize,
    total_allocated: usize,
    
    pub fn alloc(self: *Arena, comptime T: type, count: usize) ![]T {
        const size = @sizeOf(T) * count;
        const alignment = @alignOf(T);
        
        // Align the current offset
        const aligned_offset = std.mem.alignForward(self.current_offset, alignment);
        
        // Check if we have enough space
        if (aligned_offset + size > self.block_size) {
            try self.allocateNewBlock();
        }
        
        // Return the allocated memory
        const ptr = self.blocks.items[self.current_block].ptr + aligned_offset;
        self.current_offset = aligned_offset + size;
        self.total_allocated += size;
        
        return @ptrCast(@alignCast(ptr))[0..count];
    }
};
```

## SkipList

The SkipList provides the foundation for the MemTable with O(log n) operations.

```mermaid
graph TB
    subgraph "SkipList Structure"
        A[Level 3] --> B[Node A] --> C[Node D]
        D[Level 2] --> E[Node A] --> F[Node B] --> G[Node D]
        H[Level 1] --> I[Node A] --> J[Node B] --> K[Node C] --> L[Node D]
        M[Level 0] --> N[Node A] --> O[Node B] --> P[Node C] --> Q[Node D] --> R[Node E]
    end
    
    subgraph "Node Structure"
        S[Key] --> T[Value]
        T --> U[Timestamp]
        U --> V[Meta Flags]
        V --> W[Forward Pointers]
    end
```

### Skip List Operations

```mermaid
sequenceDiagram
    participant C as Client
    participant S as SkipList
    participant N as Node
    
    Note over C,N: Search Operation
    C->>S: search(key)
    S->>S: Start from top level
    loop For each level
        S->>N: Compare key
        alt Key found
            N->>S: Return node
        else Key > current
            S->>S: Move right
        else Key < current
            S->>S: Drop to next level
        end
    end
    S->>C: Return result
```

### Implementation Details

```zig
pub const SkipList = struct {
    const MAX_LEVEL = 16;
    const PROBABILITY = 0.5;
    
    pub const Node = struct {
        key: []const u8,
        value: ValueStruct,
        forward: [MAX_LEVEL]?*Node,
        level: u8,
    };
    
    head: *Node,
    level: u8,
    size: usize,
    arena: *Arena,
    
    pub fn search(self: *const SkipList, key: []const u8) ?*Node {
        var current = self.head;
        
        var i: i32 = @intCast(self.level);
        while (i >= 0) : (i -= 1) {
            const level_idx = @intCast(i);
            while (current.forward[level_idx]) |next| {
                const cmp = std.mem.order(u8, key, next.key);
                if (cmp == .gt) {
                    current = next;
                } else if (cmp == .eq) {
                    return next;
                } else {
                    break;
                }
            }
        }
        
        return null;
    }
};
```

## Bloom Filter

Bloom filters provide probabilistic membership testing to avoid unnecessary disk I/O.

```mermaid
graph TB
    subgraph "Bloom Filter Structure"
        A[Bit Array] --> B[Hash Function 1]
        A --> C[Hash Function 2]
        A --> D[Hash Function 3]
    end
    
    subgraph "Insert Operation"
        E[Key] --> F[Hash 1]
        E --> G[Hash 2]
        E --> H[Hash 3]
        F --> I[Set Bit]
        G --> J[Set Bit]
        H --> K[Set Bit]
    end
    
    subgraph "Query Operation"
        L[Key] --> M[Hash 1]
        L --> N[Hash 2]
        L --> O[Hash 3]
        M --> P{Bit Set?}
        N --> Q{Bit Set?}
        O --> R{Bit Set?}
        P --> S{All Set?}
        Q --> S
        R --> S
        S -->|Yes| T[Maybe Present]
        S -->|No| U[Definitely Not Present]
    end
```

### Implementation Details

```zig
pub const BloomFilter = struct {
    bits: []u8,
    num_hashes: u32,
    num_bits: u32,
    
    pub fn insert(self: *BloomFilter, key: []const u8) void {
        const hash1 = std.hash.Fnv1a_64.hash(key);
        const hash2 = std.hash.Wyhash.hash(0, key);
        
        for (0..self.num_hashes) |i| {
            const hash = hash1 +% (@as(u64, @intCast(i)) * hash2);
            const bit_index = hash % self.num_bits;
            const byte_index = bit_index / 8;
            const bit_offset = @intCast(bit_index % 8);
            
            self.bits[byte_index] |= (@as(u8, 1) << bit_offset);
        }
    }
    
    pub fn contains(self: *const BloomFilter, key: []const u8) bool {
        const hash1 = std.hash.Fnv1a_64.hash(key);
        const hash2 = std.hash.Wyhash.hash(0, key);
        
        for (0..self.num_hashes) |i| {
            const hash = hash1 +% (@as(u64, @intCast(i)) * hash2);
            const bit_index = hash % self.num_bits;
            const byte_index = bit_index / 8;
            const bit_offset = @intCast(bit_index % 8);
            
            if ((self.bits[byte_index] & (@as(u8, 1) << bit_offset)) == 0) {
                return false;
            }
        }
        
        return true;
    }
};
```

## SSTable Structure

SSTables store sorted key-value pairs with efficient indexing and compression.

```mermaid
graph TB
    subgraph "SSTable File Format"
        A[Header] --> B[Data Blocks]
        B --> C[Index Block]
        C --> D[Bloom Filter]
        D --> E[Footer]
    end
    
    subgraph "Data Block Structure"
        F[Block Header] --> G[Key-Value Pairs]
        G --> H[Restart Points]
        H --> I[Block Footer]
    end
    
    subgraph "Index Structure"
        J[Index Entry] --> K[Last Key]
        K --> L[Block Offset]
        L --> M[Block Size]
    end
```

### Block Layout

```mermaid
graph LR
    subgraph "Block Content"
        A[Entry 1] --> B[Entry 2]
        B --> C[Entry 3]
        C --> D[...]
        D --> E[Entry N]
    end
    
    subgraph "Entry Format"
        F[Key Size] --> G[Value Size]
        G --> H[Key Data]
        H --> I[Value Data]
    end
    
    subgraph "Restart Points"
        J[Offset 1] --> K[Offset 2]
        K --> L[Offset N]
        L --> M[Restart Count]
    end
```

## Value Log Structure

The Value Log stores large values separately to reduce write amplification.

```mermaid
graph TB
    subgraph "Value Log File"
        A[File Header] --> B[Entry 1]
        B --> C[Entry 2]
        C --> D[Entry 3]
        D --> E[...]
        E --> F[Entry N]
    end
    
    subgraph "Entry Format"
        G[CRC32] --> H[Timestamp]
        H --> I[Key Size]
        I --> J[Value Size]
        J --> K[Key Data]
        K --> L[Value Data]
    end
    
    subgraph "Pointer Structure"
        M[File ID] --> N[Offset]
        N --> O[Size]
        O --> P[Timestamp]
    end
```

### Space Reclaim Process

```mermaid
graph LR
    subgraph "Reclaim Selection"
        A[Scan VLog Files] --> B[Calculate Garbage Ratio]
        B --> C{Ratio > Threshold?}
        C -->|Yes| D[Select for Reclaim]
        C -->|No| E[Skip File]
    end
    
    subgraph "Reclaim Process"
        D --> F[Read Valid Entries]
        F --> G[Write to New VLog]
        G --> H[Update Pointers]
        H --> I[Delete Old VLog]
    end
```

## Manifest Structure

The Manifest tracks metadata about SSTable files and database state.

```mermaid
graph TB
    subgraph "Manifest File"
        A[Version Header] --> B[Edit Records]
        B --> C[Checksum]
    end
    
    subgraph "Edit Record Types"
        D[Add File] --> E[Delete File]
        E --> F[Compaction]
        F --> G[Level Change]
    end
    
    subgraph "File Metadata"
        H[File Number] --> I[Level]
        I --> J[File Size]
        J --> K[Smallest Key]
        K --> L[Largest Key]
    end
```

## Compression Layer

Data compression is applied at the block level for space efficiency.

```mermaid
graph LR
    subgraph "Compression Pipeline"
        A[Raw Data] --> B[Compression Algorithm]
        B --> C[Compressed Data]
        C --> D[Size Header]
        D --> E[Stored Block]
    end
    
    subgraph "Supported Algorithms"
        F[None] --> G[Identity]
        H[Zlib] --> I[Deflate]
        J[Future] --> K[LZ4/Snappy]
    end
```

## Memory Layout

```mermaid
graph TB
    subgraph "Memory Organization"
        A[Arena Blocks] --> B[SkipList Nodes]
        B --> C[String Data]
        C --> D[Metadata]
    end
    
    subgraph "Cache Hierarchy"
        E[Block Cache] --> F[LRU Eviction]
        G[Table Cache] --> H[File Handles]
        I[Filter Cache] --> J[Bloom Filters]
    end
```

## Algorithm Complexity

| Operation | Time Complexity | Space Complexity |
|-----------|----------------|------------------|
| SkipList Insert | O(log n) | O(1) |
| SkipList Search | O(log n) | O(1) |
| Bloom Filter Insert | O(k) | O(1) |
| Bloom Filter Query | O(k) | O(1) |
| SSTable Search | O(log n) | O(1) |
| Compaction | O(n log n) | O(n) |

Where:
- n = number of elements
- k = number of hash functions

This comprehensive overview of data structures provides the foundation for understanding Wombat's high-performance storage engine implementation.