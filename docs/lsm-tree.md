# LSM-Tree Fundamentals

This document explains the Log-Structured Merge-Tree (LSM-Tree) data structure that forms the core of Wombat's storage engine.

## What is an LSM-Tree?

An LSM-Tree is a data structure optimized for write-heavy workloads. It maintains data in multiple levels, with newer data in memory and older data in sorted files on disk.

## LSM-Tree Structure

```mermaid
graph TB
    subgraph "Memory Layer"
        A[MemTable - Level 0<br/>Skip List Structure<br/>64MB default]
        B[Immutable MemTable<br/>Being flushed to disk]
    end
    
    subgraph "Disk Layer"
        C[Level 0 SSTables<br/>Overlapping ranges<br/>Recently flushed]
        D[Level 1 SSTables<br/>Non-overlapping<br/>~10MB each]
        E[Level 2 SSTables<br/>Non-overlapping<br/>~100MB each]
        F[Level 3+ SSTables<br/>Non-overlapping<br/>~1GB+ each]
    end
    
    A -->|Flush when full| C
    B -->|Background flush| C
    C -->|Compaction| D
    D -->|Compaction| E
    E -->|Compaction| F
```

## Write Path

```mermaid
sequenceDiagram
    participant C as Client
    participant W as WAL
    participant M as MemTable
    participant S as SSTable
    participant BG as Background Worker
    
    C->>W: 1. Write to WAL
    W->>M: 2. Insert into MemTable
    M->>M: 3. Check size threshold
    
    alt MemTable full
        M->>BG: 4. Trigger flush
        BG->>S: 5. Create L0 SSTable
        BG->>BG: 6. Schedule compaction
    end
    
    M->>C: 7. Acknowledge write
```

## Read Path

```mermaid
sequenceDiagram
    participant C as Client
    participant M as MemTable
    participant L0 as Level 0
    participant L1 as Level 1+
    participant VL as Value Log
    
    C->>M: 1. Check MemTable
    
    alt Found in MemTable
        M->>C: Return value
    else Not found
        C->>L0: 2. Search L0 SSTables
        
        alt Found in L0
            L0->>VL: 3. Get value from VLog
            VL->>C: Return value
        else Not found
            C->>L1: 4. Search L1+ levels
            
            alt Found in L1+
                L1->>VL: 5. Get value from VLog
                VL->>C: Return value
            else Not found
                C->>C: Return null
            end
        end
    end
```

## Compaction Process

```mermaid
graph LR
    subgraph "Compaction Selection"
        A[Priority Calculator] --> B{Level Priority}
        B -->|Highest| C[Select Level]
        C --> D[Select Input Files]
    end
    
    subgraph "Compaction Execution"
        D --> E[Merge Sort]
        E --> F[Deduplication]
        F --> G[Compression]
        G --> H[Write Output Files]
    end
    
    subgraph "Cleanup"
        H --> I[Update Manifest]
        I --> J[Delete Old Files]
        J --> K[Update Statistics]
    end
```

## Space Reclaim Process

```mermaid
graph TB
    subgraph "Value Log Management"
        A[Value Log Files] --> B{Space Reclaim<br/>Threshold Check}
        B -->|>70% garbage| C[Select VLog File]
        B -->|<70% garbage| D[Skip Reclaim]
        
        C --> E[Scan Valid Entries]
        E --> F[Rewrite Valid Data]
        F --> G[Update Pointers]
        G --> H[Delete Old VLog]
    end
    
    subgraph "SSTable Updates"
        G --> I[Update SSTable Entries]
        I --> J[Trigger Compaction]
    end
```

## Level Structure

```mermaid
graph TB
    subgraph "Level Sizing"
        L0["Level 0<br/>5 files max<br/>Overlapping ranges"]
        L1["Level 1<br/>10MB total<br/>Non-overlapping"]
        L2["Level 2<br/>100MB total<br/>Non-overlapping"]
        L3["Level 3<br/>1GB total<br/>Non-overlapping"]
        L4["Level 4<br/>10GB total<br/>Non-overlapping"]
    end
    
    L0 --> L1
    L1 --> L2
    L2 --> L3
    L3 --> L4
```

## Key Properties

### Write Performance
- **Sequential Writes**: All writes go to WAL and MemTable
- **Batch Optimization**: Multiple writes batched together
- **Minimal Disk I/O**: Only background compaction touches disk

### Read Performance
- **Memory First**: Hot data served from MemTable
- **Bloom Filters**: Avoid unnecessary disk reads
- **Level Ordering**: Search from newest to oldest

### Space Efficiency
- **Compression**: Data compressed at each level
- **Deduplication**: Compaction removes duplicate keys
- **Space Reclaim**: Background garbage collection

## Configuration Trade-offs

```mermaid
graph LR
    subgraph "Memory vs Disk"
        A[Large MemTable] --> B[Fewer Flushes]
        B --> C[More Memory Usage]
        
        D[Small MemTable] --> E[More Flushes]
        E --> F[Less Memory Usage]
    end
    
    subgraph "Compaction"
        G[Frequent Compaction] --> H[Lower Read Amplification]
        H --> I[Higher Write Amplification]
        
        J[Infrequent Compaction] --> K[Higher Read Amplification]
        K --> L[Lower Write Amplification]
    end
```

## Performance Characteristics

### Write Amplification
- **Definition**: Total data written vs. user data written
- **LSM Factor**: Each level multiplies by level size factor
- **Optimization**: Larger levels, better compression

### Read Amplification
- **Definition**: Data read vs. user data returned
- **LSM Factor**: May need to check multiple levels
- **Optimization**: Bloom filters, caching

### Space Amplification
- **Definition**: Total space used vs. logical data size
- **LSM Factor**: Multiple copies during compaction
- **Optimization**: Efficient compaction, compression

## Comparison with B-Trees

```mermaid
graph LR
    subgraph "LSM-Tree"
        A[Write-Optimized] --> B[Sequential I/O]
        B --> C[High Throughput]
        C --> D[Complex Reads]
    end
    
    subgraph "B-Tree"
        E[Read-Optimized] --> F[Random I/O]
        F --> G[Simple Reads]
        G --> H[Write Bottleneck]
    end
```

## Best Practices

### When to Use LSM-Trees
- **Write-heavy workloads**
- **Time-series data**
- **Log aggregation**
- **Analytics workloads**

### When to Avoid LSM-Trees
- **Read-heavy workloads**
- **Point queries only**
- **Small datasets**
- **Memory-constrained environments**

This LSM-Tree implementation in Wombat provides a solid foundation for high-performance, write-optimized storage with configurable trade-offs between memory usage, write amplification, and read performance.