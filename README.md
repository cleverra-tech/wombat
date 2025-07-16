# Wombat

LSM-tree key-value database implemented in Zig.

**Early Development** - Core components still being implemented.

## Status

- [x] Basic data structures (Arena, SkipList, WAL)
- [x] Memory-mapped I/O
- [ ] Storage layer (MemTable, SSTable, Levels) - partial
- [ ] Transaction system
- [ ] Value log
- [ ] Compression/Encryption

## Building

```bash
zig build test
```

## License

MIT