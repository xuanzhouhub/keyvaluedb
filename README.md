# KVDB — LSM-Tree Key-Value Storage Engine

A from-scratch C++17 implementation of a log-structured merge-tree (LSM-tree) key-value database engine with MVCC, WAL, B+-tree memtable, SSTable v4, TCP server/client, and range scans.

## Build

```powershell
cmake -B build -S .
cmake --build build
```

**Requirements**: CMake 3.16+, C++17 compiler (MSVC 2019+, GCC 8+, Clang 7+).

## Test

```powershell
cd build
ctest --output-on-failure
```

Or run individual test executables:

```powershell
.\build\bin\test_memtable.exe
.\build\bin\test_sstable.exe
.\build\bin\test_engine.exe
```

## Quick Start

```cpp
#include <kvdb/engine.hpp>

kvdb::LSMTreeEngine engine("./mydata");

engine.Insert("hello", "world");
engine.Insert("foo", "bar");

engine.WaitForPendingFlushes();
```

## Architecture

```
┌─────────────────────────────────────────────────┐
│                 LSMTreeEngine                    │
│  ┌──────────────────┐   ┌─────────────────────┐ │
│  │  Active MemTable  │   │  SSTable Files      │ │
│  │  (in-memory,      │   │  (on-disk, binary)  │ │
│  │   std::map-based) │   │  sstable_0.sst ...  │ │
│  └───────┬──────────┘   └─────────┬───────────┘ │
│          │ freeze + async flush    │              │
│          ▼                         │              │
│  ┌──────────────┐                  │              │
│  │ Frozen       │──────────────────┘              │
│  │ MemTables    │                                 │
│  └──────────────┘                                 │
└─────────────────────────────────────────────────┘
```

### Write Path

 1. `Insert(key, value)` writes to the in-memory **MemTable** (B+-tree, sorted by key)
 2. When MemTable reaches the size limit, it is **frozen** and a new MemTable takes its place
 3. The frozen MemTable is **flushed** to a binary **SSTable** file synchronously
 4. SSTable files are tracked in the **MANIFEST**; WAL ensures crash recovery
 5. Point lookups scan MemTables then SSTables newest-first with bloom/range filters

### File Format (v2)

SSTable files use a **block-structured binary layout** with per-block CRC32 checksums:

| Offset  | Field                  | Size      |
|---------|------------------------|-----------|
| 0       | Magic: `4B 53 53 54`   | 4 bytes   |
| 4       | Version (2)            | 4 bytes   |
| 8       | Block size (default 4 KB) | 4 bytes |
| 12      | Total entry count      | 4 bytes   |
| *Blocks*| *Per block:*           | variable  |
|         | - CRC32 (uint32)       | 4 bytes   |
|         | - Entry count in block | 4 bytes   |
|         | - Entries: key_len + key + value_len + value | variable |
| *Footer*| - Block count          | 4 bytes   |
|         | - Block offsets (uint64 each) | N×8 |
|         | - Footer magic: `4B 45 4E 44` | 4 bytes |

## Project Status

| Feature              | Status      |
|----------------------|-------------|
| MemTable insert      | Done        |
| MemTable freeze      | Done        |
| Async SSTable flush  | Done        |
| Backpressure         | Done        |
| Per-block CRC32      | Done        |
| Thread safety        | Done        |
| WAL + Recovery       | Done        |
| MVCC point lookup    | Done        |
| B+-tree MemTable     | Done        |
| Bloom filter         | Done        |
| Range filter         | Done        |
| Snappy compression   | Done        |
| Range scan           | Done        |
| TCP Server/Client    | Done        |
| Manifest (catalog)   | Done        |
| Fuzz test (recovery) | Done        |
| Compaction           | Done        |
| Delete / Tombstone   | Done        |
| Prefix scan          | Done        |
| Batch writes         | Pending     |

## Documentation

- [Design Document](docs/design.md)
- [API Reference](docs/api.md)

## License

MIT
