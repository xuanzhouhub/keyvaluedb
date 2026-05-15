# KVDB — LSM-Tree Key-Value Storage Engine

A from-scratch C++17 implementation of a log-structured merge-tree (LSM-tree) key-value database engine. Currently implements **Step 1**: in-memory write buffer (MemTable) with automatic asynchronous flush to immutable on-disk SSTable files.

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

1. `Insert(key, value)` writes to the in-memory **MemTable** (`std::map`, sorted by key)
2. When MemTable reaches 4 MB (`kDefaultMemTableMaxBytes`), it is **frozen** (made immutable) and a new empty MemTable takes its place
3. The frozen MemTable is **asynchronously flushed** to a binary **SSTable** file (`kvdb_data/sstable_N.sst`) on a background thread
4. SSTable files pile up sequentially — compaction is deferred to a later step

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

| Feature          | Status      |
|------------------|-------------|
| MemTable insert  | Done        |
| MemTable freeze  | Done        |
| Async SSTable flush | Done     |
| Backpressure (tunable) | Done  |
| Per-block CRC32  | Done        |
| Thread safety    | Done        |
| WAL              | Pending     |
| Compaction       | Pending     |
| Point lookup     | Pending     |
| Range scan       | Pending     |
| Bloom filter     | Pending     |

## Documentation

- [Design Document](docs/design.md)
- [API Reference](docs/api.md)

## License

MIT
