# KVDB — LSM-Tree Key-Value Storage Engine

[![CI](https://github.com/xuanzhouhub/keyvaluedb/actions/workflows/ci.yml/badge.svg)](https://github.com/xuanzhouhub/keyvaluedb/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue.svg)](https://en.cppreference.com/w/cpp/17)

This is a log-structured merge-tree (LSM-tree) key-value database engine developed from scratch by Xuan Zhou using OpenCode V1.17 and DeepSeek V4 Pro. It is a fully functional engine with MVCC, WAL, Lock free B+-tree memtable, SSTable v4, TCP server/client, range scans, prioritized batch writes and atomic compare-and-swap. C++17 is used for the implementation. It is fast.

## On AI Coding 

The author didn't write a single line of code. Neither did he review any block of Code in detail. He just communicated with the Agent (OpenCode V1.17 + DeepSeek V4 Pro) to finish the project.

It consumed 6 billion tokens of DeepSeek V4 Pro, costing around 260 RMB.

The Agent did make tons of logical mistakes and misjudgments. But this is manageable as long as the developer has in-depth knowledge about how the system should work.

[Here](Prompts.md) are the main prompts used in the development. 

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
| KV Cache (LRU)       | Done        |
| SSTable Cache (LRU)  | Done        |

## Documentation

- [Usage Guide](docs/usage.md)
- [Design Document](docs/design.md)
- [API Reference](docs/api.md)

## License

MIT
