# keyvaluedb — LSM-Tree Key-Value Storage Engine (C++17)

## ⚠️ BEFORE MARKING ANY TASK COMPLETE, VERIFY ALL THREE:
1. **Tests** — New feature or fix? Add/update tests. Run full suite (`ctest --output-on-failure`). All must pass.
2. **Docs** — Update `AGENTS.md`, `docs/api.md`, and `docs/design.md` with any changed API, architecture, or feature status.
3. **Build** — `cmake --build .` must succeed with zero errors.

## IMPORTANT: AI Agent Instructions
- This project is the ONLY one you should work on. Ignore other folders in the parent directory.
- The user gives instructions step by step. Do NOT start new steps without being asked.
- If uncertain about any implementation decision, ASK the user first.
- All code must be in C++17, built with CMake.
- You are responsible for documentation and comprehensive testing.
- After making changes, always build and run tests to verify.

## User Instructions

**Step 1**: Storage layout — MemTable + SSTable, insertion only, SSTables pile up.
**Step 2**: WAL + Recovery, explicit checkpointing, group commit via background sync thread.
**Step 3**: MVCC point lookup, TCP Server/Client, single writer queue, concurrent reads.
**Step 4**: B+-tree MemTable index, contiguous-page design, blob values, prefix compression scaffold.
**Step 5**: SSTable v4 with bloom filter + range filter + Snappy compression + block index + range scan.

## Current Status: Step 5 (complete)

### MemTable
- **B+-tree** (contiguous-page): `LeafPage` 4KB `_aligned_malloc`, `alignas(4096)`. Slotted page: slot directory (14B/entry) + inline records. Linked leaf chain for O(n) ordered export. `MemTableWalk` skips leaves with `count=0` (can occur after splits). `Free()` returns 0 when `data_start < slot_end` to prevent unsigned wrap-around.
- **Internal nodes**: `std::vector`-based (keys, children, child_leaves), fanout 16.
- **Blob values**: `> page/2` stored as `[size:8B][data]` external blob. Pointer stored inline.
- **MVCC**: `Find()` returns leftmost match; new versions inserted left (`[newest, ..., oldest]`). `Lookup()` scans right for first visible timestamp. `memory_usage_` tracks physical entries (including old versions) so `IsFull()` triggers correctly.
- Prefix compression scaffold (`prefix`, `FullKey()` — activation pending).

### SSTable v4
- **Format**: `[Header:24B][Blocks][FilterArea][Footer]`
- **Blocks**: `[CRC32:4B][CompType:1B][CompSize:4B][NumEntries:4B][entries...]` — CRC over compressed payload.
- **Bloom filter**: Per-SSTable, 1% FPR, double hashing (FNV-1a + multiplicative). `MightContain(key)`.
- **Range filter**: `min_key` / `max_key` — `key < min || key > max → skip`.
- **Compression**: `kCompressionSnappy` active — hash-based LZ77, ~2x ratio, block-level.
- **Entry format**: `[KeyLen:4B][Key][ValueLen:4B][Value][Timestamp:8B]` — CRC covers full entry.
- **Export dedup**: `WriteFromWalk` picks the highest-timestamp version for each key across leaf boundaries (MVCC versions may span multiple leaves after splits).

### WAL
- `Buffer(key, value, ts)` → in-memory buffer (non-blocking). `Sync()` → fsync, returns batch seq.
- Per-entry CRC32. `Recover(checkpoint_ts)` — reads from last checkpoint, returns entries after it.
- `WriteCheckpoint(ts)` — sentinel record `[CRC32][0xFFFFFFFF][ts:8B]`.
- `TrimToLastCheckpoint()` — truncates WAL before last checkpoint.

### MANIFEST
- Persistent SSTable catalog. Each record: `[CRC32:4B][Type:1B][Payload]` — atomic fwrite+fsync.
- `AddSSTable(seq, meta)`, `RemoveSSTable(seq)`. `Recover()` reconstructs full catalog.

### Engine
- **Write**: `Insert()` → WAL::Buffer → bg sync worker → memtable insert → if full: freeze → DoFlush → Manifest::Add → WAL::Checkpoint.
- **Lookup**: `Lookup(key)` → snapshot `read_ts` → check MemTable (active + frozen) → SSTables (bloom+range filter, newest-first).
- **Recovery**: MANIFEST → rebuild SSTable catalog → WAL from last checkpoint → replay → flush → checkpoint.
- **Concurrency**: Reader-writer lock (`shared_mutex`) on memtable. Single writer thread. MVCC snapshot reads.
- **Backpressure**: Write queue byte-capped (16MB). Timestamps: `global_ts_` monotonic counter, key cap 1KB, KV cap 4MB.
- **MemTable Recycling**: After flush, memtable kept in `frozen_memtables_` with a `fence_ts`. `DrainRecyclePending()` checks `SnapshotTracker::MinActiveTS() >= fence_ts` — when all active readers see the SSTable, the memtable is unlinked and released.

### Server/Client
- TCP server: connection-per-client threads, single writer queue, concurrent reads via MVCC.
- Binary protocol: `W + key + value` (write), `R + key` (read), `O/V/N/E` (responses).
- Byte-capped write queue (16MB) with CV-based backpressure.

### Tests
- Custom lightweight framework, 6 suites: `test_memtable`, `test_sstable`, `test_engine`, `test_wal`, `test_manifest`, `test_server`.
- Separate fuzz test (`test_fuzz`, `-DKVDB_BUILD_FUZZ=ON`): 32 threads, fault injection, persistence verification.
- All suites pass.

## Build Commands
```powershell
cmake -B build -S .
cmake --build build
cd build && ctest --output-on-failure
```

## Project Structure
```
keyvaluedb/
├── CMakeLists.txt
├── include/kvdb/
│   ├── bloom.hpp           (BloomFilter)
│   ├── bptree.hpp          (BPlusTree — contiguous-page)
│   ├── client.hpp          (TCP client)
│   ├── config.hpp          (constants)
│   ├── engine.hpp          (LSMTreeEngine)
│   ├── iterator.hpp        (RangeIterator + SourceIterators)
│   ├── manifest.hpp        (SSTable catalog)
│   ├── memtable.hpp        (MemTable)
│   ├── protocol.hpp        (wire protocol + socket helpers)
│   ├── server.hpp          (TCP server + write queue)
│   ├── snappy.hpp          (Snappy compression)
│   ├── snp_tracker.hpp     (Snapshot tracker)
│   ├── sstable.hpp         (SSTable v4)
│   ├── types.hpp           (KeyValuePair)
│   ├── wal.hpp             (WAL with checkpointing)
│   └── internal/crc32.hpp  (CRC-32/ISO-HDLC)
├── src/
│   ├── client.cpp
│   ├── engine.cpp
│   ├── manifest.cpp
│   ├── memtable.cpp
│   ├── protocol.cpp
│   ├── server.cpp
│   ├── sstable.cpp
│   └── wal.cpp
├── tests/
│   ├── test_common.hpp
│   ├── test_engine.cpp
│   ├── test_fuzz.cpp
│   ├── test_manifest.cpp
│   ├── test_memtable.cpp
│   ├── test_server.cpp
│   ├── test_sstable.cpp
│   └── test_wal.cpp
└── docs/
```

## Pending Steps (DO NOT start unless user explicitly asks)
| Step | Feature | Status |
|------|---------|--------|
| 6 | Compaction (level-based SSTable merging) | Not started |
| 7 | Atomicity / batch writes | Not started |
