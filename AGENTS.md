# keyvaluedb — LSM-Tree Key-Value Storage Engine (C++17)

## ⚠️ BEFORE MARKING ANY TASK COMPLETE, VERIFY ALL THREE:
1. **Tests** — New feature or fix? Add/update tests. Run full suite (`ctest --output-on-failure`). All must pass.
2. **Docs** — Update `AGENTS.md`, `docs/api.md`, and `docs/design.md` with any changed API, architecture, or feature status.
3. **Build** — `cmake --build build` must succeed with zero errors.

## IMPORTANT: AI Agent Instructions
- This project is in `keyvaluedb/`. This is the ONLY project you should work on. Ignore other folders in the parent directory.
- The user gives instructions step by step. Do NOT start new steps without being asked.
- If uncertain about any implementation decision, ASK the user first.
- All code must be in C++17, built with CMake.
- You are responsible for documentation and comprehensive testing.
- After making changes, always build and run tests to verify.
- **CRC COVERAGE RULE**: Every byte written to an SSTable file MUST be covered by a CRC. If you add a new metadata section to the SSTable format, place it inside the `CRC-COVERED REGION` markers in `src/sstable.cpp` (between `filter_off` and `meta_end`). If you add a new data section, give it a per-section CRC. Add `docs/design.md` CRC coverage table entry. Write a test that corrupts the new section and verifies the CRC catches it.

## User Instructions

**Step 1**: Storage layout — MemTable + SSTable, insertion only, SSTables pile up.
**Step 2**: WAL + Recovery, explicit checkpointing, group commit via background sync thread.
**Step 3**: MVCC point lookup, TCP Server/Client, single writer queue, concurrent reads.
**Step 4**: B+-tree MemTable index, contiguous-page design, blob values, prefix compression scaffold.
**Step 5**: SSTable v4 with bloom filter + range filter + Snappy compression + block index + range scan.
**Step 6**: Compaction — leveling, tombstone GC, background worker.
**Step 7**: Caching — KV Cache + SSTable Block Cache with BlockReader abstraction.
**Step 8**: Deferred File GC — SSTable files deleted only after all pre-swap readers finish.

## Current Status: All steps complete (Steps 1–8)

### MemTable
- **B+-tree** (contiguous-page): `LeafPage` 4KB `_aligned_malloc`, `alignas(4096)`. Slotted page: slot directory (14B/entry) + inline records. Linked leaf chain for O(n) ordered export. `MemTableWalk` skips leaves with `count=0` (can occur after splits). `Free()` returns 0 when `data_start < slot_end` to prevent unsigned wrap-around.
- **Internal nodes**: `std::vector`-based (keys, children, child_leaves), fanout 16.
- **Blob values**: `> page/2` stored as `[size:8B][data]` external blob. Pointer stored inline.
- **MVCC**: `Find()` returns leftmost match; new versions inserted left (`[newest, ..., oldest]`). `Lookup()` scans right for first visible timestamp. `memory_usage_` tracks physical entries (including old versions) so `IsFull()` triggers correctly.
- Prefix compression scaffold (`prefix`, `FullKey()` — activation pending).

### SSTable v5
- **Format**: `[Header:24B][Blocks][FilterArea][Footer]`
- **Blocks**: `[CRC32:4B][CompType:1B][CompSize:4B][NumEntries:4B][entries...]` — CRC over compressed payload.
- **Bloom filter**: Per-SSTable, 1% FPR, double hashing (FNV-1a + multiplicative). `MightContain(key)`.
- **Range filter**: `min_key` / `max_key` — `key < min || key > max → skip`.
- **Compression**: `kCompressionSnappy` active — hash-based LZ77, ~2x ratio, block-level.
- **Entry format**: `[KeyLen:4B][Key][ValueLen:4B][Value][Timestamp:8B][Flags:1B]` — bit 0 = tombstone (`fl&1`). `kLargeValFlag=0x7FFF` to avoid conflict with tombstone bit 15 (`0x8000`).
- **Export dedup**: `WriteFromWalk` picks the highest-timestamp version for each key across leaf boundaries (MVCC versions may span multiple leaves after splits).

### Metadata
- `SSTable::Metadata` includes `manifest_seq` (uint64_t) — the sequence number assigned at flush/compaction time. Used as cache key instead of filepath.
- `ReadMetadata(filepath, cache, manifest_seq)` — always reads file header from disk (no early return from cache); caches bloom + block offsets at the end.

### WAL
- `Buffer(key, value, ts)` → in-memory buffer (non-blocking). `Sync()` → fsync, returns batch seq.
- Per-entry CRC32. `Recover(checkpoint_ts)` — reads from last checkpoint, returns entries after it.
- `WriteCheckpoint(ts)` — sentinel record `[CRC32][0xFFFFFFFF][ts:8B]`.
- `TrimToLastCheckpoint()` — truncates WAL before last checkpoint.

### MANIFEST
- Persistent SSTable catalog. Each record: `[CRC32:4B][Type:1B][Payload]` — atomic fwrite+fsync.
- `AddSSTable(seq, meta)`, `RemoveSSTable(seq)`. `Recover()` reconstructs full catalog.
- Stores `manifest_seq` for each SSTable on recovery.

### Engine
- **Write**: `Insert()` → WAL::Buffer → bg sync worker → memtable insert → if full: freeze → DoFlush → Manifest::Add → WAL::Checkpoint.
- **Lookup**: `Lookup(key)` → check KV Cache → snapshot `read_ts` → check MemTable (active + frozen) → SSTables (bloom+range filter, newest-first, L0 then L1+ binary search, BlockReader cache-aware).
- **Tombstone in LookupKey**: when `fl&1` (tombstone), returns `true` with empty value so the engine stops searching older SSTables and returns false.
- **Recovery**: MANIFEST → rebuild SSTable catalog → WAL from last checkpoint → replay → flush → checkpoint. Manifest recovery populates `manifest_seq` and loads bloom/range metadata from files.
- **Concurrency**: Reader-writer lock (`shared_mutex`) on memtable. Single writer thread. MVCC snapshot reads.
- **Backpressure**: Write queue byte-capped (16MB). Timestamps: `global_ts_` monotonic counter, key cap 1KB, KV cap 4MB.
- **MemTable Recycling**: After flush, memtable kept in `frozen_memtables_` with a `fence_ts`. `DrainRecyclePending()` checks `SnapshotTracker::MinActiveTS() >= fence_ts` — when all active readers see the SSTable, the memtable is unlinked and released.

### Compaction
- **Tombstones**: `Delete(key)` inserts an entry with empty value (tombstone). Tombstones survive flushes; `Lookup` returns false for tombstone entries.
- **Leveling strategy**: Level 0 (flushes, can overlap). Levels 1+ (non-overlapping sorted runs). Threshold: 8 SSTables triggers compaction.
- **Size multiplier**: 10× per level. Base SSTable size: 4MB. SSTables split when approaching level max size.
- **Background worker**: `CompactionWorkerLoop` periodically scans levels. Compaction merges SSTables from level L into L+1, picks newest version per key, drops tombstones at the last level.
- **Visibility**: New SSTables are written, manifest is updated atomically (remove old, add new). Old SSTable files are **deferred-GC'd** (not deleted immediately).
- **Concurrency**: Compaction uses `sstable_metadata_mutex_` for atomic swap. Flush worker writes to Level 0 concurrently.
- **Cache during compaction**: Iterators created with `populate=false` (1-arg constructor with `reader_=nullptr`) — compaction does not pollute the LRU cache.

### Caching
- **KV Cache** (`kv_cache.hpp`): LRU key-value cache for point lookups. Write-through (populated after WAL sync). Tombstones erase cache entries. Blobs (> 2KB) are not cached. Default: 10K entries / 16MB.
- **SSTable Block Cache** (`block_cache.hpp`): LRU cache for SSTable metadata (bloom, block offsets) and uncompressed data blocks. 
  - `BlockReader` (`block_reader.hpp`) — pure virtual interface: `GetBloom`, `PutBloom`, `GetBlockOffsets`, `PutBlockOffsets`, `GetBlock`, `PutBlock`, `Invalidate`.
  - `SSTableCache` implements `BlockReader` with two partitioned LRU caches (heavy metadata + data blocks). Default: 1024 blocks / 256 metadata / 64MB.
  - **Cache keys**: `uint64_t manifest_seq` (not filepath). Block keys: `BlockKey(seq, idx) = (seq << 32) | idx`.
  - Engine stores `std::unique_ptr<BlockReader> sst_cache_` initialized to `SSTableCache`.
  - `Invalidate(seq)` evicts all heavy metadata and blocks for that `manifest_seq`.
- **Cache-aware iterators**: `SSTableIterator(filepath, BlockReader&, manifest_seq, populate)` — checks cache via `GetBlock`, populates on miss if `populate_` is true. `LevelIterator` forwards `BlockReader&` to chained `SSTableIterator`.
- **Cache populates during**: flush (`WriteFromWalk`), point lookup (`LookupKey`), range scan. NOT during compaction (`populate=false`).

### Deferred File GC
- **Problem**: Compaction swaps SSTable metadata atomically, but pre-swap readers may still have open file handles to old SSTables. Immediate `std::filesystem::remove` fails on Windows (open handles) and risks use-after-free on Linux.
- **Solution**: `PendingFileGC` struct (`filepath`, `manifest_seq`, `fence_ts`).
- `DeferFileGC(f, seq, fence_ts)` — pushes to `pending_gc_` vector (protected by `pending_gc_mutex_`).
- `DrainFileGC()` — for each entry where `SnapshotTracker::MinActiveTS() >= fence_ts`: invalidate cache for that `manifest_seq`, call `std::filesystem::remove`, pop from list.
- Called from `CompactLevel` (after manifest swap), `CompactionWorkerLoop` (every iteration), and `~LSMTreeEngine` (final cleanup).
- Cache invalidation happens immediately at defer time via `sst_cache_->Invalidate(seq)`.

### Server/Client
- TCP server: connection-per-client threads, single writer queue, concurrent reads via MVCC.
- Binary protocol: `W + key + value` (write), `R + key` (read), `O/V/N/E` (responses).
- Byte-capped write queue (16MB) with CV-based backpressure.

### Tests
- 9 test suites: `test_memtable`, `test_sstable`, `test_engine`, `test_wal`, `test_manifest`, `test_server`, `test_wfw`, `test_small`, `test_restart`.
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
├── .work_state           (AI agent crash-resume state)
├── include/kvdb/
│   ├── block_cache.hpp   (SSTableCache — LRU block/metadata cache)
│   ├── block_reader.hpp  (BlockReader — pure virtual cache interface)
│   ├── bloom.hpp         (BloomFilter)
│   ├── bptree.hpp        (BPlusTree — contiguous-page)
│   ├── client.hpp        (TCP client)
│   ├── config.hpp        (constants)
│   ├── engine.hpp        (LSMTreeEngine)
│   ├── iterator.hpp      (RangeIterator + SourceIterators, cache-aware)
│   ├── kv_cache.hpp      (KVCache — LRU key-value cache)
│   ├── manifest.hpp      (SSTable catalog)
│   ├── memtable.hpp      (MemTable)
│   ├── protocol.hpp      (wire protocol + socket helpers)
│   ├── server.hpp        (TCP server + write queue)
│   ├── snappy.hpp        (Snappy compression)
│   ├── snp_tracker.hpp   (Snapshot tracker)
│   ├── sstable.hpp       (SSTable v5, BlockReader-aware)
│   ├── types.hpp         (KeyValuePair, RangeBound)
│   ├── wal.hpp           (WAL with checkpointing)
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
│   ├── test_frz.cpp
│   ├── test_fuzz.cpp
│   ├── test_manifest.cpp
│   ├── test_memtable.cpp
│   ├── test_min.cpp
│   ├── test_restart.cpp
│   ├── test_server.cpp
│   ├── test_small.cpp
│   ├── test_sstable.cpp
│   ├── test_wal.cpp
│   └── test_wfw.cpp
└── docs/
    ├── api.md
    └── design.md
```

## Pending Steps (DO NOT start unless user explicitly asks)
| Step | Feature | Status |
|------|---------|--------|
| — | (none pending) | — |
