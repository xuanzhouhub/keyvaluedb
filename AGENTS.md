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
- **WRITE BENCHMARKING**: The system has a single-writer thread. Write throughput must be tested via multiple concurrent clients to saturate the write queue. Never call `Insert()` from multiple threads directly — use the client-server infrastructure. For client-side tests, spawn many client connections writing concurrently. For server-side tests, verify `write_queue_` is never empty. Batch writes are the exception — only one client at a time since concurrent batches are rejected.

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
- **B+-tree** (contiguous-page, lock-free): `LeafPage` 4KB `_aligned_malloc`, `alignas(4096)`. Slotted page. Linked leaf chain.
- **Internal nodes**: `StaticVec`-based (inline key storage `key_data[1024]`), fanout 16. Contiguous single allocation.
- **Lock-free reads**: SeqLock version check on internal nodes, immutable leaves (CoW).
- **CoW writes**: Pre-built sub-tree, one spinlock + one pointer write per operation. Unified `SplitCoW` handles all levels.
- **EBR**: `ReadGuard` announces readers; retired nodes stamped with fence timestamp; `DrainRetired(MinActiveTS)` reclaims when oldest reader has passed the fence. **Counter-based drain (active_readers_ == 0) is a TOCTOU race — must never gate node freeing.** Drain only via timestamp comparison or at destructor.
- **Retired drain capping**: `kRetiredDrainBufSize=64` (local buffer arrays), `kRetiredDrainThreshold=60` (derived as `buf_size - kMaxBTreeDepth=4`). All write paths block with `std::this_thread::yield()` when `PendingRetiredSize() >= threshold`, preventing `DrainRetiredWithFence` local buffer overflow.
- **Leaf pool**: 64 pooled 4KB leaves, placement-new reuse — eliminates `_aligned_malloc` per insert. Pool replenished only at destructor or engine-driven drain.
- **Leaf chain range scan**: `MemTableSource` lazy iterates via `MemTableWalk` (test/point paths). `MaterializedMemTableSource` deep-copies overlapping leaf pages for range scans — releases `read_ts` early to avoid blocking EBR drain. O(1) range-bound overlap check via first/last key per leaf. `ExportEntries()` retained for tests only. `WriteFromWalk` uses `MemTableWalk` directly for flush.
- **Blob values**: `> page/2` stored as external blobs. Blob cleanup in destructor (MSVC codegen limitation).
- **MVCC**: `Find()` returns leftmost match; new versions inserted left. `Lookup()` scans for visible timestamp.

### Concurrency
- **SnapshotTracker**: lock-free 256-slot CAS array with random probe, mutex fallback + `fallback_used_` flag.
- **Dual Tracker**: two trackers — `tracker_` (memtable) and `tracker_sst_` (SSTable). `Lookup` and `RangeScan` release `tracker_` after the memtable phase, acquire `tracker_sst_` for the SSTable phase. EBR drain (`DrainRetired`) checks only `tracker_` — unblocked as soon as readers leave the memtable. File GC (`DrainFileGC`) checks `min(tracker_, tracker_sst_)` — safe because SSTable files must survive active SSTable-phase readers.
- **memtable_mutex_**: `shared_mutex` — readers briefly copy pointers, tree traversal fully outside lock. Writer lock fires only on freeze+swap (~once per 16MB).
- **sstable_metadata_mutex_**: `shared_mutex` — concurrent readers share; compaction swap takes exclusive briefly.
- **EBR drain**: engine-driven `DrainRetired(MinActiveTS())` after each Insert, progressive under read load.
- **Writer**: single-threaded. Internal node version spinlock (`TryLock`/`UnlockAndBump`) — one CAS per write.
- **Readers**: zero locks, zero CAS. SeqLock version validation + immutable leaves. ReadGuard for EBR safety.

### SSTable v5
- **Format**: `[Header:24B][Blocks][FilterArea][Footer]`
- **Blocks**: `[CRC32:4B][CompType:1B][CompSize:4B][NumEntries:4B][entries...]` — CRC over compressed payload.
- **Bloom filter**: Per-SSTable, 1% FPR, double hashing (FNV-1a + multiplicative). `MightContain(key)`.
- **Range filter**: `min_key` / `max_key` — `key < min || key > max → skip`.
- **Compression**: `kCompressionNone` active (Snappy compiled but disabled by default).
- **Entry format**: `[KeyLen:4B][Key][ValueLen:4B][Value][Timestamp:8B][Flags:1B]` — bit 0 = tombstone (`fl&1`).
- **ReadMetadata**: Bulk-read I/O (2 file reads: header + filter area). CRC32 over filter area for integrity. Flat buffer for block_first_keys — stored as single contiguous string `[len:2][key]...`, eliminating 3,449 individual string allocations per read.
- **LookupKey warm path**: Reads `CachedHeavy` directly from cache via shared_ptr — no data copies. Bloom filter check before block index scan. Block index scan uses sequential pass over flat buffer. `CachedHeavy.aborted_batch_ts` provides abort filtering without cache miss.

### Metadata
- `SSTable::Metadata` includes `manifest_seq` (uint64_t). Uses flat buffer `block_first_key_buf` instead of `vector<string>`. Access via `FirstKey(i)`, `FirstKeyCount()`, `FirstKeyView(i)`.
- `ReadMetadata(filepath, cache, manifest_seq)` — cache-first: returns cached bloom+offsets+buf without I/O on hit. Falls back to disk read on miss, then populates cache.
- Last block's `first_key` correctly set in `WriteFromWalk` (line 217).

### WAL
- `Buffer(key, value, ts)` → in-memory buffer (non-blocking). `Sync(force)` → if force or buffer ≥ 4MB: fwrite+fflush+fsync, returns `{seq, true}`. If !force and buffer < 4MB: bump counter only, returns `{seq, false}`.
- **Group Commit**: `SyncWorkerLoop` polls `ready_cv` (200us timeout). If `requested_seq > synced_seq`, wakes immediately. On timeout or idle, checks timer: if idle for `kWALIdleSyncUs` (default 200us), forces fsync. Updates `synced_seq` (atomic) on persist. No CV notification — Writer polls `synced_seq` independently.
  - `kWALIdleSyncUs = 200` — configurable tradeoff: shorter = lower latency, longer = larger batches per fsync.
  - `kWALMinSyncBytes = 4MB` — throughput: buffer must reach this for non-forced sync.
  - `timeBeginPeriod(1)` called at server startup for ~1ms timer resolution on Windows.
- **Batch Write**: `StartBatch()` reserves 1M timestamps. `BatchInsert`/`BatchDelete` buffer in WAL, return seq immediately (same as Insert). `CommitBatch()` polls `synced_seq` atomically (no CV wait). `CommitBatchAsync()` buffers commit sentinel + notifies sync; `CommitBatchFinalize()` idempotently swaps `global_ts_` when `synced_seq >= commit_seq`. Server uses Async+Finalize for non-blocking batch commit.
- **Batch Abort**: `AbortBatch()` writes WAL abort sentinel → MANIFEST abort record (`[CRC32][0x03][batch_ts:8B][fence_seq:8B]`) → adds `batch_ts` to memtable + SSTable metadata. Crash-unfinished batches are auto-aborted on recovery. Three persistence layers: WAL (primary, survives crash), MANIFEST (survives TrimWAL), SSTable file metadata (permanent, written by next flush). `Manifest::Compact()` at startup drops dead SSTable records + stale abort records whose pre-abort files are all gone.
- Per-entry CRC32. `Recover(checkpoint_ts)` — reads from last checkpoint, returns entries after it.
- `WriteCheckpoint(ts)` — sentinel record `[CRC32][0xFFFFFFFF][ts:8B]`.
- `TrimToLastCheckpoint()` — truncates WAL before last checkpoint.

### MANIFEST
- Persistent SSTable catalog. Each record: `[CRC32:4B][Type:1B][Payload]` — atomic fwrite+fsync.
- `AddSSTable(seq, meta)`, `RemoveSSTable(seq)`. `Recover()` reconstructs full catalog.
- Stores `manifest_seq` for each SSTable on recovery.

### Engine
- **Write**: `Insert()` → WAL::Buffer → notify SyncWorker → memtable insert → return `uint64_t` seq (~1 us). Non-blocking — no internal `cv.wait`. Persistence gated by `synced_seq` (atomic, advanced by SyncWorker). Writer polls `SyncedSequence()` to fulfill client promises.
- **Retired drain**: After each Insert/Delete/BatchInsert/BatchDelete, calls `DrainRetired(MinActiveTS())`, then spin-yields while `PendingRetiredSize() >= kRetiredDrainThreshold` — blocks writer when retired node queue is near buffer capacity, preventing overflow in `DrainRetiredWithFence` local arrays.
- **Lookup**: `Lookup(key)` → check KV Cache → snapshot `read_ts` → check MemTable (active + frozen) → SSTables (bloom+range filter, newest-first, L0 then L1+ binary search, BlockReader cache-aware).
- **Tombstone in LookupKey**: when `fl&1` (tombstone), returns `true` with empty value so the engine stops searching older SSTables and returns false.
- **Recovery**: MANIFEST → rebuild SSTable catalog → WAL from last checkpoint → replay → flush → checkpoint. Manifest recovery populates `manifest_seq` and loads bloom/range metadata from files.
- **Concurrency**: Lock-free B+-tree for reads/web_single writer thread. `memtable_mutex_` only gates freeze+swap. `sstable_metadata_mutex_` uses `shared_mutex` — concurrent reader metadata copies. MVCC snapshot reads via `SnapshotTracker::Acquire/Release`. EBR drain via `MinActiveTS()` fence check.
- **Backpressure**: Write queue byte-capped (16MB). Timestamps: `global_ts_` monotonic counter, key cap 1KB, KV cap 4MB.
- **MemTable Recycling**: After flush, memtable kept in `frozen_memtables_` with a `fence_ts`. `DrainRecyclePending()` checks `SnapshotTracker::MinActiveTS() >= fence_ts` — when all active readers see the SSTable, the memtable is unlinked and released.

### Compaction
- **Tombstones**: `Delete(key)` inserts an entry with empty value (tombstone). Tombstones survive flushes; `Lookup` returns false for tombstone entries.
- **Leveling strategy**: Level 0 (flushes, can overlap). Levels 1+ (non-overlapping sorted runs). Threshold: 8 SSTables triggers compaction.
- **Size multiplier**: 10× per level. Base SSTable size: 4MB. SSTables split when approaching level max size.
- **Background worker**: `CompactionWorkerLoop` periodically scans levels (2s interval). Compaction merges SSTables from level L into L+1, picks newest version per key, drops tombstones at the last level. Aborted batch entries are filtered during merge via `aborted_batch_ts`. Abort timestamps propagate into output SSTables and are dropped only at the bottom level when all aborted entries are physically removed.
- **Manual compaction**:
  - `LevelCounts() → vector<size_t>` — per-level SSTable count snapshot (L0=N, L1=M, ...)
  - `ManualCompact(min_sstables=2, from_level=0, cascade=true) → int` — scans from `from_level` upward, compacts the first level with ≥ `min_sstables`. If `cascade`, continues through adjacent qualifying levels. Returns number of levels compacted, or 0 if no level qualifies / threshold < 2 / another compaction is in progress.
  - Serialized by `compaction_mutex_` with `try_lock` — at most one compaction runs at a time. Contending callers (including the background worker) are rejected immediately (return 0 / skip cycle).
  - Exposed via client protocol: `L` (LevelCounts) → `V`+CSV string, `M` (ManualCompact) → `V`+count string.
- **Visibility**: New SSTables are written, manifest is updated atomically (remove old, add new). Old SSTable files are **deferred-GC'd** (not deleted immediately).
- **Concurrency**: Compaction uses `sstable_metadata_mutex_` for atomic swap. Flush worker writes to Level 0 concurrently.
- **Cache during compaction**: Iterators created with `populate=false` (1-arg constructor with `reader_=nullptr`) — compaction does not pollute the LRU cache.

### Caching
- **KV Cache** (`kv_cache.hpp`): Flat-array, open-addressing, intrusive-LRU shard design. LRU key-value cache for point lookups. Write-through (populated after WAL sync). Tombstones erase cache entries. Blobs (> 2KB) are not cached. Default: 10K entries / 16MB.
- **FlatCache** (`internal/flat_cache.hpp`): Reusable template `FlatCache<Key, Value, Hash, KeyEqual, SizeBytes>` — flat entry array, open-addressing hash table with tombstone support, intrusive doubly-linked LRU, per-shard mutex. No per-node heap allocations. Stores `shared_ptr<const Value>`, returns `shared_ptr` on Get (no data copies). Values tracked by customizable SizeBytes callable. Used as underlying implementation for SSTable Block Cache.
- **SSTable Block Cache** (`block_cache.hpp`): Uses `FlatCache` for both heavy metadata and data blocks.
  - `BlockReader` (`block_reader.hpp`) — pure virtual interface: `GetHeavy(seq)` returns `shared_ptr<const CachedHeavy>` (bloom + offsets + first_key_buf + aborted_batch_ts), `PutHeavy(seq, bloom, offsets, buf, aborted)`, `GetBlock`, `PutBlock`, `Invalidate`.
  - `SSTableCache` implements `BlockReader`. Default: 1024 blocks / 512 metadata / 64MB, 16 shards. Configurable via engine constructor.
  - **Cache keys**: `uint64_t manifest_seq` (not filepath). Block keys: `BlockKey(seq, idx) = (seq << 32) | idx`.
  - `Invalidate(seq)` erases heavy entry, does `EraseIf` on blocks for the seq range.
  - Block data also cached on disk read path in `LookupKey` (via `PutBlock` after disk read).
- **Block index binary search**: `first_key_offsets` precomputed at `ReadMetadata` time and cached in `CachedHeavy`. `LookupKey` uses O(log N) binary search instead of O(N) sequential scan. Speeds up L2 reads from 27µs → 17µs.
- **Cache populates during**: flush (`WriteFromWalk`), point lookup (`LookupKey`), range scan. NOT during compaction (`populate=false`).
- **Warm path**: `ReadMetadata` returns cached data via single `GetHeavy` call (no copies). `LookupKey` reads `CachedHeavy` directly from cache via `shared_ptr`, avoiding all data copies.

### Deferred File GC
- **Problem**: Compaction swaps SSTable metadata atomically, but pre-swap readers may still have open file handles to old SSTables. Immediate `std::filesystem::remove` fails on Windows (open handles) and risks use-after-free on Linux.
- **Solution**: `PendingFileGC` struct (`filepath`, `manifest_seq`, `fence_ts`).
- `DeferFileGC(f, seq, fence_ts)` — pushes to `pending_gc_` vector (protected by `pending_gc_mutex_`).
- `DrainFileGC()` — for each entry where `SnapshotTracker::MinActiveTS() >= fence_ts`: invalidate cache for that `manifest_seq`, call `std::filesystem::remove`, pop from list.
- Called from `CompactLevel` (after manifest swap), `CompactionWorkerLoop` (every iteration), and `~LSMTreeEngine` (final cleanup).
- Cache invalidation happens immediately at defer time via `sst_cache_->Invalidate(seq)`.

### Range Scan
- **Merge iterator**: `RangeIterator` builds a min-heap over `MaterializedMemTableSource` + `SSTableIterator` + `LevelIterator` sources. Pops smallest key, collects duplicates, resolves via MVCC (newest timestamp ≤ read_ts).
- **Memtable materialization**: Before building iterators, `RangeScan` deep-copies overlapping leaf pages into standalone `_aligned_malloc`'d copies (`MaterializedMemTableSource`). Per-leaf range check is O(1) via first/last key bounds. `read_ts` is released immediately after materialization to avoid blocking EBR drain. `ScanPinned` guard pins SSTable metadata via `shared_ptr<void>`.
- **SeekToKey**: `SSTableIterator` uses **block-index binary search** on `block_first_key_buf`, then jumps to the target block via `file.seekg(block_offsets[target])`. Linear scan within the target block only. Setup cost: ~29 us (was 1,300 us with entry-level linear scan). Iteration: ~950 ns/entry.
- `SSTableIterator` constructors accept optional `block_offsets` and `block_first_key_buf` pointers for indexed seek. Engine RangeScan, Compaction, and LevelIterator pass these from snapshotted metadata.
- Concurrent range scans served directly on client threads, scale near-linearly with thread count.
- **Abort filtering**: `MaterializedMemTableSource` filters aborted entries via `aborted_` pointer to the memtable's aborted timestamp set. `SSTableIterator` filters via `aborted_` pointer from metadata snapshot.

### Server/Client
- TCP server: connection-per-client threads, single writer queue, concurrent reads via MVCC.
- **WriterLoop**: polls `write_queue_cv_` with `kWALIdleSyncUs` timeout. Drains ALL normal requests per iteration. Calls `engine_.Insert/Delete` (returns immediately), pushes to `pending_writes_`, resolves promises via `checkAndFulfill()` which polls `SyncedSequence()`. `checkAndFulfill()` runs at top of every loop iteration — even idle.
- **Batch commit (server)**: `HandleClient` sets `batch_commit_requested_` (wakes Writer) and `batch_commit_pending_` (blocks client). Writer picks up via `batch_commit_requested_`, calls `CommitBatchAsync()`, sets `commit_finalizing_`, clears `batch_commit_requested_`. `checkAndFulfill()` calls `CommitBatchFinalize()` when synced, clears `batch_commit_pending_`, notifies client handler. The two flags prevent the Writer `wait_for` predicate from busy-spinning while waiting for sync.
- Binary protocol: `W + key + value` (sync write), `w + key + value` (async write), `D + key` (sync delete), `d + key` (async delete), `R + key` (read), `O/V/N/E` (responses).
- `L` (LevelCounts): no payload, returns `V` + comma-separated per-level SSTable counts. `M + min_sst:4B + from_level:4B + cascade:1B` (ManualCompact): returns `V` + compacted-levels string.
- Client API: `LevelCounts() → vector<size_t>`, `ManualCompact(min_sstables, from_level, cascade) → int`.
- Async writes respond OK immediately after enqueue (no persistence wait). Queue backpressure still applies via `write_queue_not_full_cv_`.
- **CAS**: synchronous. Writer defers conflicting requests to `deferred_requests_`. Drain loop bounded by `kMaxCasDeferred` (64): stop draining and re-check CAS after 64 non-conflicting requests or when `deferred_requests_` hits the cap. CAS Lookup runs on `std::async` thread; Writer poll-spins (microseconds) until ready.
- Byte-capped write queue (16MB) with CV-based backpressure.
- `timeBeginPeriod(1)` at `Start()` for ~1ms timer resolution.

### Tests
- 10 test suites: `test_memtable`, `test_sstable`, `test_engine`, `test_wal`, `test_manifest`, `test_server`, `test_wfw`, `test_small`, `test_restart`, `test_flat_cache`.
- `test_engine`: includes `TestLevelCounts`, `TestManualCompactEngine`, `TestManualCompactRejectsLowThreshold`, `TestManualCompactConcurrentReject`.
- `test_server`: includes `TestClientManualCompact` (end-to-end via client protocol).
- Separate fuzz test (`test_fuzz`, `-DKVDB_BUILD_FUZZ=ON`): 32 threads, fault injection, persistence verification.
- All suites pass (10/10, 100%).

## Performance Profile (Release build, 16B values, 15 SSTables ~50K entries)

| Scenario | Engine Direct | Via Server |
|----------|-------------|------------|
| Write 16B 1-thr | 946,000/s | 513/s |
| Write 16B 4-thr | — | 2,322/s |
| Write 16B 8-thr | — | 4,078/s |
| Write 16B 16-thr | — | 8,135/s |
| **Async write 16B 1-thr** | — | **31,069/s** |
| **Async write 16B 4-thr** | — | **122,290/s** |
| **Async write 16B 16-thr** | — | **183,960/s** |
| **Async write 1KB 8-thr** | — | **124,290/s** (121 MB/s) |
| Read KV HIT 1-thr | 310,000/s | 43,100/s |
| Read KV HIT 4-thr | 1,040,000/s | 98,500/s |
| Read KV HIT 8-thr | 1,620,000/s | 157,000/s |
| Read SSTable warm 1-thr | 322,000/s | — |
| Bloom MISS 1-thr | 5,190,000/s | — |
| **Range scan** 100 entries | 4,194,000/s | — |
| **Range scan** 1,000 entries | 9,641,000/s | — |
| **Range scan** 5,000 entries | 11,068,000/s | — |

- **Engine Insert**: ~1 us (946K/s) — non-blocking, return immediately
- **Server write latency**: ~1.9 ms/write (1-thr, 16B) — dominated by `_commit` fsync (~0.86ms) + Writer poll interval (~1ms)
- **SSTable::LookupKey warm**: ~4 us (250K/s) — GetHeavy + block index scan + block read
- **Engine Lookup warm**: ~5 us (200K/s) — KV cache miss + memtable + SSTable path
- **Engine Lookup KV HIT**: ~2 us (500K/s)
- **Bloom/range MISS**: ~0.2 us (5M/s)
- ReadMetadata cold: ~470 us (79% CRC32, 19% I/O, 2% parse)
- ReadMetadata warm (cache): <1 us

## Pending Steps (DO NOT start unless user explicitly asks)
| Step | Feature | Status |
|------|---------|--------|
| — | Bulk-read I/O + flat buffer for SSTable metadata | **Done** |
| — | FlatCache — reusable flat-array LRU cache (open-addressing, intrusive LRU) | **Done** |
| — | SSTableCache refactored to use FlatCache | **Done** |
| — | BlockReader API: merged GetBloom+GetBlockOffsets → GetHeavy/PutHeavy (shared_ptr, no copies) | **Done** |
| — | LookupKey warm path: reads CachedHeavy directly, no data copies | **Done** |
| — | Cache index from 0 (removed manifest_seq != 0 check) | **Done** |
| — | Group commit: non-blocking Insert, atomic synced_seq, polling Writer | **Done** |
