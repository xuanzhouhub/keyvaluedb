# Design Document

## Architecture Overview

```
Client (TCP)
    │
    ▼
┌─────────────────────────────────┐
│  Server (connection threads)     │  Reads: inline via MVCC snapshot
│    ┌─────────────────────────┐   │  Writes: enqueue → single writer
│    │  Write Queue (16MB cap) │   │
│    └───────────┬─────────────┘   │
└────────────────┼─────────────────┘
                 │
┌────────────────▼─────────────────┐
│  LSMTreeEngine                   │
│                                   │
│  Insert → WAL::Buffer → Sync    │
│         → MemTable::Insert      │
│         → If full: Freeze       │
│             → notify FlushWorker│
│                                   │
│  FlushWorker (background):       │
│         → DoFlush → SSTable     │
│         → Manifest::Add         │
│         → WAL::Checkpoint       │
│                                   │
│  CompactionWorker (background):   │
│         → Merge levels L→L+1    │
│         → Tombstone GC at last  │
│         → Manifest swap         │
│         → DeferFileGC (safe)    │
│         → DrainFileGC (periodic)│
│                                   │
│  Lookup:                          │
│    1. KV Cache (LRU write-thru)  │
│    2. Active MemTable (shared)   │
│    3. Frozen MemTables (shared)  │
│    4. SSTables newest-first      │
│       (bloom → range → scan)    │
│       → BlockReader cache-aware │
└───────────────────────────────────┘
```

## Storage Layout

### MemTable (B+-tree)
- **Leaf page**: 4KB aligned slab (`_aligned_malloc`), slotted page layout
- Slot directory (14B/entry) grows right, records grow left from top
- Linked leaves for O(n) ordered export
- Internal nodes: `std::vector`-based, fanout 16
- Values > 2KB stored as external blobs

### SSTable v5
```
[Header: 24B]
  Magic | Version=5 | BlockSize | EntryCount | Compression | KeyLens

[Blocks]
  [CRC32:4B][CompType:1B][CompSize:4B][NumEntries:4B][Entries...]
  CRC covers compressed payload
  Entry: [KeyLen:4B][Key][ValueLen:4B][Value][Timestamp:8B][Flags:1B]
         bit 0 = tombstone (fl & 1)
         kLargeValFlag = 0x7FFF (avoids conflict with tombstone bit 0x8000)

[Filter Area]
  [MinKey][MaxKey][BloomBitCount:4B][HashCount:4B][DataLen:4B][BloomData]

[Footer]
  [BlockCount:4B][Offsets...][FilterOffset:8B][FooterMagic:4B]
```

### WAL
```
Record: [CRC32:4B][KeyLen:4B][Key][ValueLen:4B][Value][Timestamp:8B]
Checkpoint: [CRC32:4B][0xFFFFFFFF][Timestamp:8B]
```

### MANIFEST
```
Record: [CRC32:4B][Type:1B][Payload:N]
Types: 0x01 = AddSSTable, 0x02 = RemoveSSTable
Stores manifest_seq per SSTable for cache-key consistency.
```

## Caching Architecture

### BlockReader Interface
```
class BlockReader (block_reader.hpp) — pure virtual:
  GetBloom(seq, bloom_out) -> bool
  PutBloom(seq, bloom)
  GetBlockOffsets(seq, offsets_out, first_keys_out) -> bool
  PutBlockOffsets(seq, offsets, first_keys)
  GetBlock(seq, block_idx, data_out, entry_count_out) -> bool
  PutBlock(seq, block_idx, data, entry_count)
  Invalidate(seq)
```

### SSTableCache
```
SSTableCache : BlockReader (block_cache.hpp):
  Two-partition LRU:
    Heavy map:   bloom + block_offsets + first_keys, keyed by manifest_seq
    Block map:   uncompressed data blocks, keyed by BlockKey(seq, idx) = (seq << 32) | idx
  Default: 1024 blocks / 256 metadata / 64MB
  Eviction: bytes-aware for blocks, count-aware for metadata
  Invalidate(seq): evicts all heavy entries + all blocks matching seq range
```

### Cache Integration Points
| Operation | BlockReader& passed? | Populate? |
|-----------|---------------------|-----------|
| `WriteFromWalk` (flush) | Yes (`sst_cache_`) | Yes — cache all blocks |
| `ReadMetadata` (load) | Yes (`sst_cache_`) | Yes — cache bloom + offsets |
| `LookupKey` (point read) | Yes (`sst_cache_`) | Yes — read-through + populate |
| `SSTableIterator` (range scan) | Yes (`sst_cache_`) | Yes — read-through + populate |
| `Compact` (compaction) | No (1-arg constructor) | No — `reader_=nullptr`, skip cache |

Key: compact uses the 1-arg `SSTableIterator(filepath)` constructor with `reader_=nullptr`, which skips all cache operations. This prevents compaction from evicting user-hot blocks.

## Concurrency Model

### Threads

| Thread | Role | Cardinality |
|--------|------|-------------|
| Writer | Dequeues from write queue, calls `Insert()` | **1** (server `WriterLoop`) |
| WAL sync worker | Periodically `fsync`s WAL buffer to disk | **1** (`EngineSyncState`) |
| Flush worker | Drains `frozen_memtables_`, writes SSTables | **1** (`FlushState`) |
| Compaction worker | Merges SSTables across levels when threshold reached | **1** (`CompactionState`) |
| Readers | `HandleClient` threads calling `Lookup()` / `RangeScan()` | **N** (connection threads) |
| Main | Recovery, `Flush()`, `WaitForPendingFlushes()`, destructor | **1** |

### Latches (lock acquisition order A → B → C)

```
                    A: flush_->mtx          (std::mutex)
                   / \
                  /   \
    B: memtable_mutex_  C: pending_recycle_mutex_  E: pending_gc_mutex_
    (std::shared_mutex)    (std::mutex)               (std::mutex)
                  |
          D: sstable_metadata_mutex_
             (std::mutex)
```

**Rule**: always acquire in order. If a path needs A and B, it takes A first then B. The DAG has no cycles.
Locks C and E are independent (mutex per queue) — acquired without nesting with each other.

Separate from the above — acquired without nesting:
- `sync_->mtx` — WAL sync coordination
- `wal_` internal mutex — WAL buffer / file
- `manifest_` internal mutex — MANIFEST file
- `SSTableCache` internal mutex — cache operations

### Write Path (Writer thread)

```
Insert(key, value):
  1. ts = global_ts_.fetch_add(1) + 1            ← atomic, no lock
  2. wal_->Buffer(key, value, ts)                 ← wal_ mutex
  3. notify WAL sync worker                       ← sync_->mtx
  4. Lock B: unique_lock<shared_mutex>
       active_memtable_->Insert(key, value, ts)   ← single writer, no internal lock needed
       if IsFull():
         Freeze → push to frozen_memtables_
         Lock A: flush_->pending++
         notify flush worker via flush_->cv
         swap active_memtable_
     Unlock B
  5. Wait for WAL sync: sync_->cv.wait(synced_seq >= my_seq)
  6. kv_cache_->Put(key, value, ts)               ← KV cache write-through
  7. DrainRecyclePending()
       Lock C → Lock B (in order)
```

Key point: the SSTable flush has been moved to the **Flush worker**. The writer only freezes the memtable and notifies the worker — it never blocks on disk I/O.

### Read Path (Reader threads)

```
Lookup(key):
  1. kv_cache_->Get(key, read_ts, value_out)      ← KV cache (fast path)
  2. read_ts = global_ts_.load()                  ← atomic
  3. SnapshotTracker::Acquire(read_ts)            ← internal mutex
  4. Lock B: shared_lock<shared_mutex>
       search active_memtable_                    ← read-only, concurrent with other readers
       search frozen_memtables_ (newest first)
     Unlock B
  5. Lock D: copy sstable_metadata_
     Unlock D
  6. For each SSTable (newest first, L0 then L1+ binary search):
       skip if source_table_id already scanned (from frozen memtables)
       skip via range filter (min_key / max_key)
       skip via bloom filter
       SSTable::LookupKey(filepath, key, read_ts, sst_cache_, manifest_seq)
         → BlockReader cache: read-through bloom + blocks
  7. Tombstone: if found and empty value → return false
  8. SnapshotTracker::Release(read_ts)
```

Readers never block writers (shared_lock vs unique_lock on B). They never block the flush worker either. SSTable file reads are lock-free with respect to engine state.

### Flush Path (Flush worker)

```
FlushWorkerLoop:
  loop:
    Lock A: wait flush_->cv (pending > 0 or should_stop)
    if should_stop and pending == 0 → break
    Lock B: unique_lock<shared_mutex>
      pop front from frozen_memtables_
    Unlock B
    DoFlush(memtable):                             ← writes SSTable file, updates manifest
      sstable_seq_.fetch_add(1)                    ← atomic
      SSTable::WriteFromWalk(…, sst_cache_, seq)   ← disk I/O + cache populate
      SSTable::ReadMetadata(…, sst_cache_, seq)    ← cache bloom + offsets
      Lock D: push to sstable_metadata_
      manifest_->AddSSTable(seq, meta)             ← manifest_ mutex + fsync
    wal_->WriteCheckpoint(global_ts_)              ← wal_ mutex
    DeferRecycle(memtable)
      Lock C: push to pending_recycle_
    DrainRecyclePending()
      Lock C → Lock B (in order)
    Lock A: pending--
    notify done_cv
```

The flush worker holds no locks during disk I/O (`DoFlush`). It only takes B briefly to pop a memtable, then releases it before the heavy work.

### Compaction Path (Compaction worker)

**Trigger**: Every 2s, counts SSTables per level. When level L reaches 8, compaction fires. To avoid wasted intermediate compactions, the worker checks consecutive levels: if L+1 also has ≥ 8, L+L+1 are compacted together into L+2.

**Algorithm** (`CompactLevel(from, top)` → `SSTable::Compact`):
1. Snapshot `sstable_metadata_`, collect all SSTables from levels `from` through `top`
2. Open iterators: individual `SSTableIterator` per L0 file (they overlap); one `LevelIterator` per L1+ level (chains non-overlapping files). All use 1-arg constructor (no cache interaction).
3. K-way merge-sort: smallest key across all iterators → collect all versions → keep highest timestamp
4. Tombstone removal: if `to_level == kMaxLevel`, drop entries where the winner is a tombstone
5. Output: batch entries, write as `sstable_{seq}.sst` with `level = to_level`, split when approaching `4MB * 10^(to_level)`
6. Lock D: swap `sstable_metadata_` (remove old, add new)
7. MANIFEST: `RemoveSSTable` for old, `AddSSTable` for new, fsync
8. **DeferFileGC**: invalidate cache for old manifest_seq, push `{filepath, seq, fence_ts}` to `pending_gc_`
9. **DrainFileGC**: called on every compaction worker iteration (and at engine destructor). When `MinActiveTS() >= fence_ts`, delete the file from disk.

**Leveling strategy**:
- L0: SSTables from flushes, can overlap, no size limit per file
- L1+: SSTables from compaction, **non-overlapping** within the level (output is globally sorted)
- Size: L1 = 40MB, L2 = 400MB, L3 = 4GB … `4MB × 10^level`
- Threshold: 8 SSTables triggers compaction
- Cascade: `L0(10) + L1(8)` → compact both → L2 in one pass

### WAL Sync Path (WAL sync worker)

```
SyncWorkerLoop:
  loop:
    wait for ready_cv (requested_seq > synced_seq) or timeout 200µs
    wal_->Sync()                                   ← wal_ mutex + fsync
    synced_seq = batch_seq
    notify cv (wakes writers waiting on sync)
```

Group commit: the sync worker batches multiple `Buffer()` calls into one `fsync`. Writers wait on `sync_->cv` after their batch seq becomes durable.

### MemTable Recycling

```
DeferRecycle(memtable):
  fence_ts = global_ts_.load()                     ← snapshot the current max timestamp
  push {memtable, fence_ts} to pending_recycle_

DrainRecyclePending():
  for each entry in pending_recycle_:
    if SnapshotTracker::MinActiveTS() >= fence_ts:
      remove from pending_recycle_
      Lock B: erase from frozen_memtables_          ← last shared_ptr released → MemTable freed
```

A frozen memtable stays alive until every active reader has a `read_ts >= fence_ts`. At that point every reader sees the SSTable version (newest), so the memtable's older versions are no longer needed.

### Deferred File GC

```
DeferFileGC(filepath, manifest_seq, fence_ts):
  Lock E: push {filepath, manifest_seq, fence_ts} to pending_gc_

DrainFileGC():
  for each entry in pending_gc_ where MinActiveTS() >= fence_ts:
    sst_cache_->Invalidate(manifest_seq)
    std::filesystem::remove(filepath)
    pop from pending_gc_
```

Called from:
- `CompactLevel`: after manifest swap (with `fence_ts = global_ts_`)
- `CompactionWorkerLoop`: every 2s iteration (ensures deferred files drain even without new compactions)
- `~LSMTreeEngine`: final cleanup (all readers gone → all entries drain)

The `fence_ts` is set after the manifest swap. New readers will see the new metadata and open new files. When `MinActiveTS() >= fence_ts`, no reader that started before the swap is still active — old file handles are all released, so files can be safely deleted.

### Lock Acquisition Summary

| Path | Lock order |
|------|-----------|
| Insert (freeze) | B → A |
| Insert (recycle) | C → B |
| FlushWorkerLoop | A → B → (D, wal_, manifest_) → C → B |
| CompactionWorkerLoop | D (snapshot) → [disk I/O, no locks] → D (swap) → manifest_ → E (DeferFileGC) → E (DrainFileGC) |
| Lookup | B (shared) → D |
| RangeScan | B (shared) → D |
| Flush() | B → A → A (WaitForPendingFlushes) |
| WaitForPendingFlushes | A |
| DeferRecycle | C |
| DrainRecyclePending | C → B |
| DeferFileGC | E |
| DrainFileGC | E |
| Destructor | stop compactor → stop flush → stop sync → B (drain) → E (DrainFileGC) |

### Invariants

1. **Single writer**: only the Writer thread inserts into `active_memtable_`. No internal B+-tree locking needed.
2. **Immutable frozen memtables**: once frozen, a memtable is never modified. Readers and the flush worker can access it safely without synchronization.
3. **Timestamp monotonicity**: `global_ts_` is a single atomic counter, strictly increasing. The write queue is FIFO → timestamps reflect enqueue order.
4. **WAL-before-memtable**: `wal_->Buffer()` always precedes `active_memtable_->Insert()`. On recovery, WAL replay restores lost memtable data.
5. **Checkpoint-after-flush**: the flush worker writes `WriteCheckpoint` only after `Manifest::AddSSTable` is fsync'd. After recovery, the SSTable is visible and the checkpoint guarantees WAL truncation is safe.
6. **L1+ non-overlapping**: SSTables from compaction are globally sorted output → no key-range overlap within a level. `LevelIterator` relies on this to chain files sequentially.
7. **Tombstone removal only at last level**: tombstones propagate downward through compactions and are dropped only when merged into `kMaxLevel`, since no deeper level could hold an older version.
8. **Compaction visibility**: new SSTables become visible to readers only after the atomic `sstable_metadata_` swap. Old files are deleted only after all pre-swap readers release their snapshots (`MinActiveTS() >= fence_ts`).
9. **Cache key stability**: cache entries are keyed by `manifest_seq` (monotonic, immutable across manifest operations) rather than filepath. Filepath-based keys would be fragile across compaction file swapping.
10. **Compaction cache isolation**: compaction uses the 1-arg `SSTableIterator` constructor (`reader_=nullptr`), which skips all cache operations. Compaction I/O never evicts user-hot blocks from the LRU cache.
