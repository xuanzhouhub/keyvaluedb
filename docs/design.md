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
- Linked leaves for O(n) ordered iteration via `MemTableWalk`
- Internal nodes: `StaticVec`-based, fanout 16, inline key storage (1024B)
- Values > 2KB stored as external blobs
- **Leaves are immutable** (Copy-on-Write) — never modified in place
- **Internal nodes** carry a **sequence-lock version** (`std::atomic<uint64_t>`)

#### B+-tree Read Protocol (lock-free)

```
FindLeaf(key):
  node = root_
  while node:
    v1 = node.version.load(acquire)
    if v1 & 1: continue              // node locked → retry same node
    idx = binary_search(node.keys, key)
    child = node.children[idx] or node.child_leaves[idx]
    v2 = node.version.load(acquire)
    if v1 != v2: continue            // version changed → retry same node
    if child is leaf: return child    // done — leaf is immutable
    node = child                      // descend to next internal node
```

Properties:
- **No locks**: readers never acquire any mutex or spinlock — pure atomic loads
- **No CAS**: readers only `load(acquire)`, never `compare_exchange`
- **No cross-level retries**: each node retry is self-contained
- **Leaves read freely**: leaves are immutable (CoW), no version check needed
- **Odd version → writer present**: reader skips the node (equivalent to spinlock observe)

#### B+-tree Write Protocol (Copy-on-Write, one pointer write per operation)

The odd bit of the version is the **spinlock**:
```
TryLock(v):    CAS even → odd (set lock bit)    — writer acquires
IsLocked(v):   v & 1                             — reader observes
UnlockAndBump: fetch_add(1)                      — clears odd, bumps counter
```

**Non-split** (leaf has room for new entry):
```
nleaf = NewLeaf()
CopyLeafContent(nleaf, leaf)          // 1. prepare (no lock)
nleaf.InsertEntry(key, value, ts)     // 2. insert (no lock)

lock(leaf.parent)                     // 3. CAS even→odd
leaf.parent.child_leaves[idx] = nleaf // 4. one pointer write
unlock(leaf.parent)                   // 5. fetch_add(1): odd→even, bump counter

RetireLeaf(leaf)                      // 6. defer old leaf to EBR
```

**Split** (leaf full, unified ascension loop):
```
left, right = split leaf + InsertEntry           // 1. prepare leaves (no lock)
sep = right.first_key

// 2. ascend: build replacement nodes (no lock)
pair = (left, right), pair_is_leaf = true
for lv = leaf.parent down to root:
    cp = path[lv], cidx = indices[lv]
    nn = NewInternal(), copy cp's Store
    replace cidx with pair + insert sep
    if nn.KeyCount() < fanout:
        saved_nn = nn, lock_target = cp.parent    // fits here
        placed = true, break
    split nn → new (node_l, node_r) pair, ascend  // overflow

// 3. splice leaf chain (no lock)
link left/right in place of old leaf

// 4. one lock + one pointer write
if placed:
    lock(lock_target)                              // CAS even→odd
    lock_target.children[idx] = saved_nn           // one pointer write
    unlock(lock_target)                            // fetch_add(1): odd→even, bump

else:
    root_ = new_root                               // no lock (single writer)

RetireLeaf(leaf)
```

Properties:
- **Entire sub-tree pre-built outside lock**: allocation, copying, splitting — all lock-free
- **One lock per write**: `lock(leaf.parent)` (non-split) or `lock(node.parent)` (split) — exactly one CAS+fetch_add
- **One pointer write per write**: always replacing a pointer in the parent node
- **No in-place Store mutation**: new nodes are created, pointers replaced — never `SwapAll` or vector insertion into live nodes
- **Version tracking**: writer bumps from even_old → odd (lock) → even_new (unlock). Readers see the bump and retry if their v1 is stale
- **Lock scope**: nanoseconds — single pointer assignment

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
  GetHeavy(seq) -> shared_ptr<const CachedHeavy>  // bloom + offsets + first_key_buf
  PutHeavy(seq, bloom, offsets, first_key_buf)
  GetBlock(seq, block_idx) -> shared_ptr<const string>
  PutBlock(seq, block_idx, data)
  Invalidate(seq)
```

### SSTableCache (FlatCache-based)
```
SSTableCache (block_cache.hpp) uses kvdb::internal::FlatCache:
  - FlatCache<K,V>: flat entry array, open-addressing hash table,
    intrusive doubly-linked LRU, per-shard mutex.
  - No per-node heap allocations (replaces std::unordered_map + std::list).
  - Stores shared_ptr<const V>, returns shared_ptr on Get (no data copies).
  - Tombstone-safe open addressing with cycle guard.
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

### Batch Write Path

Batch writes allow many keys to be committed atomically with a single timestamp. The design has three layers: client protocol, server queue management, and engine timestamp control.

#### Timestamp Assignment

```
StartBatch:
  batch_ts = global_ts_.load() + batch_increment_gap  (gap = 1M, configurable)
  All batch writes share batch_ts.
  Only one batch active at a time (batch_mutex_ + batch_in_progress_).

Normal writes during batch:
  Increment global_ts_ as usual (fetch_add(1) + 1).
  If global_ts reaches batch_ts → BLOCK at batch_cv_ until batch completes.
  Gap ensures 1M normal writes fit before blocking.

Reader visibility:
  read_ts = global_ts_.load()  → always < batch_ts during batch.
  Batch entries invisible to readers until commit.

CommitBatch:
  global_ts_.store(batch_ts + 1)              ← jump over batch_ts
  All batch entries become visible to future readers.
  Blocked normal writers unblocked via batch_cv_.
```

#### Server Dual-Queue + Mini-Batch

```
Two queues: normal (write_queue_, priority) + batch (batch_queue_, secondary).

Writer loop:
  Normal writes: always first — pop one, process, repeat.
  Batch writes: enqueued async. Writer drains in mini-batches.

Mini-batch triggers:
  (a) batch_queue_.size() >= mini_batch_size_  (default 1000)
  (b) batch_queue_bytes_ >= max_queue_bytes_ / 2
  (c) batch_commit_pending_ is true

When triggered: drain up to mini_batch_size_ entries, call engine.BatchInsert/BatchDelete.
During mini-batch processing: normal writes WAIT (atomic mini-batch).
After mini-batch: normal writes resume priority.

Commit: HandleClient sets batch_commit_pending_, waits on batch_commit_done_cv_.
         Writer drains all remaining, calls engine.CommitBatch(), signals CV.

Abort (small batch, nothing touched memtable):
  Clear batch_queue_ — instantaneous, zero pollution.

Abort (large batch, some mini-batches persisted):
  Clear remaining queue entries.
  engine.AbortBatch() writes WAL abort record.
  Add batch_ts to aborted_batch_ts_ on all memtables + SSTable metadata.
```

#### Aborted Entry Visibility

Aborted entries are marked — not deleted — because MVCC entries are immutable. The mark (`aborted_batch_ts_`) propagates through:

```
MemTable (BPlusTree::aborted_batch_ts_):
  - Lookup: skip entries with timestamp in aborted set
  - MemTableWalk (flush): skip aborted entries — never written to SSTable

SSTable (Metadata::aborted_batch_ts_):
  - Persisted in CRC-COVERED REGION of SSTable file (one per level)
  - scanBlock (LookupKey): skip entries with aborted timestamp
  - SSTableIterator (RangeScan): skip via Next() check
  - LevelIterator: propagates from Metadata to SSTableIterator

Compaction:
  - Union aborted_batch_ts_ from all input SSTables
  - Pass unioned set to SSTableIterator → auto-skip aborted entries
  - Output metadata inherits union (NOT at bottom level)
  - At bottom level: marks dropped — all batch entries should be gone by now

#### Recovery: Batch WAL State Detection

Recovery handles 8 cases based on what's in the WAL after the last checkpoint:

| # | WAL state | Recovery action |
|---|-----------|-----------------|
| 1 | Begin + entries + **commit** | Commit closes batch. Entries replayed, visible. |
| 2 | Begin + entries + **abort** | Abort closes batch, ts added to aborted. Memtable + SSTable metadata marked. Entries skipped during replay. |
| 3 | Begin + entries, **no commit/abort** (crash) | Auto-abort: ts added to aborted. Same as case 2. |
| 4 | **Begin-only**, no entries, no commit/abort | No entries persisted. Silently ignored. Zero overhead. |
| 5 | Checkpoint **after** commit | Checkpoint carries global_ts ≥ batch_ts+1 (batch_ts field = 0). Entries trimmed. No tracking needed. |
| 6 | Checkpoint **during** batch, commit after | Checkpoint carries batch_ts → batch_opened. Commit marker after checkpoint closes it. Entries visible. |
| 7 | Checkpoint **during** batch, crash before commit | Same as case 3 — auto-abort via checkpoint's batch_ts. |
| 8 | Entries flushed to SSTable before crash, no commit/abort | Auto-abort marks active memtable + all SSTable metadata. Flushed entries hidden by SSTable's aborted set. |

**Implementation**: Second pass tracks three sets — `batch_opened` (begin markers), `batch_closed` (commit/abort markers), `batch_has_entries` (actual entry timestamps). Auto-abort only fires when a batch has entries but no close marker. Checkpoint records carry the active batch_ts so the pre-checkpoint WAL doesn't need to be scanned.
```

When a reader skips an aborted entry, it naturally finds the **next older committed version** — the `continue` in the lookup loop proceeds to the next MVCC entry for the same key.

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
| StartBatch | batch_mutex_ |
| CommitBatch | batch_mutex_ → batch_cv_ notify |
| AbortBatch  | batch_mutex_ → B (shared) → D (if SSTable marks) |
| BatchInsert | batch_mutex_(shared via timestamp) → B |
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
11. **Batch write timestamps**: assigned at `StartBatch()` time — `batch_ts = global_ts + gap`. All batch writes share this timestamp. Reads naturally skip batch entries (read_ts < batch_ts) until `CommitBatch()` jumps `global_ts` past it. *(Previous design of assigning timestamp at commit time was abandoned in favor of start-time assignment, which avoids timestamp indirection in WAL/SSTable records.)*
12. **Batch atomicity**: all writes in a batch share one timestamp. They become visible or invisible as a unit — no partial visibility.
13. **Batch abort safety**: aborted batch entries are skipped via `aborted_batch_ts_` checks at every read layer. The next older committed version is returned instead. Aborted entries are never lost — they survive in the SSTable until compaction GC.
14. **Deletion mark propagation**: aborted batch timestamps persist through flushes (memtable → SSTable) and compactions (input SSTable → output SSTable). Marks are dropped only at the bottom compaction level, once all aborted entries have been physically removed.
15. **Zero-pollution abort**: if no mini-batch drained before abort (small batches), no WAL record or deletion mark is written — the batch queue is simply cleared.
16. **Mini-batch atomicity**: once a mini-batch begins processing, normal writes wait. The mini-batch completes atomically to avoid mixed visibility within the batch.
