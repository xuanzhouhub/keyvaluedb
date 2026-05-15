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
│  Lookup:                          │
│    1. Active MemTable (shared lock)
│    2. Frozen MemTables (shared lock)
│    3. SSTables newest-first       │
│       (bloom → range → scan)     │
└───────────────────────────────────┘
```

## Storage Layout

### MemTable (B+-tree)
- **Leaf page**: 4KB aligned slab (`_aligned_malloc`), slotted page layout
- Slot directory (14B/entry) grows right, records grow left from top
- Linked leaves for O(n) ordered export
- Internal nodes: `std::vector`-based, fanout 16
- Values > 2KB stored as external blobs

### SSTable v4
```
[Header: 24B]
  Magic | Version=4 | BlockSize | EntryCount | Compression | KeyLens

[Blocks]
  [CRC32:4B][CompType:1B][CompSize:4B][NumEntries:4B][Entries...]
  CRC covers compressed payload

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
```

## Concurrency Model

### Threads

| Thread | Role | Cardinality |
|--------|------|-------------|
| Writer | Dequeues from write queue, calls `Insert()` | **1** (server `WriterLoop`) |
| WAL sync worker | Periodically `fsync`s WAL buffer to disk | **1** (`EngineSyncState`) |
| Flush worker | Drains `frozen_memtables_`, writes SSTables | **1** (`FlushState`) |
| Readers | `HandleClient` threads calling `Lookup()` / `RangeScan()` | **N** (connection threads) |
| Main | Recovery, `Flush()`, `WaitForPendingFlushes()`, destructor | **1** |

### Latches (lock acquisition order A → B → C)

```
                    A: flush_->mtx          (std::mutex)
                   / \
                  /   \
    B: memtable_mutex_  C: pending_recycle_mutex_
    (std::shared_mutex)    (std::mutex)
                  |
          D: sstable_metadata_mutex_
             (std::mutex)
```

**Rule**: always acquire in order. If a path needs A and B, it takes A first then B. The DAG has no cycles.

Separate from the above — acquired without nesting:
- `sync_->mtx` — WAL sync coordination
- `wal_` internal mutex — WAL buffer / file
- `manifest_` internal mutex — MANIFEST file

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
  6. DrainRecyclePending()
       Lock C → Lock B (in order)
```

Key point: the SSTable flush (step 6 in old code) has been moved to the **Flush worker**. The writer only freezes the memtable and notifies the worker — it never blocks on disk I/O.

### Read Path (Reader threads)

```
Lookup(key):
  1. read_ts = global_ts_.load()                  ← atomic
  2. SnapshotTracker::Acquire(read_ts)            ← internal mutex
  3. Lock B: shared_lock<shared_mutex>
       search active_memtable_                    ← read-only, concurrent with other readers
       search frozen_memtables_ (newest first)
     Unlock B
  4. Lock D: copy sstable_metadata_
     Unlock D
  5. For each SSTable (newest first):
       skip if source_table_id already scanned (from frozen memtables)
       skip via range filter (min_key / max_key)
       skip via bloom filter
       SSTable::LookupKey(filepath, key, read_ts) ← reads file, no engine lock needed
  6. SnapshotTracker::Release(read_ts)
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
      SSTable::WriteFromWalk(…)                    ← disk I/O
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

### Lock Acquisition Summary

| Path | Lock order |
|------|-----------|
| Insert (freeze) | B → A |
| Insert (recycle) | C → B |
| FlushWorkerLoop | A → B → (D, wal_, manifest_) → C → B |
| Lookup | B (shared) → D |
| RangeScan | B (shared) → D |
| Flush() | B → A → A (WaitForPendingFlushes) |
| WaitForPendingFlushes | A |
| DeferRecycle | C |
| DrainRecyclePending | C → B |
| Destructor | stop flush worker → stop sync worker → B (drain) |

### Invariants

1. **Single writer**: only the Writer thread inserts into `active_memtable_`. No internal B+-tree locking needed.
2. **Immutable frozen memtables**: once frozen, a memtable is never modified. Readers and the flush worker can access it safely without synchronization.
3. **Timestamp monotonicity**: `global_ts_` is a single atomic counter, strictly increasing. The write queue is FIFO → timestamps reflect enqueue order.
4. **WAL-before-memtable**: `wal_->Buffer()` always precedes `active_memtable_->Insert()`. On recovery, WAL replay restores lost memtable data.
5. **Checkpoint-after-flush**: the flush worker writes `WriteCheckpoint` only after `Manifest::AddSSTable` is fsync'd. After recovery, the SSTable is visible and the checkpoint guarantees WAL truncation is safe.

## Recovery Flow

1. Read MANIFEST → reconstruct SSTable catalog, set `sstable_seq_`
2. Read WAL → find last checkpoint → set `global_ts_` from checkpoint timestamp
3. Replay WAL entries after checkpoint → insert into memtable → flush to SSTable → Manifest::Add → WAL::Checkpoint

## MemTable Lifecycle & Recycling

```
Active MemTable (writable)
    │  IsFull() → Freeze
    ▼
Frozen MemTable (immutable, in frozen_memtables_)
    │  DoFlush() → Write SSTable → Manifest::Add → DeferRecycle()
    │
    ├─┬ Coexistence window ──────────────────┐
    │ │  Readers with read_ts < fence_ts      │
    │ │  still hit the frozen memtable        │
    │ │  (SSTable only has newest version)    │
    │ └───────────────────────────────────────┘
    │
    │  DrainRecyclePending():
    │  MinActiveTS() >= fence_ts
    │    → unlink from frozen_memtables_
    │    → release shared_ptr
    ▼
Recycled (destroyed)
```

- **Fence**: `global_ts_` captured at `DeferRecycle()` time
- **Gate**: `SnapshotTracker::MinActiveTS() >= fence_ts` — every active reader sees the SSTable's versions
- **Drain**: checked in `Insert()` (after WAL sync) and `WaitForPendingFlushes()`
