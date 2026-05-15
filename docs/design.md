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
│             → Flush to SSTable  │
│             → Manifest::Add     │
│             → WAL::Checkpoint   │
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

- **Writes**: serialized by single writer thread (server write queue)
- **Reads**: concurrent via MVCC snapshot isolation
- **MemTable**: `std::shared_mutex` — writes `unique_lock`, reads `shared_lock`
- **SSTable metadata**: `std::mutex`
- **Timestamps**: `std::atomic<uint64_t> global_ts_` monotonic counter

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
