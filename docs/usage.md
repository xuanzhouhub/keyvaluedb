# KVDB Usage Guide

## Setup

### Build

```powershell
# Requirements: CMake 3.16+, MSVC 2019+ / GCC 8+ / Clang 7+
cmake -B build -S .
cmake --build build --config Release
```

### Verify

```powershell
cd build
ctest --output-on-failure          # 10 suites, all should pass
```

---

## Running the Server

The server wraps a single `LSMTreeEngine` instance and exposes it over TCP.

```cpp
#include <kvdb/engine.hpp>
#include <kvdb/server.hpp>

kvdb::LSMTreeEngine engine("./kvdb_data");
kvdb::Server server(engine, 9000);
server.Start();

// ... use clients ...

server.Stop();
```

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `data_dir` | `"./kvdb_data"` | Directory for WAL, MANIFEST, SSTables |
| `port` | any | TCP port (e.g. 9000) |
| `memtable_max_bytes` | 4 MB | Memtable size before freeze+flush |
| `max_pending_flushes` | 2 | Max concurrent flushes |
| `kv_cache_shards` | 16 | KV cache shard count |
| `block_cache_shards` | 16 | SSTable block cache shard count |

---

## Client Operations

### Connect

```cpp
#include <kvdb/client.hpp>

kvdb::Client client;
client.Connect("127.0.0.1", 9000);
// ... operations ...
client.Disconnect();
```

### Write (sync — blocks until persisted)

```cpp
client.Write("key", "value");        // returns true on success
client.Delete("key");                 // tombstone delete
```

### Write (async — returns after enqueue, no persistence wait)

```cpp
client.WriteAsync("key", "value");    // ~30 µs per call
client.DeleteAsync("key");            // ~24 µs per call
```

### Point Read

```cpp
std::string value;
if (client.Read("key", value)) {
    // value found
} else {
    // not found or tombstone
}
```

### Range Scan

Scans return results into a `std::vector<KeyValuePair>`. Iterate through them to access key, value, timestamp, and tombstone status:

```cpp
std::vector<kvdb::KeyValuePair> results;

// Bounded: [start, end] inclusive
client.RangeScan(kvdb::RangeBound::Inclusive("a"),
                 kvdb::RangeBound::Inclusive("z"), results);

// Bounded: (start, end) exclusive
client.RangeScan(kvdb::RangeBound::Exclusive("a"),
                 kvdb::RangeBound::Exclusive("z"), results);

// Half-bounded: [m, ∞)
client.RangeScan(kvdb::RangeBound::Inclusive("m"),
                 kvdb::RangeBound::Unbounded(), results);

// Full scan (everything)
client.RangeScan(kvdb::RangeBound::Unbounded(),
                 kvdb::RangeBound::Unbounded(), results);

// Iterate results
for (auto& kv : results) {
    if (kv.is_tombstone) {
        std::cout << kv.key << " (deleted at ts=" << kv.timestamp << ")\n";
    } else {
        std::cout << kv.key << " = " << kv.value
                  << " (ts=" << kv.timestamp << ")\n";
    }
}
```

### Prefix Scan

Scans all keys with a given prefix. Returns results same as RangeScan:

```cpp
std::vector<kvdb::KeyValuePair> results;
client.PrefixScan("user:", results);
for (auto& kv : results) {
    std::cout << kv.key << " = " << kv.value << "\n";
}
```

### Batch Write (atomic all-or-nothing)

A batch groups multiple writes into a single atomic transaction. Within a batch,
writes use a reserved timestamp range — no other writes can see partial batch
results. When committed, all entries become visible at once. On abort, none are.

```cpp
// Step 1: begin a batch
if (!client.StartBatch()) {
    std::cerr << "another batch is already in progress\n";
    return;
}

// Step 2: buffer writes (these are fast — no persistence wait)
client.BatchPut("account_A", "500");
client.BatchPut("account_B", "700");
client.BatchDelete("account_C");        // tombstone, removed atomically

// Step 3: commit — blocks until WAL fsync finishes
bool ok = client.CommitBatch();

// Or abort to discard everything
// client.AbortBatch();

if (ok) {
    std::cout << "batch committed, all changes visible\n";
} else {
    std::cerr << "commit failed\n";
}
```

**Important**: only one batch can be in progress at a time across all clients.
`StartBatch()` returns false if another batch is active. Concurrency note:
- While a batch is buffering (`BatchPut`/`BatchDelete`), normal writes
  (`Write`/`Delete`) continue to work independently
- Batch entries are invisible to readers until `CommitBatch` finishes
- If `CommitBatch` fails or the server crashes before fsync, no batch
  entries are visible on recovery

### Batch Abort

```cpp
// Abort an active batch — discard all buffered writes
client.StartBatch();
client.BatchPut("k1", "v1");
client.BatchPut("k2", "v2");
client.AbortBatch();  // k1, k2 are discarded and will never be visible

// Verify abort filtered
std::string v;
assert(!client.Read("k1", v));  // not found
```

```cpp
// Engine-direct abort
engine.StartBatch();
engine.BatchInsert("key", "val");
engine.AbortBatch();
// Aborted entries are filtered at every read layer:
//   - MemTable point lookup
//   - SSTable point lookup (cold cache + warm cache)
//   - Range scan (memtable + SSTable)
//   - Compaction merge
```

**Abort durability:** the abort is persisted in three places:

| Layer | When written | Survives |
|-------|-------------|----------|
| WAL | During `AbortBatch()` | Survives crash, but removed by `TrimWAL` after checkpoint |
| MANIFEST | During `AbortBatch()` | Independent of WAL — survives `TrimWAL` and all checkpoints |
| SSTable file | Next flush/compaction | Permanently embedded in file metadata |

On recovery, MANIFEST abort records are replayed into every SSTable's in-memory metadata. WAL abort sentinels prevent aborted entries from being re-inserted during WAL replay.

**MANIFEST compact:** on every startup, the MANIFEST file is compacted — dead SSTable records and stale abort records (whose pre-abort files have all been compacted away) are dropped. This prevents the MANIFEST from growing without bound.

**Crash abort:** if the server crashes mid-batch (no explicit `AbortBatch`), recovery automatically aborts the incomplete batch — entries are marked invisible and filtered on all subsequent reads.

### Compare-And-Swap

```cpp
bool swapped = client.CompareAndSwap("key", "expected_value", "new_value");
// true  → key had expected_value, now set to new_value
// false → key didn't match, no change
```

### Compaction Management

```cpp
// Inspect per-level SSTable distribution
auto levels = client.LevelCounts();
// e.g. {12, 3, 0, 0, 0, 0, 0, 0} → 12 L0, 3 L1, rest empty

// Trigger manual compaction (threshold 4, cascade on)
int done = client.ManualCompact(4, 0, true);
// Returns number of levels compacted, or 0 if:
//  - no level has ≥ 4 SSTables
//  - threshold < 2
//  - another compaction is already running
```

---

## Using the Engine Directly (no server)

```cpp
#include <kvdb/engine.hpp>

kvdb::LSMTreeEngine engine("./data");

// Writes
uint64_t seq1 = engine.Insert("key1", "value1");
uint64_t seq2 = engine.Delete("key2");

// Read
std::string value;
bool found = engine.Lookup("key1", value);

// Range scan via iterator (MVCC snapshot — concurrent writes invisible)
auto iter = engine.RangeScan(
    kvdb::RangeBound::Inclusive("a"),
    kvdb::RangeBound::Exclusive("z"));
while (iter.Valid()) {
    std::cout << iter.Key() << " = " << iter.Value()
              << " (ts=" << iter.Timestamp()
              << ", tombstone=" << iter.IsTombstone() << ")\n";
    iter.Next();
}
// Iterator releases its snapshot when destroyed (goes out of scope)

// You can also walk entry-by-entry with more control:
auto iter2 = engine.RangeScan();
while (iter2.Valid()) {
    auto& kv = iter2.CurrentPair();   // returns KeyValuePair reference
    // ... process kv.key, kv.value, kv.timestamp, kv.is_tombstone ...
    iter2.Next();
}

// Flush and wait
engine.Flush();
engine.WaitForPendingFlushes();

// Compaction
auto levels = engine.LevelCounts();
int done = engine.ManualCompact(4, 0, true);

// Inspection
size_t entries = engine.ActiveMemTableEntryCount();
size_t sstables = engine.SSTableCount();
auto metadata  = engine.GetSSTableMetadata();
```

---

## Configuration Quick Reference

| Constant | Value | Description |
|----------|-------|-------------|
| `kDefaultMemTableMaxBytes` | 4 MB | Memtable flush threshold |
| `kMaxKeyBytes` | 1024 | Max key size |
| `kMaxKeyValuePairBytes` | 4 MB | Max KV pair size |
| `kMaxWriteQueueBytes` | 16 MB | Server write queue backpressure |
| `kDefaultCompactionThreshold` | 8 | SSTables per level to trigger compaction |
| `kMaxLevel` | 7 | Maximum compaction level |
| `kLevelBaseSSTableSize` | 4 MB | Base SSTable size for level sizing |
| `kLevelSizeMultiplier` | 10× | Per-level size multiplier |
| `kDefaultKVMaxEntries` | 10000 | KV cache capacity |
| `kDefaultKVMaxBytes` | 16 MB | KV cache byte limit |
| `kDefaultBlockCacheBlocks` | 1024 | SSTable block cache capacity |
| `kDefaultBlockCacheBytes` | 64 MB | Block cache byte limit |
| `kWALMinSyncBytes` | 4 MB | WAL buffer before forced sync |
| `kWALIdleSyncUs` | 200 µs | WAL idle timeout before sync |

---

## Performance Tips

- **High throughput writes**: use async writes (`WriteAsync`) with multiple concurrent clients. Sync writes pay a per-write fsync cost (~0.86 ms on Windows).
- **Read-heavy workloads**: the KV cache + SSTable block cache provide multi-level cacheing. Warm caches deliver ~5 µs point lookups.
- **Range scans**: materialize memtable data upfront (automatic). Block-index binary search enables O(log N) SeekToKey. Iteration ~7–9M rows/s.
- **Compaction**: the background worker fires every 2s when any level reaches 8 SSTables. Use `ManualCompact` with a lower threshold (e.g. 4) to proactively reduce read amplification.
- **Memory**: WAL buffer (4 MB) + memtable (4 MB) + KV cache (16 MB) + block cache (64 MB) ≈ 88 MB typical. Adjust via constructor parameters.

---

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Write returns false | Server queue full (backpressure). Use async writes or add more clients to drain. |
| Read returns false for known key | Key may be tombstoned. Check if `Delete` was called. |
| Compaction never fires | Not enough SSTables at any level (need ≥ 8). Use smaller memtable or more data, or call `ManualCompact` with lower threshold. |
| `ManualCompact` returns 0 | Threshold too low (< 2), no level qualifies, or another compaction is running (try again). |
| Crash on startup | Corrupt WAL or MANIFEST. Delete `./kvdb_data/` and restart (data loss). |
| Slow range scan | Data entirely in SSTables (disk I/O). Use smaller scan range or wait for compaction to consolidate. |
| Port already in use | Kill the old server process or use a different port. |
