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

> **When to use:** data that must be durable before the call returns — configuration updates, critical counters.
> **When to avoid:** high-throughput ingestion. Each sync write pays a per-call fsync (~0.86 ms on Windows). Use async writes + periodic checkpoint for bulk loads.
> **Note:** `Delete` writes a tombstone, not a physical removal. Space is reclaimed later by compaction.

### Write (async — returns after enqueue, no persistence wait)

```cpp
client.WriteAsync("key", "value");    // ~30 µs per call
client.DeleteAsync("key");            // ~24 µs per call
```

> **When to use:** bulk data loading, high-throughput logging, event streams. Writes return fast and are flushed lazily by the WAL sync worker.
> **When to avoid:** data that must survive a crash immediately. Async writes may be lost if the server crashes before the next WAL sync (~200 µs idle timeout or 4 MB buffer threshold).
> **Throughput tip:** saturate with multiple concurrent clients. A single client is bottlenecked by TCP round-trips; 8-16 clients can reach ~180K async writes/s.

### Point Read

```cpp
std::string value;
if (client.Read("key", value)) {
    // value found
} else {
    // not found or tombstone
}
```

> **Performance:** warm reads ~5 µs (KV cache hit), cold reads ~500 µs (disk). Keys larger than 2 KB bypass the KV cache. Tombstoned keys return false — the deletion is visible immediately.

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

> **Materialization cost:** large scans (100K+ entries) spend ~1ms copying memtable leaf pages upfront. Data already in SSTables has no copy cost.
> **Memory:** the full result set is collected into a `vector`. For unbounded or very large scans, use the engine-direct iterator to stream entry-by-entry without buffering.
> **Tombstones:** deleted keys appear with `is_tombstone = true`. The tombstone itself counts as a result — filter it out or process it depending on your logic.

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

> **Primary use case:** bulk loading of data, not general-purpose transactions. A batch reserves 1M timestamps from the global counter — these are wasted if the batch is small. Aim for thousands of entries per batch.
> **Restrictions:** only one batch can be in progress at a time across all clients. `StartBatch()` returns false if another batch is active. Normal writes (sync/async) continue to work independently while a batch is buffering.
> **Visibility:** batch entries are invisible to ALL readers until `CommitBatch()` finishes. After commit, they appear atomically. If the server crashes before commit, recovery auto-aborts the batch — no partial batch data ever leaks.
> **Performance:** `BatchPut` / `BatchDelete` are ~1 µs each (in-memory only). `CommitBatch` blocks until WAL fsync (~1 ms). For maximum throughput, fill the batch close to the WAL's 4 MB sync threshold.

### Batch Abort

```cpp
client.StartBatch();
client.BatchPut("k1", "v1");
client.BatchPut("k2", "v2");
client.AbortBatch();  // k1, k2 are discarded and will never be visible
```

> **Use sparingly.** Abort writes a WAL record + MANIFEST record + updates every SSTable's in-memory metadata. Each abort adds a persistent timestamp that is checked on every subsequent read. Frequent aborts accumulate overhead.
> **Alternatives:** if you only need to discard writes on failure, let the batch crash without committing — recovery auto-aborts incomplete batches at no runtime cost.
> **What abort does NOT do:** it does not physically delete data from existing SSTable files. The aborted timestamp is stored and entries are filtered at read time. The data is removed only when all pre-abort SSTables are compacted away.

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

> **Cost:** CAS requires a point read + conditional write, both serialized through the writer queue. It's the slowest single-key operation (~2-5 ms). Use only when atomic check-and-set semantics are required; prefer `WriteAsync` for simple updates.
> **Concurrency:** CAS is synchronous — the writer blocks deferred requests during the lookup. Concurrent CAS on the same key are serialized. The deferred queue is capped at 64 entries; exceeding this may cause additional CAS attempts to fail.

### Compaction Management

```cpp
// Inspect per-level SSTable distribution
auto levels = client.LevelCounts();
// e.g. {12, 3, 0, 0, 0, 0, 0, 0} → 12 L0, 3 L1, rest empty

// Trigger manual compaction (threshold 4, cascade on)
int done = client.ManualCompact(4, 0, true);
```

> **When to compact manually:** after bulk-loading with many small flushes, before a large range scan, or when read latency is high due to too many L0 files.
> **Threshold guidance:** 2-4 for aggressive compaction (fewer files per level = faster reads, more write I/O). 8+ matches the background worker's default — use ManualCompact only to force it sooner than the 2s polling interval.
> **Returns 0 if:** threshold < 2, no level qualifies, or another compaction is already running (serialized by try-lock). Wait and retry.
> **Background worker:** still runs independently at 2s intervals with threshold 8. Manual compaction does not disable it.

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

- **Write throughput**: async writes (`WriteAsync`) scale with concurrent clients — use 8-16 to saturate the writer queue. Sync writes are fsync-bound (~500-1000/s per client). Batch writes for bulk loads: fill near the WAL 4 MB threshold before committing.
- **Read throughput**: point reads scale near-linearly with threads (lock-free B+-tree). Warm KV cache delivers ~5 µs. Cold reads hit disk (~500 µs).
- **Range scans**: large scans materialize memtable data upfront (~1 ms per 50K entries). Use bounded ranges, not full scans, when possible. For streaming large results, use engine-direct `RangeIterator` (no client-side buffering).
- **Compaction tuning**: the background worker at threshold 8 keeps read amplification low. For read-heavy workloads, lower the threshold to 4 with `ManualCompact` after bulk inserts. Compaction is I/O-intensive — avoid triggering it during peak write traffic.
- **Memory budget**: ~88 MB default (WAL 4 MB + memtable 4 MB + KV cache 16 MB + block cache 64 MB). Reduce `kDefaultBlockCacheBytes` for memory-constrained environments. The memtable size directly controls flush frequency — larger = fewer flushes, more memory.
- **Value sizes**: values ≤ 2 KB are KV-cache-friendly (fast reads). Values > 2 KB (half a page) are stored as external blobs — still fast but skip the KV cache. Max single KV pair is 4 MB.
- **Key design**: short, ordered keys leverage the B+-tree and block-index binary search. Keys > 1 KB are rejected. Prefix scans are efficient for hierarchical key spaces (e.g. `user:`, `log:`).

---

## Troubleshooting

| Symptom | Check |
|---------|-------|
| Write returns false | Server queue full (backpressure). Use async writes or add more clients to drain. |
| Read returns false for known key | Key may be tombstoned. Check if `Delete` was called. |
| Batch entries visible after abort | The abort timestamp may not have propagated to all SSTable caches. Call `Flush()` to force SSTable file metadata update, or restart the server. |
| Compaction never fires | Not enough SSTables at any level (need ≥ 8). Use smaller memtable or more data, or call `ManualCompact` with lower threshold. |
| `ManualCompact` returns 0 | Threshold too low (< 2), no level qualifies, or another compaction is running (try again). |
| Crash on startup | Corrupt WAL or MANIFEST. Delete data directory and restart (data loss). |
| Slow range scan | Data entirely in SSTables (disk I/O). Use smaller scan range or wait for compaction to consolidate. |
| Memory grows over time | Check `LevelCounts()` — many L0 SSTables accumulate. Compact or reduce flush frequency (larger memtable). |
| Port already in use | Kill the old server process or use a different port. |
| High CPU with no clients | Background compaction worker or flush worker may be active. Check `LevelCounts()` — if levels are noisy, compaction is ongoing. |
