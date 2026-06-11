# API Reference

All public API in the `kvdb` namespace.

## `LSMTreeEngine`

```cpp
#include <kvdb/engine.hpp>

kvdb::LSMTreeEngine engine("./data", 4*1024*1024);
// Full form with custom block cache sizes:
// kvdb::LSMTreeEngine engine(data_dir, memtable_max_bytes, max_pending_flushes,
//                            kv_cache_shards, block_cache_shards, batch_increment_gap,
//                            block_cache_blocks, block_cache_meta, block_cache_bytes);
//
// block_cache_meta default: 512 (256 minimum recommended for LSM-tree workloads)

// Write (non-blocking, returns WAL seq for tracking persistence)
uint64_t seq = engine.Insert("key", "value");      // returns immediately (~1 us)
uint64_t synced = engine.SyncedSequence();         // last persisted seq (atomic)

// Delete (tombstone)
uint64_t seq = engine.Delete("key");

// Batch write (all-or-nothing atomic commit)
engine.StartBatch();                      // reserve 1M timestamps
engine.BatchInsert("k1", "v1");          // uses reserved batch_ts_
engine.BatchInsert("k2", "v2");
engine.BatchDelete("k3");                // tombstone with batch_ts_
bool ok = engine.CommitBatch();          // blocks until persisted (polls synced_seq)
// Non-blocking variant (used by server WriterLoop):
// engine.CommitBatchAsync();            // buffer commit sentinel + notify sync worker
// engine.CommitBatchFinalize();         // finish when synced_seq >= commit_seq
// or engine.AbortBatch();               // marks batch_ts as aborted, filters out reads

// Compare-And-Swap (CAS)

// Point lookup (MVCC snapshot)
std::string value;
bool found = engine.Lookup("key", value);

// Range scan with bounds
auto iter = engine.RangeScan(RangeBound::Inclusive("a"), RangeBound::Exclusive("z"));

// Prefix scan
auto iter = engine.PrefixScan("user:");

// Manual flush
engine.Flush();

// Wait for pending flushes and memtable recycling
engine.WaitForPendingFlushes();

// WAL management
engine.TrimWAL();       // discard WAL before last checkpoint
bool has = engine.HasWALData();

// Inspection
size_t count = engine.ActiveMemTableEntryCount();
size_t sst  = engine.SSTableCount();
auto meta   = engine.GetSSTableMetadata();
bool full   = engine.NeedsFlush();
auto levels = engine.LevelCounts();      // vector<size_t> — per-level SSTable counts
int done    = engine.ManualCompact(4, 0, true);  // threshold, from_level, cascade
// ManualCompact returns number of levels compacted, 0 if no level qualifies
// or threshold < 2 or another compaction is in progress (serialized via try_lock)
```

## `KeyValuePair`

```cpp
#include <kvdb/types.hpp>

struct KeyValuePair {
    std::string key;
    std::string value;
    uint64_t timestamp;     // MVCC version
    bool is_tombstone;       // true if this entry is a tombstone delete
};
```

## `RangeBound`

```cpp
#include <kvdb/types.hpp>

struct RangeBound {
    std::string key;
    bool inclusive = true;
    bool unbounded = false;

    static RangeBound Inclusive(const std::string& k);
    static RangeBound Exclusive(const std::string& k);
    static RangeBound Unbounded();
    bool IsUnbounded() const;
};
```

## `SSTable`

```cpp
#include <kvdb/sstable.hpp>

// Write entries to disk
std::vector<KeyValuePair> entries;
kvdb::SSTable::Write("path.sst", entries);

// Write from B+tree walk (dedup + export)
kvdb::SSTable::WriteFromWalk(filepath, walk, entry_count, cache, manifest_seq);

// Read all entries
auto read = kvdb::SSTable::ReadAll("path.sst");

// Read metadata only (bloom filter, key range, entry count, block index)
// Bulk-read I/O: reads header (24B) + filter area (218KB) in 2 file.read() calls.
// Caches bloom + offsets + first_key_buf. Returns cached data on subsequent calls.
auto meta = kvdb::SSTable::ReadMetadata("path.sst", cache, manifest_seq);
// meta.filepath, meta.entry_count, meta.file_size
// meta.manifest_seq     — monotonic sequence for cache keying
// meta.min_key, meta.max_key, meta.bloom
// meta.block_offsets, meta.block_first_key_buf  (flat buffer, not vector<string>)
// meta.min_key_len, meta.max_key_len
// meta.source_table_id, meta.level

// Point lookup (cache-aware, bloom+range filter, MVCC)
bool hit = kvdb::SSTable::LookupKey(filepath, key, read_ts, value_out, cache, manifest_seq);

// Compaction: merge input SSTables into output SSTables
kvdb::SSTable::Compact(inputs, output_dir, output_seq_start,
                        output_level, max_sstable_size, is_last_level,
                        range_lower, range_upper, outputs, garbage_files);
```

## `BlockReader` / `SSTableCache`

```cpp
#include <kvdb/block_reader.hpp>
#include <kvdb/block_cache.hpp>

// CachedHeavy — metadata stored in cache (no copies on get)
struct CachedHeavy {
    BloomFilter bloom;
    std::vector<uint64_t> block_offsets;
    std::string block_first_key_buf;
};

// BlockReader — pure virtual interface (shared_ptr, no copies)
class BlockReader {
    virtual std::shared_ptr<const CachedHeavy> GetHeavy(uint64_t seq) = 0;
    virtual void PutHeavy(uint64_t seq, BloomFilter bloom,
                          std::vector<uint64_t> offsets, std::string buf) = 0;
    virtual std::shared_ptr<const std::string> GetBlock(
        uint64_t seq, uint32_t block_idx) = 0;
    virtual void PutBlock(uint64_t seq, uint32_t block_idx,
                          std::string data) = 0;
    virtual void Invalidate(uint64_t seq) = 0;
};

// SSTableCache — uses FlatCache (flat-array LRU, open-addressing, intrusive LRU)
// Keys: uint64_t manifest_seq. Block keys: (seq << 32) | block_idx
SSTableCache cache(1024, 256, 64 * 1024 * 1024, 16);  // blocks, meta, bytes, shards
```

## `WAL`

```cpp
#include <kvdb/wal.hpp>

kvdb::WAL wal("./data/wal.log");

wal.Buffer("key", "value", timestamp);  // non-blocking append
wal.Sync();                              // fsync all buffered data
auto entries = wal.Recover(&checkpoint_ts);  // recover from last checkpoint
wal.WriteCheckpoint(global_ts);         // mark durability boundary
wal.TrimToLastCheckpoint();             // discard old entries
wal.Clear();                            // truncate everything
```

## `KVCache` / `FlatCache`

```cpp
#include <kvdb/kv_cache.hpp>
#include <kvdb/internal/flat_cache.hpp>

// KVCache: Internal LRU key-value cache for point lookups
// Write-through: populated during Insert() (non-blocking write)
// Tombstones erase cache entries
// Blobs (> 2KB) are not cached
// Default: 10K entries / 16MB

// FlatCache<K,V>: Reusable flat-array, open-addressing, intrusive-LRU
// template used by both SSTableCache and (in future) KVCache.
// No per-node heap allocations. Stores shared_ptr<const V>, returns
// shared_ptr on Get (no data copies). Configurable SizeBytes for byte-aware
// eviction. Tombstone-safe open addressing.
kvdb::internal::FlatCache<int, std::string> cache(1024, 64*1024*1024, 16);
cache.Put(42, std::make_shared<std::string>("value"));
auto v = cache.Get(42);  // shared_ptr<const std::string>
cache.Erase(42);
cache.EraseIf([](int k) { return k > 100; });  // batch erase
```
```

## `Manifest`

```cpp
#include <kvdb/manifest.hpp>

kvdb::Manifest manifest("./data/MANIFEST");

manifest.AddSSTable(seq, meta);
manifest.RemoveSSTable(seq);
manifest.Sync();                       // fsync
auto catalog = manifest.Recover();     // reconstruct SSTable catalog
```

## `SnapshotTracker`

```cpp
#include <kvdb/snp_tracker.hpp>

// Internal: tracks active reader timestamps for safe GC
// Acquire(read_ts) / Release(read_ts)
// MinActiveTS() → minimum active read_ts (UINT64_MAX if none)
// Used by DrainRecyclePending() and DrainFileGC() to gate resource release
```

## `BloomFilter`

```cpp
#include <kvdb/bloom.hpp>

kvdb::BloomFilter bloom(1000, 0.01); // 1000 entries, 1% false positive
bloom.Add("key");
bool maybe = bloom.MightContain("key");
size_t bits = bloom.BitCount();      // non-zero if initialized
```

## Range Scan

```cpp
// Full scan
auto iter = engine.RangeScan();

// Bounded: [start, end] inclusive
auto iter = engine.RangeScan(RangeBound::Inclusive("a"), RangeBound::Inclusive("z"));

// Bounded: (start, end) exclusive
auto iter = engine.RangeScan(RangeBound::Exclusive("a"), RangeBound::Exclusive("z"));

// Half-bounded
auto iter = engine.RangeScan(RangeBound::Inclusive("m"), RangeBound::Unbounded());

// Prefix scan
auto iter = engine.PrefixScan("user:");

while (iter.Valid()) {
    std::cout << iter.Key() << "=" << iter.Value() << "\n";
    iter.Next();
}
// Snapshot released when iter destroyed
```

## `Server` / `Client`

```cpp
#include <kvdb/server.hpp>
#include <kvdb/client.hpp>

// Server
kvdb::Server server(engine, 9000);
server.Start();
server.Stop();

// Client
kvdb::Client client;
client.Connect("127.0.0.1", 9000);
client.Write("key", "value");             // sync: blocks until persisted
client.Delete("key");                     // sync: blocks until persisted
client.WriteAsync("key", "value");        // async: returns after enqueue (~30 us)
client.DeleteAsync("key");                // async: returns after enqueue (~24 us)
std::string val;
bool found = client.Read("key", val);
std::vector<KeyValuePair> results;
client.RangeScan(RangeBound::Inclusive("a"), RangeBound::Unbounded(), results);
client.PrefixScan("prefix:", results);

// Batch write
client.StartBatch();
client.BatchPut("bk1", "bv1");
client.BatchPut("bk2", "bv2");
bool ok = client.CommitBatch();   // or client.AbortBatch();

// Compare-and-swap
bool swapped = client.CompareAndSwap("key", "expected", "desired");

// Compaction management
auto levels = client.LevelCounts();        // vector<size_t> — per-level SSTable counts
int done    = client.ManualCompact(4, 0, true);  // threshold, from_lvl, cascade

client.Disconnect();
```

## `Config` Constants

```cpp
#include <kvdb/config.hpp>

Config::kSSTableMagic               // 0x4B535354
Config::kSSTableVersion             // 6
Config::kSSTableBlockSize           // 4096
Config::kDefaultMemTableMaxBytes    // 4 MB
Config::kMaxKeyBytes                // 1024
Config::kMaxKeyValuePairBytes       // 4 MB
Config::kMaxWriteQueueBytes         // 16 MB
Config::kDefaultMaxPendingFlushes   // 2
Config::kDefaultDataDir             // "./kvdb_data"
Config::kCompressionSnappy          // 1
Config::kCompressionNone            // 0
Config::kDefaultKVMaxEntries        // 10000
Config::kDefaultKVMaxBytes          // 16 MB
Config::kDefaultBlockCacheBlocks    // 1024
Config::kDefaultBlockCacheMeta      // 512
Config::kDefaultBlockCacheBytes     // 64 MB
Config::kDefaultCompactionThreshold // 8
Config::kMaxLevel                   // 3
Config::kLevelBaseSSTableSize       // 4 MB
Config::kLevelSizeMultiplier        // 10
Config::kLargeValFlag               // 0x7FFF (value > page/2)
```

## `SSTableIterator` / `LevelIterator` / `RangeIterator`

```cpp
#include <kvdb/iterator.hpp>

// SSTableIterator — cache-aware or standalone
// Constructors accept optional block_offsets and block_first_key_buf
// for block-index-based SeekToKey (O(log N) block-level seek)
SSTableIterator(filepath);                                    // no cache (compaction)
SSTableIterator(filepath, BlockReader&, manifest_seq, populate,
                aborted, offsets, first_key_buf);             // cache-aware, indexed seek

// LevelIterator — chains non-overlapping SSTables within a level
LevelIterator(files);  // constructs 1-arg SSTableIterators internally

// RangeIterator — k-way merge over multiple source iterators
RangeIterator(sources, read_ts, guard, lower, upper);

// All derive from SourceIterator:
bool Valid() const;
const KeyValuePair& Current() const;  // .key, .value, .timestamp, .is_tombstone
void Next();
void SeekToKey(const std::string& key);
```
