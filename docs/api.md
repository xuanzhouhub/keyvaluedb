# API Reference

All public API in the `kvdb` namespace.

## `LSMTreeEngine`

```cpp
#include <kvdb/engine.hpp>

kvdb::LSMTreeEngine engine("./data", 4*1024*1024);

// Write (blocks until durable)
engine.Insert("key", "value");

// Point lookup (MVCC snapshot)
std::string value;
bool found = engine.Lookup("key", value);

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
```

## `KeyValuePair`

```cpp
#include <kvdb/types.hpp>

struct KeyValuePair {
    std::string key;
    std::string value;
    uint64_t timestamp;  // MVCC version
};
```

## `SSTable`

```cpp
#include <kvdb/sstable.hpp>

// Write entries to disk
std::vector<KeyValuePair> entries;
kvdb::SSTable::Write("path.sst", entries);

// Read all entries
auto read = kvdb::SSTable::ReadAll("path.sst");

// Read metadata only (bloom filter, key range, entry count)
auto meta = kvdb::SSTable::ReadMetadata("path.sst");
// meta.filepath, meta.entry_count, meta.file_size
// meta.min_key, meta.max_key, meta.bloom, meta.min_key_len, meta.max_key_len
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
client.Write("key", "value");
std::string val;
bool found = client.Read("key", val);
client.Disconnect();
```

## `BloomFilter`

```cpp
#include <kvdb/bloom.hpp>

kvdb::BloomFilter bloom(1000, 0.01); // 1000 entries, 1% false positive
bloom.Add("key");
bool maybe = bloom.MightContain("key");
```

## Range Scan

```cpp
auto iter = engine.RangeScan();
while (iter.Valid()) {
    std::cout << iter.Key() << "=" << iter.Value() << "\n";
    iter.Next();
}
// Snapshot released when iter destroyed
```

## `SnapshotTracker`

```cpp
#include <kvdb/snp_tracker.hpp>
// Internal: tracks active reader timestamps for safe GC
// Acquire(read_ts) / Release(read_ts) / MinActiveTS()
```

## `Config` Constants

```cpp
#include <kvdb/config.hpp>

Config::kSSTableMagic          // 0x4B535354
Config::kSSTableVersion        // 4
Config::kSSTableBlockSize      // 4096
Config::kDefaultMemTableMaxBytes  // 4 MB
Config::kMaxKeyBytes           // 1024
Config::kMaxKeyValuePairBytes  // 4 MB
Config::kMaxWriteQueueBytes    // 16 MB
Config::kDefaultMaxPendingFlushes // 2
Config::kDefaultDataDir        // "./kvdb_data"
```
