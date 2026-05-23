#pragma once

#include "config.hpp"
#include "iterator.hpp"
#include "manifest.hpp"
#include "memtable.hpp"
#include "snp_tracker.hpp"
#include "sstable.hpp"
#include "types.hpp"
#include "wal.hpp"

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <vector>

namespace kvdb {

class KVCache;
class BlockReader;
struct EngineSyncState;
struct FlushState;
struct CompactionState;

class LSMTreeEngine {
public:
    explicit LSMTreeEngine(const std::string& data_dir = Config::kDefaultDataDir,
                           size_t memtable_max_bytes = Config::kDefaultMemTableMaxBytes,
                           size_t max_pending_flushes = Config::kDefaultMaxPendingFlushes,
                           size_t kv_cache_shards = Config::kDefaultKVCacheShards,
                           size_t block_cache_shards = Config::kDefaultBlockCacheShards,
                           uint64_t batch_increment_gap = Config::kDefaultBatchIncrementGap);

    ~LSMTreeEngine();

    LSMTreeEngine(const LSMTreeEngine&) = delete;
    LSMTreeEngine& operator=(const LSMTreeEngine&) = delete;

    void Insert(const std::string& key, const std::string& value);
    void Delete(const std::string& key);

    bool StartBatch();
    bool CommitBatch();
    void BatchInsert(const std::string& key, const std::string& value);
    void BatchDelete(const std::string& key);
    uint64_t BatchGap() const { return batch_increment_gap_; }

    bool Lookup(const std::string& key, std::string& value_out) const;

    bool NeedsFlush() const;

    void Flush();

    void WaitForPendingFlushes();

    size_t ActiveMemTableEntryCount() const;

    size_t ActiveMemTableMemoryUsage() const;

    size_t SSTableCount() const;

    std::vector<SSTable::Metadata> GetSSTableMetadata() const;

    bool HasWALData() const;

    void TrimWAL();

    RangeIterator RangeScan() const;
    RangeIterator RangeScan(const RangeBound& lower, const RangeBound& upper) const;
    RangeIterator PrefixScan(const std::string& prefix) const;

private:
    void EnsureDataDirectoryExists();
    void RecoverFromWAL();
    void DoFlush(std::shared_ptr<MemTable> frozen_memtable);
    void DeferRecycle(std::shared_ptr<MemTable> frozen_memtable);
    void DrainRecyclePending();
    void DeferFileGC(const std::string& filepath, uint64_t seq,
                     uint64_t fence_ts);
    void DrainFileGC();
    void FlushWorkerLoop();
    void CompactionWorkerLoop();
    void CompactLevel(int from_level, int top_level);

    struct PendingRecycle {
        std::shared_ptr<MemTable> memtable;
        uint64_t fence_ts;
    };

    struct PendingFileGC {
        std::string filepath;
        uint64_t manifest_seq;
        uint64_t fence_ts;
    };

    std::string data_dir_;
    size_t memtable_max_bytes_;
    size_t max_pending_flushes_;

    std::shared_ptr<MemTable> active_memtable_;
    std::vector<std::shared_ptr<MemTable>> frozen_memtables_;
    mutable std::shared_mutex memtable_mutex_;

    std::atomic<uint64_t> next_table_id_{0};
    std::vector<SSTable::Metadata> sstable_metadata_;
    mutable std::mutex sstable_metadata_mutex_;

    std::atomic<uint64_t> sstable_seq_{0};
    std::atomic<uint64_t> global_ts_{0};

    std::mutex batch_mutex_;
    std::condition_variable batch_cv_;
    uint64_t batch_increment_gap_ = Config::kDefaultBatchIncrementGap;
    bool batch_in_progress_ = false;
    uint64_t batch_ts_ = 0;

    std::unique_ptr<WAL> wal_;
    std::unique_ptr<Manifest> manifest_;

    mutable SnapshotTracker tracker_;

    std::unique_ptr<EngineSyncState> sync_;
    std::unique_ptr<FlushState> flush_;
    std::unique_ptr<CompactionState> compaction_;
    mutable std::unique_ptr<KVCache> kv_cache_;
    std::unique_ptr<BlockReader> sst_cache_;

    std::vector<PendingRecycle> pending_recycle_;
    std::mutex pending_recycle_mutex_;

    std::vector<PendingFileGC> pending_gc_;
    mutable std::mutex pending_gc_mutex_;
};

} // namespace kvdb
