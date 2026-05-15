#pragma once

#include "config.hpp"
#include "iterator.hpp"
#include "manifest.hpp"
#include "memtable.hpp"
#include "snp_tracker.hpp"
#include "sstable.hpp"
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

struct EngineSyncState;
struct FlushState;
struct CompactionState;

class LSMTreeEngine {
public:
    explicit LSMTreeEngine(const std::string& data_dir = Config::kDefaultDataDir,
                           size_t memtable_max_bytes = Config::kDefaultMemTableMaxBytes,
                           size_t max_pending_flushes = Config::kDefaultMaxPendingFlushes);

    ~LSMTreeEngine();

    LSMTreeEngine(const LSMTreeEngine&) = delete;
    LSMTreeEngine& operator=(const LSMTreeEngine&) = delete;

    void Insert(const std::string& key, const std::string& value);
    void Delete(const std::string& key);

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
    RangeIterator RangeScan(const std::string& start, const std::string& end) const;

private:
    void EnsureDataDirectoryExists();
    void RecoverFromWAL();
    void DoFlush(std::shared_ptr<MemTable> frozen_memtable);
    void DeferRecycle(std::shared_ptr<MemTable> frozen_memtable);
    void DrainRecyclePending();
    void FlushWorkerLoop();
    void CompactionWorkerLoop();
    void CompactLevel(int from_level, int top_level);

    struct PendingRecycle {
        std::shared_ptr<MemTable> memtable;
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

    std::unique_ptr<WAL> wal_;
    std::unique_ptr<Manifest> manifest_;

    mutable SnapshotTracker tracker_;

    std::unique_ptr<EngineSyncState> sync_;
    std::unique_ptr<FlushState> flush_;
    std::unique_ptr<CompactionState> compaction_;

    std::vector<PendingRecycle> pending_recycle_;
    std::mutex pending_recycle_mutex_;
};

} // namespace kvdb
