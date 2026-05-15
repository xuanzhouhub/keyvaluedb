#include "kvdb/engine.hpp"

#include "kvdb/bptree.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <unordered_set>

namespace kvdb {

struct kvdb::EngineSyncState {
    std::thread worker;
    std::mutex mtx;
    std::condition_variable cv;
    std::condition_variable ready_cv;
    size_t requested_seq = 0;
    size_t synced_seq = 0;
    std::atomic<bool> should_stop{false};
    WAL* wal = nullptr;
};

static void SyncWorkerLoop(kvdb::EngineSyncState* s) {
    while (!s->should_stop.load()) {
        {
            std::unique_lock<std::mutex> lock(s->mtx);
            s->ready_cv.wait_for(lock, std::chrono::microseconds(200), [s] {
                return s->should_stop.load() || s->requested_seq > s->synced_seq;
            });
        }

        size_t synced = s->wal->Sync();

        {
            std::lock_guard<std::mutex> lock(s->mtx);
            s->synced_seq = synced;
        }

        s->cv.notify_all();
    }

    size_t synced = s->wal->Sync();
    {
        std::lock_guard<std::mutex> lock(s->mtx);
        s->synced_seq = synced;
    }
    s->cv.notify_all();
}

LSMTreeEngine::LSMTreeEngine(const std::string& data_dir, size_t memtable_max_bytes, size_t max_pending_flushes)
    : data_dir_(data_dir)
    , memtable_max_bytes_(memtable_max_bytes)
    , max_pending_flushes_(max_pending_flushes)
    , active_memtable_(std::make_shared<MemTable>(next_table_id_.fetch_add(1), memtable_max_bytes)) {
    EnsureDataDirectoryExists();

    std::string manifest_path = data_dir_ + "/MANIFEST";
    manifest_ = std::make_unique<Manifest>(manifest_path);

    auto recovered_meta = manifest_->Recover();
    std::cerr << "[Recovery] MANIFEST: " << recovered_meta.size() << " SSTables" << std::endl;
    for (size_t i = 0; i < recovered_meta.size(); ++i)
        std::cerr << "  [" << i << "] " << recovered_meta[i].filepath
                  << " entries=" << recovered_meta[i].entry_count << std::endl;
    {
        std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
        sstable_metadata_ = std::move(recovered_meta);
    }
    sstable_seq_ = sstable_metadata_.size();

    std::string wal_path = data_dir_ + "/wal.log";
    wal_ = std::make_unique<WAL>(wal_path);

    RecoverFromWAL();

    sync_ = std::make_unique<EngineSyncState>();
    sync_->wal = wal_.get();
    sync_->worker = std::thread(SyncWorkerLoop, sync_.get());
}

LSMTreeEngine::~LSMTreeEngine() {
    if (sync_) {
        sync_->should_stop = true;
        if (sync_->worker.joinable()) {
            sync_->worker.join();
        }
    }

    try {
        wal_->Sync();
        for (auto& frozen : frozen_memtables_) {
            DoFlush(frozen);
        }
        frozen_memtables_.clear();
        if (active_memtable_ && active_memtable_->EntryCount() > 0) {
            auto frozen = active_memtable_;
            active_memtable_.reset();
            DoFlush(frozen);
            wal_->WriteCheckpoint(global_ts_.load());
            wal_->Sync();
        }
    } catch (...) {}
}

void LSMTreeEngine::RecoverFromWAL() {
    uint64_t checkpoint_ts = 0;
    auto recovered = wal_->Recover(&checkpoint_ts);
    if (checkpoint_ts > global_ts_.load()) {
        global_ts_ = checkpoint_ts;
    }
    if (recovered.empty()) {
        return;
    }

    for (const auto& kv : recovered) {
        active_memtable_->Insert(kv.key, kv.value, kv.timestamp);
        if (kv.timestamp > global_ts_.load()) {
            global_ts_ = kv.timestamp;
        }

        if (active_memtable_->IsFull()) {
            auto frozen = active_memtable_;
            frozen->Freeze();
            active_memtable_ = std::make_shared<MemTable>(next_table_id_.fetch_add(1), memtable_max_bytes_);
            DoFlush(frozen);
            wal_->WriteCheckpoint(global_ts_.load());
        }
    }

    if (active_memtable_->EntryCount() > 0) {
        auto frozen = active_memtable_;
        frozen->Freeze();
        active_memtable_ = std::make_shared<MemTable>(next_table_id_.fetch_add(1), memtable_max_bytes_);
        DoFlush(frozen);
    }

    wal_->WriteCheckpoint(global_ts_.load());
    wal_->Sync();
}

void LSMTreeEngine::Insert(const std::string& key, const std::string& value) {
    if (key.size() > Config::kMaxKeyBytes) {
        throw std::invalid_argument(
            "Key size (" + std::to_string(key.size())
            + " bytes) exceeds maximum allowed ("
            + std::to_string(Config::kMaxKeyBytes) + " bytes)");
    }
    size_t entry_size = key.size() + value.size() + Config::kMemTableEntryOverheadBytes;
    if (entry_size > Config::kMaxKeyValuePairBytes) {
        throw std::invalid_argument(
            "Key-value pair size (" + std::to_string(entry_size)
            + " bytes) exceeds maximum allowed ("
            + std::to_string(Config::kMaxKeyValuePairBytes) + " bytes)");
    }

    uint64_t ts = global_ts_.fetch_add(1) + 1;

    wal_->Buffer(key, value, ts);
    size_t my_seq = wal_->CurrentBatchSeq();

    {
        std::unique_lock<std::mutex> lock(sync_->mtx);
        sync_->requested_seq = my_seq;
        sync_->ready_cv.notify_one();
    }

    std::shared_ptr<MemTable> frozen;
    {
        std::unique_lock<std::shared_mutex> lock(memtable_mutex_);
        active_memtable_->Insert(key, value, ts);

        if (active_memtable_->IsFull()) {
            frozen = active_memtable_;
            frozen->Freeze();
            frozen_memtables_.push_back(frozen);
            active_memtable_ = std::make_shared<MemTable>(next_table_id_.fetch_add(1), memtable_max_bytes_);
        }
    }

    if (frozen) {
        DoFlush(frozen);
        wal_->WriteCheckpoint(global_ts_.load());
        DeferRecycle(frozen);
    }

    {
        std::unique_lock<std::mutex> lock(sync_->mtx);
        sync_->cv.wait(lock, [this, my_seq] {
            return sync_->synced_seq >= my_seq;
        });
    }

    DrainRecyclePending();
}

bool LSMTreeEngine::Lookup(const std::string& key, std::string& value_out) const {
    uint64_t read_ts = global_ts_.load();
    tracker_.Acquire(read_ts);

    struct Guard {
        SnapshotTracker& t;
        uint64_t ts;
        ~Guard() { t.Release(ts); }
    } guard{tracker_, read_ts};

    {
        std::shared_lock<std::shared_mutex> lock(memtable_mutex_);
        if (active_memtable_->Lookup(key, read_ts, value_out)) {
            return true;
        }
        for (auto it = frozen_memtables_.rbegin();
             it != frozen_memtables_.rend(); ++it) {
            if ((*it)->Lookup(key, read_ts, value_out)) {
                return true;
            }
        }
    }

    std::unordered_set<uint64_t> scanned_ids;
    {
        std::shared_lock<std::shared_mutex> lock(memtable_mutex_);
        for (const auto& m : frozen_memtables_) {
            scanned_ids.insert(m->Id());
        }
    }

    std::vector<SSTable::Metadata> metadata;
    {
        std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
        metadata = sstable_metadata_;
    }

    for (auto it = metadata.rbegin(); it != metadata.rend(); ++it) {
        if (scanned_ids.count(it->source_table_id)) continue;

        if (!it->min_key.empty() && key < it->min_key) continue;
        if (!it->max_key.empty() && key > it->max_key) continue;
        if (it->bloom.BitCount() > 0 && !it->bloom.MightContain(key)) continue;

        try {
            if (SSTable::LookupKey(it->filepath, key, read_ts, value_out)) return true;
        } catch (const std::exception&) {
            continue;
        }
    }

    return false;
}

bool LSMTreeEngine::NeedsFlush() const {
    std::shared_lock<std::shared_mutex> lock(memtable_mutex_);
    return active_memtable_->IsFull();
}

size_t LSMTreeEngine::ActiveMemTableEntryCount() const {
    std::shared_lock<std::shared_mutex> lock(memtable_mutex_);
    return active_memtable_->EntryCount();
}

size_t LSMTreeEngine::ActiveMemTableMemoryUsage() const {
    std::shared_lock<std::shared_mutex> lock(memtable_mutex_);
    return active_memtable_->ApproximateMemoryUsage();
}

void LSMTreeEngine::Flush() {
    std::shared_ptr<MemTable> frozen;
    {
        std::unique_lock<std::shared_mutex> lock(memtable_mutex_);
        if (active_memtable_->EntryCount() == 0) {
            return;
        }
        frozen = active_memtable_;
        frozen->Freeze();
        frozen_memtables_.push_back(frozen);
        active_memtable_ = std::make_shared<MemTable>(next_table_id_.fetch_add(1), memtable_max_bytes_);
    }

    DoFlush(frozen);
    wal_->WriteCheckpoint(global_ts_.load());
    DeferRecycle(frozen);

    DrainRecyclePending();
}

void LSMTreeEngine::WaitForPendingFlushes() {
    DrainRecyclePending();
}

size_t LSMTreeEngine::SSTableCount() const {
    std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
    return sstable_metadata_.size();
}

std::vector<SSTable::Metadata> LSMTreeEngine::GetSSTableMetadata() const {
    std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
    return sstable_metadata_;
}

bool LSMTreeEngine::HasWALData() const {
    return wal_->HasData();
}

void LSMTreeEngine::TrimWAL() {
    wal_->TrimToLastCheckpoint();
}

RangeIterator LSMTreeEngine::RangeScan() const {
    uint64_t read_ts = global_ts_.load();
    tracker_.Acquire(read_ts);

    std::shared_ptr<void> guard(new char, [this, read_ts](void* p) {
        delete static_cast<char*>(p);
        tracker_.Release(read_ts);
    });

    std::vector<std::unique_ptr<SourceIterator>> sources;

    {
        std::shared_lock<std::shared_mutex> lock(memtable_mutex_);
        if (active_memtable_->EntryCount() > 0)
            sources.push_back(std::make_unique<VectorIterator>(active_memtable_->ExportEntries()));
        for (const auto& m : frozen_memtables_) {
            if (m->EntryCount() > 0)
                sources.push_back(std::make_unique<MemTableSource>(m));
        }
    }

    {
        std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
        for (const auto& meta : sstable_metadata_) {
            try {
                auto iter = std::make_unique<SSTableIterator>(meta.filepath);
                if (iter->Valid()) sources.push_back(std::move(iter));
            } catch (...) {}
        }
    }

    return RangeIterator(std::move(sources), read_ts, guard);
}

void LSMTreeEngine::EnsureDataDirectoryExists() {
    std::filesystem::create_directories(data_dir_);
}

void LSMTreeEngine::DoFlush(std::shared_ptr<MemTable> frozen_memtable) {
    if (!frozen_memtable || frozen_memtable->EntryCount() == 0) {
        return;
    }

    uint64_t seq = sstable_seq_.fetch_add(1);
    std::ostringstream oss;
    oss << data_dir_ << "/sstable_" << seq << ".sst";
    std::string filepath = oss.str();

    size_t entry_count = frozen_memtable->EntryCount();
    auto walk = BPlusTree::MemTableWalk(frozen_memtable->GetTree());
    SSTable::WriteFromWalk(filepath, walk, entry_count);

    SSTable::Metadata meta;
    try {
        meta = SSTable::ReadMetadata(filepath);
    } catch (const std::exception& e) {
        std::cerr << "ReadMetadata failed: " << e.what() << std::endl;
        auto entries = SSTable::ReadAll(filepath);
        std::cerr << "ReadAll found " << entries.size() << " entries" << std::endl;
        for (size_t i = 0; i < std::min(entries.size(), size_t(10)); ++i)
            std::cerr << "  [" << i << "] " << entries[i].key << "=" << entries[i].value << " ts=" << entries[i].timestamp << std::endl;
        throw;
    }
    meta.source_table_id = frozen_memtable->Id();
    meta.filepath = filepath;

    {
        std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
        sstable_metadata_.push_back(meta);
    }

    manifest_->AddSSTable(seq, meta);
}

void LSMTreeEngine::DeferRecycle(std::shared_ptr<MemTable> frozen_memtable) {
    uint64_t fence_ts = global_ts_.load();
    std::lock_guard<std::mutex> lock(pending_recycle_mutex_);
    pending_recycle_.push_back({std::move(frozen_memtable), fence_ts});
}

void LSMTreeEngine::DrainRecyclePending() {
    std::vector<PendingRecycle> ready;
    {
        std::lock_guard<std::mutex> lock(pending_recycle_mutex_);
        auto& pr = pending_recycle_;
        pr.erase(
            std::remove_if(pr.begin(), pr.end(), [&](const PendingRecycle& p) {
                if (tracker_.MinActiveTS() >= p.fence_ts) {
                    ready.push_back(std::move(p));
                    return true;
                }
                return false;
            }),
            pr.end());
    }

    for (auto& entry : ready) {
        {
            std::unique_lock<std::shared_mutex> lock(memtable_mutex_);
            auto it = std::find(frozen_memtables_.begin(),
                                frozen_memtables_.end(), entry.memtable);
            if (it != frozen_memtables_.end())
                frozen_memtables_.erase(it);
        }
    }
}

} // namespace kvdb
