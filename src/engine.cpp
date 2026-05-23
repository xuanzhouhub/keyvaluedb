#include "kvdb/engine.hpp"

#include "kvdb/block_cache.hpp"
#include "kvdb/bptree.hpp"
#include "kvdb/kv_cache.hpp"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
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

struct kvdb::FlushState {
    std::thread worker;
    std::mutex mtx;
    std::condition_variable cv;
    std::condition_variable done_cv;
    std::atomic<bool> should_stop{false};
    std::atomic<size_t> pending{0};
    LSMTreeEngine* engine = nullptr;
};

struct kvdb::CompactionState {
    std::thread worker;
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> should_stop{false};
    LSMTreeEngine* engine = nullptr;
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

void LSMTreeEngine::FlushWorkerLoop() {
    while (!flush_->should_stop.load()) {
        std::shared_ptr<MemTable> to_flush;
        {
            std::unique_lock<std::mutex> lock(flush_->mtx);
            flush_->cv.wait(lock, [this] {
                return flush_->should_stop.load() || flush_->pending.load() > 0;
            });
            if (flush_->should_stop.load() && flush_->pending.load() == 0)
                break;
        }

        {
            std::unique_lock<std::shared_mutex> lock(memtable_mutex_);
            if (!frozen_memtables_.empty()) {
                to_flush = frozen_memtables_.front();
                frozen_memtables_.erase(frozen_memtables_.begin());
            }
        }

        if (to_flush && to_flush->EntryCount() > 0) {
            try {
                DoFlush(to_flush);
                wal_->WriteCheckpoint(global_ts_.load());
                DeferRecycle(std::move(to_flush));
                DrainRecyclePending();
            } catch (...) {}
        }
        {
            std::lock_guard<std::mutex> lock(flush_->mtx);
            if (flush_->pending > 0) flush_->pending--;
        }
        flush_->done_cv.notify_all();
    }
}

void LSMTreeEngine::CompactionWorkerLoop() {
    while (!compaction_->should_stop.load()) {
        {
            std::unique_lock<std::mutex> lock(compaction_->mtx);
            compaction_->cv.wait_for(lock, std::chrono::seconds(2), [this] {
                return compaction_->should_stop.load();
            });
        }
        if (compaction_->should_stop.load()) break;

        std::vector<SSTable::Metadata> snapshot;
        {
            std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
            snapshot = sstable_metadata_;
        }

        std::vector<size_t> counts(Config::kMaxLevel + 1, 0);
        for (auto& m : snapshot) {
            int lvl = m.level;
            if (lvl >= 0 && lvl <= static_cast<int>(Config::kMaxLevel))
                counts[static_cast<size_t>(lvl)]++;
        }

        int trigger = -1;
        for (int lvl = 0; lvl <= static_cast<int>(Config::kMaxLevel); ++lvl) {
            if (counts[static_cast<size_t>(lvl)] >= Config::kDefaultCompactionThreshold) {
                trigger = lvl;
                break;
            }
        }
        if (trigger < 0) { DrainFileGC(); continue; }

        int top = trigger;
        for (int lvl = trigger + 1; lvl <= static_cast<int>(Config::kMaxLevel); ++lvl) {
            if (counts[static_cast<size_t>(lvl)] >= Config::kDefaultCompactionThreshold)
                top = lvl;
            else
                break;
        }

        try {
            CompactLevel(trigger, top);
        } catch (...) {}
        DrainFileGC();
    }
}

LSMTreeEngine::LSMTreeEngine(const std::string& data_dir, size_t memtable_max_bytes, size_t max_pending_flushes, size_t kv_cache_shards, size_t block_cache_shards, uint64_t batch_increment_gap)
    : data_dir_(data_dir)
    , memtable_max_bytes_(memtable_max_bytes)
    , max_pending_flushes_(max_pending_flushes)
    , batch_increment_gap_(batch_increment_gap)
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

    {
        std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
        for (auto& meta : sstable_metadata_) {
            if (meta.min_key.empty() || meta.max_key.empty()) {
                try {
                    auto full = SSTable::ReadMetadata(meta.filepath, sst_cache_.get(),
                                                       meta.manifest_seq);
                    meta.min_key = std::move(full.min_key);
                    meta.max_key = std::move(full.max_key);
                    meta.bloom = std::move(full.bloom);
                    meta.block_offsets = std::move(full.block_offsets);
                    meta.block_first_keys = std::move(full.block_first_keys);
                } catch (...) {}
            }
        }
    }

    std::string wal_path = data_dir_ + "/wal.log";
    wal_ = std::make_unique<WAL>(wal_path);

    RecoverFromWAL();

    sync_ = std::make_unique<EngineSyncState>();
    sync_->wal = wal_.get();
    sync_->worker = std::thread(SyncWorkerLoop, sync_.get());

    flush_ = std::make_unique<FlushState>();
    flush_->engine = this;
    flush_->worker = std::thread([this]() { this->FlushWorkerLoop(); });

    compaction_ = std::make_unique<CompactionState>();
    compaction_->engine = this;
    compaction_->worker = std::thread([this]() { this->CompactionWorkerLoop(); });

    kv_cache_ = std::make_unique<KVCache>(Config::kDefaultKVMaxEntries,
                                            Config::kDefaultKVMaxBytes,
                                            kv_cache_shards);
    sst_cache_ = std::make_unique<SSTableCache>(
        Config::kDefaultBlockCacheBlocks,
        Config::kDefaultBlockCacheMeta,
        Config::kDefaultBlockCacheBytes,
        block_cache_shards);
}

LSMTreeEngine::~LSMTreeEngine() {
    if (compaction_) {
        compaction_->should_stop = true;
        compaction_->cv.notify_one();
        if (compaction_->worker.joinable()) {
            compaction_->worker.join();
        }
    }

    if (flush_) {
        flush_->should_stop = true;
        flush_->cv.notify_one();
        if (flush_->worker.joinable()) {
            flush_->worker.join();
        }
    }

    if (sync_) {
        sync_->should_stop = true;
        if (sync_->worker.joinable()) {
            sync_->worker.join();
        }
    }

    try {
        wal_->Sync();
        {
            std::unique_lock<std::shared_mutex> lock(memtable_mutex_);
            while (!frozen_memtables_.empty()) {
                auto frozen = frozen_memtables_.front();
                frozen_memtables_.erase(frozen_memtables_.begin());
                lock.unlock();
                DoFlush(frozen);
                lock.lock();
            }
        }
        if (active_memtable_ && active_memtable_->EntryCount() > 0) {
            auto frozen = active_memtable_;
            active_memtable_.reset();
            DoFlush(frozen);
            wal_->WriteCheckpoint(global_ts_.load());
            wal_->Sync();
        }
        DrainFileGC();
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

    uint64_t ts;
    {
        std::unique_lock<std::mutex> lock(batch_mutex_);
        while (batch_in_progress_ && global_ts_.load() + 1 > batch_ts_)
            batch_cv_.wait(lock);
        ts = global_ts_.fetch_add(1) + 1;
    }

    wal_->Buffer(key, value, ts);
    size_t my_seq = wal_->CurrentBatchSeq();

    {
        std::unique_lock<std::mutex> lock(sync_->mtx);
        sync_->requested_seq = my_seq;
        sync_->ready_cv.notify_one();
    }

    {
        std::unique_lock<std::shared_mutex> lock(memtable_mutex_);
        active_memtable_->Insert(key, value, ts);

        if (active_memtable_->IsFull()) {
            auto frozen = active_memtable_;
            frozen->Freeze();
            frozen_memtables_.push_back(frozen);
            {
                std::lock_guard<std::mutex> lk(flush_->mtx);
                flush_->pending++;
            }
            flush_->cv.notify_one();
            active_memtable_ = std::make_shared<MemTable>(next_table_id_.fetch_add(1), memtable_max_bytes_);
        }
    }

    {
        std::unique_lock<std::mutex> lock(sync_->mtx);
        sync_->cv.wait(lock, [this, my_seq] {
            return sync_->synced_seq >= my_seq;
        });
    }

    kv_cache_->Put(key, value, ts);

    DrainRecyclePending();
}

void LSMTreeEngine::Delete(const std::string& key) {
    if (key.size() > Config::kMaxKeyBytes) {
        throw std::invalid_argument(
            "Key size (" + std::to_string(key.size())
            + " bytes) exceeds maximum allowed ("
            + std::to_string(Config::kMaxKeyBytes) + " bytes)");
    }

    uint64_t ts;
    {
        std::unique_lock<std::mutex> lock(batch_mutex_);
        while (batch_in_progress_ && global_ts_.load() + 1 > batch_ts_)
            batch_cv_.wait(lock);
        ts = global_ts_.fetch_add(1) + 1;
    }

    wal_->Buffer(key, "", ts);
    size_t my_seq = wal_->CurrentBatchSeq();

    {
        std::unique_lock<std::mutex> lock(sync_->mtx);
        sync_->requested_seq = my_seq;
        sync_->ready_cv.notify_one();
    }

    {
        std::unique_lock<std::shared_mutex> lock(memtable_mutex_);
        active_memtable_->Insert(key, "", ts, true);

        if (active_memtable_->IsFull()) {
            auto frozen = active_memtable_;
            frozen->Freeze();
            frozen_memtables_.push_back(frozen);
            {
                std::lock_guard<std::mutex> lk(flush_->mtx);
                flush_->pending++;
            }
            flush_->cv.notify_one();
            active_memtable_ = std::make_shared<MemTable>(next_table_id_.fetch_add(1), memtable_max_bytes_);
        }
    }

    {
        std::unique_lock<std::mutex> lock(sync_->mtx);
        sync_->cv.wait(lock, [this, my_seq] {
            return sync_->synced_seq >= my_seq;
        });
    }

    kv_cache_->Erase(key);

    DrainRecyclePending();
}

bool LSMTreeEngine::Lookup(const std::string& key, std::string& value_out) const {
    uint64_t read_ts = global_ts_.load();

    if (kv_cache_->Get(key, read_ts, value_out))
        return !value_out.empty();

    tracker_.Acquire(read_ts);

    struct Guard {
        SnapshotTracker& t;
        uint64_t ts;
        ~Guard() { t.Release(ts); }
    } guard{tracker_, read_ts};

    std::string found_val;
    uint64_t found_ts = 0;
    auto tryResult = [&](const std::string& val, uint64_t ts) -> bool {
        if (val.empty()) { found_val = val; found_ts = ts; return true; }
        found_val = val; found_ts = ts; return true;
    };

    bool hit = false;
    {
        std::shared_lock<std::shared_mutex> lock(memtable_mutex_);
        if (active_memtable_->Lookup(key, read_ts, found_val)) {
            hit = true;
        } else {
            for (auto it = frozen_memtables_.rbegin();
                 it != frozen_memtables_.rend(); ++it) {
                if ((*it)->Lookup(key, read_ts, found_val)) {
                    hit = true; break;
                }
            }
        }
    }

    if (!hit) {
        std::unordered_set<uint64_t> scanned_ids;
        {
            std::shared_lock<std::shared_mutex> lock(memtable_mutex_);
            for (const auto& m : frozen_memtables_)
                scanned_ids.insert(m->Id());
        }

        std::vector<SSTable::Metadata> metadata;
        {
            std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
            metadata = sstable_metadata_;
        }

        for (auto it = metadata.rbegin(); it != metadata.rend() && !hit; ++it) {
            if (it->level != 0) continue;
            if (scanned_ids.count(it->source_table_id)) continue;
            if (!it->min_key.empty() && key < it->min_key) continue;
            if (!it->max_key.empty() && key > it->max_key) continue;
            if (it->bloom.BitCount() > 0 && !it->bloom.MightContain(key)) continue;
            try {
                hit = SSTable::LookupKey(it->filepath, key, read_ts, found_val, sst_cache_.get(),
                                         it->manifest_seq);
            } catch (const std::exception&) {}
        }

        if (!hit) {
            std::map<int, std::vector<SSTable::Metadata>> levels;
            for (auto& meta : metadata) {
                if (meta.level == 0) continue;
                levels[meta.level].push_back(meta);
            }
            for (auto& [lvl, files] : levels) {
                std::sort(files.begin(), files.end(),
                          [](const SSTable::Metadata& a, const SSTable::Metadata& b)
                          { return a.min_key < b.min_key; });
                auto it = std::lower_bound(files.begin(), files.end(), key,
                    [](const SSTable::Metadata& m, const std::string& k)
                    { return m.max_key < k; });
                if (it == files.end()) continue;
                if (key < it->min_key || key > it->max_key) continue;
                if (scanned_ids.count(it->source_table_id)) continue;
                if (it->bloom.BitCount() > 0 && !it->bloom.MightContain(key)) continue;
                try {
                    hit = SSTable::LookupKey(it->filepath, key, read_ts, found_val, sst_cache_.get(),
                                         it->manifest_seq);
                } catch (const std::exception&) {}
                if (hit) break;
            }
        }
    }

    if (!hit) { value_out.clear(); return false; }

    if (found_val.empty()) {
        value_out.clear();
        return false;
    }

    value_out = found_val;
    kv_cache_->Put(key, found_val, read_ts);
    return true;
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
    {
        std::unique_lock<std::shared_mutex> lock(memtable_mutex_);
        if (active_memtable_->EntryCount() == 0) {
            return;
        }
        auto frozen = active_memtable_;
        frozen->Freeze();
        frozen_memtables_.push_back(frozen);
        {
            std::lock_guard<std::mutex> lk(flush_->mtx);
            flush_->pending++;
        }
        flush_->cv.notify_one();
        active_memtable_ = std::make_shared<MemTable>(next_table_id_.fetch_add(1), memtable_max_bytes_);
    }
    WaitForPendingFlushes();
}

void LSMTreeEngine::WaitForPendingFlushes() {
    {
        std::unique_lock<std::mutex> lock(flush_->mtx);
        flush_->done_cv.wait(lock, [this] {
            return flush_->pending.load() == 0;
        });
    }
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
    return RangeScan(RangeBound::Unbounded(), RangeBound::Unbounded());
}

RangeIterator LSMTreeEngine::RangeScan(const RangeBound& lower, const RangeBound& upper) const {
    uint64_t read_ts = global_ts_.load();
    tracker_.Acquire(read_ts);

    std::shared_ptr<void> guard(new char, [this, read_ts](void* p) {
        delete static_cast<char*>(p);
        tracker_.Release(read_ts);
    });

    std::vector<std::unique_ptr<SourceIterator>> sources;

    {
        std::shared_lock<std::shared_mutex> lock(memtable_mutex_);
        if (active_memtable_->EntryCount() > 0) {
            auto vi = std::make_unique<VectorIterator>(active_memtable_->ExportEntries());
            if (!lower.IsUnbounded()) vi->SeekToKey(lower.key);
            if (vi->Valid()) sources.push_back(std::move(vi));
        }
        for (const auto& m : frozen_memtables_) {
            if (m->EntryCount() > 0) {
                auto ms = std::make_unique<MemTableSource>(m);
                if (!lower.IsUnbounded()) ms->SeekToKey(lower.key);
                if (ms->Valid()) sources.push_back(std::move(ms));
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
        std::map<int, std::vector<SSTable::Metadata>> groups;
        for (const auto& meta : sstable_metadata_) {
            if (!upper.IsUnbounded() && !meta.min_key.empty() && meta.min_key > upper.key) continue;
            if (!lower.IsUnbounded() && !meta.max_key.empty() && meta.max_key < lower.key) continue;
            if (meta.level == 0) {
                try {
                    auto iter = std::make_unique<SSTableIterator>(meta.filepath,
                        *sst_cache_, meta.manifest_seq);
                    if (iter->Valid()) {
                        if (!lower.IsUnbounded()) iter->SeekToKey(lower.key);
                        if (iter->Valid()) sources.push_back(std::move(iter));
                    }
                } catch (...) {}
            } else {
                groups[meta.level].push_back(meta);
            }
        }
        for (auto& [lvl, files] : groups) {
            try {
                auto li = std::make_unique<LevelIterator>(files, *sst_cache_);
                if (li->Valid()) {
                    if (!lower.IsUnbounded()) li->SeekToKey(lower.key);
                    if (li->Valid()) sources.push_back(std::move(li));
                }
            } catch (...) {}
        }
    }

    return RangeIterator(std::move(sources), read_ts, guard, lower, upper);
}

RangeIterator LSMTreeEngine::PrefixScan(const std::string& prefix) const {
    std::string upper = prefix;
    while (!upper.empty() && static_cast<unsigned char>(upper.back()) == 0xFF)
        upper.pop_back();
    if (upper.empty())
        return RangeScan(RangeBound::Inclusive(prefix), RangeBound::Unbounded());
    upper.back() = static_cast<char>(static_cast<unsigned char>(upper.back()) + 1);
    return RangeScan(RangeBound::Inclusive(prefix), RangeBound::Exclusive(upper));
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
    SSTable::WriteFromWalk(filepath, walk, entry_count, sst_cache_.get(), seq);

    SSTable::Metadata meta;
    try {
        meta = SSTable::ReadMetadata(filepath, sst_cache_.get(), seq);
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
    meta.manifest_seq = seq;

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

void LSMTreeEngine::DeferFileGC(const std::string& filepath, uint64_t seq,
                                uint64_t fence_ts) {
    std::lock_guard<std::mutex> lock(pending_gc_mutex_);
    pending_gc_.push_back({filepath, seq, fence_ts});
}

void LSMTreeEngine::DrainFileGC() {
    std::vector<PendingFileGC> ready;
    {
        std::lock_guard<std::mutex> lock(pending_gc_mutex_);
        auto& pg = pending_gc_;
        pg.erase(
            std::remove_if(pg.begin(), pg.end(), [&](const PendingFileGC& p) {
                if (tracker_.MinActiveTS() >= p.fence_ts) {
                    ready.push_back(std::move(p));
                    return true;
                }
                return false;
            }),
            pg.end());
    }

    for (auto& entry : ready) {
        sst_cache_->Invalidate(entry.manifest_seq);
        std::error_code ec;
        std::filesystem::remove(entry.filepath, ec);
    }
}

void LSMTreeEngine::CompactLevel(int from_level, int top_level) {
    std::vector<SSTable::Metadata> snapshot;
    {
        std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
        snapshot = sstable_metadata_;
    }

    int to_level = top_level + 1;
    if (to_level > static_cast<int>(Config::kMaxLevel)) return;

    std::vector<SSTable::Metadata> inputs;
    for (auto& m : snapshot) {
        if (m.level >= from_level && m.level <= top_level)
            inputs.push_back(m);
    }
    if (inputs.empty()) return;

    size_t max_sst = Config::kLevelBaseSSTableSize;
    for (int l = 1; l <= to_level; ++l) max_sst *= Config::kLevelSizeMultiplier;
    bool is_last = (to_level == static_cast<int>(Config::kMaxLevel));

    uint64_t seq = sstable_seq_.fetch_add(64);
    std::vector<SSTable::Metadata> outputs;
    std::vector<std::string> garbage;
    SSTable::Compact(inputs, data_dir_, seq, to_level,
                     max_sst, is_last, "", "", outputs, garbage, *sst_cache_,
                     global_ts_.load());

    {
        std::lock_guard<std::mutex> lock(sstable_metadata_mutex_);
        auto& all = sstable_metadata_;
        for (auto& in : inputs) {
            all.erase(std::remove_if(all.begin(), all.end(),
                [&](const SSTable::Metadata& m) { return m.filepath == in.filepath; }),
                all.end());
        }
        for (auto& out : outputs) all.push_back(out);
    }

    for (auto& in : inputs)
        manifest_->RemoveSSTable(sstable_seq_.load());
    for (auto& out : outputs)
        manifest_->AddSSTable(sstable_seq_.fetch_add(1), out);
    manifest_->Sync();

    uint64_t gc_fence = global_ts_.load();
    for (auto& f : garbage) {
        uint64_t gc_seq = 0;
        for (auto& in : inputs)
            if (in.filepath == f) { gc_seq = in.manifest_seq; break; }
        sst_cache_->Invalidate(gc_seq);
        DeferFileGC(f, gc_seq, gc_fence);
    }
}

bool LSMTreeEngine::StartBatch() {
    std::lock_guard<std::mutex> lock(batch_mutex_);
    if (batch_in_progress_) return false;
    batch_ts_ = global_ts_.load() + batch_increment_gap_;
    batch_in_progress_ = true;
    return true;
}

bool LSMTreeEngine::CommitBatch() {
    std::unique_lock<std::mutex> lock(batch_mutex_);
    if (!batch_in_progress_) return false;
    global_ts_.store(batch_ts_ + 1);
    batch_in_progress_ = false;
    lock.unlock();
    batch_cv_.notify_all();
    return true;
}

void LSMTreeEngine::BatchInsert(const std::string& key, const std::string& value) {
    if (key.size() > Config::kMaxKeyBytes)
        throw std::invalid_argument("Key too large for batch");
    size_t entry_size = key.size() + value.size() + Config::kMemTableEntryOverheadBytes;
    if (entry_size > Config::kMaxKeyValuePairBytes)
        throw std::invalid_argument("KV pair too large for batch");

    {
        std::unique_lock<std::shared_mutex> lock(memtable_mutex_);
        active_memtable_->Insert(key, value, batch_ts_, false);
        if (active_memtable_->IsFull()) {
            auto frozen = active_memtable_;
            frozen->Freeze();
            frozen_memtables_.push_back(frozen);
            {
                std::lock_guard<std::mutex> lk(flush_->mtx);
                flush_->pending++;
            }
            flush_->cv.notify_one();
            active_memtable_ = std::make_shared<MemTable>(next_table_id_.fetch_add(1), memtable_max_bytes_);
        }
    }
}

void LSMTreeEngine::BatchDelete(const std::string& key) {
    if (key.size() > Config::kMaxKeyBytes)
        throw std::invalid_argument("Key too large for batch");

    {
        std::unique_lock<std::shared_mutex> lock(memtable_mutex_);
        active_memtable_->Insert(key, "", batch_ts_, true);
        if (active_memtable_->IsFull()) {
            auto frozen = active_memtable_;
            frozen->Freeze();
            frozen_memtables_.push_back(frozen);
            {
                std::lock_guard<std::mutex> lk(flush_->mtx);
                flush_->pending++;
            }
            flush_->cv.notify_one();
            active_memtable_ = std::make_shared<MemTable>(next_table_id_.fetch_add(1), memtable_max_bytes_);
        }
    }
}

} // namespace kvdb
