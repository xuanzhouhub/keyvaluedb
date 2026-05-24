#pragma once

#include "block.hpp"
#include "block_reader.hpp"
#include "bptree.hpp"
#include "config.hpp"
#include "memtable.hpp"
#include "sstable.hpp"
#include "types.hpp"

#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

namespace kvdb {

struct SourceIterator {
    virtual bool Valid() const = 0;
    virtual const KeyValuePair& Current() const = 0;
    virtual void Next() = 0;
    virtual void SeekToKey(const std::string& key) { while (Valid() && Current().key < key) Next(); }
    virtual ~SourceIterator() = default;
};

struct VectorIterator : SourceIterator {
    std::vector<KeyValuePair> entries;
    size_t pos = 0;
    VectorIterator(std::vector<KeyValuePair> e) : entries(std::move(e)) {}
    bool Valid() const override { return pos < entries.size(); }
    const KeyValuePair& Current() const override { return entries[pos]; }
    void Next() override { ++pos; }
    void SeekToKey(const std::string& key) override {
        auto it = std::lower_bound(entries.begin(), entries.end(), key,
            [](const KeyValuePair& a, const std::string& k) { return a.key < k; });
        pos = static_cast<size_t>(it - entries.begin());
    }
};

struct MemTableSource : SourceIterator {
    std::shared_ptr<MemTable> ref;   // keep memtable alive
    BPlusTree::MemTableWalk walk;
    KeyValuePair current_;

    MemTableSource(std::shared_ptr<MemTable> memtable)
        : ref(std::move(memtable)), walk(ref->GetTree()) {
        if (walk.Valid()) { current_.key = walk.Key(); current_.value = walk.Value(); current_.timestamp = walk.Timestamp(); }
    }
    bool Valid() const override { return walk.Valid(); }
    const KeyValuePair& Current() const override { return current_; }
    void Next() override {
        walk.Next();
        if (walk.Valid()) { current_.key = walk.Key(); current_.value = walk.Value(); current_.timestamp = walk.Timestamp(); }
    }
};

struct SSTableIterator : SourceIterator {
    std::ifstream file;
    std::string block_data;
    std::shared_ptr<const std::string> cached_block_;
    Block block_;
    size_t pos = 0;
    uint32_t total_entries = 0;
    uint32_t read_entries = 0;
    KeyValuePair current;

    BlockReader* reader_ = nullptr;
    uint64_t manifest_seq_ = 0;
    uint32_t cur_block_ = 0;
    bool populate_ = true;
    std::string filepath_;
    const std::unordered_set<uint64_t>* aborted_ = nullptr;

    SSTableIterator(const std::string& filepath,
                    const std::unordered_set<uint64_t>* aborted = nullptr);

    SSTableIterator(const std::string& filepath,
                    BlockReader& reader,
                    uint64_t manifest_seq,
                    bool populate = true,
                    const std::unordered_set<uint64_t>* aborted = nullptr);

    ~SSTableIterator() { if (file.is_open()) file.close(); }

    bool Valid() const override { return pos < block_.Count() || (read_entries < total_entries && block_.Count() > 0); }

    const KeyValuePair& Current() const override { return current; }

    void Next() override {
        do {
            ++pos;
            if (pos >= block_.Count()) { ReadNextBlock(); return; }
            else { block_.Seek(pos); block_.Read(current); }
        } while (aborted_ && aborted_->count(current.timestamp));
    }

private:
    uint32_t read_u32() { uint32_t v=0; v|=uint8_t(file.get()); v|=uint8_t(file.get())<<8; v|=uint8_t(file.get())<<16; v|=uint8_t(file.get())<<24; return v; }
    void ReadNextBlock();
    static void DecompressBlock(uint8_t comp, std::string& data);
};

class LevelIterator : public SourceIterator {
public:
    LevelIterator(const std::vector<SSTable::Metadata>& files) : files_(files) {
        std::sort(files_.begin(), files_.end(),
                  [](const SSTable::Metadata& a, const SSTable::Metadata& b)
                  { return a.min_key < b.min_key; });
        OpenNext();
    }

    LevelIterator(const std::vector<SSTable::Metadata>& files,
                  BlockReader& reader)
        : files_(files), reader_(&reader) {
        std::sort(files_.begin(), files_.end(),
                  [](const SSTable::Metadata& a, const SSTable::Metadata& b)
                  { return a.min_key < b.min_key; });
        OpenNext();
    }

    bool Valid() const override { return current_ && current_->Valid(); }
    const KeyValuePair& Current() const override { return current_->Current(); }
    void Next() override {
        current_->Next();
        if (!current_->Valid()) OpenNext();
    }

    void SeekToKey(const std::string& key) override {
        while (idx_ < files_.size() && !files_[idx_].max_key.empty() && files_[idx_].max_key < key)
            ++idx_;
        if (idx_ < files_.size()) {
            current_ = MakeIter(files_[idx_]); ++idx_;
            if (current_->Valid()) current_->SeekToKey(key);
            if (!current_->Valid()) OpenNext();
        }
    }

private:
    std::unique_ptr<SSTableIterator> MakeIter(const SSTable::Metadata& m) {
        if (reader_)
            return std::make_unique<SSTableIterator>(m.filepath, *reader_, m.manifest_seq, true, &m.aborted_batch_ts);
        return std::make_unique<SSTableIterator>(m.filepath, &m.aborted_batch_ts);
    }

    void OpenNext() {
        while (idx_ < files_.size()) {
            current_ = MakeIter(files_[idx_]); ++idx_;
            if (current_->Valid()) return;
        }
        current_.reset();
    }

    std::vector<SSTable::Metadata> files_;
    size_t idx_ = 0;
    BlockReader* reader_ = nullptr;
    std::unique_ptr<SSTableIterator> current_;
};

class RangeIterator {
    struct HeapEntry { size_t idx; };
    struct HeapCmp {
        const std::vector<std::unique_ptr<SourceIterator>>* sources;
        bool operator()(const HeapEntry& a, const HeapEntry& b) const {
            return (*sources)[a.idx]->Current().key > (*sources)[b.idx]->Current().key;
        }
    };

public:
    RangeIterator() = default;

    RangeIterator(std::vector<std::unique_ptr<SourceIterator>> sources, uint64_t read_ts,
                  std::shared_ptr<void> guard = {},
                  const RangeBound& lower = RangeBound::Unbounded(),
                  const RangeBound& upper = RangeBound::Unbounded())
        : sources_(std::move(sources)), guard_(std::move(guard)), read_ts_(read_ts),
          lower_(lower), upper_(upper) {
        if (!lower_.IsUnbounded())
            for (auto& s : sources_) s->SeekToKey(lower_.key);
        if (!lower_.IsUnbounded() && !lower_.inclusive) {
            for (auto& s : sources_)
                while (s->Valid() && s->Current().key == lower_.key) s->Next();
        }
        heap_cmp_.sources = &sources_;
        heap_ = decltype(heap_)(heap_cmp_);
        for (size_t i = 0; i < sources_.size(); ++i)
            if (sources_[i]->Valid()) heap_.push({i});
        FindNext();
    }

    bool Valid() const { return valid_; }
    void Next() { FindNext(); }
    const std::string& Key() const { return current_.key; }
    const std::string& Value() const { return current_.value; }
    uint64_t Timestamp() const { return current_.timestamp; }
    bool IsTombstone() const { return current_.is_tombstone; }
    const KeyValuePair& CurrentPair() const { return current_; }

private:
    void FindNext() {
        valid_ = false;
        while (!heap_.empty()) {
            HeapEntry top = heap_.top(); heap_.pop();
            const auto& entry = sources_[top.idx]->Current();

            if (!upper_.IsUnbounded()) {
                if (upper_.inclusive && entry.key > upper_.key) { valid_ = false; return; }
                if (!upper_.inclusive && entry.key >= upper_.key) { valid_ = false; return; }
            }

            std::vector<size_t> same_key;
            same_key.push_back(top.idx);
            while (!heap_.empty()
                && sources_[heap_.top().idx]->Current().key == entry.key) {
                same_key.push_back(heap_.top().idx);
                heap_.pop();
            }

            const KeyValuePair* best = nullptr;
            for (auto idx : same_key) {
                const auto& e = sources_[idx]->Current();
                if (e.timestamp <= read_ts_)
                    if (!best || e.timestamp > best->timestamp) best = &e;
            }

            KeyValuePair chosen;
            if (best) chosen = *best;

            for (auto idx : same_key) {
                sources_[idx]->Next();
                if (sources_[idx]->Valid()) heap_.push({idx});
            }

            if (best) { current_ = std::move(chosen); valid_ = true; return; }
        }
    }

    std::vector<std::unique_ptr<SourceIterator>> sources_;
    std::shared_ptr<void> guard_;
    HeapCmp heap_cmp_;
    std::priority_queue<HeapEntry, std::vector<HeapEntry>, HeapCmp> heap_{heap_cmp_};
    uint64_t read_ts_ = 0;
    RangeBound lower_;
    RangeBound upper_;
    KeyValuePair current_;
    bool valid_ = false;
};

} // namespace kvdb
