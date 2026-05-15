#pragma once

#include "bptree.hpp"
#include "config.hpp"
#include "memtable.hpp"
#include "snappy.hpp"
#include "sstable.hpp"
#include "types.hpp"

#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <queue>
#include <sstream>
#include <string>
#include <vector>

namespace kvdb {

struct SourceIterator {
    virtual bool Valid() const = 0;
    virtual const KeyValuePair& Current() const = 0;
    virtual void Next() = 0;
    virtual ~SourceIterator() = default;
};

struct VectorIterator : SourceIterator {
    std::vector<KeyValuePair> entries;
    size_t pos = 0;
    VectorIterator(std::vector<KeyValuePair> e) : entries(std::move(e)) {}
    bool Valid() const override { return pos < entries.size(); }
    const KeyValuePair& Current() const override { return entries[pos]; }
    void Next() override { ++pos; }
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
    size_t block_pos = 0;
    uint32_t block_entries = 0;
    uint32_t total_entries = 0;
    uint32_t read_entries = 0;
    size_t pos = 0;
    KeyValuePair current;

    SSTableIterator(const std::string& filepath) {
        file.open(filepath, std::ios::binary);
        if (!file.is_open()) { total_entries = 0; return; }
        read_u32(); read_u32(); read_u32();
        total_entries = read_u32();
        file.seekg(4, std::ios::cur); file.seekg(4, std::ios::cur);
        ReadNextBlock();
    }

    ~SSTableIterator() { if (file.is_open()) file.close(); }

    bool Valid() const override { return pos < block_entries || (read_entries < total_entries && block_entries > 0); }

    const KeyValuePair& Current() const override { return current; }

    void Next() override {
        ++pos;
        if (pos >= block_entries) ReadNextBlock();
        else ParseCurrent();
    }

private:
    uint32_t read_u32() { uint32_t v=0; v|=uint8_t(file.get()); v|=uint8_t(file.get())<<8; v|=uint8_t(file.get())<<16; v|=uint8_t(file.get())<<24; return v; }

    void ReadNextBlock() {
        pos = 0;
        if (read_entries >= total_entries) { block_entries = 0; return; }
        read_u32();
        uint8_t comp = uint8_t(file.get());
        uint32_t csz = read_u32();
        block_data.resize(csz); file.read(&block_data[0], csz);
        DecompressBlock(comp, block_data);
        block_pos = 0;
        block_entries = read_u32_internal();
        read_entries += block_entries;
        ParseCurrent();
    }

    uint32_t read_u32_internal() {
        if (block_pos + 4 > block_data.size()) return 0;
        uint32_t v = uint8_t(block_data[block_pos]) | (uint8_t(block_data[block_pos+1])<<8)
                   | (uint8_t(block_data[block_pos+2])<<16) | (uint8_t(block_data[block_pos+3])<<24);
        block_pos += 4;
        return v;
    }

    void ParseCurrent() {
        if (pos >= block_entries) { current = {}; return; }
        uint32_t kl = read_u32_internal();
        current.key.assign(block_data.data() + block_pos, kl); block_pos += kl;
        uint32_t vl = read_u32_internal();
        current.value.assign(block_data.data() + block_pos, vl); block_pos += vl;
        current.timestamp = 0;
        for (int b=0;b<8;++b) current.timestamp |= uint64_t(uint8_t(block_data[block_pos++]))<<(b*8);
        current.is_tombstone = current.value.empty();
    }

    static void DecompressBlock(uint8_t comp, std::string& data) {
        if (comp == Config::kCompressionSnappy) {
            std::string d; Snappy::Uncompress(data.data(), data.size(), d);
            data = std::move(d);
        }
    }

    static constexpr int kCompression = Config::kCompressionSnappy ? 1 : 0;
};

class LevelIterator : public SourceIterator {
public:
    LevelIterator(const std::vector<SSTable::Metadata>& files) : files_(files) {
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

private:
    void OpenNext() {
        while (idx_ < files_.size()) {
            current_ = std::make_unique<SSTableIterator>(files_[idx_].filepath);
            ++idx_;
            if (current_->Valid()) return;
        }
        current_.reset();
    }

    std::vector<SSTable::Metadata> files_;
    size_t idx_ = 0;
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
                  std::shared_ptr<void> guard = {})
        : sources_(std::move(sources)), guard_(std::move(guard)), read_ts_(read_ts) {
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
    KeyValuePair current_;
    bool valid_ = false;
};

} // namespace kvdb
