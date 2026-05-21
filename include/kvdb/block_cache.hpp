#pragma once

#include "block_reader.hpp"
#include "sstable.hpp"

#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <memory>
#include <string>
#include <unordered_map>

namespace kvdb {

class SSTableCache : public BlockReader {
public:
    SSTableCache(size_t max_blocks = 1024, size_t max_meta = 256,
                 size_t max_bytes = 64 * 1024 * 1024);

    SSTableCache(const SSTableCache&) = delete;
    SSTableCache& operator=(const SSTableCache&) = delete;

    bool GetBloom(uint64_t seq, BloomFilter& bloom_out);
    void PutBloom(uint64_t seq, const BloomFilter& bloom);

    bool GetBlockOffsets(uint64_t seq, std::vector<uint64_t>& offsets_out,
                         std::vector<std::string>& first_keys_out);
    void PutBlockOffsets(uint64_t seq, const std::vector<uint64_t>& offsets,
                         const std::vector<std::string>& first_keys);

    std::shared_ptr<const std::string> GetBlock(
        uint64_t seq, uint32_t block_idx) override;

    void PutBlock(uint64_t seq, uint32_t block_idx,
                  std::string data, uint32_t entry_count) override;

    void Invalidate(uint64_t seq) override;

    void Clear();

private:
    void EvictBlock();
    static uint64_t BlockKey(uint64_t seq, uint32_t idx) {
        return (seq << 32) | idx;
    }

    struct CachedHeavy {
        BloomFilter bloom;
        std::vector<uint64_t> block_offsets;
        std::vector<std::string> block_first_keys;
        std::list<uint64_t>::iterator lru_it;
    };

    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, CachedHeavy> heavy_map_;
    std::list<uint64_t> heavy_lru_;
    size_t max_meta_;

    std::unordered_map<uint64_t, std::shared_ptr<std::string>> block_data_;
    std::unordered_map<uint64_t, uint32_t> block_entry_counts_;
    std::list<uint64_t> block_lru_;
    size_t max_blocks_;
    size_t max_bytes_;
    size_t current_bytes_;
};

inline SSTableCache::SSTableCache(size_t max_blocks, size_t max_meta,
                                  size_t max_bytes)
    : max_meta_(max_meta), max_blocks_(max_blocks),
      max_bytes_(max_bytes), current_bytes_(0) {}

inline bool SSTableCache::GetBloom(uint64_t seq, BloomFilter& bloom_out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = heavy_map_.find(seq);
    if (it == heavy_map_.end()) return false;
    bloom_out = it->second.bloom;
    heavy_lru_.erase(it->second.lru_it);
    heavy_lru_.push_front(seq);
    it->second.lru_it = heavy_lru_.begin();
    return true;
}

inline void SSTableCache::PutBloom(uint64_t seq, const BloomFilter& bloom) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = heavy_map_.find(seq);
    if (it != heavy_map_.end()) {
        it->second.bloom = bloom;
        heavy_lru_.erase(it->second.lru_it);
        heavy_lru_.push_front(seq);
        it->second.lru_it = heavy_lru_.begin();
        return;
    }
    while (heavy_map_.size() >= max_meta_ && !heavy_lru_.empty()) {
        heavy_map_.erase(heavy_lru_.back());
        heavy_lru_.pop_back();
    }
    heavy_lru_.push_front(seq);
    CachedHeavy ch;
    ch.bloom = bloom;
    ch.lru_it = heavy_lru_.begin();
    heavy_map_[seq] = std::move(ch);
}

inline bool SSTableCache::GetBlockOffsets(
    uint64_t seq, std::vector<uint64_t>& offsets_out,
    std::vector<std::string>& first_keys_out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = heavy_map_.find(seq);
    if (it == heavy_map_.end()) return false;
    offsets_out = it->second.block_offsets;
    first_keys_out = it->second.block_first_keys;
    heavy_lru_.erase(it->second.lru_it);
    heavy_lru_.push_front(seq);
    it->second.lru_it = heavy_lru_.begin();
    return true;
}

inline void SSTableCache::PutBlockOffsets(
    uint64_t seq, const std::vector<uint64_t>& offsets,
    const std::vector<std::string>& first_keys) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = heavy_map_.find(seq);
    if (it != heavy_map_.end()) {
        it->second.block_offsets = offsets;
        it->second.block_first_keys = first_keys;
        heavy_lru_.erase(it->second.lru_it);
        heavy_lru_.push_front(seq);
        it->second.lru_it = heavy_lru_.begin();
        return;
    }
    while (heavy_map_.size() >= max_meta_ && !heavy_lru_.empty()) {
        heavy_map_.erase(heavy_lru_.back());
        heavy_lru_.pop_back();
    }
    heavy_lru_.push_front(seq);
    CachedHeavy ch;
    ch.block_offsets = offsets;
    ch.block_first_keys = first_keys;
    ch.lru_it = heavy_lru_.begin();
    heavy_map_[seq] = std::move(ch);
}

inline std::shared_ptr<const std::string> SSTableCache::GetBlock(
    uint64_t seq, uint32_t block_idx) {
    uint64_t key = BlockKey(seq, block_idx);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = block_data_.find(key);
    if (it == block_data_.end()) return nullptr;
    block_lru_.erase(block_lru_.begin());  // touch: erase by iterator later
    block_lru_.push_front(key);
    return it->second;
}

inline void SSTableCache::PutBlock(uint64_t seq, uint32_t block_idx,
                                    std::string data,
                                    uint32_t entry_count) {
    uint64_t key = BlockKey(seq, block_idx);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = block_data_.find(key);
    if (it != block_data_.end()) {
        current_bytes_ -= it->second->size();
        *it->second = data;
        current_bytes_ += data.size();
        block_entry_counts_[key] = entry_count;
        return;
    }
    while ((block_data_.size() >= max_blocks_ ||
            current_bytes_ + data.size() > max_bytes_) && !block_lru_.empty())
        EvictBlock();
    block_lru_.push_front(key);
    block_data_[key] = std::make_shared<std::string>(data);
    block_entry_counts_[key] = entry_count;
    current_bytes_ += data.size();
}

inline void SSTableCache::Invalidate(uint64_t seq) {
    std::lock_guard<std::mutex> lock(mutex_);
    heavy_map_.erase(seq);
    uint64_t lo = BlockKey(seq, 0);
    uint64_t hi = BlockKey(seq + 1, 0);
    for (auto it = block_data_.begin(); it != block_data_.end(); ) {
        if (it->first >= lo && it->first < hi) {
            current_bytes_ -= it->second->size();
            block_entry_counts_.erase(it->first);
            it = block_data_.erase(it);
        } else { ++it; }
    }
    block_lru_.remove_if([lo, hi](uint64_t k) { return k >= lo && k < hi; });
}

inline void SSTableCache::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    heavy_map_.clear();
    heavy_lru_.clear();
    block_data_.clear();
    block_entry_counts_.clear();
    block_lru_.clear();
    current_bytes_ = 0;
}

inline void SSTableCache::EvictBlock() {
    if (block_lru_.empty()) return;
    uint64_t key = block_lru_.back();
    auto it = block_data_.find(key);
    if (it != block_data_.end()) {
        current_bytes_ -= it->second->size();
        block_data_.erase(it);
        block_entry_counts_.erase(key);
    }
    block_lru_.pop_back();
}

} // namespace kvdb
