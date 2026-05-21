#pragma once

#include "block_reader.hpp"

#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvdb {

class SSTableCache : public BlockReader {
public:
    SSTableCache(size_t max_blocks = 1024, size_t max_meta = 256,
                 size_t max_bytes = 64 * 1024 * 1024);

    bool GetBloom(uint64_t seq, BloomFilter& bloom_out) override;
    void PutBloom(uint64_t seq, const BloomFilter& bloom) override;
    bool GetBlockOffsets(uint64_t seq, std::vector<uint64_t>& offsets_out,
                         std::vector<std::string>& first_keys_out) override;
    void PutBlockOffsets(uint64_t seq, const std::vector<uint64_t>& offsets,
                         const std::vector<std::string>& first_keys) override;
    bool GetBlock(uint64_t seq, uint32_t block_idx,
                  std::string& data_out, uint32_t& entry_count_out) override;
    void PutBlock(uint64_t seq, uint32_t block_idx,
                  const std::string& data, uint32_t entry_count) override;
    void Invalidate(uint64_t seq) override;

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

    struct CachedBlock {
        std::string data;
        uint32_t entry_count;
        std::list<uint64_t>::iterator lru_it;
    };

    mutable std::mutex mutex_;
    std::unordered_map<uint64_t, CachedHeavy> heavy_map_;
    std::list<uint64_t> heavy_lru_;
    size_t max_meta_;

    std::unordered_map<uint64_t, CachedBlock> block_map_;
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

inline bool SSTableCache::GetBlock(uint64_t seq, uint32_t block_idx,
                                   std::string& data_out,
                                   uint32_t& entry_count_out) {
    uint64_t key = BlockKey(seq, block_idx);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = block_map_.find(key);
    if (it == block_map_.end()) return false;
    data_out = it->second.data;
    entry_count_out = it->second.entry_count;
    block_lru_.erase(it->second.lru_it);
    block_lru_.push_front(key);
    it->second.lru_it = block_lru_.begin();
    return true;
}

inline void SSTableCache::PutBlock(uint64_t seq, uint32_t block_idx,
                                   const std::string& data,
                                   uint32_t entry_count) {
    uint64_t key = BlockKey(seq, block_idx);
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = block_map_.find(key);
    if (it != block_map_.end()) {
        current_bytes_ -= it->second.data.size();
        it->second.data = data;
        it->second.entry_count = entry_count;
        current_bytes_ += data.size();
        block_lru_.erase(it->second.lru_it);
        block_lru_.push_front(key);
        it->second.lru_it = block_lru_.begin();
        return;
    }
    while ((block_map_.size() >= max_blocks_ ||
            current_bytes_ + data.size() > max_bytes_) && !block_lru_.empty())
        EvictBlock();
    block_lru_.push_front(key);
    CachedBlock cb;
    cb.data = data;
    cb.entry_count = entry_count;
    cb.lru_it = block_lru_.begin();
    block_map_[key] = std::move(cb);
    current_bytes_ += data.size();
}

inline void SSTableCache::Invalidate(uint64_t seq) {
    std::lock_guard<std::mutex> lock(mutex_);
    heavy_map_.erase(seq);
    uint64_t lo = BlockKey(seq, 0);
    uint64_t hi = BlockKey(seq + 1, 0);
    for (auto it = block_map_.begin(); it != block_map_.end(); ) {
        if (it->first >= lo && it->first < hi) {
            current_bytes_ -= it->second.data.size();
            block_lru_.erase(it->second.lru_it);
            it = block_map_.erase(it);
        } else { ++it; }
    }
    block_lru_.remove_if([lo, hi](uint64_t k) { return k >= lo && k < hi; });
}

inline void SSTableCache::EvictBlock() {
    if (block_lru_.empty()) return;
    uint64_t key = block_lru_.back();
    auto it = block_map_.find(key);
    if (it != block_map_.end()) {
        current_bytes_ -= it->second.data.size();
        block_map_.erase(it);
    }
    block_lru_.pop_back();
}

} // namespace kvdb
