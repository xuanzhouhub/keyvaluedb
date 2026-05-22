#pragma once

#include "block_reader.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvdb {

class SSTableCache : public BlockReader {
public:
    SSTableCache(size_t max_blocks = 1024,
                 size_t max_meta = 256,
                 size_t max_bytes = 64 * 1024 * 1024,
                 size_t num_shards = 16);

    bool GetBloom(uint64_t seq, BloomFilter& bloom_out) override;
    void PutBloom(uint64_t seq, const BloomFilter& bloom) override;
    bool GetBlockOffsets(uint64_t seq, std::vector<uint64_t>& offsets_out,
                         std::vector<std::string>& first_keys_out) override;
    void PutBlockOffsets(uint64_t seq, const std::vector<uint64_t>& offsets,
                         const std::vector<std::string>& first_keys) override;
    std::shared_ptr<const std::string> GetBlock(
        uint64_t seq, uint32_t block_idx) override;
    void PutBlock(uint64_t seq, uint32_t block_idx,
                  std::string data) override;
    void Invalidate(uint64_t seq) override;

private:
    static uint64_t BlockKey(uint64_t seq, uint32_t idx) {
        return (seq << 32) | idx;
    }

    struct CachedHeavy {
        BloomFilter bloom;
        std::vector<uint64_t> block_offsets;
        std::vector<std::string> block_first_keys;
        std::list<uint64_t>::iterator lru_it;
    };

    struct Shard {
        mutable std::mutex mutex;
        std::unordered_map<uint64_t, CachedHeavy> heavy_map;
        std::list<uint64_t> heavy_lru;
        size_t max_meta;

        std::unordered_map<uint64_t, std::shared_ptr<std::string>> block_data;
        std::unordered_map<uint64_t, std::list<uint64_t>::iterator> block_lru_iters;
        std::list<uint64_t> block_lru;
        size_t max_blocks;
        size_t max_bytes;
        size_t current_bytes = 0;

        void EvictBlock_nolock();

        bool GetBloom_nolock(uint64_t seq, BloomFilter& bloom_out);
        void PutBloom_nolock(uint64_t seq, const BloomFilter& bloom);
        bool GetBlockOffsets_nolock(uint64_t seq,
                                     std::vector<uint64_t>& offsets_out,
                                     std::vector<std::string>& first_keys_out);
        void PutBlockOffsets_nolock(uint64_t seq,
                                     const std::vector<uint64_t>& offsets,
                                     const std::vector<std::string>& first_keys);
        std::shared_ptr<const std::string> GetBlock_nolock(
            uint64_t seq, uint32_t block_idx);
        void PutBlock_nolock(uint64_t seq, uint32_t block_idx,
                             std::string data);
        void Invalidate_nolock(uint64_t seq);
    };

    size_t num_shards_;
    std::vector<Shard> shards_;

    Shard& ShardForSeq(uint64_t seq) {
        return shards_[std::hash<uint64_t>{}(seq) % shards_.size()];
    }
};

inline SSTableCache::SSTableCache(size_t max_blocks, size_t max_meta,
                                   size_t max_bytes, size_t num_shards)
    : num_shards_(num_shards), shards_(num_shards) {
    size_t per_blocks = std::max(size_t(1), max_blocks / num_shards);
    size_t per_meta   = std::max(size_t(1), max_meta / num_shards);
    size_t per_bytes  = std::max(size_t(1), max_bytes / num_shards);
    for (auto& s : shards_) {
        s.max_blocks = per_blocks;
        s.max_meta   = per_meta;
        s.max_bytes  = per_bytes;
    }
}

inline bool SSTableCache::GetBloom(uint64_t seq, BloomFilter& bloom_out) {
    auto& s = ShardForSeq(seq);
    std::lock_guard<std::mutex> lock(s.mutex);
    return s.GetBloom_nolock(seq, bloom_out);
}

inline void SSTableCache::PutBloom(uint64_t seq, const BloomFilter& bloom) {
    auto& s = ShardForSeq(seq);
    std::lock_guard<std::mutex> lock(s.mutex);
    s.PutBloom_nolock(seq, bloom);
}

inline bool SSTableCache::GetBlockOffsets(
    uint64_t seq, std::vector<uint64_t>& offsets_out,
    std::vector<std::string>& first_keys_out) {
    auto& s = ShardForSeq(seq);
    std::lock_guard<std::mutex> lock(s.mutex);
    return s.GetBlockOffsets_nolock(seq, offsets_out, first_keys_out);
}

inline void SSTableCache::PutBlockOffsets(
    uint64_t seq, const std::vector<uint64_t>& offsets,
    const std::vector<std::string>& first_keys) {
    auto& s = ShardForSeq(seq);
    std::lock_guard<std::mutex> lock(s.mutex);
    s.PutBlockOffsets_nolock(seq, offsets, first_keys);
}

inline std::shared_ptr<const std::string> SSTableCache::GetBlock(
    uint64_t seq, uint32_t block_idx) {
    auto& s = ShardForSeq(seq);
    std::lock_guard<std::mutex> lock(s.mutex);
    return s.GetBlock_nolock(seq, block_idx);
}

inline void SSTableCache::PutBlock(uint64_t seq, uint32_t block_idx,
                                    std::string data) {
    auto& s = ShardForSeq(seq);
    std::lock_guard<std::mutex> lock(s.mutex);
    s.PutBlock_nolock(seq, block_idx, std::move(data));
}

inline void SSTableCache::Invalidate(uint64_t seq) {
    auto& s = ShardForSeq(seq);
    std::lock_guard<std::mutex> lock(s.mutex);
    s.Invalidate_nolock(seq);
}

// ─── Shard internals ─────────────────────────────────────────────

inline bool SSTableCache::Shard::GetBloom_nolock(
    uint64_t seq, BloomFilter& bloom_out) {
    auto it = heavy_map.find(seq);
    if (it == heavy_map.end()) return false;
    bloom_out = it->second.bloom;
    heavy_lru.erase(it->second.lru_it);
    heavy_lru.push_front(seq);
    it->second.lru_it = heavy_lru.begin();
    return true;
}

inline void SSTableCache::Shard::PutBloom_nolock(
    uint64_t seq, const BloomFilter& bloom) {
    auto it = heavy_map.find(seq);
    if (it != heavy_map.end()) {
        it->second.bloom = bloom;
        heavy_lru.erase(it->second.lru_it);
        heavy_lru.push_front(seq);
        it->second.lru_it = heavy_lru.begin();
        return;
    }
    while (heavy_map.size() >= max_meta && !heavy_lru.empty()) {
        heavy_map.erase(heavy_lru.back());
        heavy_lru.pop_back();
    }
    heavy_lru.push_front(seq);
    CachedHeavy ch;
    ch.bloom = bloom;
    ch.lru_it = heavy_lru.begin();
    heavy_map[seq] = std::move(ch);
}

inline bool SSTableCache::Shard::GetBlockOffsets_nolock(
    uint64_t seq, std::vector<uint64_t>& offsets_out,
    std::vector<std::string>& first_keys_out) {
    auto it = heavy_map.find(seq);
    if (it == heavy_map.end()) return false;
    offsets_out = it->second.block_offsets;
    first_keys_out = it->second.block_first_keys;
    heavy_lru.erase(it->second.lru_it);
    heavy_lru.push_front(seq);
    it->second.lru_it = heavy_lru.begin();
    return true;
}

inline void SSTableCache::Shard::PutBlockOffsets_nolock(
    uint64_t seq, const std::vector<uint64_t>& offsets,
    const std::vector<std::string>& first_keys) {
    auto it = heavy_map.find(seq);
    if (it != heavy_map.end()) {
        it->second.block_offsets = offsets;
        it->second.block_first_keys = first_keys;
        heavy_lru.erase(it->second.lru_it);
        heavy_lru.push_front(seq);
        it->second.lru_it = heavy_lru.begin();
        return;
    }
    while (heavy_map.size() >= max_meta && !heavy_lru.empty()) {
        heavy_map.erase(heavy_lru.back());
        heavy_lru.pop_back();
    }
    heavy_lru.push_front(seq);
    CachedHeavy ch;
    ch.block_offsets = offsets;
    ch.block_first_keys = first_keys;
    ch.lru_it = heavy_lru.begin();
    heavy_map[seq] = std::move(ch);
}

inline std::shared_ptr<const std::string> SSTableCache::Shard::GetBlock_nolock(
    uint64_t seq, uint32_t block_idx) {
    uint64_t key = BlockKey(seq, block_idx);
    auto it = block_data.find(key);
    if (it == block_data.end()) return nullptr;
    auto lru_it = block_lru_iters.find(key);
    if (lru_it != block_lru_iters.end()) {
        block_lru.erase(lru_it->second);
        block_lru.push_front(key);
        lru_it->second = block_lru.begin();
    }
    return it->second;
}

inline void SSTableCache::Shard::PutBlock_nolock(
    uint64_t seq, uint32_t block_idx, std::string data) {
    uint64_t key = BlockKey(seq, block_idx);
    auto it = block_data.find(key);
    if (it != block_data.end()) {
        current_bytes -= it->second->size();
        *it->second = std::move(data);
        current_bytes += it->second->size();
        auto lru_it = block_lru_iters.find(key);
        if (lru_it != block_lru_iters.end()) {
            block_lru.erase(lru_it->second);
            block_lru.push_front(key);
            lru_it->second = block_lru.begin();
        }
        return;
    }
    while ((block_data.size() >= max_blocks ||
            current_bytes + data.size() > max_bytes) && !block_lru.empty())
        EvictBlock_nolock();
    block_lru.push_front(key);
    auto sp = std::make_shared<std::string>(std::move(data));
    block_data[key] = sp;
    block_lru_iters[key] = block_lru.begin();
    current_bytes += sp->size();
}

inline void SSTableCache::Shard::Invalidate_nolock(uint64_t seq) {
    heavy_map.erase(seq);
    uint64_t lo = BlockKey(seq, 0);
    uint64_t hi = BlockKey(seq + 1, 0);
    for (auto it = block_data.begin(); it != block_data.end(); ) {
        if (it->first >= lo && it->first < hi) {
            current_bytes -= it->second->size();
            auto lru_it = block_lru_iters.find(it->first);
            if (lru_it != block_lru_iters.end())
                block_lru.erase(lru_it->second);
            block_lru_iters.erase(it->first);
            it = block_data.erase(it);
        } else { ++it; }
    }
    block_lru.remove_if([lo, hi](uint64_t k) { return k >= lo && k < hi; });
}

inline void SSTableCache::Shard::EvictBlock_nolock() {
    if (block_lru.empty()) return;
    uint64_t key = block_lru.back();
    auto it = block_data.find(key);
    if (it != block_data.end()) {
        current_bytes -= it->second->size();
        block_data.erase(it);
        block_lru_iters.erase(key);
    }
    block_lru.pop_back();
}

} // namespace kvdb
