#pragma once

#include "sstable.hpp"

#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

namespace kvdb {

class SSTableCache {
public:
    SSTableCache(size_t max_blocks = 1024, size_t max_meta = 256,
                 size_t max_bytes = 64 * 1024 * 1024);

    bool GetMetadata(const std::string& filepath,
                     SSTable::Metadata& meta_out);
    void PutMetadata(const std::string& filepath,
                     const SSTable::Metadata& meta);

    bool GetBlock(const std::string& filepath, uint32_t block_idx,
                  std::string& data_out, uint32_t& entry_count_out);
    void PutBlock(const std::string& filepath, uint32_t block_idx,
                  const std::string& data, uint32_t entry_count);

    void Invalidate(const std::string& filepath);

private:
    void EvictBlock();
    static std::string BlockKey(const std::string& filepath, uint32_t idx);

    struct CachedMeta {
        SSTable::Metadata meta;
        std::list<std::string>::iterator lru_it;
    };

    struct CachedBlock {
        std::string data;
        uint32_t entry_count;
        std::list<std::string>::iterator lru_it;
    };

    mutable std::mutex mutex_;
    std::unordered_map<std::string, CachedMeta> meta_map_;
    std::list<std::string> meta_lru_;
    size_t max_meta_;

    std::unordered_map<std::string, CachedBlock> block_map_;
    std::list<std::string> block_lru_;
    size_t max_blocks_;
    size_t max_bytes_;
    size_t current_bytes_;
};

inline SSTableCache::SSTableCache(size_t max_blocks, size_t max_meta,
                                  size_t max_bytes)
    : max_meta_(max_meta), max_blocks_(max_blocks),
      max_bytes_(max_bytes), current_bytes_(0) {}

inline bool SSTableCache::GetMetadata(const std::string& filepath,
                                      SSTable::Metadata& meta_out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = meta_map_.find(filepath);
    if (it == meta_map_.end()) return false;
    meta_out = it->second.meta;
    meta_lru_.erase(it->second.lru_it);
    meta_lru_.push_front(filepath);
    it->second.lru_it = meta_lru_.begin();
    return true;
}

inline void SSTableCache::PutMetadata(const std::string& filepath,
                                      const SSTable::Metadata& meta) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = meta_map_.find(filepath);
    if (it != meta_map_.end()) {
        it->second.meta = meta;
        meta_lru_.erase(it->second.lru_it);
        meta_lru_.push_front(filepath);
        it->second.lru_it = meta_lru_.begin();
        return;
    }
    while (meta_map_.size() >= max_meta_ && !meta_lru_.empty()) {
        meta_map_.erase(meta_lru_.back());
        meta_lru_.pop_back();
    }
    meta_lru_.push_front(filepath);
    CachedMeta cm;
    cm.meta = meta;
    cm.lru_it = meta_lru_.begin();
    meta_map_[filepath] = std::move(cm);
}

inline bool SSTableCache::GetBlock(const std::string& filepath,
                                   uint32_t block_idx,
                                   std::string& data_out,
                                   uint32_t& entry_count_out) {
    std::string key = BlockKey(filepath, block_idx);
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

inline void SSTableCache::PutBlock(const std::string& filepath,
                                   uint32_t block_idx,
                                   const std::string& data,
                                   uint32_t entry_count) {
    std::string key = BlockKey(filepath, block_idx);
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

inline void SSTableCache::Invalidate(const std::string& filepath) {
    std::lock_guard<std::mutex> lock(mutex_);
    meta_map_.erase(filepath);
    std::string prefix = filepath + ":";
    for (auto it = block_map_.begin(); it != block_map_.end(); ) {
        if (it->first.compare(0, prefix.size(), prefix) == 0) {
            current_bytes_ -= it->second.data.size();
            block_lru_.erase(it->second.lru_it);
            it = block_map_.erase(it);
        } else { ++it; }
    }
}

inline std::string SSTableCache::BlockKey(const std::string& filepath,
                                          uint32_t idx) {
    return filepath + ":" + std::to_string(idx);
}

inline void SSTableCache::EvictBlock() {
    if (block_lru_.empty()) return;
    const auto& key = block_lru_.back();
    auto it = block_map_.find(key);
    if (it != block_map_.end()) {
        current_bytes_ -= it->second.data.size();
        block_map_.erase(it);
    }
    block_lru_.pop_back();
}

} // namespace kvdb
