#pragma once

#include "config.hpp"

#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>

namespace kvdb {

class KVCache {
public:
    explicit KVCache(size_t max_entries = 10000,
                     size_t max_bytes = 16 * 1024 * 1024);

    KVCache(const KVCache&) = delete;
    KVCache& operator=(const KVCache&) = delete;

    bool Get(const std::string& key, uint64_t read_ts, std::string& value_out);

    void Put(const std::string& key, const std::string& value, uint64_t ts);

    void Erase(const std::string& key);

    size_t Size() const;

    void Clear();

private:
    void Touch(const std::string& key);
    void Evict();

    struct Entry {
        std::string value;
        uint64_t timestamp;
        std::list<std::string>::iterator lru_it;
    };

    mutable std::mutex mutex_;
    std::unordered_map<std::string, Entry> map_;
    std::list<std::string> lru_;
    size_t max_entries_;
    size_t max_bytes_;
    size_t current_bytes_;
};

inline KVCache::KVCache(size_t max_entries, size_t max_bytes)
    : max_entries_(max_entries), max_bytes_(max_bytes), current_bytes_(0) {}

inline bool KVCache::Get(const std::string& key, uint64_t read_ts,
                         std::string& value_out) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) return false;
    if (it->second.timestamp > read_ts) return false;
    value_out = it->second.value;
    Touch(key);
    return true;
}

inline void KVCache::Put(const std::string& key, const std::string& value,
                         uint64_t ts) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (value.size() > Config::kPageSize / 2) return;
    auto it = map_.find(key);
    if (it != map_.end()) {
        current_bytes_ -= it->second.value.size();
        it->second.value = value;
        it->second.timestamp = ts;
        current_bytes_ += value.size();
        Touch(key);
    } else {
        while (map_.size() >= max_entries_ || current_bytes_ + value.size() > max_bytes_)
            Evict();
        lru_.push_front(key);
        Entry e;
        e.value = value;
        e.timestamp = ts;
        e.lru_it = lru_.begin();
        map_[key] = std::move(e);
        current_bytes_ += value.size();
    }
}

inline void KVCache::Erase(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = map_.find(key);
    if (it != map_.end()) {
        current_bytes_ -= it->second.value.size();
        lru_.erase(it->second.lru_it);
        map_.erase(it);
    }
}

inline size_t KVCache::Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return map_.size();
}

inline void KVCache::Clear() {
    std::lock_guard<std::mutex> lock(mutex_);
    map_.clear();
    lru_.clear();
    current_bytes_ = 0;
}

inline void KVCache::Touch(const std::string& key) {
    auto it = map_.find(key);
    if (it != map_.end()) {
        lru_.erase(it->second.lru_it);
        lru_.push_front(key);
        it->second.lru_it = lru_.begin();
    }
}

inline void KVCache::Evict() {
    if (lru_.empty()) return;
    const auto& key = lru_.back();
    auto it = map_.find(key);
    if (it != map_.end()) {
        current_bytes_ -= it->second.value.size();
        map_.erase(it);
    }
    lru_.pop_back();
}

} // namespace kvdb
