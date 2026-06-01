#pragma once

#include "config.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvdb {

class KVCache {
public:
    explicit KVCache(size_t max_entries = 10000,
                     size_t max_bytes = 16 * 1024 * 1024,
                     size_t num_shards = 16);

    KVCache(const KVCache&) = delete;
    KVCache& operator=(const KVCache&) = delete;

    bool Get(const std::string& key, uint64_t read_ts, std::string& value_out);
    void Put(const std::string& key, const std::string& value, uint64_t ts);
    void Erase(const std::string& key);
    size_t Size() const;
    void Clear();

private:
    struct Shard {
        struct Entry {
            std::string value;
            std::string key;
            uint64_t timestamp;
            Entry* prev = nullptr;
            Entry* next = nullptr;
        };

        mutable std::mutex mutex;
        std::unordered_map<std::string, Entry> map;
        Entry* lru_head = nullptr;
        Entry* lru_tail = nullptr;
        size_t max_entries;
        size_t max_bytes;
        size_t current_bytes = 0;

        void LruRemove(Entry* e);
        void LruPushFront(Entry* e);
        void Touch_nolock(Entry* e);
        Entry* LruTail_nolock() const { return lru_tail; }
        void Evict_nolock();

        bool Get_nolock(const std::string& key, uint64_t read_ts, std::string& value_out);
        void Put_nolock(const std::string& key, const std::string& value, uint64_t ts);
        void Erase_nolock(const std::string& key);
        void Clear_nolock();
    };

    size_t num_shards_;
    std::vector<Shard> shards_;

    Shard& ShardForKey(const std::string& key) {
        return shards_[std::hash<std::string>{}(key) % shards_.size()];
    }
    const Shard& ShardForKey(const std::string& key) const {
        return shards_[std::hash<std::string>{}(key) % shards_.size()];
    }
};

inline KVCache::KVCache(size_t max_entries, size_t max_bytes, size_t num_shards)
    : num_shards_(num_shards), shards_(num_shards) {
    size_t per_shard_entries = std::max(size_t(1), max_entries / num_shards);
    size_t per_shard_bytes   = std::max(size_t(1), max_bytes / num_shards);
    for (auto& s : shards_) {
        s.max_entries = per_shard_entries;
        s.max_bytes   = per_shard_bytes;
    }
}

inline bool KVCache::Get(const std::string& key, uint64_t read_ts, std::string& value_out) {
    auto& s = ShardForKey(key);
    std::lock_guard<std::mutex> lock(s.mutex);
    return s.Get_nolock(key, read_ts, value_out);
}

inline void KVCache::Put(const std::string& key, const std::string& value, uint64_t ts) {
    if (value.size() > Config::kPageSize / 2) return;
    auto& s = ShardForKey(key);
    std::lock_guard<std::mutex> lock(s.mutex);
    s.Put_nolock(key, value, ts);
}

inline void KVCache::Erase(const std::string& key) {
    auto& s = ShardForKey(key);
    std::lock_guard<std::mutex> lock(s.mutex);
    s.Erase_nolock(key);
}

inline size_t KVCache::Size() const {
    size_t total = 0;
    for (auto& s : shards_) {
        std::lock_guard<std::mutex> lock(s.mutex);
        total += s.map.size();
    }
    return total;
}

inline void KVCache::Clear() {
    for (auto& s : shards_) {
        std::lock_guard<std::mutex> lock(s.mutex);
        s.Clear_nolock();
    }
}

// ── Intrusive LRU list operations ──
inline void KVCache::Shard::LruRemove(Entry* e) {
    if (e->prev) e->prev->next = e->next;
    else lru_head = e->next;
    if (e->next) e->next->prev = e->prev;
    else lru_tail = e->prev;
    e->prev = e->next = nullptr;
}

inline void KVCache::Shard::LruPushFront(Entry* e) {
    e->prev = nullptr;
    e->next = lru_head;
    if (lru_head) lru_head->prev = e;
    else lru_tail = e;
    lru_head = e;
}

inline void KVCache::Shard::Touch_nolock(Entry* e) {
    LruRemove(e);
    LruPushFront(e);
}

inline bool KVCache::Shard::Get_nolock(const std::string& key, uint64_t read_ts,
                                        std::string& value_out) {
    auto it = map.find(key);
    if (it == map.end()) return false;
    if (it->second.timestamp > read_ts) return false;
    value_out = it->second.value;
    Touch_nolock(&it->second);
    return true;
}

inline void KVCache::Shard::Put_nolock(const std::string& key,
                                        const std::string& value,
                                        uint64_t ts) {
    auto it = map.find(key);
    if (it != map.end()) {
        current_bytes -= it->second.value.size();
        it->second.value = value;
        it->second.timestamp = ts;
        current_bytes += value.size();
        Touch_nolock(&it->second);
    } else {
        while (map.size() >= max_entries
               || current_bytes + value.size() > max_bytes)
            Evict_nolock();
        Entry e;
        e.value = value;
        e.key = key;
        e.timestamp = ts;
        auto [nit, _] = map.emplace(key, std::move(e));
        LruPushFront(&nit->second);
        current_bytes += value.size();
    }
}

inline void KVCache::Shard::Erase_nolock(const std::string& key) {
    auto it = map.find(key);
    if (it != map.end()) {
        current_bytes -= it->second.value.size();
        LruRemove(&it->second);
        map.erase(it);
    }
}

inline void KVCache::Shard::Clear_nolock() {
    map.clear();
    lru_head = lru_tail = nullptr;
    current_bytes = 0;
}

inline void KVCache::Shard::Evict_nolock() {
    Entry* e = LruTail_nolock();
    if (!e) return;
    LruRemove(e);
    current_bytes -= e->value.size();
    map.erase(e->key);
}

} // namespace kvdb
