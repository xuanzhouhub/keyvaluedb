#pragma once

#include "config.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <string>
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
    struct Entry {
        std::string value;
        std::string key;
        uint64_t timestamp;
        Entry* prev = nullptr;
        Entry* next = nullptr;
    };

    struct Shard {
        struct Bucket { uint32_t hash = 0; uint32_t idx = kEmpty; };
        static constexpr uint32_t kEmpty = ~0u;

        mutable std::mutex mutex;
        std::vector<Bucket> buckets;
        std::vector<Entry> entries;
        Entry* lru_head = nullptr;
        Entry* lru_tail = nullptr;
        size_t entry_count = 0;
        size_t max_entries;
        size_t max_bytes;
        size_t current_bytes = 0;

        void Init(size_t per_e, size_t per_b);
        uint32_t FindSlot(const std::string& key, uint32_t hash) const;
        void Rehash();

        void LruRemove(Entry* e);
        void LruPushFront(Entry* e);
        void Evict_nolock();

        bool Get_nolock(const std::string& key, uint64_t read_ts, std::string& value_out);
        void Put_nolock(const std::string& key, const std::string& value, uint64_t ts);
        void Erase_nolock(const std::string& key);
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
    size_t per_e = std::max(size_t(1), max_entries / num_shards);
    size_t per_b = std::max(size_t(1), max_bytes / num_shards);
    for (auto& s : shards_) s.Init(per_e, per_b);
}

inline void KVCache::Shard::Init(size_t per_e, size_t per_b) {
    max_entries = per_e;
    max_bytes   = per_b;
    size_t bc = 1;
    while (bc < per_e * 2) bc *= 2;
    buckets.resize(bc);
    for (auto& b : buckets) b.idx = kEmpty;
    entries.resize(per_e);
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
        total += s.entry_count;
    }
    return total;
}

inline void KVCache::Clear() {
    for (auto& s : shards_) {
        std::lock_guard<std::mutex> lock(s.mutex);
        s.entry_count = 0;
        s.current_bytes = 0;
        s.lru_head = s.lru_tail = nullptr;
        for (auto& b : s.buckets) b.idx = Shard::kEmpty;
    }
}

// ─── LRU ───
inline void KVCache::Shard::LruRemove(Entry* e) {
    if (e->prev) e->prev->next = e->next;
    else lru_head = e->next;
    if (e->next) e->next->prev = e->prev;
    else lru_tail = e->prev;
}
inline void KVCache::Shard::LruPushFront(Entry* e) {
    e->prev = nullptr;
    e->next = lru_head;
    if (lru_head) lru_head->prev = e;
    else lru_tail = e;
    lru_head = e;
}

// ─── Open-addressing hash table (power-of-2 buckets) ───
inline uint32_t KVCache::Shard::FindSlot(const std::string& key, uint32_t hash) const {
    uint32_t mask = static_cast<uint32_t>(buckets.size() - 1);
    uint32_t i = hash & mask;
    while (buckets[i].idx != kEmpty) {
        if (buckets[i].hash == hash && entries[buckets[i].idx].key == key)
            return i;
        i = (i + 1) & mask;
    }
    return kEmpty;
}
inline void KVCache::Shard::Rehash() {
    std::vector<Bucket> old = std::move(buckets);
    size_t new_sz = std::max(old.size() * 2, size_t(8));
    buckets.resize(new_sz);
    for (auto& b : buckets) b.idx = kEmpty;
    uint32_t mask = static_cast<uint32_t>(buckets.size() - 1);
    for (auto& b : old) {
        if (b.idx == kEmpty) continue;
        uint32_t i = b.hash & mask;
        while (buckets[i].idx != kEmpty) i = (i + 1) & mask;
        buckets[i] = b;
    }
}

inline bool KVCache::Shard::Get_nolock(const std::string& key, uint64_t read_ts, std::string& value_out) {
    uint32_t hash = static_cast<uint32_t>(std::hash<std::string>{}(key));
    uint32_t slot = FindSlot(key, hash);
    if (slot == kEmpty) return false;
    Entry& e = entries[buckets[slot].idx];
    if (e.timestamp > read_ts) return false;
    value_out = e.value;
    LruRemove(&e);
    LruPushFront(&e);
    return true;
}

inline void KVCache::Shard::Put_nolock(const std::string& key, const std::string& value, uint64_t ts) {
    uint32_t hash = static_cast<uint32_t>(std::hash<std::string>{}(key));
    uint32_t slot = FindSlot(key, hash);
    if (slot != kEmpty) {
        Entry& e = entries[buckets[slot].idx];
        current_bytes -= e.value.size();
        e.value = value;
        e.timestamp = ts;
        current_bytes += value.size();
        LruRemove(&e);
        LruPushFront(&e);
        return;
    }
    while (entry_count >= max_entries || current_bytes + value.size() > max_bytes)
        Evict_nolock();
    if (entry_count * 2 >= buckets.size()) Rehash();
    hash = static_cast<uint32_t>(std::hash<std::string>{}(key));
    uint32_t idx = static_cast<uint32_t>(entry_count++);
    entries[idx].key = key;
    entries[idx].value = value;
    entries[idx].timestamp = ts;
    entries[idx].prev = entries[idx].next = nullptr;

    uint32_t mask = static_cast<uint32_t>(buckets.size() - 1);
    uint32_t i = hash & mask;
    while (buckets[i].idx != kEmpty) i = (i + 1) & mask;
    buckets[i].hash = hash;
    buckets[i].idx = idx;

    LruPushFront(&entries[idx]);
    current_bytes += value.size();
}

inline void KVCache::Shard::Erase_nolock(const std::string& key) {
    uint32_t hash = static_cast<uint32_t>(std::hash<std::string>{}(key));
    uint32_t slot = FindSlot(key, hash);
    if (slot == kEmpty) return;
    Entry& e = entries[buckets[slot].idx];
    current_bytes -= e.value.size();
    LruRemove(&e);
    buckets[slot].idx = kEmpty;
}

inline void KVCache::Shard::Evict_nolock() {
    if (!lru_tail) return;
    uint32_t hash = static_cast<uint32_t>(std::hash<std::string>{}(lru_tail->key));
    uint32_t slot = FindSlot(lru_tail->key, hash);
    if (slot != kEmpty) {
        current_bytes -= lru_tail->value.size();
        buckets[slot].idx = kEmpty;
    }
    LruRemove(lru_tail);
}

} // namespace kvdb
