#pragma once

#include <cstddef>
#include <cstdint>
#include <list>
#include <mutex>
#include <unordered_map>

namespace kvdb {

template<typename K, typename V>
class ShardedLRU {
public:
    explicit ShardedLRU(size_t max_entries = 10000,
                        size_t max_bytes   = 16 * 1024 * 1024,
                        size_t num_shards  = 16,
                        size_t (*size_fn)(const V&) = nullptr);

    bool Get(const K& key, V& value_out);
    void Put(const K& key, const V& value);
    void Erase(const K& key);
    size_t Size() const;

private:
    struct Entry {
        V value;
        typename std::list<K>::iterator lru_it;
    };

    struct Shard {
        mutable std::mutex mutex;
        std::unordered_map<K, Entry> map;
        std::list<K> lru;
        size_t max_entries;
        size_t max_bytes;
        size_t current_bytes = 0;
        size_t (*size_fn)(const V&) = nullptr;

        void Evict_nolock();
        bool Get_nolock(const K& key, V& value_out);
        void Put_nolock(const K& key, const V& value);
        void Erase_nolock(const K& key);
    };

    size_t num_shards_;
    std::vector<Shard> shards_;
    size_t (*size_fn_)(const V&);

    Shard& ShardForKey(const K& key) {
        return shards_[std::hash<K>{}(key) % num_shards_];
    }
};

// ─── Constructor ─────────────────────────────────────────────────
template<typename K, typename V>
ShardedLRU<K,V>::ShardedLRU(size_t max_entries, size_t max_bytes,
                             size_t num_shards, size_t (*sf)(const V&))
    : num_shards_(num_shards), shards_(num_shards), size_fn_(sf) {
    size_t per_e = std::max(size_t(1), max_entries / num_shards);
    size_t per_b = std::max(size_t(1), max_bytes  / num_shards);
    for (auto& s : shards_) {
        s.max_entries = per_e;
        s.max_bytes   = per_b;
        s.size_fn     = sf;
    }
}

// ─── Public API ──────────────────────────────────────────────────
template<typename K, typename V>
bool ShardedLRU<K,V>::Get(const K& key, V& value_out) {
    auto& s = ShardForKey(key);
    std::lock_guard<std::mutex> lock(s.mutex);
    return s.Get_nolock(key, value_out);
}

template<typename K, typename V>
void ShardedLRU<K,V>::Put(const K& key, const V& value) {
    auto& s = ShardForKey(key);
    std::lock_guard<std::mutex> lock(s.mutex);
    s.Put_nolock(key, value);
}

template<typename K, typename V>
void ShardedLRU<K,V>::Erase(const K& key) {
    auto& s = ShardForKey(key);
    std::lock_guard<std::mutex> lock(s.mutex);
    s.Erase_nolock(key);
}

template<typename K, typename V>
size_t ShardedLRU<K,V>::Size() const {
    size_t total = 0;
    for (auto& s : shards_) {
        std::lock_guard<std::mutex> lock(s.mutex);
        total += s.map.size();
    }
    return total;
}

// ─── Shard internals ─────────────────────────────────────────────
template<typename K, typename V>
void ShardedLRU<K,V>::Shard::Evict_nolock() {
    if (lru.empty()) return;
    K key = lru.back();
    auto it = map.find(key);
    if (it != map.end()) {
        if (size_fn) current_bytes -= size_fn(it->second.value);
        map.erase(it);
    }
    lru.pop_back();
}

template<typename K, typename V>
bool ShardedLRU<K,V>::Shard::Get_nolock(const K& key, V& value_out) {
    auto it = map.find(key);
    if (it == map.end()) return false;
    value_out = it->second.value;
    lru.erase(it->second.lru_it);
    lru.push_front(key);
    it->second.lru_it = lru.begin();
    return true;
}

template<typename K, typename V>
void ShardedLRU<K,V>::Shard::Put_nolock(const K& key, const V& value) {
    auto it = map.find(key);
    if (it != map.end()) {
        if (size_fn) current_bytes -= size_fn(it->second.value);
        it->second.value = value;
        if (size_fn) current_bytes += size_fn(value);
        lru.erase(it->second.lru_it);
        lru.push_front(key);
        it->second.lru_it = lru.begin();
        return;
    }
    size_t val_sz = size_fn ? size_fn(value) : 0;
    while ((map.size() >= max_entries || current_bytes + val_sz > max_bytes) && !lru.empty())
        Evict_nolock();
    lru.push_front(key);
    map[key] = {value, lru.begin()};
    if (size_fn) current_bytes += val_sz;
}

template<typename K, typename V>
void ShardedLRU<K,V>::Shard::Erase_nolock(const K& key) {
    auto it = map.find(key);
    if (it == map.end()) return;
    if (size_fn) current_bytes -= size_fn(it->second.value);
    lru.erase(it->second.lru_it);
    map.erase(it);
}

} // namespace kvdb
