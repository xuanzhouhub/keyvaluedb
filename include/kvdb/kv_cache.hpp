#pragma once

#include "config.hpp"
#include "sharded_lru.hpp"

#include <cstddef>
#include <cstdint>
#include <string>

namespace kvdb {

struct KVCacheEntry {
    std::string value;
    uint64_t timestamp = 0;
};

static size_t KVCacheEntrySize(const KVCacheEntry& e) { return e.value.size(); }

class KVCache {
public:
    explicit KVCache(size_t max_entries = 10000,
                     size_t max_bytes = 16 * 1024 * 1024,
                     size_t num_shards = 16)
        : lru_(max_entries, max_bytes, num_shards, KVCacheEntrySize) {}

    bool Get(const std::string& key, uint64_t read_ts, std::string& value_out) {
        KVCacheEntry e;
        if (!lru_.Get(key, e)) return false;
        if (e.timestamp > read_ts) return false;
        value_out = std::move(e.value);
        return true;
    }

    void Put(const std::string& key, const std::string& value, uint64_t ts) {
        if (value.size() > Config::kPageSize / 2) return;
        KVCacheEntry e{value, ts};
        lru_.Put(key, e);
    }

    void Erase(const std::string& key) { lru_.Erase(key); }
    size_t Size() const { return lru_.Size(); }

private:
    ShardedLRU<std::string, KVCacheEntry> lru_;
};

} // namespace kvdb
