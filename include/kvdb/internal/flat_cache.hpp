#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

namespace kvdb {
namespace internal {

template <typename Key, typename Value, typename Hash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>,
          typename SizeBytes = std::integral_constant<size_t, 0>>
class FlatCache {
public:
    FlatCache(size_t max_entries, size_t max_bytes, size_t num_shards)
        : num_shards_(num_shards), shards_(num_shards) {
        size_t per_e = std::max(size_t(1), max_entries / num_shards);
        size_t per_b = std::max(size_t(1), max_bytes / num_shards);
        for (auto& s : shards_) {
            s.max_entries = per_e;
            s.max_bytes   = per_b;
            s.Init(per_e, per_b);
        }
    }

    std::shared_ptr<const Value> Get(const Key& key) {
        auto& s = Shard(key);
        std::lock_guard<std::mutex> lock(s.mutex);
        return s.Get_nolock(key);
    }

    void Put(const Key& key, std::shared_ptr<const Value> val) {
        auto& s = Shard(key);
        std::lock_guard<std::mutex> lock(s.mutex);
        s.Put_nolock(key, std::move(val));
    }

    void Erase(const Key& key) {
        auto& s = Shard(key);
        std::lock_guard<std::mutex> lock(s.mutex);
        s.Erase_nolock(key);
    }

    void Clear() {
        for (auto& s : shards_) {
            std::lock_guard<std::mutex> lock(s.mutex);
            s.Clear_nolock();
        }
    }

    template <typename Pred>
    void EraseIf(Pred pred) {
        for (auto& s : shards_) {
            std::lock_guard<std::mutex> lock(s.mutex);
            s.EraseIf_nolock(pred);
        }
    }

private:
    static constexpr uint32_t kEmpty = ~0u;
    static constexpr uint32_t kTombstone = kEmpty - 1;

    struct Bucket { size_t hash = 0; uint32_t idx = kEmpty; };

    struct Entry {
        Key key;
        std::shared_ptr<const Value> value;
        Entry* prev = nullptr;
        Entry* next = nullptr;
    };

    struct Shard {
        mutable std::mutex mutex;
        std::vector<Bucket> buckets;
        std::vector<Entry> entries;
        Entry* lru_head = nullptr;
        Entry* lru_tail = nullptr;
        size_t max_entries = 0;
        size_t max_bytes = 0;
        size_t current_bytes = 0;
        size_t entry_count = 0;

        void Init(size_t per_e, size_t per_b) {
            max_entries = per_e;
            max_bytes   = per_b;
            size_t bc = 1;
            while (bc < per_e * 2) bc *= 2;
            buckets.resize(bc);
            for (auto& b : buckets) b.idx = kEmpty;
            entries.resize(per_e);
        }

        uint32_t FindSlot(const Key& key, size_t hash) const {
            uint32_t mask = static_cast<uint32_t>(buckets.size() - 1);
            uint32_t i = static_cast<uint32_t>(hash & mask);
            uint32_t steps = 0;
            while (buckets[i].idx != kEmpty) {
                if (++steps > buckets.size()) return kEmpty;
                if (buckets[i].idx != kTombstone &&
                    buckets[i].hash == hash && KeyEqual{}(entries[buckets[i].idx].key, key))
                    return i;
                i = (i + 1) & mask;
            }
            return kEmpty;
        }

        void Rehash() {
            std::vector<Bucket> old = std::move(buckets);
            size_t new_sz = std::max(old.size() * 2, size_t(8));
            buckets.resize(new_sz);
            for (auto& b : buckets) b.idx = kEmpty;
            uint32_t mask = static_cast<uint32_t>(buckets.size() - 1);
            for (auto& b : old) {
                if (b.idx == kEmpty || b.idx == kTombstone) continue;
                uint32_t i = static_cast<uint32_t>(b.hash & mask);
                while (buckets[i].idx != kEmpty) i = (i + 1) & mask;
                buckets[i] = b;
            }
        }

        void LruRemove(Entry* e) {
            if (e->prev) e->prev->next = e->next;
            else lru_head = e->next;
            if (e->next) e->next->prev = e->prev;
            else lru_tail = e->prev;
        }
        void LruPushFront(Entry* e) {
            e->prev = nullptr;
            e->next = lru_head;
            if (lru_head) lru_head->prev = e;
            else lru_tail = e;
            lru_head = e;
        }

        uint32_t Evict_nolock() {
            if (!lru_tail) return kEmpty;
            size_t hash = Hash{}(lru_tail->key);
            uint32_t slot = FindSlot(lru_tail->key, hash);
            uint32_t idx = kEmpty;
            if (slot != kEmpty) {
                idx = buckets[slot].idx;
                current_bytes -= ByteSize(lru_tail->value);
                buckets[slot].idx = kTombstone;
            }
            Entry* tail = lru_tail;
            LruRemove(tail);
            return idx;
        }

        std::shared_ptr<const Value> Get_nolock(const Key& key) {
            size_t hash = Hash{}(key);
            uint32_t slot = FindSlot(key, hash);
            if (slot == kEmpty) return nullptr;
            Entry* e = &entries[buckets[slot].idx];
            auto result = e->value;
            LruRemove(e);
            LruPushFront(e);
            return result;
        }

        void Put_nolock(const Key& key, std::shared_ptr<const Value> val) {
            size_t hash0 = Hash{}(key);
            uint32_t slot = FindSlot(key, hash0);
            size_t new_bytes = ByteSize(val);

            if (slot != kEmpty) {
                Entry& e = entries[buckets[slot].idx];
                current_bytes -= ByteSize(e.value);
                e.value = std::move(val);
                current_bytes += new_bytes;
                LruRemove(&e);
                LruPushFront(&e);
                return;
            }

            uint32_t idx = kEmpty;
            while (entry_count >= max_entries || current_bytes + new_bytes > max_bytes) {
                uint32_t evicted = Evict_nolock();
                if (evicted == kEmpty) return;
                idx = evicted;
                entry_count--;
            }
            if (entry_count * 2 >= buckets.size()) Rehash();

            if (idx == kEmpty) idx = static_cast<uint32_t>(entry_count++);
            else entry_count++;

            entries[idx].key = key;
            entries[idx].value = std::move(val);
            entries[idx].prev = entries[idx].next = nullptr;

            {
                size_t hh = Hash{}(entries[idx].key);
                uint32_t mask = static_cast<uint32_t>(buckets.size() - 1);
                uint32_t i = static_cast<uint32_t>(hh & mask);
                uint32_t steps = 0;
                while (buckets[i].idx != kEmpty && buckets[i].idx != kTombstone) {
                    i = (i + 1) & mask;
                    if (++steps > buckets.size()) { std::fprintf(stderr, "INSERT STUCK hh=%zu\n", hh); std::exit(1); }
                }
                buckets[i].hash = hh;
                buckets[i].idx = idx;
            }

            LruPushFront(&entries[idx]);
            current_bytes += new_bytes;
        }

        void Erase_nolock(const Key& key) {
            size_t hash = Hash{}(key);
            uint32_t slot = FindSlot(key, hash);
            if (slot == kEmpty) return;
            Entry& e = entries[buckets[slot].idx];
            current_bytes -= ByteSize(e.value);
            LruRemove(&e);
            buckets[slot].idx = kTombstone;
        }

        void Clear_nolock() {
            entry_count = 0;
            current_bytes = 0;
            lru_head = lru_tail = nullptr;
            for (auto& b : buckets) { b.idx = kEmpty; b.hash = 0; }
        }

        template <typename Pred>
        void EraseIf_nolock(Pred pred) {
            for (size_t i = 0; i < entry_count; ) {
                // Entries at [0, entry_count) are active
                Entry& e = entries[i];
                if (pred(e.key)) {
                    size_t hash = Hash{}(e.key);
                    uint32_t slot = FindSlot(e.key, hash);
                    if (slot != kEmpty) buckets[slot].idx = kTombstone;
                    current_bytes -= ByteSize(e.value);
                    LruRemove(&e);
                    // Move last active entry into this slot
                    if (i != entry_count - 1) {
                        Entry& last = entries[entry_count - 1];
                        // Update bucket pointing to last
                        size_t lh = Hash{}(last.key);
                        uint32_t ls = FindSlot(last.key, lh);
                        if (ls != kEmpty) buckets[ls].idx = static_cast<uint32_t>(i);
                        // Update LRU pointers
                        if (last.prev) last.prev->next = &entries[i];
                        else lru_head = &entries[i];
                        if (last.next) last.next->prev = &entries[i];
                        else lru_tail = &entries[i];
                        // Move data
                        entries[i].key = std::move(last.key);
                        entries[i].value = std::move(last.value);
                        entries[i].prev = last.prev;
                        entries[i].next = last.next;
                    }
                    entry_count--;
                } else {
                    i++;
                }
            }
        }

        static size_t ByteSize(const std::shared_ptr<const Value>& v) {
            if constexpr (std::is_invocable_v<SizeBytes, const Value&>)
                return v ? SizeBytes{}(*v) : 0;
            else
                return v ? sizeof(Value) : 0;
        }
    };

    size_t num_shards_;
    std::vector<Shard> shards_;

    Shard& Shard(const Key& key) {
        return shards_[Hash{}(key) % shards_.size()];
    }
};

} // namespace internal
} // namespace kvdb
