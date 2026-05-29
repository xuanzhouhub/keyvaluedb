#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <set>
#include <thread>

namespace kvdb {

class SnapshotTracker {
public:
    static constexpr int kSlots = 256;

    void Acquire(uint64_t read_ts) {
        uint64_t zero = 0;
        int start = static_cast<int>(std::hash<std::thread::id>{}(std::this_thread::get_id())) & (kSlots - 1);
        for (int t = 0; t < kSlots; ++t) {
            int i = (start + t) & (kSlots - 1);
            if (slots_[i].compare_exchange_strong(zero, read_ts,
                    std::memory_order_acquire, std::memory_order_relaxed)) {
                slot_ = i;
                return;
            }
            zero = 0;
        }
        fallback_used_.store(true, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lock(mtx_);
        active_.insert(read_ts);
        slot_ = -1;
    }

    void Release(uint64_t read_ts) {
        if (slot_ >= 0) {
            slots_[slot_].store(0, std::memory_order_release);
            return;
        }
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = active_.find(read_ts);
        if (it != active_.end()) active_.erase(it);
        if (active_.empty()) fallback_used_.store(false, std::memory_order_relaxed);
    }

    uint64_t MinActiveTS() const {
        uint64_t min_ts = UINT64_MAX;
        for (int i = 0; i < kSlots; ++i) {
            uint64_t ts = slots_[i].load(std::memory_order_acquire);
            if (ts != 0 && ts < min_ts) min_ts = ts;
        }
        if (fallback_used_.load(std::memory_order_relaxed)) {
            std::lock_guard<std::mutex> lock(mtx_);
            if (!active_.empty()) {
                uint64_t set_min = *active_.begin();
                if (set_min < min_ts) min_ts = set_min;
            }
        }
        return min_ts;
    }

private:
    std::atomic<uint64_t> slots_[kSlots] = {};
    static thread_local int slot_;
    std::atomic<bool> fallback_used_{false};
    mutable std::mutex mtx_;
    std::multiset<uint64_t> active_;
};

inline thread_local int SnapshotTracker::slot_ = -2;

} // namespace kvdb
