#pragma once

#include <cstdint>
#include <mutex>
#include <set>

namespace kvdb {

class SnapshotTracker {
public:
    void Acquire(uint64_t read_ts) {
        std::lock_guard<std::mutex> lock(mtx_);
        active_.insert(read_ts);
    }

    void Release(uint64_t read_ts) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = active_.find(read_ts);
        if (it != active_.end()) active_.erase(it);
    }

    uint64_t MinActiveTS() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return active_.empty() ? UINT64_MAX : *active_.begin();
    }

private:
    mutable std::mutex mtx_;
    std::multiset<uint64_t> active_;
};

} // namespace kvdb
