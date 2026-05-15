#include "kvdb/memtable.hpp"

namespace kvdb {

MemTable::MemTable(uint64_t id, size_t max_bytes)
    : id_(id)
    , max_bytes_(max_bytes)
    , frozen_(false) {}

void MemTable::Insert(const std::string& key, const std::string& value, uint64_t timestamp) {
    if (frozen_) return;
    tree_.Insert(key, value, timestamp);
}

bool MemTable::Lookup(const std::string& key, uint64_t read_ts,
                      std::string& value_out) const {
    return tree_.Lookup(key, read_ts, value_out);
}

bool MemTable::IsFull() const {
    return tree_.MemoryUsage() >= max_bytes_;
}

size_t MemTable::ApproximateMemoryUsage() const {
    return tree_.MemoryUsage();
}

size_t MemTable::EntryCount() const {
    return tree_.Size();
}

void MemTable::Freeze() {
    frozen_ = true;
}

bool MemTable::IsFrozen() const {
    return frozen_;
}

std::vector<KeyValuePair> MemTable::ExportEntries() const {
    std::vector<KeyValuePair> entries;
    tree_.Export(entries);
    return entries;
}

} // namespace kvdb
