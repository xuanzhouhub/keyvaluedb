#pragma once

#include "config.hpp"
#include "bptree.hpp"
#include "types.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <unordered_set>
#include <vector>

namespace kvdb {

class MemTable {
public:
    explicit MemTable(uint64_t id, size_t max_bytes = Config::kDefaultMemTableMaxBytes,
                      std::atomic<uint64_t>* fence_source = nullptr)
        : tree_(fence_source), id_(id), max_bytes_(max_bytes), frozen_(false) {}

    void DrainRetired(uint64_t min_ts) { tree_.DrainRetired(min_ts); }
    size_t PendingRetiredSize() const { return tree_.PendingRetiredSize(); }

    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;
    MemTable(MemTable&&) = delete;
    MemTable& operator=(MemTable&&) = delete;

    void Insert(const std::string& key, const std::string& value, uint64_t timestamp = 0, bool is_tombstone = false);

    bool Lookup(const std::string& key, uint64_t read_ts,
                std::string& value_out) const;

    bool IsFull() const;

    size_t ApproximateMemoryUsage() const;

    size_t EntryCount() const;

    void Freeze();

    bool IsFrozen() const;

    std::vector<KeyValuePair> ExportEntries() const;

    uint64_t Id() const { return id_; }
    const BPlusTree& GetTree() const { return tree_; }

    void AddAbortedBatch(uint64_t ts) { tree_.AddAbortedBatch(ts); }
    const std::unordered_set<uint64_t>& AbortedBatches() const { return tree_.AbortedBatches(); }

private:
    BPlusTree tree_;
    uint64_t id_;
    size_t max_bytes_;
    bool frozen_;
};

} // namespace kvdb
