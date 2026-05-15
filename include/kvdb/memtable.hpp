#pragma once

#include "config.hpp"
#include "bptree.hpp"
#include "types.hpp"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace kvdb {

class MemTable {
public:
    explicit MemTable(uint64_t id, size_t max_bytes = Config::kDefaultMemTableMaxBytes);

    MemTable(const MemTable&) = delete;
    MemTable& operator=(const MemTable&) = delete;
    MemTable(MemTable&&) = delete;
    MemTable& operator=(MemTable&&) = delete;

    void Insert(const std::string& key, const std::string& value, uint64_t timestamp = 0);

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

private:
    BPlusTree tree_;
    uint64_t id_;
    size_t max_bytes_;
    bool frozen_;
};

} // namespace kvdb
