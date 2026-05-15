#pragma once

#include "sstable.hpp"

#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <vector>

namespace kvdb {

class Manifest {
public:
    Manifest(const std::string& filepath);

    ~Manifest();

    Manifest(const Manifest&) = delete;
    Manifest& operator=(const Manifest&) = delete;
    Manifest(Manifest&&) = delete;
    Manifest& operator=(Manifest&&) = delete;

    void AddSSTable(uint64_t seq, const SSTable::Metadata& meta);

    void RemoveSSTable(uint64_t seq);

    void Sync();

    std::vector<SSTable::Metadata> Recover();

private:
    void WriteRecord(uint8_t type, const std::vector<char>& payload);

    std::string filepath_;
    FILE* file_ = nullptr;
    mutable std::mutex mutex_;
};

} // namespace kvdb
