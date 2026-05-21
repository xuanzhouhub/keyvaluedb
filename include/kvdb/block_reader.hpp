#pragma once

#include "sstable.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace kvdb {

class BlockReader {
public:
    virtual ~BlockReader() = default;

    virtual bool GetMetadata(const std::string& filepath,
                             SSTable::Metadata& meta_out) = 0;
    virtual void PutMetadata(const std::string& filepath,
                             const SSTable::Metadata& meta) = 0;

    virtual bool GetBlock(const std::string& filepath, uint32_t block_idx,
                          std::string& data_out, uint32_t& entry_count_out) = 0;
    virtual void PutBlock(const std::string& filepath, uint32_t block_idx,
                          const std::string& data, uint32_t entry_count) = 0;

    virtual void Invalidate(const std::string& filepath) = 0;
};

} // namespace kvdb
