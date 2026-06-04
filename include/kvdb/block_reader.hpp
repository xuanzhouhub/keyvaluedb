#pragma once

#include "sstable.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace kvdb {

struct CachedHeavy {
    BloomFilter bloom;
    std::vector<uint64_t> block_offsets;
    std::string block_first_key_buf;
};

class BlockReader {
public:
    virtual ~BlockReader() = default;

    virtual std::shared_ptr<const CachedHeavy> GetHeavy(uint64_t seq) = 0;
    virtual void PutHeavy(uint64_t seq,
                          BloomFilter bloom,
                          std::vector<uint64_t> offsets,
                          std::string first_key_buf) = 0;

    virtual std::shared_ptr<const std::string> GetBlock(
        uint64_t seq, uint32_t block_idx) = 0;
    virtual void PutBlock(uint64_t seq, uint32_t block_idx,
                          std::string data) = 0;

    virtual void Invalidate(uint64_t seq) = 0;
};

} // namespace kvdb
