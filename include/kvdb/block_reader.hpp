#pragma once

#include "sstable.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace kvdb {

class BlockReader {
public:
    virtual ~BlockReader() = default;

    virtual bool GetBloom(uint64_t seq, BloomFilter& bloom_out) = 0;
    virtual void PutBloom(uint64_t seq, const BloomFilter& bloom) = 0;

    virtual bool GetBlockOffsets(uint64_t seq,
                                 std::vector<uint64_t>& offsets_out,
                                 std::vector<std::string>& first_keys_out) = 0;
    virtual void PutBlockOffsets(uint64_t seq,
                                 const std::vector<uint64_t>& offsets,
                                 const std::vector<std::string>& first_keys) = 0;

    virtual bool GetBlock(uint64_t seq, uint32_t block_idx,
                          std::string& data_out, uint32_t& entry_count_out) = 0;
    virtual void PutBlock(uint64_t seq, uint32_t block_idx,
                          const std::string& data, uint32_t entry_count) = 0;

    virtual void Invalidate(uint64_t seq) = 0;
};

} // namespace kvdb
