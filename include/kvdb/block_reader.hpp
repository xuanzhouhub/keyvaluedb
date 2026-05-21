#pragma once

#include "sstable.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace kvdb {

class BlockReader {
public:
    virtual ~BlockReader() = default;

    virtual std::shared_ptr<const std::string> GetBlock(
        uint64_t seq, uint32_t block_idx) = 0;

    virtual void PutBlock(uint64_t seq, uint32_t block_idx,
                          std::string data, uint32_t entry_count) = 0;

    virtual bool GetBloom(uint64_t seq, BloomFilter& bloom_out) = 0;
    virtual void PutBloom(uint64_t seq, const BloomFilter& bloom) = 0;

    virtual bool GetBlockOffsets(uint64_t seq,
                                 std::vector<uint64_t>& offsets_out,
                                 std::vector<std::string>& first_keys_out) = 0;
    virtual void PutBlockOffsets(uint64_t seq,
                                 const std::vector<uint64_t>& offsets,
                                 const std::vector<std::string>& first_keys) = 0;

    virtual void Invalidate(uint64_t seq) = 0;
};

class NullBlockReader : public BlockReader {
public:
    std::shared_ptr<const std::string> GetBlock(uint64_t, uint32_t) override
        { return nullptr; }
    void PutBlock(uint64_t, uint32_t, std::string, uint32_t) override {}
    bool GetBloom(uint64_t, BloomFilter&) override { return false; }
    void PutBloom(uint64_t, const BloomFilter&) override {}
    bool GetBlockOffsets(uint64_t, std::vector<uint64_t>&,
                         std::vector<std::string>&) override { return false; }
    void PutBlockOffsets(uint64_t, const std::vector<uint64_t>&,
                         const std::vector<std::string>&) override {}
    void Invalidate(uint64_t) override {}
};

} // namespace kvdb
