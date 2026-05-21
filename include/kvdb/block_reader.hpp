#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace kvdb {

class BlockReader {
public:
    virtual ~BlockReader() = default;

    virtual std::shared_ptr<const std::string> GetBlock(
        uint64_t seq, uint32_t block_idx) = 0;

    virtual void PutBlock(uint64_t seq, uint32_t block_idx,
                          std::string data, uint32_t entry_count) = 0;

    virtual void Invalidate(uint64_t seq) = 0;
};

} // namespace kvdb
