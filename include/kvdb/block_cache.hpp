#pragma once

#include "block_reader.hpp"
#include "internal/flat_cache.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace kvdb {

class SSTableCache : public BlockReader {
public:
    SSTableCache(size_t max_blocks = 1024,
                 size_t max_meta = 256,
                 size_t max_bytes = 64 * 1024 * 1024,
                 size_t num_shards = 16)
        : heavy_(max_meta, max_bytes, num_shards),
          blocks_(max_blocks, max_bytes, num_shards) {}

    std::shared_ptr<const CachedHeavy> GetHeavy(uint64_t seq) override {
        return heavy_.Get(seq);
    }

    void PutHeavy(uint64_t seq, BloomFilter bloom,
                  std::vector<uint64_t> offsets,
                  std::string first_key_buf,
                  std::vector<uint32_t> first_key_offsets,
                  const std::unordered_set<uint64_t>& aborted) override {
        auto sp = std::make_shared<CachedHeavy>();
        sp->bloom = std::move(bloom);
        sp->block_offsets = std::move(offsets);
        sp->block_first_key_buf = std::move(first_key_buf);
        sp->first_key_offsets = std::move(first_key_offsets);
        sp->aborted_batch_ts = aborted;
        heavy_.Put(seq, sp);
    }

    std::shared_ptr<const std::string> GetBlock(
        uint64_t seq, uint32_t block_idx) override {
        return blocks_.Get(BlockKey(seq, block_idx));
    }

    void PutBlock(uint64_t seq, uint32_t block_idx,
                  std::string data) override {
        blocks_.Put(BlockKey(seq, block_idx),
                    std::make_shared<std::string>(std::move(data)));
    }

    void Invalidate(uint64_t seq) override {
        heavy_.Erase(seq);
        uint64_t lo = BlockKey(seq, 0);
        uint64_t hi = BlockKey(seq + 1, 0);
        blocks_.EraseIf([lo, hi](uint64_t key) { return key >= lo && key < hi; });
    }

private:
    static uint64_t BlockKey(uint64_t seq, uint32_t idx) {
        return (seq << 32) | idx;
    }

    struct SizeBytes {
        size_t operator()(const CachedHeavy& h) const {
            return h.bloom.Data().size() * sizeof(uint8_t)
                 + h.block_offsets.size() * sizeof(uint64_t)
                 + h.block_first_key_buf.size()
                 + h.first_key_offsets.size() * sizeof(uint32_t)
                 + h.aborted_batch_ts.size() * sizeof(uint64_t);
        }
    };

    internal::FlatCache<uint64_t, CachedHeavy, std::hash<uint64_t>,
                        std::equal_to<uint64_t>, SizeBytes> heavy_;
    internal::FlatCache<uint64_t, std::string> blocks_;
};

} // namespace kvdb
