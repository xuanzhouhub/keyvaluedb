#pragma once

#include "bloom.hpp"
#include "memtable.hpp"

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace kvdb {

class BlockReader;

class SSTable {
public:
    static void Write(const std::string& filepath, const std::vector<KeyValuePair>& entries,
                      const std::unordered_set<uint64_t>* aborted = nullptr);
    static void WriteFromWalk(const std::string& filepath, BPlusTree::MemTableWalk& walk,
                              size_t entry_count, BlockReader* cache = nullptr,
                              uint64_t manifest_seq = 0,
                              const std::unordered_set<uint64_t>* aborted = nullptr);

    struct Metadata {
        std::string filepath;
        uint64_t manifest_seq = 0;
        size_t entry_count = 0;
        uint32_t min_key_len = 0;
        uint32_t max_key_len = 0;
        uint64_t file_size = 0;
        uint64_t source_table_id = 0;
        int level = 0;
        std::string min_key;
        std::string max_key;
        BloomFilter bloom;
        std::vector<uint64_t> block_offsets;
        std::string block_first_key_buf;
        std::unordered_set<uint64_t> aborted_batch_ts;

        size_t FirstKeyCount() const { return block_offsets.size(); }
        bool FirstKeysEmpty() const { return block_first_key_buf.empty(); }
        std::string FirstKey(size_t i) const {
            const char* p = block_first_key_buf.data();
            const char* end = p + block_first_key_buf.size();
            for (size_t j = 0; j < i && p < end; ++j) {
                uint16_t kl = static_cast<uint16_t>(static_cast<uint8_t>(p[0]) | (static_cast<uint8_t>(p[1]) << 8));
                p += 2 + kl;
            }
            if (p < end) {
                uint16_t kl = static_cast<uint16_t>(static_cast<uint8_t>(p[0]) | (static_cast<uint8_t>(p[1]) << 8));
                return std::string(p + 2, kl);
            }
            return {};
        }
        std::string_view FirstKeyView(size_t i) const {
            const char* p = block_first_key_buf.data();
            const char* end = p + block_first_key_buf.size();
            for (size_t j = 0; j < i && p < end; ++j) {
                uint16_t kl = static_cast<uint16_t>(static_cast<uint8_t>(p[0]) | (static_cast<uint8_t>(p[1]) << 8));
                p += 2 + kl;
            }
            if (p < end) {
                uint16_t kl = static_cast<uint16_t>(static_cast<uint8_t>(p[0]) | (static_cast<uint8_t>(p[1]) << 8));
                return std::string_view(p + 2, kl);
            }
            return {};
        }
    };

    static bool LookupKey(const std::string& filepath, const std::string& key,
                          uint64_t read_ts, std::string& value_out,
                          BlockReader* cache = nullptr,
                          uint64_t manifest_seq = 0);

    static Metadata ReadMetadata(const std::string& filepath,
                                 BlockReader* cache = nullptr,
                                 uint64_t manifest_seq = 0);

    static std::vector<KeyValuePair> ReadAll(const std::string& filepath);

    static void Compact(const std::vector<Metadata>& inputs,
                        const std::string& output_dir,
                        uint64_t output_seq_start,
                        int output_level,
                        size_t max_sstable_size,
                        bool is_last_level,
                        const std::string& range_lower,
                        const std::string& range_upper,
                        std::vector<Metadata>& outputs,
                        std::vector<std::string>& garbage_files,
                        BlockReader& cache,
                        uint64_t visible_ts = UINT64_MAX);

    static void WriteUint32LE(std::ostream& os, uint32_t value);
    static void WriteUint32LE(std::vector<char>& buf, uint32_t value);
    static void WriteUint64LE(std::ostream& os, uint64_t value);
    static uint32_t ReadUint32LE(std::istream& is);
    static uint64_t ReadUint64LE(std::istream& is);

private:
};

} // namespace kvdb
