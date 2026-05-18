#pragma once

#include "bloom.hpp"
#include "memtable.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace kvdb {

class SSTableCache;

class SSTable {
public:
    static void Write(const std::string& filepath, const std::vector<KeyValuePair>& entries);
    static void WriteFromWalk(const std::string& filepath, BPlusTree::MemTableWalk& walk,
                              size_t entry_count, SSTableCache* cache = nullptr,
                              uint64_t manifest_seq = 0);

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
        std::vector<std::string> block_first_keys;
    };

    static bool LookupKey(const std::string& filepath, const std::string& key,
                          uint64_t read_ts, std::string& value_out,
                          SSTableCache* cache = nullptr,
                          uint64_t manifest_seq = 0);

    static Metadata ReadMetadata(const std::string& filepath,
                                 SSTableCache* cache = nullptr,
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
                        std::vector<uint64_t>& garbage_seqs);

    static void WriteUint32LE(std::ostream& os, uint32_t value);
    static void WriteUint32LE(std::vector<char>& buf, uint32_t value);
    static void WriteUint64LE(std::ostream& os, uint64_t value);
    static uint32_t ReadUint32LE(std::istream& is);
    static uint64_t ReadUint64LE(std::istream& is);

private:
};

} // namespace kvdb
