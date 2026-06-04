#include "kvdb/iterator.hpp"
#include "kvdb/snappy.hpp"

#include <cstdint>
#include <cstring>
#include <fstream>
#include <memory>
#include <string>

namespace kvdb {

SSTableIterator::SSTableIterator(const std::string& filepath,
                                 const std::unordered_set<uint64_t>* aborted,
                                 const std::vector<uint64_t>* offsets,
                                 const std::string* first_key_buf)
    : reader_(nullptr), manifest_seq_(0), aborted_(aborted),
      block_offsets_(offsets), block_first_key_buf_(first_key_buf) {
    file.open(filepath, std::ios::binary);
    if (!file.is_open()) { total_entries = 0; return; }
    read_u32(); read_u32(); read_u32();
    total_entries = read_u32();
    file.seekg(4, std::ios::cur); file.seekg(4, std::ios::cur);
    ReadNextBlock();
}

SSTableIterator::SSTableIterator(const std::string& filepath,
                                 BlockReader& reader,
                                 uint64_t manifest_seq,
                                 bool populate,
                                 const std::unordered_set<uint64_t>* aborted,
                                 const std::vector<uint64_t>* offsets,
                                 const std::string* first_key_buf)
    : filepath_(filepath), reader_(&reader), manifest_seq_(manifest_seq),
      populate_(populate), aborted_(aborted),
      block_offsets_(offsets), block_first_key_buf_(first_key_buf) {
    file.open(filepath, std::ios::binary);
    if (!file.is_open()) { total_entries = 0; return; }
    read_u32(); read_u32(); read_u32();
    total_entries = read_u32();
    file.seekg(4, std::ios::cur); file.seekg(4, std::ios::cur);
    ReadNextBlock();
}

void SSTableIterator::SeekToKey(const std::string& key) {
    if (!block_offsets_ || !block_first_key_buf_ || block_offsets_->empty()) {
        while (Valid() && Current().key < key) Next();
        return;
    }

    // Binary search block index to find target block
    uint32_t lo = 0, hi = static_cast<uint32_t>(block_offsets_->size());
    const char* p = block_first_key_buf_->data();
    while (lo < hi) {
        uint32_t mid = (lo + hi) / 2;
        const char* scan = p;
        for (uint32_t j = 0; j < mid; ++j) {
            uint16_t kl = static_cast<uint16_t>(static_cast<uint8_t>(scan[0]) | (static_cast<uint8_t>(scan[1]) << 8));
            scan += 2 + kl;
        }
        uint16_t kl = static_cast<uint16_t>(static_cast<uint8_t>(scan[0]) | (static_cast<uint8_t>(scan[1]) << 8));
        std::string_view mid_key(scan + 2, kl);
        if (mid_key < key) lo = mid + 1; else hi = mid;
    }

    uint32_t target = lo > 0 ? lo - 1 : 0;
    if (target != cur_block_ - 1) {
        file.seekg(static_cast<std::streamoff>((*block_offsets_)[target]));
        cur_block_ = target;
        read_entries = 0;
        pos = 0;
        block_ = Block();
        ReadNextBlock();
    }
    while (Valid() && Current().key < key) Next();
}

void SSTableIterator::JumpToBlock(uint32_t block_idx) {
    if (block_idx >= block_offsets_->size()) return;
    file.seekg(static_cast<std::streamoff>((*block_offsets_)[block_idx]));
    cur_block_ = block_idx;
    read_entries = 0; // approximate — real count unknown
    ReadNextBlock();
}

void SSTableIterator::ReadNextBlock() {
    pos = 0;
    if (read_entries >= total_entries) { block_ = Block(); return; }
    if (reader_ && manifest_seq_ != 0) {
        cached_block_ = reader_->GetBlock(manifest_seq_, cur_block_);
        if (cached_block_) {
            block_ = Block(cached_block_->data(), cached_block_->size());
            read_entries += block_.Count();
            ++cur_block_;
            block_.Seek(0); block_.Read(current);
            while (aborted_ && aborted_->count(current.timestamp)) {
                ++pos;
                if (pos >= block_.Count()) break;
                block_.Seek(pos); block_.Read(current);
            }
            return;
        }
    }
        cached_block_.reset();
        read_u32();
        uint8_t comp = uint8_t(file.get());
        uint32_t csz = read_u32();
        block_data.resize(csz); file.read(&block_data[0], csz);
        DecompressBlock(comp, block_data);
        block_ = Block(block_data);
        read_entries += block_.Count();
        ++cur_block_;
        block_.Seek(0); block_.Read(current);
        while (aborted_ && aborted_->count(current.timestamp)) {
            ++pos;
            if (pos >= block_.Count()) break;
            block_.Seek(pos); block_.Read(current);
        }
        if (populate_ && reader_ && manifest_seq_ != 0)
            reader_->PutBlock(manifest_seq_, cur_block_ - 1, block_data);
    }

void SSTableIterator::DecompressBlock(uint8_t comp, std::string& data) {
    if (comp == Config::kCompressionSnappy) {
        std::string out;
        Snappy::Uncompress(data.data(), data.size(), out);
        data = std::move(out);
    }
}

} // namespace kvdb
