#include "kvdb/iterator.hpp"
#include "kvdb/snappy.hpp"

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>

namespace kvdb {

SSTableIterator::SSTableIterator(const std::string& filepath)
    : reader_(nullptr), manifest_seq_(0) {
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
                                 bool populate)
    : filepath_(filepath), reader_(&reader), manifest_seq_(manifest_seq),
      populate_(populate) {
    file.open(filepath, std::ios::binary);
    if (!file.is_open()) { total_entries = 0; return; }
    read_u32(); read_u32(); read_u32();
    total_entries = read_u32();
    file.seekg(4, std::ios::cur); file.seekg(4, std::ios::cur);
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
    if (populate_ && reader_ && manifest_seq_ != 0)
        reader_->PutBlock(manifest_seq_, cur_block_ - 1, block_data);
}

void SSTableIterator::DecompressBlock(uint8_t comp, std::string& data) {
    if (comp == Config::kCompressionSnappy) {
        std::string d; Snappy::Uncompress(data.data(), data.size(), d);
        data = std::move(d);
    }
}

} // namespace kvdb
