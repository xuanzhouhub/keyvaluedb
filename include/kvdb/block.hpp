#pragma once

#include "types.hpp"

#include <cstdint>
#include <cstring>

namespace kvdb {

class Block {
public:
    Block() : data_(nullptr), size_(0) {}

    Block(const char* data, size_t size)
        : data_(data), size_(size) {
        count_ = (size_ >= 4) ? ReadU32LE(data_) : 0;
        pos_ = 4;
    }

    Block(const std::string& s) : Block(s.data(), s.size()) {}

    uint32_t Count() const { return count_; }
    const char* Data() const { return data_; }
    size_t Size() const { return size_; }

    void Seek(uint32_t idx) {
        if (idx < idx_) { pos_ = 4; idx_ = 0; }
        while (idx_ < idx && pos_ + 4 <= size_) {
            uint32_t kl = ReadU32LE(data_ + pos_); pos_ += 4;
            pos_ += kl;
            if (pos_ + 4 > size_) break;
            uint32_t vl = ReadU32LE(data_ + pos_); pos_ += 4;
            pos_ += vl + 8 + 1;
            ++idx_;
        }
    }

    bool Read(KeyValuePair& out) {
        if (idx_ >= count_ || pos_ + 4 > size_) return false;
        uint32_t kl = ReadU32LE(data_ + pos_); pos_ += 4;
        if (pos_ + kl > size_) return false;
        out.key.assign(data_ + pos_, kl); pos_ += kl;
        if (pos_ + 4 > size_) return false;
        uint32_t vl = ReadU32LE(data_ + pos_); pos_ += 4;
        if (pos_ + vl > size_) return false;
        out.value.assign(data_ + pos_, vl); pos_ += vl;
        if (pos_ + 8 + 1 > size_) return false;
        out.timestamp = 0;
        for (int b = 0; b < 8; ++b)
            out.timestamp |= uint64_t(uint8_t(data_[pos_++])) << (b * 8);
        out.is_tombstone = (uint8_t(data_[pos_++]) & 1) != 0;
        ++idx_;
        return true;
    }

    void Reset() { pos_ = 4; idx_ = 0; }

    static uint32_t ReadU32LE(const char* d) {
        return uint8_t(d[0]) | (uint8_t(d[1]) << 8)
             | (uint8_t(d[2]) << 16) | (uint8_t(d[3]) << 24);
    }

private:
    const char* data_;
    size_t size_;
    size_t pos_ = 0;
    uint32_t count_ = 0;
    uint32_t idx_ = 0;
};

} // namespace kvdb
