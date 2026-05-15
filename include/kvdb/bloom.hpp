#pragma once

#include <cmath>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace kvdb {

class BloomFilter {
public:
    BloomFilter() = default;

    BloomFilter(size_t num_entries, double false_positive_rate = 0.01) {
        double bits = -static_cast<double>(num_entries) * std::log(false_positive_rate)
                    / (std::log(2.0) * std::log(2.0));
        bits_ = static_cast<size_t>(bits + 0.5);
        if (bits_ < 64) bits_ = 64;
        bytes_.resize((bits_ + 7) / 8, 0);

        hash_count_ = static_cast<uint32_t>(
            static_cast<double>(bits_) / static_cast<double>(num_entries) * std::log(2.0) + 0.5);
        if (hash_count_ < 1) hash_count_ = 1;
        if (hash_count_ > 30) hash_count_ = 30;
    }

    void Add(const std::string& key) {
        uint32_t h1 = Hash1(key);
        uint32_t h2 = Hash2(key);
        for (uint32_t i = 0; i < hash_count_; ++i) {
            size_t bit = (static_cast<size_t>(h1) + static_cast<size_t>(i) * static_cast<size_t>(h2)) % bits_;
            bytes_[bit / 8] |= static_cast<uint8_t>(1 << (bit % 8));
        }
    }

    bool MightContain(const std::string& key) const {
        uint32_t h1 = Hash1(key);
        uint32_t h2 = Hash2(key);
        for (uint32_t i = 0; i < hash_count_; ++i) {
            size_t bit = (static_cast<size_t>(h1) + static_cast<size_t>(i) * static_cast<size_t>(h2)) % bits_;
            if (!(bytes_[bit / 8] & (1 << (bit % 8)))) return false;
        }
        return true;
    }

    const std::vector<uint8_t>& Data() const { return bytes_; }
    size_t BitCount() const { return bits_; }
    uint32_t HashCount() const { return hash_count_; }

    static BloomFilter FromRaw(const uint8_t* data, size_t bit_count, uint32_t hash_count) {
        BloomFilter bf;
        bf.bits_ = bit_count;
        bf.hash_count_ = hash_count;
        bf.bytes_.assign(data, data + (bit_count + 7) / 8);
        return bf;
    }

private:
    static uint32_t Hash1(const std::string& key) {
        uint32_t h = 2166136261u;
        for (char c : key) { h ^= static_cast<uint8_t>(c); h *= 16777619u; }
        return h;
    }

    static uint32_t Hash2(const std::string& key) {
        uint32_t h = 0;
        for (char c : key) { h = static_cast<uint8_t>(c) + (h << 6) + (h << 16) - h; }
        return h;
    }

    size_t bits_ = 0;
    uint32_t hash_count_ = 0;
    std::vector<uint8_t> bytes_;
};

} // namespace kvdb
