#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace kvdb {

class Snappy {
public:
    static constexpr size_t kMaxBlockSize = 65536;

    static bool Compress(const char* input, size_t input_len, std::string& output) {
        output.clear();
        if (input_len == 0) return true;

        const size_t kHashBits = 14;
        const size_t kHashSize = 1 << kHashBits;
        const uint32_t kHashMul = 0x1e35a7bd;
        size_t table[kHashSize];
        std::fill_n(table, kHashSize, static_cast<size_t>(-1));

        size_t lit_start = 0;
        auto EmitLiteral = [&](size_t n) {
            if (n < 60) {
                output.push_back(static_cast<char>((n << 2)));
            } else if (n <= 255) {
                output.push_back(static_cast<char>(60 << 2));
                output.push_back(static_cast<char>(n - 60));
            } else if (n <= 65535) {
                output.push_back(static_cast<char>(61 << 2));
                n -= 256;
                output.push_back(static_cast<char>(n & 0xFF));
                output.push_back(static_cast<char>((n >> 8) & 0xFF));
            } else {
                output.push_back(static_cast<char>(62 << 2));
                n -= 65536;
                output.push_back(static_cast<char>(n & 0xFF));
                output.push_back(static_cast<char>((n >> 8) & 0xFF));
                output.push_back(static_cast<char>((n >> 16) & 0xFF));
            }
            output.append(input + lit_start, n);
        };

        size_t i = 0;
        while (i + 4 <= input_len) {
            uint32_t word = static_cast<uint8_t>(input[i])
                | (static_cast<uint8_t>(input[i+1]) << 8)
                | (static_cast<uint8_t>(input[i+2]) << 16)
                | (static_cast<uint8_t>(input[i+3]) << 24);
            uint32_t h = (word * kHashMul) >> (32 - kHashBits);
            size_t prev = table[h];
            table[h] = i;

            if (prev != static_cast<size_t>(-1) && i - prev < 32768) {
                size_t match_len = 0;
                while (match_len < 64 && i + match_len < input_len
                       && input[prev + match_len] == input[i + match_len])
                    match_len++;

                if (match_len >= 4 && (i - lit_start > 0 || match_len > 4 || i + 4 >= input_len)) {
                    if (i > lit_start) {
                        EmitLiteral(i - lit_start);
                    }
                    size_t off = i - prev;
                    size_t len = match_len;
                    output.push_back(static_cast<char>(((len - 1) << 2) | 2));
                    output.push_back(static_cast<char>(off & 0xFF));
                    output.push_back(static_cast<char>((off >> 8) & 0xFF));
                    output.push_back(static_cast<char>((off >> 16) & 0xFF));
                    output.push_back(static_cast<char>((off >> 24) & 0xFF));
                    i += match_len;
                    lit_start = i;

                    for (size_t j = i - match_len + 1; j + 4 <= i && j < input_len; ++j) {
                        word = static_cast<uint8_t>(input[j])
                            | (static_cast<uint8_t>(input[j+1]) << 8)
                            | (static_cast<uint8_t>(input[j+2]) << 16)
                            | (static_cast<uint8_t>(input[j+3]) << 24);
                        h = (word * kHashMul) >> (32 - kHashBits);
                        table[h] = j;
                    }
                    continue;
                }
            }
            ++i;
        }

        if (i > lit_start || lit_start == 0) {
            EmitLiteral(input_len - lit_start);
        }
        return true;
    }

    static bool Uncompress(const char* input, size_t input_len, std::string& output) {
        output.clear();
        size_t pos = 0;
        while (pos < input_len) {
            uint8_t tag = static_cast<uint8_t>(input[pos++]);
            uint8_t type = tag & 3;

            if (type == 0) {
                size_t len = tag >> 2;
                if (len >= 60) {
                    size_t extra = len - 59;
                    size_t base = (len == 60) ? 0 : (len == 61) ? 255 : (len == 62) ? 65535 : 16777215;
                    len = 0;
                    for (size_t i = 0; i < extra && pos < input_len; ++i)
                        len += static_cast<size_t>(static_cast<uint8_t>(input[pos++])) << (i * 8);
                    len += base;
                }
                if (pos + len > input_len) break;
                output.append(input + pos, len);
                pos += len;
            } else {
                size_t off;
                size_t len;
                if (type == 1) {
                    if (pos + 2 > input_len) break;
                    off = static_cast<uint8_t>(input[pos])
                        | (static_cast<uint8_t>(input[pos+1]) << 8);
                    pos += 2;
                    len = ((tag >> 2) & 7) + 4;
                } else if (type == 2) {
                    if (pos + 4 > input_len) break;
                    off = static_cast<uint8_t>(input[pos])
                        | (static_cast<uint8_t>(input[pos+1]) << 8)
                        | (static_cast<uint8_t>(input[pos+2]) << 16)
                        | (static_cast<uint8_t>(input[pos+3]) << 24);
                    pos += 4;
                    len = (tag >> 2) + 1;
                } else {
                    break;
                }
                if (off == 0 || off > output.size()) break;
                size_t src = output.size() - off;
                for (size_t i = 0; i < len; ++i)
                    output.push_back(output[src + (i % off)]);
            }
        }
        return true;
    }
};

} // namespace kvdb
