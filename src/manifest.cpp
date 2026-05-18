#include "kvdb/manifest.hpp"
#include "kvdb/internal/crc32.hpp"

#include <cstring>
#include <stdexcept>

#ifdef _WIN32
#include <io.h>
#endif

namespace kvdb {
namespace {

#ifdef _WIN32
void FsyncFile(FILE* f) {
    fflush(f);
    _commit(_fileno(f));
}
#else
#include <unistd.h>
void FsyncFile(FILE* f) {
    fflush(f);
    fdatasync(fileno(f));
}
#endif

void WriteUint16LE(std::vector<char>& buf, uint16_t v) {
    buf.push_back(static_cast<char>(v & 0xFF));
    buf.push_back(static_cast<char>((v >> 8) & 0xFF));
}

void WriteUint32LE(std::vector<char>& buf, uint32_t v) {
    buf.push_back(static_cast<char>(v & 0xFF));
    buf.push_back(static_cast<char>((v >> 8) & 0xFF));
    buf.push_back(static_cast<char>((v >> 16) & 0xFF));
    buf.push_back(static_cast<char>((v >> 24) & 0xFF));
}

void WriteUint64LE(std::vector<char>& buf, uint64_t v) {
    buf.push_back(static_cast<char>(v & 0xFF));
    buf.push_back(static_cast<char>((v >> 8) & 0xFF));
    buf.push_back(static_cast<char>((v >> 16) & 0xFF));
    buf.push_back(static_cast<char>((v >> 24) & 0xFF));
    buf.push_back(static_cast<char>((v >> 32) & 0xFF));
    buf.push_back(static_cast<char>((v >> 40) & 0xFF));
    buf.push_back(static_cast<char>((v >> 48) & 0xFF));
    buf.push_back(static_cast<char>((v >> 56) & 0xFF));
}

bool ReadUint16LE(FILE* f, uint16_t& out) {
    unsigned char buf[2];
    if (std::fread(buf, 1, 2, f) != 2) return false;
    out = buf[0] | (static_cast<uint16_t>(buf[1]) << 8);
    return true;
}

bool ReadUint32LE(FILE* f, uint32_t& out) {
    unsigned char buf[4];
    if (std::fread(buf, 1, 4, f) != 4) return false;
    out = buf[0] | (static_cast<uint32_t>(buf[1]) << 8)
        | (static_cast<uint32_t>(buf[2]) << 16)
        | (static_cast<uint32_t>(buf[3]) << 24);
    return true;
}

bool ReadUint64LE(FILE* f, uint64_t& out) {
    unsigned char buf[8];
    if (std::fread(buf, 1, 8, f) != 8) return false;
    out = static_cast<uint64_t>(buf[0])
        | (static_cast<uint64_t>(buf[1]) << 8)
        | (static_cast<uint64_t>(buf[2]) << 16)
        | (static_cast<uint64_t>(buf[3]) << 24)
        | (static_cast<uint64_t>(buf[4]) << 32)
        | (static_cast<uint64_t>(buf[5]) << 40)
        | (static_cast<uint64_t>(buf[6]) << 48)
        | (static_cast<uint64_t>(buf[7]) << 56);
    return true;
}

const uint8_t kTypeAddSSTable    = 0x01;
const uint8_t kTypeRemoveSSTable = 0x02;

} // anonymous namespace

Manifest::Manifest(const std::string& filepath)
    : filepath_(filepath) {
    file_ = std::fopen(filepath_.c_str(), "ab");
    if (!file_) {
        throw std::runtime_error("Failed to open MANIFEST file: " + filepath_);
    }
}

Manifest::~Manifest() {
    if (file_) {
        std::fclose(file_);
    }
}

void Manifest::WriteRecord(uint8_t type, const std::vector<char>& payload) {
    std::vector<char> body;
    body.push_back(static_cast<char>(type));
    body.insert(body.end(), payload.begin(), payload.end());

    CRC32 crc;
    crc.Update(body.data(), body.size());
    uint32_t checksum = crc.Finalize();

    std::vector<char> record;
    WriteUint32LE(record, checksum);
    record.insert(record.end(), body.begin(), body.end());

    std::lock_guard<std::mutex> lock(mutex_);
    std::fwrite(record.data(), 1, record.size(), file_);
    if (std::ferror(file_)) {
        throw std::runtime_error("Failed to write MANIFEST record: " + filepath_);
    }
    FsyncFile(file_);
}

void Manifest::AddSSTable(uint64_t seq, const SSTable::Metadata& meta) {
    uint16_t path_len = static_cast<uint16_t>(meta.filepath.size());

    std::vector<char> payload;
    WriteUint64LE(payload, seq);
    WriteUint64LE(payload, static_cast<uint64_t>(meta.entry_count));
    WriteUint32LE(payload, meta.min_key_len);
    WriteUint32LE(payload, meta.max_key_len);
    WriteUint64LE(payload, meta.file_size);
    WriteUint32LE(payload, static_cast<uint32_t>(meta.level));
    WriteUint16LE(payload, path_len);
    payload.insert(payload.end(), meta.filepath.begin(), meta.filepath.end());

    WriteRecord(kTypeAddSSTable, payload);
}

void Manifest::RemoveSSTable(uint64_t seq) {
    std::vector<char> payload;
    WriteUint64LE(payload, seq);

    WriteRecord(kTypeRemoveSSTable, payload);
}

void Manifest::Sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    FsyncFile(file_);
}

std::vector<SSTable::Metadata> Manifest::Recover() {
    std::vector<SSTable::Metadata> results;
    std::vector<uint64_t> removed_seqs;

    std::fclose(file_);
    file_ = nullptr;

    FILE* infile = std::fopen(filepath_.c_str(), "rb");
    if (!infile) {
        file_ = std::fopen(filepath_.c_str(), "ab");
        return results;
    }

    std::fseek(infile, 0, SEEK_END);
    long file_size = std::ftell(infile);
    std::fseek(infile, 0, SEEK_SET);

    if (file_size == 0) {
        std::fclose(infile);
        file_ = std::fopen(filepath_.c_str(), "ab");
        return results;
    }

    while (true) {
        uint32_t checksum;
        if (!ReadUint32LE(infile, checksum)) break;

        int type_byte = std::fgetc(infile);
        if (type_byte == EOF) break;
        uint8_t type = static_cast<uint8_t>(type_byte);

        if (type == kTypeAddSSTable) {
            long body_start = std::ftell(infile) - 1;

            uint64_t seq;
            if (!ReadUint64LE(infile, seq)) break;

            uint64_t entry_count;
            if (!ReadUint64LE(infile, entry_count)) break;

            uint32_t min_key_len;
            if (!ReadUint32LE(infile, min_key_len)) break;

            uint32_t max_key_len;
            if (!ReadUint32LE(infile, max_key_len)) break;

            uint64_t file_size_rec;
            if (!ReadUint64LE(infile, file_size_rec)) break;

            uint32_t level_val = 0;
            if (!ReadUint32LE(infile, level_val)) break;

            uint16_t path_len;
            if (!ReadUint16LE(infile, path_len)) break;

            std::vector<char> path_buf(path_len);
            if (std::fread(path_buf.data(), 1, path_len, infile) != path_len) break;

            long body_end = std::ftell(infile);
            long body_len = body_end - body_start;

            std::fseek(infile, body_start, SEEK_SET);
            std::vector<char> body(body_len);
            if (std::fread(body.data(), 1, body_len, infile) != static_cast<size_t>(body_len)) break;

            CRC32 crc;
            crc.Update(body.data(), body.size());
            if (crc.Finalize() != checksum) {
                break;
            }

            SSTable::Metadata meta;
            meta.filepath = std::string(path_buf.begin(), path_buf.end());
            meta.manifest_seq = seq;
            meta.entry_count = static_cast<size_t>(entry_count);
            meta.min_key_len = min_key_len;
            meta.max_key_len = max_key_len;
            meta.file_size = file_size_rec;
            meta.level = static_cast<int>(level_val);

            results.push_back(std::move(meta));
        } else if (type == kTypeRemoveSSTable) {
            long body_start = std::ftell(infile) - 1;

            uint64_t seq;
            if (!ReadUint64LE(infile, seq)) break;

            long body_end = std::ftell(infile);
            long body_len = body_end - body_start;

            std::fseek(infile, body_start, SEEK_SET);
            std::vector<char> body(body_len);
            if (std::fread(body.data(), 1, body_len, infile) != static_cast<size_t>(body_len)) break;

            CRC32 crc;
            crc.Update(body.data(), body.size());
            if (crc.Finalize() != checksum) {
                break;
            }

            removed_seqs.push_back(seq);
        } else {
            break;
        }
    }

    std::fclose(infile);

    file_ = std::fopen(filepath_.c_str(), "ab");
    if (!file_) {
        throw std::runtime_error("Failed to reopen MANIFEST file after recovery: " + filepath_);
    }

    return results;
}

} // namespace kvdb
