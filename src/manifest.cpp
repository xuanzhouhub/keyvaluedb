#include "kvdb/manifest.hpp"
#include "kvdb/internal/crc32.hpp"

#include <cstring>
#include <filesystem>
#include <stdexcept>
#include <unordered_set>
#include <utility>

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
const uint8_t kTypeAbortBatch    = 0x03;

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

void Manifest::AddAbortBatch(uint64_t batch_ts, uint64_t fence_seq) {
    std::vector<char> payload;
    WriteUint64LE(payload, batch_ts);
    WriteUint64LE(payload, fence_seq);
    WriteRecord(kTypeAbortBatch, payload);
}

void Manifest::Sync() {
    std::lock_guard<std::mutex> lock(mutex_);
    FsyncFile(file_);
}

std::vector<SSTable::Metadata> Manifest::Recover() {
    std::vector<SSTable::Metadata> results;
    std::vector<uint64_t> removed_seqs;
    std::vector<std::pair<uint64_t, uint64_t>> aborted_with_fence;

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
        } else if (type == kTypeAbortBatch) {
            long body_start = std::ftell(infile) - 1;

            uint64_t batch_ts;
            if (!ReadUint64LE(infile, batch_ts)) break;
            uint64_t fence_seq;
            if (!ReadUint64LE(infile, fence_seq)) { fence_seq = batch_ts; std::fseek(infile, body_start + 9, SEEK_SET); }
            // if reading fence_seq fails, this is an old-format record with no fence_seq
            // we already read batch_ts; fence_seq defaults to batch_ts (conservative)

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

            aborted_with_fence.push_back({batch_ts, fence_seq});
        } else {
            break;
        }
    }

    std::fclose(infile);

    // Apply removes
    std::unordered_set<uint64_t> removed_set(removed_seqs.begin(), removed_seqs.end());
    results.erase(
        std::remove_if(results.begin(), results.end(),
            [&](const SSTable::Metadata& m) { return removed_set.count(m.manifest_seq) > 0; }),
        results.end());

    // Apply aborts to surviving SSTables
    for (auto& [batch_ts, fence_seq] : aborted_with_fence)
        for (auto& meta : results)
            if (meta.manifest_seq <= fence_seq)
                meta.aborted_batch_ts.insert(batch_ts);

    file_ = std::fopen(filepath_.c_str(), "ab");
    if (!file_) {
        throw std::runtime_error("Failed to reopen MANIFEST file after recovery: " + filepath_);
    }

    return results;
}

void Manifest::Compact() {
    // Step 1: close the append-mode file, reopen for reading
    std::fclose(file_);
    file_ = nullptr;

    FILE* infile = std::fopen(filepath_.c_str(), "rb");
    if (!infile) {
        file_ = std::fopen(filepath_.c_str(), "ab");
        return;
    }

    // Step 2: replay all records, track catalog + removes + aborts
    std::vector<SSTable::Metadata> catalog;
    std::vector<uint64_t> removed;
    std::vector<std::pair<uint64_t, uint64_t>> aborts;  // {batch_ts, fence_seq}

    std::fseek(infile, 0, SEEK_END);
    long fsz = std::ftell(infile);
    std::fseek(infile, 0, SEEK_SET);
    if (fsz == 0) { std::fclose(infile); file_ = std::fopen(filepath_.c_str(), "ab"); return; }

    while (true) {
        uint32_t checksum;
        if (!ReadUint32LE(infile, checksum)) break;
        int type_byte = std::fgetc(infile);
        if (type_byte == EOF) break;
        uint8_t type = static_cast<uint8_t>(type_byte);

        if (type == kTypeAddSSTable) {
            long body_start = std::ftell(infile) - 1;
            uint64_t seq; if (!ReadUint64LE(infile, seq)) break;
            uint64_t ec;  if (!ReadUint64LE(infile, ec)) break;
            uint32_t mkl; if (!ReadUint32LE(infile, mkl)) break;
            uint32_t mxl; if (!ReadUint32LE(infile, mxl)) break;
            uint64_t fsz_r;if (!ReadUint64LE(infile, fsz_r)) break;
            uint32_t lvl; if (!ReadUint32LE(infile, lvl)) break;
            uint16_t pl;  if (!ReadUint16LE(infile, pl)) break;
            std::vector<char> pb(pl);
            if (std::fread(pb.data(), 1, pl, infile) != pl) break;

            long body_end = std::ftell(infile);
            std::fseek(infile, body_start, SEEK_SET);
            std::vector<char> body(body_end - body_start);
            if (std::fread(body.data(), 1, body.size(), infile) != body.size()) break;
            CRC32 crc; crc.Update(body.data(), body.size());
            if (crc.Finalize() != checksum) break;

            SSTable::Metadata meta;
            meta.filepath = std::string(pb.begin(), pb.end());
            meta.manifest_seq = seq;
            meta.entry_count = static_cast<size_t>(ec);
            meta.min_key_len = mkl; meta.max_key_len = mxl;
            meta.file_size = fsz_r; meta.level = static_cast<int>(lvl);
            catalog.push_back(std::move(meta));

        } else if (type == kTypeRemoveSSTable) {
            long body_start = std::ftell(infile) - 1;
            uint64_t seq; if (!ReadUint64LE(infile, seq)) break;
            long body_end = std::ftell(infile);
            std::fseek(infile, body_start, SEEK_SET);
            std::vector<char> body(body_end - body_start);
            if (std::fread(body.data(), 1, body.size(), infile) != body.size()) break;
            CRC32 crc; crc.Update(body.data(), body.size());
            if (crc.Finalize() != checksum) break;
            removed.push_back(seq);

        } else if (type == kTypeAbortBatch) {
            long body_start = std::ftell(infile) - 1;
            uint64_t batch_ts; if (!ReadUint64LE(infile, batch_ts)) break;
            uint64_t fence_seq; if (!ReadUint64LE(infile, fence_seq)) { fence_seq = batch_ts; std::fseek(infile, body_start + 9, SEEK_SET); }
            long body_end = std::ftell(infile);
            std::fseek(infile, body_start, SEEK_SET);
            std::vector<char> body(body_end - body_start);
            if (std::fread(body.data(), 1, body.size(), infile) != body.size()) break;
            CRC32 crc; crc.Update(body.data(), body.size());
            if (crc.Finalize() != checksum) break;
            aborts.push_back({batch_ts, fence_seq});

        } else { break; }
    }
    std::fclose(infile);

    // Step 3: remove dead SSTables
    std::unordered_set<uint64_t> dead(removed.begin(), removed.end());
    catalog.erase(std::remove_if(catalog.begin(), catalog.end(),
        [&](const SSTable::Metadata& m) { return dead.count(m.manifest_seq) > 0; }),
        catalog.end());

    // Step 4: keep only still-needed abort records
    std::vector<std::pair<uint64_t, uint64_t>> needed_aborts;
    for (auto& [batch_ts, fence_seq] : aborts) {
        bool needed = false;
        for (auto& m : catalog)
            if (m.manifest_seq <= fence_seq) { needed = true; break; }
        if (needed) needed_aborts.push_back({batch_ts, fence_seq});
    }

    // Step 5: write to temp file, then rename
    std::string tmp_path = filepath_ + ".tmp";
    FILE* out = std::fopen(tmp_path.c_str(), "wb");
    if (!out) { file_ = std::fopen(filepath_.c_str(), "ab"); return; }

    for (auto& m : catalog) {
        uint16_t pl = static_cast<uint16_t>(m.filepath.size());
        std::vector<char> payload;
        WriteUint64LE(payload, m.manifest_seq);
        WriteUint64LE(payload, static_cast<uint64_t>(m.entry_count));
        WriteUint32LE(payload, m.min_key_len);
        WriteUint32LE(payload, m.max_key_len);
        WriteUint64LE(payload, m.file_size);
        WriteUint32LE(payload, static_cast<uint32_t>(m.level));
        WriteUint16LE(payload, pl);
        payload.insert(payload.end(), m.filepath.begin(), m.filepath.end());

        std::vector<char> body;
        body.push_back(static_cast<char>(kTypeAddSSTable));
        body.insert(body.end(), payload.begin(), payload.end());
        CRC32 crc; crc.Update(body.data(), body.size());
        std::vector<char> record;
        WriteUint32LE(record, crc.Finalize());
        record.insert(record.end(), body.begin(), body.end());
        std::fwrite(record.data(), 1, record.size(), out);
    }

    for (auto& [batch_ts, fence_seq] : needed_aborts) {
        std::vector<char> payload;
        WriteUint64LE(payload, batch_ts);
        WriteUint64LE(payload, fence_seq);

        std::vector<char> body;
        body.push_back(static_cast<char>(kTypeAbortBatch));
        body.insert(body.end(), payload.begin(), payload.end());
        CRC32 crc; crc.Update(body.data(), body.size());
        std::vector<char> record;
        WriteUint32LE(record, crc.Finalize());
        record.insert(record.end(), body.begin(), body.end());
        std::fwrite(record.data(), 1, record.size(), out);
    }

    FsyncFile(out);
    std::fclose(out);

    std::error_code ec;
    std::filesystem::remove(filepath_, ec);
    std::filesystem::rename(tmp_path, filepath_, ec);

    file_ = std::fopen(filepath_.c_str(), "ab");
}

} // namespace kvdb
