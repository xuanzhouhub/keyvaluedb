#include "kvdb/wal.hpp"
#include "kvdb/internal/crc32.hpp"

#include <cstring>
#include <stdexcept>
#include <unordered_set>

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

void WriteUint32LE(std::vector<char>& buf, uint32_t v) {
    buf.push_back(static_cast<char>(v & 0xFF));
    buf.push_back(static_cast<char>((v >> 8) & 0xFF));
    buf.push_back(static_cast<char>((v >> 16) & 0xFF));
    buf.push_back(static_cast<char>((v >> 24) & 0xFF));
}

void WriteSentinelRecord(uint32_t sentinel, uint64_t ts, std::vector<char>& buf_out) {
    CRC32 crc;
    std::vector<char> body;
    WriteUint32LE(body, sentinel);
    for (int i = 0; i < 8; ++i)
        body.push_back(static_cast<char>((ts >> (i * 8)) & 0xFF));
    crc.Update(body.data(), body.size());
    uint32_t crc_val = crc.Finalize();
    WriteUint32LE(buf_out, crc_val);
    buf_out.insert(buf_out.end(), body.begin(), body.end());
}

} // anonymous namespace

WAL::WAL(const std::string& filepath)
    : filepath_(filepath) {
    file_ = std::fopen(filepath_.c_str(), "ab");
    if (!file_) {
        throw std::runtime_error("Failed to open WAL file: " + filepath_);
    }
}

WAL::~WAL() {
    if (file_) {
        try {
            if (!write_buf_.empty()) {
                Sync();
            }
        } catch (...) {}
        std::fclose(file_);
    }
}

void WAL::SerializeRecord(std::vector<char>& buf,
                          const std::string& key,
                          const std::string& value,
                          uint64_t timestamp) {
    std::vector<char> body;
    WriteUint32LE(body, static_cast<uint32_t>(key.size()));
    body.insert(body.end(), key.begin(), key.end());
    WriteUint32LE(body, static_cast<uint32_t>(value.size()));
    body.insert(body.end(), value.begin(), value.end());

    body.push_back(static_cast<char>(timestamp & 0xFF));
    body.push_back(static_cast<char>((timestamp >> 8) & 0xFF));
    body.push_back(static_cast<char>((timestamp >> 16) & 0xFF));
    body.push_back(static_cast<char>((timestamp >> 24) & 0xFF));
    body.push_back(static_cast<char>((timestamp >> 32) & 0xFF));
    body.push_back(static_cast<char>((timestamp >> 40) & 0xFF));
    body.push_back(static_cast<char>((timestamp >> 48) & 0xFF));
    body.push_back(static_cast<char>((timestamp >> 56) & 0xFF));

    CRC32 crc;
    crc.Update(body.data(), body.size());
    uint32_t checksum = crc.Finalize();

    WriteUint32LE(buf, checksum);
    buf.insert(buf.end(), body.begin(), body.end());
}

void WAL::Buffer(const std::string& key, const std::string& value, uint64_t timestamp) {
    std::lock_guard<std::mutex> lock(mutex_);
    SerializeRecord(write_buf_, key, value, timestamp);
    batch_seq_++;
    buffered_entries_++;
}

void WAL::BufferAbort(uint64_t batch_ts) {
    std::lock_guard<std::mutex> lock(mutex_);
    WriteSentinelRecord(0xFFFFFFFE, batch_ts, write_buf_);
}

void WAL::BufferBatchBegin(uint64_t batch_ts) {
    std::lock_guard<std::mutex> lock(mutex_);
    WriteSentinelRecord(0xFFFFFFFA, batch_ts, write_buf_);
}

void WAL::BufferBatchCommit(uint64_t batch_ts) {
    std::lock_guard<std::mutex> lock(mutex_);
    WriteSentinelRecord(0xFFFFFFFC, batch_ts, write_buf_);
}

void WAL::WriteCheckpoint(uint64_t timestamp, uint64_t batch_ts) {
    std::lock_guard<std::mutex> lock(mutex_);

    std::vector<char> body;
    WriteUint32LE(body, Config::kWALCheckpointSentinel);

    for (int i = 0; i < 8; ++i)
        body.push_back(static_cast<char>((timestamp >> (i * 8)) & 0xFF));
    for (int i = 0; i < 8; ++i)
        body.push_back(static_cast<char>((batch_ts >> (i * 8)) & 0xFF));

    CRC32 crc;
    crc.Update(body.data(), body.size());
    uint32_t checksum = crc.Finalize();

    WriteUint32LE(write_buf_, checksum);
    write_buf_.insert(write_buf_.end(), body.begin(), body.end());
    batch_seq_++;
}

size_t WAL::Sync(bool force) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (write_buf_.empty()) {
        return synced_seq_;
    }
    if (!force && write_buf_.size() < Config::kWALMinSyncBytes) {
        size_t current = batch_seq_;
        synced_seq_ = current;
        return current;
    }

    std::vector<char> to_sync;
    to_sync.swap(write_buf_);
    size_t batch_at_sync = batch_seq_;
    size_t entries_in_batch = buffered_entries_;

    lock.unlock();

    std::fwrite(to_sync.data(), 1, to_sync.size(), file_);
    if (std::ferror(file_)) {
        throw std::runtime_error("Failed to write WAL data to: " + filepath_);
    }
    FsyncFile(file_);

    lock.lock();
    synced_seq_ = batch_at_sync;
    synced_entries_ += entries_in_batch;
    buffered_entries_ = 0;

    return synced_seq_;
}

size_t WAL::CurrentBatchSeq() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return batch_seq_;
}

bool WAL::IsSynced(size_t seq) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return seq <= synced_seq_;
}

std::vector<KeyValuePair> WAL::Recover(uint64_t* checkpoint_ts,
                                        std::vector<uint64_t>* aborted) {
    Sync();

    std::fclose(file_);

    FILE* infile = std::fopen(filepath_.c_str(), "rb");
    if (!infile) {
        file_ = std::fopen(filepath_.c_str(), "ab");
        if (checkpoint_ts) *checkpoint_ts = 0;
        return {};
    }

    std::fseek(infile, 0, SEEK_END);
    long file_size = std::ftell(infile);
    std::fseek(infile, 0, SEEK_SET);

    if (file_size == 0) {
        std::fclose(infile);
        file_ = std::fopen(filepath_.c_str(), "ab");
        if (checkpoint_ts) *checkpoint_ts = 0;
        return {};
    }

    std::vector<char> file_data(file_size);
    std::fread(file_data.data(), 1, file_size, infile);
    std::fclose(infile);

    const unsigned char* p = reinterpret_cast<const unsigned char*>(file_data.data());

    auto ReadUint64From = [](const unsigned char* d) -> uint64_t {
        return static_cast<uint64_t>(d[0]) | (static_cast<uint64_t>(d[1]) << 8)
             | (static_cast<uint64_t>(d[2]) << 16) | (static_cast<uint64_t>(d[3]) << 24)
             | (static_cast<uint64_t>(d[4]) << 32) | (static_cast<uint64_t>(d[5]) << 40)
             | (static_cast<uint64_t>(d[6]) << 48) | (static_cast<uint64_t>(d[7]) << 56);
    };

    long last_checkpoint_end = 0;
    uint64_t found_ts = 0;
    uint64_t checkpoint_batch_ts = 0;

    size_t     pos = 0;
    while (pos + 4 <= file_data.size()) {
        size_t record_start = pos;

        uint32_t checksum = p[pos] | (static_cast<uint32_t>(p[pos + 1]) << 8)
                          | (static_cast<uint32_t>(p[pos + 2]) << 16)
                          | (static_cast<uint32_t>(p[pos + 3]) << 24);
        pos += 4;

        if (pos + 4 > file_data.size()) break;
        uint32_t key_len = p[pos] | (static_cast<uint32_t>(p[pos + 1]) << 8)
                         | (static_cast<uint32_t>(p[pos + 2]) << 16)
                         | (static_cast<uint32_t>(p[pos + 3]) << 24);
        pos += 4;

        if (key_len == Config::kWALCheckpointSentinel) {
            if (pos + 8 > file_data.size()) { pos -= 4; break; }
            uint64_t ckpt_ts = ReadUint64From(p + pos);
            if (pos + 16 <= file_data.size()) {
                CRC32 crc;
                crc.Update(&key_len, sizeof(key_len));
                crc.Update(p + pos, 16);
                if (crc.Finalize() == checksum) {
                    last_checkpoint_end = static_cast<long>(pos + 16);
                    found_ts = ckpt_ts;
                    checkpoint_batch_ts = ReadUint64From(p + pos + 8);
                    pos += 16;
                    continue;
                }
            }
            CRC32 crc;
            crc.Update(&key_len, sizeof(key_len));
            crc.Update(p + pos, 8);
            pos += 8;
            if (crc.Finalize() == checksum) {
                last_checkpoint_end = static_cast<long>(pos);
                found_ts = ckpt_ts;
            }
            continue;
        }

        if (key_len == 0xFFFFFFFE) {
            if (pos + 8 > file_data.size()) { pos -= 4; break; }
            pos += 8;
            continue;
        }

        if (pos + key_len > file_data.size()) break;
        CRC32 crc;
        crc.Update(&key_len, sizeof(key_len));
        crc.Update(p + pos, key_len);
        pos += key_len;

        if (pos + 4 > file_data.size()) break;
        uint32_t value_len = p[pos] | (static_cast<uint32_t>(p[pos + 1]) << 8)
                           | (static_cast<uint32_t>(p[pos + 2]) << 16)
                           | (static_cast<uint32_t>(p[pos + 3]) << 24);
        pos += 4;

        crc.Update(&value_len, sizeof(value_len));
        if (pos + value_len > file_data.size()) break;
        crc.Update(p + pos, value_len);
        pos += value_len;

        if (pos + 8 > file_data.size()) break;
        crc.Update(p + pos, 8);
        pos += 8;

        if (crc.Finalize() != checksum) {
            break;
        }
    }

    std::vector<KeyValuePair> results;
    std::unordered_set<uint64_t> batch_opened, batch_closed, batch_has_entries;

    if (checkpoint_batch_ts != 0)
        batch_opened.insert(checkpoint_batch_ts);

    pos = static_cast<size_t>(last_checkpoint_end);
    while (pos + 4 <= file_data.size()) {
        size_t record_start = pos;

        uint32_t checksum = p[pos] | (static_cast<uint32_t>(p[pos + 1]) << 8)
                          | (static_cast<uint32_t>(p[pos + 2]) << 16)
                          | (static_cast<uint32_t>(p[pos + 3]) << 24);
        pos += 4;

        if (pos + 4 > file_data.size()) break;
        uint32_t key_len = p[pos] | (static_cast<uint32_t>(p[pos + 1]) << 8)
                         | (static_cast<uint32_t>(p[pos + 2]) << 16)
                         | (static_cast<uint32_t>(p[pos + 3]) << 24);
        pos += 4;

        if (key_len == Config::kWALCheckpointSentinel) {
            continue;
        }

        if (key_len == 0xFFFFFFFA) {
            if (pos + 8 > file_data.size()) break;
            uint64_t bts = ReadUint64From(p + pos);
            pos += 8;
            batch_opened.insert(bts);
            continue;
        }

        if (key_len == 0xFFFFFFFC) {
            if (pos + 8 > file_data.size()) break;
            uint64_t bts = ReadUint64From(p + pos);
            pos += 8;
            batch_opened.erase(bts);
            batch_closed.insert(bts);
            continue;
        }

        if (key_len == 0xFFFFFFFE) {
            if (pos + 8 > file_data.size()) break;
            uint64_t aborted_ts = ReadUint64From(p + pos);
            pos += 8;
            batch_opened.erase(aborted_ts);
            batch_closed.insert(aborted_ts);
            if (aborted) aborted->push_back(aborted_ts);
            continue;
        }

        if (pos + key_len > file_data.size()) break;
        CRC32 crc;
        crc.Update(&key_len, sizeof(key_len));
        crc.Update(p + pos, key_len);
        std::string key(reinterpret_cast<const char*>(p + pos), key_len);
        pos += key_len;

        if (pos + 4 > file_data.size()) break;
        uint32_t value_len = p[pos] | (static_cast<uint32_t>(p[pos + 1]) << 8)
                           | (static_cast<uint32_t>(p[pos + 2]) << 16)
                           | (static_cast<uint32_t>(p[pos + 3]) << 24);
        pos += 4;

        crc.Update(&value_len, sizeof(value_len));
        if (pos + value_len > file_data.size()) break;
        crc.Update(p + pos, value_len);
        std::string value(reinterpret_cast<const char*>(p + pos), value_len);
        pos += value_len;

        if (pos + 8 > file_data.size()) break;
        crc.Update(p + pos, 8);
        uint64_t timestamp =
            static_cast<uint64_t>(p[pos])
            | (static_cast<uint64_t>(p[pos + 1]) << 8)
            | (static_cast<uint64_t>(p[pos + 2]) << 16)
            | (static_cast<uint64_t>(p[pos + 3]) << 24)
            | (static_cast<uint64_t>(p[pos + 4]) << 32)
            | (static_cast<uint64_t>(p[pos + 5]) << 40)
            | (static_cast<uint64_t>(p[pos + 6]) << 48)
            | (static_cast<uint64_t>(p[pos + 7]) << 56);
        pos += 8;

        if (crc.Finalize() != checksum) {
            break;
        }

        results.push_back({std::move(key), std::move(value), timestamp});
        batch_has_entries.insert(timestamp);
    }

    for (uint64_t ts : batch_opened) {
        if (!batch_closed.count(ts) && batch_has_entries.count(ts) && aborted)
            aborted->push_back(ts);
    }

    if (checkpoint_ts) *checkpoint_ts = found_ts;

    file_ = std::fopen(filepath_.c_str(), "ab");
    if (!file_) {
        throw std::runtime_error("Failed to reopen WAL file after recovery: " + filepath_);
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        batch_seq_ = 0;
        synced_seq_ = 0;
        buffered_entries_ = 0;
        synced_entries_ = 0;
        write_buf_.clear();
    }

    return results;
}

void WAL::Clear() {
    Sync();

    std::fclose(file_);
    file_ = std::fopen(filepath_.c_str(), "wb");
    if (!file_) {
        throw std::runtime_error("Failed to clear WAL file: " + filepath_);
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        batch_seq_ = 0;
        synced_seq_ = 0;
        buffered_entries_ = 0;
        synced_entries_ = 0;
    }
}

void WAL::TrimToLastCheckpoint() {
    Sync();

    std::fclose(file_);

    FILE* infile = std::fopen(filepath_.c_str(), "rb");
    if (!infile) {
        file_ = std::fopen(filepath_.c_str(), "ab");
        return;
    }

    std::fseek(infile, 0, SEEK_END);
    long file_size = std::ftell(infile);
    std::fseek(infile, 0, SEEK_SET);

    if (file_size == 0) {
        std::fclose(infile);
        file_ = std::fopen(filepath_.c_str(), "ab");
        return;
    }

    std::vector<char> file_data(file_size);
    std::fread(file_data.data(), 1, file_size, infile);
    std::fclose(infile);

    const unsigned char* p = reinterpret_cast<const unsigned char*>(file_data.data());

    long last_checkpoint_end = 0;

    size_t pos = 0;
    while (pos + 4 <= file_data.size()) {
        uint32_t checksum = p[pos] | (static_cast<uint32_t>(p[pos + 1]) << 8)
                          | (static_cast<uint32_t>(p[pos + 2]) << 16)
                          | (static_cast<uint32_t>(p[pos + 3]) << 24);
        pos += 4;

        if (pos + 4 > file_data.size()) break;
        uint32_t key_len = p[pos] | (static_cast<uint32_t>(p[pos + 1]) << 8)
                         | (static_cast<uint32_t>(p[pos + 2]) << 16)
                         | (static_cast<uint32_t>(p[pos + 3]) << 24);
        pos += 4;

        if (key_len == Config::kWALCheckpointSentinel) {
            if (pos + 8 > file_data.size()) { pos -= 4; break; }
            CRC32 crc;
            crc.Update(&key_len, sizeof(key_len));
            crc.Update(p + pos, 8);
            pos += 8;
            if (crc.Finalize() == checksum) {
                last_checkpoint_end = static_cast<long>(pos);
            }
            continue;
        }

        if (pos + key_len > file_data.size()) break;
        pos += key_len;
        if (pos + 4 > file_data.size()) break;
        uint32_t value_len = p[pos] | (static_cast<uint32_t>(p[pos + 1]) << 8)
                           | (static_cast<uint32_t>(p[pos + 2]) << 16)
                           | (static_cast<uint32_t>(p[pos + 3]) << 24);
        pos += 4;
        if (pos + value_len > file_data.size()) break;
        pos += value_len;
        if (pos + 8 > file_data.size()) break;
        pos += 8;
    }

    if (last_checkpoint_end > 0) {
#ifdef _WIN32
        {
            FILE* tf = std::fopen(filepath_.c_str(), "r+b");
            if (tf) {
                _chsize_s(_fileno(tf), last_checkpoint_end);
                std::fclose(tf);
            }
        }
        {
            std::vector<char> tail(file_size - last_checkpoint_end);
            std::memcpy(tail.data(), file_data.data() + last_checkpoint_end, tail.size());

            FILE* af = std::fopen(filepath_.c_str(), "ab");
            if (af) {
                std::fwrite(tail.data(), 1, tail.size(), af);
                std::fclose(af);
            }
        }
#else
        std::vector<char> tail(file_size - last_checkpoint_end);
        std::memcpy(tail.data(), file_data.data() + last_checkpoint_end, tail.size());
        ::truncate(filepath_.c_str(), 0);
        FILE* af = std::fopen(filepath_.c_str(), "wb");
        if (af) {
            std::fwrite(tail.data(), 1, tail.size(), af);
            std::fclose(af);
        }
#endif
    }

    file_ = std::fopen(filepath_.c_str(), "ab");
    if (!file_) {
        throw std::runtime_error("Failed to reopen WAL file after trim: " + filepath_);
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        batch_seq_ = 0;
        synced_seq_ = 0;
        buffered_entries_ = 0;
        synced_entries_ = 0;
        write_buf_.clear();
    }
}

size_t WAL::EntryCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return synced_entries_ + buffered_entries_;
}

bool WAL::HasData() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return synced_entries_ > 0 || !write_buf_.empty();
}

} // namespace kvdb
