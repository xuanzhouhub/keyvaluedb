#pragma once

#include "config.hpp"
#include "memtable.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <vector>

namespace kvdb {

class WAL {
public:
    WAL(const std::string& filepath);

    ~WAL();

    WAL(const WAL&) = delete;
    WAL& operator=(const WAL&) = delete;
    WAL(WAL&&) = delete;
    WAL& operator=(WAL&&) = delete;

    void Buffer(const std::string& key, const std::string& value, uint64_t timestamp = 0);

    void BufferBatchBegin(uint64_t batch_ts);
    void BufferBatchCommit(uint64_t batch_ts);
    void BufferAbort(uint64_t batch_ts);

    void WriteCheckpoint(uint64_t timestamp, uint64_t batch_ts = 0);

    size_t Sync(bool force = true);

    size_t CurrentBatchSeq() const;

    bool IsSynced(size_t seq) const;

    std::vector<KeyValuePair> Recover(uint64_t* checkpoint_ts = nullptr,
                                      std::vector<uint64_t>* aborted = nullptr);

    void Clear();

    void TrimToLastCheckpoint();

    size_t EntryCount() const;

    bool HasData() const;

private:
    static void SerializeRecord(std::vector<char>& buf,
                                const std::string& key,
                                const std::string& value,
                                uint64_t timestamp);

    void FlushBuffer();

    std::string filepath_;
    FILE* file_ = nullptr;

    mutable std::mutex mutex_;
    std::vector<char> write_buf_;
    size_t batch_seq_ = 0;
    size_t synced_seq_ = 0;
    size_t buffered_entries_ = 0;
    size_t synced_entries_ = 0;
};

} // namespace kvdb
