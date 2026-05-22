#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace kvdb {

struct Config {
    static constexpr uint32_t kSSTableMagic       = 0x4B535354;
    static constexpr uint32_t kSSTableFooterMagic = 0x4B454E44;
    static constexpr uint32_t kSSTableVersion     = 5;

    static constexpr size_t kSSTableBlockSize     = 4096;

    static constexpr uint8_t kCompressionNone    = 0;
    static constexpr uint8_t kCompressionSnappy  = 1;

    static constexpr size_t kDefaultMemTableMaxBytes = 4 * 1024 * 1024;

    static constexpr size_t kMaxKeyValuePairBytes = 4 * 1024 * 1024;

    static constexpr size_t kPageSize = 4096;
    static constexpr size_t kMaxKeyBytes = 1024;

    static constexpr size_t kMemTableEntryOverheadBytes = 64;

    static constexpr size_t kDefaultMaxPendingFlushes = 2;

    static constexpr const char* kDefaultDataDir = "./kvdb_data";

    static constexpr size_t kMinFlushEntries = 1;

    static constexpr size_t kMaxWriteQueueSize = 1024;

    static constexpr size_t kMaxWriteQueueBytes = 16 * 1024 * 1024;

    static constexpr size_t kDefaultCompactionThreshold = 8;
    static constexpr size_t kLevelSizeMultiplier = 10;
    static constexpr size_t kLevelBaseSSTableSize = 4 * 1024 * 1024;
    static constexpr size_t kMaxLevel = 7;

    static constexpr uint32_t kWALCheckpointSentinel = 0xFFFFFFFF;

    static constexpr size_t kDefaultKVCacheShards     = 16;
    static constexpr size_t kDefaultBlockCacheShards  = 16;
    static constexpr size_t kDefaultKVMaxEntries      = 10000;
    static constexpr size_t kDefaultKVMaxBytes        = 16 * 1024 * 1024;
};

} // namespace kvdb
