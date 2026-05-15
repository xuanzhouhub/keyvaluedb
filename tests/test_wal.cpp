#include "test_common.hpp"
#include "kvdb/wal.hpp"
#include "kvdb/config.hpp"

#include <cstdio>
#include <filesystem>
#include <string>

namespace kvdb_test {

namespace fs = std::filesystem;

static const std::string kWALTestPath = "./test_wal_data/test.wal";

void Cleanup() {
    if (fs::exists("./test_wal_data")) {
        fs::remove_all("./test_wal_data");
    }
    fs::create_directories("./test_wal_data");
}

void TestCreateAndSync() {
    Cleanup();
    {
        kvdb::WAL wal(kWALTestPath);
        ASSERT_EQ(0u, wal.EntryCount());
        ASSERT_FALSE(wal.HasData());

        wal.Buffer("hello", "world");
        ASSERT_EQ(1u, wal.EntryCount());
        ASSERT_TRUE(wal.HasData());

        wal.Sync();
        ASSERT_EQ(1u, wal.EntryCount());
        ASSERT_TRUE(wal.HasData());
    }
    Cleanup();
}

void TestRecoverSingleEntry() {
    Cleanup();
    {
        kvdb::WAL wal(kWALTestPath);
        wal.Buffer("key1", "value1");
        wal.Sync();
    }

    {
        kvdb::WAL wal(kWALTestPath);
        auto entries = wal.Recover();
        ASSERT_EQ(1u, entries.size());
        ASSERT_EQ(std::string("key1"), entries[0].key);
        ASSERT_EQ(std::string("value1"), entries[0].value);
    }
    Cleanup();
}

void TestRecoverMultipleEntries() {
    Cleanup();
    {
        kvdb::WAL wal(kWALTestPath);
        for (int i = 0; i < 100; ++i) {
            wal.Buffer("k" + std::to_string(i), "v" + std::to_string(i));
        }
        wal.Sync();
    }

    {
        kvdb::WAL wal(kWALTestPath);
        auto entries = wal.Recover();
        ASSERT_EQ(100u, entries.size());
        for (int i = 0; i < 100; ++i) {
            ASSERT_EQ(std::string("k" + std::to_string(i)), entries[static_cast<size_t>(i)].key);
            ASSERT_EQ(std::string("v" + std::to_string(i)), entries[static_cast<size_t>(i)].value);
        }
    }
    Cleanup();
}

void TestRecoverEmptyWAL() {
    Cleanup();
    {
        kvdb::WAL wal(kWALTestPath);
        wal.Sync();
    }

    {
        kvdb::WAL wal(kWALTestPath);
        auto entries = wal.Recover();
        ASSERT_EQ(0u, entries.size());
    }
    Cleanup();
}

void TestClear() {
    Cleanup();
    {
        kvdb::WAL wal(kWALTestPath);
        wal.Buffer("a", "b");
        wal.Sync();
        ASSERT_EQ(1u, wal.EntryCount());
        ASSERT_TRUE(wal.HasData());

        wal.Clear();
        ASSERT_EQ(0u, wal.EntryCount());
        ASSERT_FALSE(wal.HasData());
    }

    {
        kvdb::WAL wal(kWALTestPath);
        auto entries = wal.Recover();
        ASSERT_EQ(0u, entries.size());
    }
    Cleanup();
}

void TestBinaryData() {
    Cleanup();
    {
        kvdb::WAL wal(kWALTestPath);
        std::string key("\x00\x01\x02\xFF", 4);
        std::string value("\xAB\xCD\xEF\x00\x11\x22", 6);
        wal.Buffer(key, value);
        wal.Sync();
    }

    {
        kvdb::WAL wal(kWALTestPath);
        auto entries = wal.Recover();
        ASSERT_EQ(1u, entries.size());
        ASSERT_STREQ(std::string("\x00\x01\x02\xFF", 4), entries[0].key);
        ASSERT_STREQ(std::string("\xAB\xCD\xEF\x00\x11\x22", 6), entries[0].value);
    }
    Cleanup();
}

void TestCheckpoint() {
    Cleanup();
    {
        kvdb::WAL wal(kWALTestPath);
        wal.Buffer("pre", "1");
        wal.Sync();
        wal.WriteCheckpoint(0);
        wal.Sync();
        wal.Buffer("post", "2");
        wal.Sync();
    }

    {
        kvdb::WAL wal(kWALTestPath);
        auto entries = wal.Recover();
        ASSERT_EQ(1u, entries.size());
        ASSERT_STREQ("post", entries[0].key);
    }
    Cleanup();
}

void TestTrimToLastCheckpoint() {
    Cleanup();
    {
        kvdb::WAL wal(kWALTestPath);
        wal.Buffer("old", "data");
        wal.Sync();
        wal.WriteCheckpoint(0);
        wal.Sync();
    }

    {
        kvdb::WAL wal(kWALTestPath);
        wal.TrimToLastCheckpoint();
    }

    {
        kvdb::WAL wal(kWALTestPath);
        auto entries = wal.Recover();
        ASSERT_EQ(0u, entries.size());
    }
    Cleanup();
}

void TestCrashRecoveryPartialRecord() {
    Cleanup();
    {
        kvdb::WAL wal(kWALTestPath);
        wal.Buffer("complete1", "v1");
        wal.Buffer("complete2", "v2");
        wal.Sync();
    }

    {
        FILE* f = std::fopen(kWALTestPath.c_str(), "rb");
        ASSERT_TRUE(f != nullptr);
        std::fseek(f, 0, SEEK_END);
        long size = std::ftell(f);
        std::fseek(f, 0, SEEK_SET);
        std::vector<char> wal_bytes(size);
        std::fread(wal_bytes.data(), 1, size, f);
        std::fclose(f);

        ASSERT_TRUE(wal_bytes.size() > 10);
        wal_bytes.resize(wal_bytes.size() - 3);

        f = std::fopen(kWALTestPath.c_str(), "wb");
        std::fwrite(wal_bytes.data(), 1, wal_bytes.size(), f);
        std::fclose(f);
    }

    {
        kvdb::WAL wal(kWALTestPath);
        auto entries = wal.Recover();
        ASSERT_EQ(1u, entries.size());
        ASSERT_STREQ("complete1", entries[0].key);
    }
    Cleanup();
}

void TestLargeValueRecovery() {
    Cleanup();
    {
        kvdb::WAL wal(kWALTestPath);
        std::string value(1024 * 100, 'X');
        wal.Buffer("big", value);
        wal.Sync();
    }

    {
        kvdb::WAL wal(kWALTestPath);
        auto entries = wal.Recover();
        ASSERT_EQ(1u, entries.size());
        ASSERT_EQ(std::string("big"), entries[0].key);
        ASSERT_EQ(102400u, entries[0].value.size());
    }
    Cleanup();
}

void TestAppendAfterRecover() {
    Cleanup();
    {
        kvdb::WAL wal(kWALTestPath);
        wal.Buffer("initial", "data");
        wal.Sync();
    }

    {
        kvdb::WAL wal(kWALTestPath);
        auto entries = wal.Recover();
        ASSERT_EQ(1u, entries.size());

        wal.Buffer("after_recover", "more_data");
        wal.Sync();
        ASSERT_EQ(1u, wal.EntryCount());
    }

    {
        kvdb::WAL wal(kWALTestPath);
        auto entries = wal.Recover();
        ASSERT_EQ(2u, entries.size());
        ASSERT_STREQ("initial", entries[0].key);
        ASSERT_STREQ("after_recover", entries[1].key);
    }
    Cleanup();
}

void RunTests() {
    std::cout << "Running WAL Tests...\n\n";

    TestCreateAndSync();
    TestRecoverSingleEntry();
    TestRecoverMultipleEntries();
    TestRecoverEmptyWAL();
    TestClear();
    TestBinaryData();
    TestCheckpoint();
    TestTrimToLastCheckpoint();
    TestLargeValueRecovery();
    TestAppendAfterRecover();
    TestCrashRecoveryPartialRecord();
}

} // namespace kvdb_test

RUN_TESTS()
