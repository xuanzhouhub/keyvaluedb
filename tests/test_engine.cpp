#include "test_common.hpp"
#include "kvdb/engine.hpp"
#include "kvdb/config.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>

namespace kvdb_test {

namespace fs = std::filesystem;

static const std::string kTestDataDir = "./test_engine_data";

void CleanupTestDir() {
    if (fs::exists(kTestDataDir)) {
        fs::remove_all(kTestDataDir);
    }
}

void TestBasicInsert() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);

        ASSERT_EQ(0u, engine.ActiveMemTableEntryCount());
        ASSERT_EQ(0u, engine.ActiveMemTableMemoryUsage());

        engine.Insert("hello", "world");

        ASSERT_EQ(1u, engine.ActiveMemTableEntryCount());
        ASSERT_TRUE(engine.ActiveMemTableMemoryUsage() > 0);
    }
    CleanupTestDir();
}

void TestManualFlush() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 1024 * 1024);

        for (int i = 0; i < 100; ++i) {
            engine.Insert("key_" + std::to_string(i), "value_" + std::to_string(i));
        }

        ASSERT_EQ(100u, engine.ActiveMemTableEntryCount());
        ASSERT_EQ(0u, engine.SSTableCount());

        engine.Flush();

        ASSERT_EQ(0u, engine.ActiveMemTableEntryCount());
        ASSERT_EQ(1u, engine.SSTableCount());
    }
    CleanupTestDir();
}

void TestAutoFlushOnFull() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);

        int insert_count = 0;
        size_t prev_sstable_count = 0;

        for (int i = 0; i < 5000; ++i) {
            std::string key = "k" + std::to_string(i);
            std::string value = "v" + std::to_string(i);
            engine.Insert(key, value);
            ++insert_count;

            size_t current_sstable_count = engine.SSTableCount();
            if (current_sstable_count > prev_sstable_count) {
                prev_sstable_count = current_sstable_count;
            }
        }

        engine.WaitForPendingFlushes();

        ASSERT_TRUE(engine.SSTableCount() > 0);
        ASSERT_TRUE(insert_count > 0);
    }
    CleanupTestDir();
}

void TestMultipleFlushes() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);

        for (int i = 0; i < 5000; ++i) {
            engine.Insert("key_" + std::to_string(i), "value_" + std::to_string(i));
        }

        engine.WaitForPendingFlushes();

        ASSERT_TRUE(engine.SSTableCount() > 1);

        auto metadata = engine.GetSSTableMetadata();
        ASSERT_EQ(engine.SSTableCount(), metadata.size());

        size_t total_entries = 0;
        for (const auto& meta : metadata) {
            total_entries += meta.entry_count;
        }
        ASSERT_TRUE(total_entries > 0);
    }
    CleanupTestDir();
}

void TestFlushEmptyDoesNothing() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);

        ASSERT_EQ(0u, engine.ActiveMemTableEntryCount());
        engine.Flush();
        ASSERT_EQ(0u, engine.SSTableCount());
    }
    CleanupTestDir();
}

void TestInsertAfterFlush() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);

        engine.Insert("before_flush", "value1");
        engine.Flush();

        ASSERT_EQ(0u, engine.ActiveMemTableEntryCount());

        engine.Insert("after_flush", "value2");
        ASSERT_EQ(1u, engine.ActiveMemTableEntryCount());
    }
    CleanupTestDir();
}

void TestSSTableFilesExist() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);

        for (int i = 0; i < 1000; ++i) {
            engine.Insert("key_" + std::to_string(i), "val_" + std::to_string(i));
        }

        engine.WaitForPendingFlushes();

        auto metadata = engine.GetSSTableMetadata();
        ASSERT_TRUE(metadata.size() > 0);

        for (const auto& meta : metadata) {
            ASSERT_TRUE(fs::exists(meta.filepath));
        }
    }
    CleanupTestDir();
}

void TestReadBackFromSSTable() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);

        engine.Insert("alpha", "one");
        engine.Insert("beta", "two");
        engine.Insert("gamma", "three");

        engine.Flush();

        ASSERT_EQ(1u, engine.SSTableCount());

        auto metadata = engine.GetSSTableMetadata();
        ASSERT_EQ(1u, metadata.size());

        auto entries = kvdb::SSTable::ReadAll(metadata[0].filepath);
        ASSERT_EQ(3u, entries.size());

        ASSERT_EQ(std::string("alpha"), entries[0].key);
        ASSERT_EQ(std::string("one"), entries[0].value);
        ASSERT_EQ(std::string("beta"), entries[1].key);
        ASSERT_EQ(std::string("two"), entries[1].value);
        ASSERT_EQ(std::string("gamma"), entries[2].key);
        ASSERT_EQ(std::string("three"), entries[2].value);
    }
    CleanupTestDir();
}

void TestDataDirectoryCreated() {
    CleanupTestDir();
    ASSERT_FALSE(fs::exists(kTestDataDir));

    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        ASSERT_TRUE(fs::exists(kTestDataDir));
    }
    CleanupTestDir();
}

void TestDestructorFlushesRemaining() {
    CleanupTestDir();

    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        engine.Insert("final_key", "final_value");
    }

    std::vector<std::string> sst_files;
    for (const auto& entry : fs::directory_iterator(kTestDataDir)) {
        if (entry.path().extension() == ".sst") {
            sst_files.push_back(entry.path().string());
        }
    }

    ASSERT_TRUE(sst_files.size() > 0);

    auto entries = kvdb::SSTable::ReadAll(sst_files[0]);
    ASSERT_TRUE(entries.size() > 0);

    bool found = false;
    for (const auto& kv : entries) {
        if (kv.key == "final_key" && kv.value == "final_value") {
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);

    CleanupTestDir();
}

void TestNeedsFlush() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);

        ASSERT_FALSE(engine.NeedsFlush());

        for (int i = 0; i < 5000; ++i) {
            engine.Insert("k" + std::to_string(i), "v" + std::to_string(i));
            if (engine.NeedsFlush()) {
                break;
            }
        }

        (void)engine.NeedsFlush();
        ASSERT_TRUE(true);
    }
    CleanupTestDir();
}

void TestWaitForPendingFlushes() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);

        for (int i = 0; i < 2000; ++i) {
            engine.Insert("key_" + std::to_string(i), "value_" + std::to_string(i));
        }

        engine.WaitForPendingFlushes();

        auto metadata = engine.GetSSTableMetadata();
        for (const auto& meta : metadata) {
            ASSERT_TRUE(fs::exists(meta.filepath));
        }
    }
    CleanupTestDir();
}

void TestSSTableMetadataContent() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 1024 * 1024);

        for (int i = 0; i < 100; ++i) {
            engine.Insert("k" + std::to_string(i), "v" + std::to_string(i));
        }

        engine.Flush();

        auto metadata = engine.GetSSTableMetadata();
        ASSERT_EQ(1u, metadata.size());

        const auto& meta = metadata[0];
        ASSERT_EQ(100u, meta.entry_count);
        ASSERT_TRUE(meta.file_size > 0);
        ASSERT_TRUE(meta.max_key_len > 0);
    }
    CleanupTestDir();
}

void TestRejectOversizedKV() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 1024 * 1024);

        engine.Insert("small", "ok");
        ASSERT_EQ(1u, engine.ActiveMemTableEntryCount());

        std::string big_value(kvdb::Config::kMaxKeyValuePairBytes, 'X');

        bool caught = false;
        try {
            engine.Insert("big", big_value);
        } catch (const std::invalid_argument&) {
            caught = true;
        }
        ASSERT_TRUE(caught);

        ASSERT_EQ(1u, engine.ActiveMemTableEntryCount());
    }
    CleanupTestDir();
}

void TestInsertAtMaxSize() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 1024 * 1024);

        size_t overhead = kvdb::Config::kMemTableEntryOverheadBytes;
        size_t max_data = kvdb::Config::kMaxKeyValuePairBytes - overhead - 1;

        std::string key = "k";
        std::string value(max_data, 'V');

        bool thrown = false;
        try {
            engine.Insert(key, value);
        } catch (const std::invalid_argument&) {
            thrown = true;
        }
        ASSERT_FALSE(thrown);

        engine.WaitForPendingFlushes();
        ASSERT_EQ(1u, engine.SSTableCount());
    }
    CleanupTestDir();
}

void TestRangeScan() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 1024 * 1024);

        engine.Insert("c", "three");
        engine.Insert("a", "one");
        engine.Insert("b", "two");
        engine.Insert("d", "four");
        engine.Flush();

        auto iter = engine.RangeScan();
        ASSERT_TRUE(iter.Valid());
        ASSERT_STREQ("a", iter.Key()); ASSERT_STREQ("one", iter.Value());
        iter.Next();
        ASSERT_TRUE(iter.Valid());
        ASSERT_STREQ("b", iter.Key()); ASSERT_STREQ("two", iter.Value());
        iter.Next();
        ASSERT_TRUE(iter.Valid());
        ASSERT_STREQ("c", iter.Key()); ASSERT_STREQ("three", iter.Value());
        iter.Next();
        ASSERT_TRUE(iter.Valid());
        ASSERT_STREQ("d", iter.Key()); ASSERT_STREQ("four", iter.Value());
        iter.Next();
        ASSERT_FALSE(iter.Valid());
    }
    CleanupTestDir();
}

void TestRangeScanMultiSource() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 1024 * 1024);

        engine.Insert("a", "old");
        engine.Insert("b", "old_b");
        engine.Flush();

        engine.Insert("a", "new");
        engine.Insert("c", "new_c");

        auto iter = engine.RangeScan();
        ASSERT_TRUE(iter.Valid());
        ASSERT_STREQ("a", iter.Key()); ASSERT_STREQ("new", iter.Value());
        iter.Next();
        ASSERT_TRUE(iter.Valid());
        ASSERT_STREQ("b", iter.Key()); ASSERT_STREQ("old_b", iter.Value());
        iter.Next();
        ASSERT_TRUE(iter.Valid());
        ASSERT_STREQ("c", iter.Key()); ASSERT_STREQ("new_c", iter.Value());
        iter.Next();
        ASSERT_FALSE(iter.Valid());
    }
    CleanupTestDir();
}

void TestRangeScanEmpty() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 1024 * 1024);
        auto iter = engine.RangeScan();
        ASSERT_FALSE(iter.Valid());
    }
    CleanupTestDir();
}

void TestMVCCVersionAfterFlush() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);

        engine.Insert("k", "v1");
        engine.Insert("k", "v2");

        engine.Flush();

        std::string val;
        ASSERT_TRUE(engine.Lookup("k", val));
        ASSERT_STREQ("v2", val);

        ASSERT_EQ(1u, engine.SSTableCount());
    }
    CleanupTestDir();
}

void TestRecyclePreservesLookup() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);

        for (int i = 0; i < 500; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string value = "value_" + std::to_string(i);
            engine.Insert(key, value);
        }

        engine.Flush();
        engine.WaitForPendingFlushes();

        ASSERT_TRUE(engine.SSTableCount() > 0);
        ASSERT_EQ(0u, engine.ActiveMemTableEntryCount());

        for (int i = 0; i < 500; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string expected = "value_" + std::to_string(i);
            std::string val;
            ASSERT_TRUE(engine.Lookup(key, val));
            ASSERT_STREQ(expected, val);
        }
    }
    CleanupTestDir();
}

void TestRecycleAfterExplicitFlush() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 1024 * 1024);

        engine.Insert("a", "1");
        engine.Insert("b", "2");

        ASSERT_EQ(2u, engine.ActiveMemTableEntryCount());
        ASSERT_EQ(0u, engine.SSTableCount());

        engine.Flush();
        engine.WaitForPendingFlushes();

        ASSERT_EQ(0u, engine.ActiveMemTableEntryCount());
        ASSERT_EQ(1u, engine.SSTableCount());

        std::string val;
        ASSERT_TRUE(engine.Lookup("a", val));
        ASSERT_STREQ("1", val);
        ASSERT_TRUE(engine.Lookup("b", val));
        ASSERT_STREQ("2", val);
    }
    CleanupTestDir();
}

void TestDelete() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        engine.Insert("k", "value");
        std::string val;
        ASSERT_TRUE(engine.Lookup("k", val));
        ASSERT_STREQ("value", val);
        engine.Delete("k");
        ASSERT_FALSE(engine.Lookup("k", val));
        ASSERT_TRUE(val.empty());
    }
    CleanupTestDir();
}

void TestDeleteThenReinsert() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        engine.Insert("k", "v1");
        engine.Delete("k");
        engine.Insert("k", "v2");
        std::string val;
        ASSERT_TRUE(engine.Lookup("k", val));
        ASSERT_STREQ("v2", val);
    }
    CleanupTestDir();
}

void TestDeleteSurvivesFlush() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        engine.Insert("keep", "alive");
        engine.Insert("dead", "walking");
        engine.Delete("dead");
        engine.Flush();
        std::string val;
        ASSERT_TRUE(engine.Lookup("keep", val));
        ASSERT_STREQ("alive", val);
        ASSERT_FALSE(engine.Lookup("dead", val));
    }
    CleanupTestDir();
}

void TestCompactionBasic() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        for (int i = 1; i <= 10; ++i) {
            engine.Insert("k" + std::to_string(i), "v" + std::to_string(i));
            engine.Flush();
        }
        engine.WaitForPendingFlushes();
        ASSERT_TRUE(engine.SSTableCount() > 0);
        for (int i = 1; i <= 10; ++i) {
            std::string v;
            ASSERT_TRUE(engine.Lookup("k" + std::to_string(i), v));
            ASSERT_STREQ("v" + std::to_string(i), v);
        }
    }
    CleanupTestDir();
}

void TestCompactionPreservesData() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        for (int i = 0; i < 20; ++i) {
            engine.Insert("key_" + std::to_string(i), "value_" + std::to_string(i));
            engine.Flush();
        }
        engine.WaitForPendingFlushes();

        // Give compaction worker time to run
        std::this_thread::sleep_for(std::chrono::seconds(4));

        for (int i = 0; i < 20; ++i) {
            std::string v;
            ASSERT_TRUE(engine.Lookup("key_" + std::to_string(i), v));
            ASSERT_STREQ("value_" + std::to_string(i), v);
        }

        auto metas = engine.GetSSTableMetadata();
        int l1_count = 0;
        for (auto& m : metas) if (m.level == 1) l1_count++;
        ASSERT_TRUE(l1_count > 0);
    }
    CleanupTestDir();
}

void TestTombstonePropagatesThroughCompaction() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        engine.Insert("living", "alive");
        engine.Insert("dying", "doomed");
        engine.Flush();
        engine.Insert("living", "still_alive");
        engine.Delete("dying");
        engine.Flush();
        engine.WaitForPendingFlushes();

        std::this_thread::sleep_for(std::chrono::seconds(4));

        std::string v;
        ASSERT_TRUE(engine.Lookup("living", v));
        ASSERT_STREQ("still_alive", v);
        ASSERT_FALSE(engine.Lookup("dying", v));
    }
    CleanupTestDir();
}

void TestCompactionEmptyLevelsAfterCascade() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        for (int round = 0; round < 10; ++round) {
            for (int i = 0; i < 10; ++i) {
                engine.Insert("k" + std::to_string(round * 100 + i),
                              "v" + std::to_string(round * 100 + i));
                engine.Flush();
            }
            engine.WaitForPendingFlushes();
        }
        std::this_thread::sleep_for(std::chrono::seconds(8));

        size_t found = 0;
        for (int round = 0; round < 10; ++round)
            for (int i = 0; i < 10; ++i) {
                std::string v;
                if (engine.Lookup("k" + std::to_string(round * 100 + i), v))
                    found++;
            }
        ASSERT_TRUE(found >= 90u);
    }
    CleanupTestDir();
}

void TestKVCacheHit() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        engine.Insert("cached", "value");
        std::string v;
        ASSERT_TRUE(engine.Lookup("cached", v));
        ASSERT_STREQ("value", v);
        ASSERT_TRUE(engine.Lookup("cached", v));
        ASSERT_STREQ("value", v);
    }
    CleanupTestDir();
}

void TestKVCacheBlobNotCached() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 1024 * 1024);
        std::string big(3000, 'Z');
        engine.Insert("big", big);
        std::string v;
        ASSERT_TRUE(engine.Lookup("big", v));
        ASSERT_EQ(3000u, v.size());
    }
    CleanupTestDir();
}

void TestKVCacheTombstone() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        engine.Insert("k", "val");
        std::string v;
        ASSERT_TRUE(engine.Lookup("k", v));
        engine.Delete("k");
        ASSERT_FALSE(engine.Lookup("k", v));
    }
    CleanupTestDir();
}

void RunTests() {
    std::cout << "Running LSMTreeEngine Tests...\n\n";

    TestBasicInsert();
    TestManualFlush();
    TestAutoFlushOnFull();
    TestMultipleFlushes();
    TestFlushEmptyDoesNothing();
    TestInsertAfterFlush();
    TestSSTableFilesExist();
    TestReadBackFromSSTable();
    TestDataDirectoryCreated();
    TestDestructorFlushesRemaining();
    TestNeedsFlush();
    TestWaitForPendingFlushes();
    TestSSTableMetadataContent();
    TestRejectOversizedKV();
    TestInsertAtMaxSize();
    TestRangeScan();
    TestRangeScanMultiSource();
    TestRangeScanEmpty();
    TestMVCCVersionAfterFlush();
    TestRecyclePreservesLookup();
    TestRecycleAfterExplicitFlush();
    TestDelete();
    TestDeleteThenReinsert();
    TestDeleteSurvivesFlush();
    TestCompactionBasic();
    TestCompactionPreservesData();
    TestTombstonePropagatesThroughCompaction();
    TestCompactionEmptyLevelsAfterCascade();
    TestKVCacheHit();
    TestKVCacheBlobNotCached();
    TestKVCacheTombstone();
}

} // namespace kvdb_test

RUN_TESTS()
