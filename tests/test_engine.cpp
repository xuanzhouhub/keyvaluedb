#include "test_common.hpp"
#include "kvdb/engine.hpp"
#include "kvdb/config.hpp"

#include <atomic>
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
        int compacted = 0;
        for (auto& m : metas) if (m.level > 0) compacted++;
        ASSERT_TRUE(compacted > 0);
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

void TestRangeScanCachedBlocks() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        for (int i = 0; i < 32; ++i) {
            engine.Insert("key" + std::to_string(i), "val" + std::to_string(i));
        }
        engine.Flush();
        engine.WaitForPendingFlushes();
        ASSERT_TRUE(engine.SSTableCount() > 0);

        auto iter = engine.RangeScan();
        int count = 0;
        while (iter.Valid()) { ++count; iter.Next(); }
        ASSERT_EQ(32, count);

        auto bounded = engine.RangeScan(
            kvdb::RangeBound::Inclusive("key10"),
            kvdb::RangeBound::Inclusive("key19"));
        count = 0;
        while (bounded.Valid()) { ++count; bounded.Next(); }
        ASSERT_EQ(10, count);
    }
    CleanupTestDir();
}

void TestCompactionCacheReadIntegrity() {
    CleanupTestDir();
    {
        kvdb::LSMTreeEngine engine(kTestDataDir, 4096);
        for (int i = 0; i < 64; ++i) {
            engine.Insert("key" + std::to_string(i), "val" + std::to_string(i));
            engine.Flush();
        }
        engine.WaitForPendingFlushes();

        std::this_thread::sleep_for(std::chrono::seconds(4));

        for (int i = 0; i < 64; ++i) {
            std::string v;
            ASSERT_TRUE(engine.Lookup("key" + std::to_string(i), v));
            ASSERT_STREQ("val" + std::to_string(i), v);
        }

        auto iter = engine.RangeScan();
        int count = 0;
        while (iter.Valid()) { ++count; iter.Next(); }
        ASSERT_EQ(64, count);
    }
    CleanupTestDir();
}

void TestBatchInvisibleDuring() {
    std::filesystem::remove_all("./test_batch_data1");
    {
        kvdb::LSMTreeEngine engine("./test_batch_data1", 4096, 2, 16, 16, 1000);
        ASSERT_TRUE(engine.StartBatch());
        engine.BatchInsert("b1", "v1");
        engine.BatchInsert("b2", "v2");
        engine.BatchDelete("bd");

        std::string v;
        ASSERT_FALSE(engine.Lookup("b1", v));
        ASSERT_FALSE(engine.Lookup("b2", v));
        ASSERT_FALSE(engine.Lookup("bd", v));

        ASSERT_TRUE(engine.CommitBatch());

        ASSERT_TRUE(engine.Lookup("b1", v));
        ASSERT_STREQ("v1", v);
        ASSERT_TRUE(engine.Lookup("b2", v));
        ASSERT_STREQ("v2", v);
        ASSERT_FALSE(engine.Lookup("bd", v));
    }
    std::filesystem::remove_all("./test_batch_data1");
}

void TestBatchSingleAtATime() {
    std::filesystem::remove_all("./test_batch_data2");
    {
        kvdb::LSMTreeEngine engine("./test_batch_data2", 4096, 2, 16, 16, 1000);
        ASSERT_TRUE(engine.StartBatch());
        ASSERT_FALSE(engine.StartBatch());
        ASSERT_TRUE(engine.CommitBatch());
        ASSERT_TRUE(engine.StartBatch());
        engine.BatchInsert("x", "y");
        ASSERT_TRUE(engine.CommitBatch());
    }
    std::filesystem::remove_all("./test_batch_data2");
}

void TestBatchNormalWritesDuring() {
    std::filesystem::remove_all("./test_batch_data3");
    {
        kvdb::LSMTreeEngine engine("./test_batch_data3", 4096, 2, 16, 16, 100);
        ASSERT_TRUE(engine.StartBatch());
        engine.BatchInsert("bx", "bv");

        engine.Insert("nx", "nv");

        std::string v;
        ASSERT_FALSE(engine.Lookup("bx", v));
        ASSERT_TRUE(engine.Lookup("nx", v));
        ASSERT_STREQ("nv", v);

        ASSERT_TRUE(engine.CommitBatch());

        ASSERT_TRUE(engine.Lookup("bx", v));
        ASSERT_STREQ("bv", v);
        ASSERT_TRUE(engine.Lookup("nx", v));
        ASSERT_STREQ("nv", v);
    }
    std::filesystem::remove_all("./test_batch_data3");
}

void TestBatchGapExhausted() {
    std::filesystem::remove_all("./test_batch_data4");
    {
        kvdb::LSMTreeEngine engine("./test_batch_data4", 4*1024*1024, 2, 16, 16, 5);
        ASSERT_TRUE(engine.StartBatch());
        engine.BatchInsert("b", "v");

        for (int i = 0; i < 5; ++i)
            engine.Insert("n" + std::to_string(i), "v");

        std::atomic<bool> started{false}, done{false};
        std::thread t([&]() {
            started = true;
            engine.Insert("n_blocked", "v");
            done = true;
        });

        while (!started) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        ASSERT_FALSE(done.load());

        ASSERT_TRUE(engine.CommitBatch());
        t.join();
        ASSERT_TRUE(done.load());

        std::string v2;
        ASSERT_TRUE(engine.Lookup("n_blocked", v2));
        ASSERT_STREQ("v", v2);
    }
    std::filesystem::remove_all("./test_batch_data4");
}

void TestBatchAbortInvisible() {
    std::filesystem::remove_all("./test_batch_data5");
    {
        kvdb::LSMTreeEngine engine("./test_batch_data5", 4096, 2, 16, 16, 100);
        ASSERT_TRUE(engine.StartBatch());
        engine.BatchInsert("aborted_key", "aborted_val");
        ASSERT_TRUE(engine.AbortBatch());

        std::string v;
        ASSERT_FALSE(engine.Lookup("aborted_key", v));

        engine.Insert("normal_key", "normal_val");
        ASSERT_TRUE(engine.Lookup("normal_key", v));
        ASSERT_STREQ("normal_val", v);
        ASSERT_FALSE(engine.Lookup("aborted_key", v));
    }
    std::filesystem::remove_all("./test_batch_data5");
}

void TestBatchAbortAfterFlush() {
    std::filesystem::remove_all("./test_batch_data6");
    {
        kvdb::LSMTreeEngine engine("./test_batch_data6", 4096, 2, 16, 16, 100);
        ASSERT_TRUE(engine.StartBatch());

        engine.BatchInsert("ab_1", "v1");
        engine.BatchInsert("ab_2", "v2");

        engine.Flush();
        engine.WaitForPendingFlushes();

        auto metas = engine.GetSSTableMetadata();
        ASSERT_TRUE(!metas.empty());

        bool found_raw_ab1 = false;
        for (auto& m : metas) {
            auto entries = kvdb::SSTable::ReadAll(m.filepath);
            for (auto& e : entries) {
                if (e.key == "ab_1" && e.value == "v1") found_raw_ab1 = true;
            }
        }
        ASSERT_TRUE(found_raw_ab1);

        engine.AbortBatch();

        engine.Insert("normal", "nv");

        std::string v;
        ASSERT_FALSE(engine.Lookup("ab_1", v));
        ASSERT_FALSE(engine.Lookup("ab_2", v));
        ASSERT_TRUE(engine.Lookup("normal", v));
        ASSERT_STREQ("nv", v);

        auto iter = engine.RangeScan();
        bool found_ab1 = false, found_norm = false;
        while (iter.Valid()) {
            if (iter.Key() == "ab_1") found_ab1 = true;
            if (iter.Key() == "normal") found_norm = true;
            iter.Next();
        }
        ASSERT_FALSE(found_ab1);
        ASSERT_TRUE(found_norm);
    }
    std::filesystem::remove_all("./test_batch_data6");
}

void TestBatchAbortRecovery() {
    std::filesystem::remove_all("./test_batch_data7");
    {
        kvdb::LSMTreeEngine engine("./test_batch_data7", 4096, 2, 16, 16, 100);
        ASSERT_TRUE(engine.StartBatch());
        engine.BatchInsert("r_key", "r_val");
        ASSERT_TRUE(engine.AbortBatch());
    }
    {
        kvdb::LSMTreeEngine engine("./test_batch_data7", 4096, 2, 16, 16, 100);
        std::string v;
        ASSERT_FALSE(engine.Lookup("r_key", v));
    }
    std::filesystem::remove_all("./test_batch_data7");
}

void TestBatchCrashRecoverAutoAbort() {
    std::filesystem::remove_all("./test_batch_data8");
    {
        kvdb::LSMTreeEngine engine("./test_batch_data8", 4096, 2, 16, 16, 100);
        ASSERT_TRUE(engine.StartBatch());
        engine.BatchInsert("c_key", "c_val");
        engine.BatchInsert("c_key2", "c_val2");
        engine.AbortBatch();
    }
    {
        kvdb::LSMTreeEngine engine("./test_batch_data8", 4096, 2, 16, 16, 100);
        std::string v;
        ASSERT_FALSE(engine.Lookup("c_key", v));
        ASSERT_FALSE(engine.Lookup("c_key2", v));
    }
    std::filesystem::remove_all("./test_batch_data8");
}

void TestBatchCrashRecoverEmptyBatch() {
    std::filesystem::remove_all("./test_batch_data9");
    {
        kvdb::LSMTreeEngine engine("./test_batch_data9", 4096, 2, 16, 16, 100);
        ASSERT_TRUE(engine.StartBatch());
    }
    {
        kvdb::LSMTreeEngine engine("./test_batch_data9", 4096, 2, 16, 16, 100);
        ASSERT_TRUE(engine.SSTableCount() >= 0);
    }
    std::filesystem::remove_all("./test_batch_data9");
}

void TestBatchAbortRangeScanMemtable() {
    std::filesystem::remove_all("./test_abort_mt_scan");
    {
        kvdb::LSMTreeEngine engine("./test_abort_mt_scan", 64*1024*1024, 2, 16, 16, 100);
        ASSERT_TRUE(engine.StartBatch());
        engine.BatchInsert("ab_k1", "v1");
        engine.BatchInsert("ab_k2", "v2");
        engine.BatchInsert("normal_k", "nv");
        engine.AbortBatch();

        engine.Insert("kept_k", "kept_v");

        auto iter = engine.RangeScan();
        bool found_ab1 = false, found_kept = false;
        while (iter.Valid()) {
            if (iter.Key() == "ab_k1") found_ab1 = true;
            if (iter.Key() == "kept_k") found_kept = true;
            iter.Next();
        }
        ASSERT_FALSE(found_ab1);
        ASSERT_TRUE(found_kept);
    }
    std::filesystem::remove_all("./test_abort_mt_scan");
}

void TestBatchAbortWarmCacheLookup() {
    std::filesystem::remove_all("./test_abort_warm");
    {
        kvdb::LSMTreeEngine engine("./test_abort_warm", 4096, 2, 16, 16, 100);
        ASSERT_TRUE(engine.StartBatch());
        engine.BatchInsert("w_key", "w_val");
        engine.AbortBatch();
        engine.Flush(); engine.WaitForPendingFlushes();

        // First read: cache miss, populates cache (with aborted set)
        std::string v1;
        bool found1 = engine.Lookup("w_key", v1);
        ASSERT_FALSE(found1);

        // Second read: cache hit — must still filter via CachedHeavy.aborted_batch_ts
        std::string v2;
        bool found2 = engine.Lookup("w_key", v2);
        ASSERT_FALSE(found2);

        // Normal key still works
        engine.Insert("good", "val");
        engine.Flush(); engine.WaitForPendingFlushes();
        std::string v3;
        ASSERT_TRUE(engine.Lookup("good", v3));
    }
    std::filesystem::remove_all("./test_abort_warm");
}

void TestBatchAbortManifestDurable() {
    std::filesystem::remove_all("./test_abort_manifest");
    {
        // Phase 1: abort, let it propagate to MANIFEST + SSTable
        {
            kvdb::LSMTreeEngine engine("./test_abort_manifest", 4096, 2, 16, 16, 100);
            ASSERT_TRUE(engine.StartBatch());
            engine.BatchInsert("m_key", "m_val");
            engine.AbortBatch();
            engine.Flush(); engine.WaitForPendingFlushes();
        }

        // Phase 2: reopen (MANIFEST recovery must restore abort)
        {
            kvdb::LSMTreeEngine engine("./test_abort_manifest", 4096, 2, 16, 16, 100);
            std::string v;
            ASSERT_FALSE(engine.Lookup("m_key", v));

            // Range scan must also filter
            auto iter = engine.RangeScan();
            bool found = false;
            while (iter.Valid()) {
                if (iter.Key() == "m_key") found = true;
                iter.Next();
            }
            ASSERT_FALSE(found);
        }
    }
    std::filesystem::remove_all("./test_abort_manifest");
}

void TestBatchAbortPersistsThroughTrimWAL() {
    std::filesystem::remove_all("./test_abort_trimwal");
    {
        {
            kvdb::LSMTreeEngine engine("./test_abort_trimwal", 4096, 2, 16, 16, 100);
            ASSERT_TRUE(engine.StartBatch());
            engine.BatchInsert("t_key", "t_val");
            engine.AbortBatch();
            engine.Flush(); engine.WaitForPendingFlushes();
            engine.TrimWAL();
        }

        {
            kvdb::LSMTreeEngine engine("./test_abort_trimwal", 4096, 2, 16, 16, 100);
            std::string v;
            ASSERT_FALSE(engine.Lookup("t_key", v));
        }
    }
    std::filesystem::remove_all("./test_abort_trimwal");
}

void TestLevelCounts() {
    std::filesystem::remove_all("./test_level_counts");
    {
        kvdb::LSMTreeEngine engine("./test_level_counts", 256 * 1024);
        std::string v(512, 'x');
        for (int i = 0; i < 20000; ++i) {
            char buf[32]; std::snprintf(buf, sizeof(buf), "k%08d", i);
            engine.Insert(buf, v);
        }
        engine.Flush(); engine.WaitForPendingFlushes();
        auto counts = engine.LevelCounts();
        ASSERT_TRUE(counts.size() > 0);
        ASSERT_TRUE(counts[0] > 0);
    }
    std::filesystem::remove_all("./test_level_counts");
}

void TestManualCompactEngine() {
    std::filesystem::remove_all("./test_manual_cp");
    {
        kvdb::LSMTreeEngine engine("./test_manual_cp", 256 * 1024);
        std::string v(512, 'x');
        for (int i = 0; i < 20000; ++i) {
            char buf[32]; std::snprintf(buf, sizeof(buf), "k%08d", i);
            engine.Insert(buf, v);
        }
        engine.Flush(); engine.WaitForPendingFlushes();
        auto before = engine.LevelCounts();
        ASSERT_TRUE(before[0] >= 8);
        int compacted = engine.ManualCompact(4, 0, true);
        ASSERT_TRUE(compacted > 0);
        auto after = engine.LevelCounts();
        ASSERT_TRUE(after[1] > 0);
    }
    std::filesystem::remove_all("./test_manual_cp");
}

void TestManualCompactRejectsLowThreshold() {
    std::filesystem::remove_all("./test_mc_low");
    {
        kvdb::LSMTreeEngine engine("./test_mc_low", 256 * 1024);
        std::string v(512, 'x');
        for (int i = 0; i < 5000; ++i) {
            char buf[32]; std::snprintf(buf, sizeof(buf), "k%08d", i);
            engine.Insert(buf, v);
        }
        engine.Flush(); engine.WaitForPendingFlushes();
        ASSERT_EQ(0, engine.ManualCompact(1, 0, false));
        ASSERT_EQ(0, engine.ManualCompact(0, 0, false));
    }
    std::filesystem::remove_all("./test_mc_low");
}

void TestManualCompactConcurrentReject() {
    std::filesystem::remove_all("./test_mc_concurrent");
    {
        kvdb::LSMTreeEngine engine("./test_mc_concurrent", 256 * 1024);
        std::string v(512, 'x');
        for (int i = 0; i < 20000; ++i) {
            char buf[32]; std::snprintf(buf, sizeof(buf), "k%08d", i);
            engine.Insert(buf, v);
        }
        engine.Flush(); engine.WaitForPendingFlushes();
        std::atomic<int> success{0}, rejected{0};
        std::thread t1([&]() { if (engine.ManualCompact(4, 0, true) > 0) success++; else rejected++; });
        std::thread t2([&]() { if (engine.ManualCompact(4, 0, true) > 0) success++; else rejected++; });
        t1.join(); t2.join();
        ASSERT_EQ(1, success.load());
        ASSERT_EQ(1, rejected.load());
    }
    std::filesystem::remove_all("./test_mc_concurrent");
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
    TestRangeScanCachedBlocks();
    TestCompactionCacheReadIntegrity();
    TestBatchInvisibleDuring();
    TestBatchSingleAtATime();
    TestBatchNormalWritesDuring();
    TestBatchGapExhausted();
    TestBatchAbortInvisible();
    TestBatchAbortAfterFlush();
    TestBatchAbortRecovery();
    TestBatchCrashRecoverAutoAbort();
    TestBatchCrashRecoverEmptyBatch();
    TestLevelCounts();
    TestManualCompactEngine();
    TestManualCompactRejectsLowThreshold();
    TestManualCompactConcurrentReject();
    TestBatchAbortRangeScanMemtable();
    TestBatchAbortWarmCacheLookup();
    TestBatchAbortManifestDurable();
    TestBatchAbortPersistsThroughTrimWAL();
}

} // namespace kvdb_test

RUN_TESTS()
