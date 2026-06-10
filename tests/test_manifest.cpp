#include "test_common.hpp"
#include "kvdb/manifest.hpp"
#include "kvdb/sstable.hpp"

#include <cstdio>
#include <filesystem>
#include <string>

namespace kvdb_test {

namespace fs = std::filesystem;

static const std::string kManifestPath = "./test_manifest_data/MANIFEST";

void Cleanup() {
    if (fs::exists("./test_manifest_data")) {
        fs::remove_all("./test_manifest_data");
    }
    fs::create_directories("./test_manifest_data");
}

void TestAddAndRecover() {
    Cleanup();
    {
        kvdb::Manifest m(kManifestPath);

        kvdb::SSTable::Metadata meta;
        meta.filepath = "sstable_0.sst";
        meta.entry_count = 42;
        meta.min_key_len = 1;
        meta.max_key_len = 100;
        meta.file_size = 4096;

        m.AddSSTable(0, meta);
    }

    {
        kvdb::Manifest m(kManifestPath);
        auto results = m.Recover();
        ASSERT_EQ(1u, results.size());
        ASSERT_STREQ("sstable_0.sst", results[0].filepath);
        ASSERT_EQ(42u, results[0].entry_count);
        ASSERT_EQ(1u, results[0].min_key_len);
        ASSERT_EQ(100u, results[0].max_key_len);
        ASSERT_EQ(4096u, results[0].file_size);
    }
    Cleanup();
}

void TestMultipleTables() {
    Cleanup();
    {
        kvdb::Manifest m(kManifestPath);

        for (int i = 0; i < 10; ++i) {
            kvdb::SSTable::Metadata meta;
            meta.filepath = "sstable_" + std::to_string(i) + ".sst";
            meta.entry_count = static_cast<size_t>(i * 10);
            meta.min_key_len = 0;
            meta.max_key_len = static_cast<uint32_t>(i);
            meta.file_size = static_cast<uint64_t>(i * 1024);
            m.AddSSTable(static_cast<uint64_t>(i), meta);
        }
    }

    {
        kvdb::Manifest m(kManifestPath);
        auto results = m.Recover();
        ASSERT_EQ(10u, results.size());

        for (int i = 0; i < 10; ++i) {
            ASSERT_EQ(static_cast<size_t>(i * 10), results[static_cast<size_t>(i)].entry_count);
        }
    }
    Cleanup();
}

void TestEmptyManifest() {
    Cleanup();
    {
        kvdb::Manifest m(kManifestPath);
        auto results = m.Recover();
        ASSERT_EQ(0u, results.size());
    }
    Cleanup();
}

void TestRemoveSSTable() {
    Cleanup();
    {
        kvdb::Manifest m(kManifestPath);

        kvdb::SSTable::Metadata meta;
        meta.filepath = "sstable_keep.sst";
        meta.entry_count = 10;
        meta.min_key_len = 1;
        meta.max_key_len = 5;
        meta.file_size = 512;
        m.AddSSTable(0, meta);

        meta.filepath = "sstable_remove.sst";
        meta.entry_count = 20;
        meta.min_key_len = 1;
        meta.max_key_len = 10;
        meta.file_size = 1024;
        m.AddSSTable(1, meta);

        m.RemoveSSTable(1);
    }

    {
        kvdb::Manifest m(kManifestPath);
        auto results = m.Recover();
        ASSERT_EQ(1u, results.size());
        ASSERT_STREQ("sstable_keep.sst", results[0].filepath);
    }
    Cleanup();
}

void TestEmptyManifestRecoverFromNonexistent() {
    fs::remove_all("./test_manifest_nonexist");
    fs::create_directories("./test_manifest_nonexist");
    {
        kvdb::Manifest m("./test_manifest_nonexist/MANIFEST");
        auto results = m.Recover();
        ASSERT_EQ(0u, results.size());
    }
    fs::remove_all("./test_manifest_nonexist");
}

void TestCompactRemovesDeadSSTables() {
    fs::remove_all("./test_mnfst_compact1");
    fs::create_directories("./test_mnfst_compact1");
    {
        kvdb::Manifest m("./test_mnfst_compact1/MANIFEST");

        kvdb::SSTable::Metadata meta;
        meta.filepath = "keep.sst"; meta.entry_count = 10;
        meta.file_size = 100; meta.level = 0;
        m.AddSSTable(1, meta);

        meta.filepath = "dead.sst"; meta.entry_count = 20;
        m.AddSSTable(2, meta);
        m.RemoveSSTable(2);

        meta.filepath = "alive.sst"; meta.entry_count = 30;
        m.AddSSTable(3, meta);

        m.Compact();
    }
    {
        kvdb::Manifest m("./test_mnfst_compact1/MANIFEST");
        auto results = m.Recover();
        ASSERT_EQ(2u, results.size());
        ASSERT_STREQ("keep.sst", results[0].filepath);
        ASSERT_STREQ("alive.sst", results[1].filepath);
    }
    fs::remove_all("./test_mnfst_compact1");
}

void TestCompactRetainsNeededAbort() {
    fs::remove_all("./test_mnfst_compact2");
    fs::create_directories("./test_mnfst_compact2");
    {
        kvdb::Manifest m("./test_mnfst_compact2/MANIFEST");

        kvdb::SSTable::Metadata meta;
        meta.filepath = "pre.sst"; meta.entry_count = 10;
        meta.file_size = 100; meta.level = 0;
        meta.manifest_seq = 0;
        m.AddSSTable(0, meta);

        m.AddAbortBatch(42, 10);

        m.Compact();
    }
    {
        kvdb::Manifest m("./test_mnfst_compact2/MANIFEST");
        auto results = m.Recover();
        ASSERT_EQ(1u, results.size());
        ASSERT_STREQ("pre.sst", results[0].filepath);
        ASSERT_TRUE(results[0].aborted_batch_ts.count(42) > 0);
    }
    fs::remove_all("./test_mnfst_compact2");
}

void TestCompactDropsStaleAbort() {
    fs::remove_all("./test_mnfst_compact3");
    fs::create_directories("./test_mnfst_compact3");
    {
        kvdb::Manifest m("./test_mnfst_compact3/MANIFEST");

        kvdb::SSTable::Metadata meta;
        meta.filepath = "post.sst"; meta.entry_count = 10;
        meta.file_size = 100; meta.level = 0;
        meta.manifest_seq = 100;
        m.AddSSTable(100, meta);

        m.AddAbortBatch(77, 50);

        m.Compact();
    }
    {
        kvdb::Manifest m("./test_mnfst_compact3/MANIFEST");
        auto results = m.Recover();
        ASSERT_EQ(1u, results.size());
        ASSERT_STREQ("post.sst", results[0].filepath);
        ASSERT_TRUE(results[0].aborted_batch_ts.count(77) == 0);
    }
    fs::remove_all("./test_mnfst_compact3");
}

void RunTests() {
    std::cout << "Running Manifest Tests...\n\n";

    TestAddAndRecover();
    TestMultipleTables();
    TestEmptyManifest();
    TestRemoveSSTable();
    TestEmptyManifestRecoverFromNonexistent();
    TestCompactRemovesDeadSSTables();
    TestCompactRetainsNeededAbort();
    TestCompactDropsStaleAbort();
}

} // namespace kvdb_test

RUN_TESTS()
