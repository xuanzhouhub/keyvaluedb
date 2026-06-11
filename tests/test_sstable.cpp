#include "test_common.hpp"
#include "kvdb/sstable.hpp"
#include "kvdb/block_reader.hpp"
#include "kvdb/block_cache.hpp"
#include "kvdb/config.hpp"
#include "kvdb/snappy.hpp"
#include "kvdb/bptree.hpp"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

namespace {
struct NullBlockReader : kvdb::BlockReader {
    std::shared_ptr<const kvdb::CachedHeavy> GetHeavy(uint64_t) override { return nullptr; }
    void PutHeavy(uint64_t, kvdb::BloomFilter, std::vector<uint64_t>, std::string,
                  std::vector<uint32_t>, const std::unordered_set<uint64_t>&) override {}
    std::shared_ptr<const std::string> GetBlock(uint64_t, uint32_t) override { return nullptr; }
    void PutBlock(uint64_t, uint32_t, std::string) override {}
    void Invalidate(uint64_t) override {}
};
}

namespace kvdb_test {

namespace fs = std::filesystem;

static const std::string kTestDataDir = "./test_sstable_data";

void CleanupTestDir() {
    if (fs::exists(kTestDataDir)) {
        fs::remove_all(kTestDataDir);
    }
}

void SetupTestDir() {
    CleanupTestDir();
    fs::create_directories(kTestDataDir);
}

void TestWriteAndReadEmpty() {
    SetupTestDir();
    std::string filepath = kTestDataDir + "/empty.sst";

    std::vector<kvdb::KeyValuePair> entries;
    kvdb::SSTable::Write(filepath, entries);

    ASSERT_TRUE(fs::exists(filepath));

    auto read_entries = kvdb::SSTable::ReadAll(filepath);
    ASSERT_EQ(0u, read_entries.size());

    CleanupTestDir();
}

void TestWriteAndReadSingle() {
    SetupTestDir();
    std::string filepath = kTestDataDir + "/single.sst";

    std::vector<kvdb::KeyValuePair> entries;
    entries.push_back({"hello", "world"});

    kvdb::SSTable::Write(filepath, entries);

    ASSERT_TRUE(fs::exists(filepath));

    auto read_entries = kvdb::SSTable::ReadAll(filepath);
    ASSERT_EQ(1u, read_entries.size());
    ASSERT_EQ(std::string("hello"), read_entries[0].key);
    ASSERT_EQ(std::string("world"), read_entries[0].value);

    CleanupTestDir();
}

void TestWriteAndReadMultiple() {
    SetupTestDir();
    std::string filepath = kTestDataDir + "/multiple.sst";

    std::vector<kvdb::KeyValuePair> entries;
    for (int i = 0; i < 1000; ++i) {
        entries.push_back({"key_" + std::to_string(i), "value_" + std::to_string(i)});
    }

    kvdb::SSTable::Write(filepath, entries);

    auto read_entries = kvdb::SSTable::ReadAll(filepath);
    ASSERT_EQ(1000u, read_entries.size());

    for (int i = 0; i < 1000; ++i) {
        ASSERT_EQ(std::string("key_" + std::to_string(i)), read_entries[i].key);
        ASSERT_EQ(std::string("value_" + std::to_string(i)), read_entries[i].value);
    }

    CleanupTestDir();
}

void TestWriteAndReadLargeValues() {
    SetupTestDir();
    std::string filepath = kTestDataDir + "/large.sst";

    std::string large_value(10000, 'X');
    std::vector<kvdb::KeyValuePair> entries;
    entries.push_back({"big_key", large_value});

    kvdb::SSTable::Write(filepath, entries);

    auto read_entries = kvdb::SSTable::ReadAll(filepath);
    ASSERT_EQ(1u, read_entries.size());
    ASSERT_EQ(std::string("big_key"), read_entries[0].key);
    ASSERT_EQ(large_value, read_entries[0].value);

    CleanupTestDir();
}

void TestWriteAndReadBinaryData() {
    SetupTestDir();
    std::string filepath = kTestDataDir + "/binary.sst";

    std::vector<kvdb::KeyValuePair> entries;
    std::string binary_key("\x00\x01\x02\xFF", 4);
    std::string binary_value("\xAA\xBB\xCC\xDD\xEE", 5);
    entries.push_back({binary_key, binary_value});

    kvdb::SSTable::Write(filepath, entries);

    auto read_entries = kvdb::SSTable::ReadAll(filepath);
    ASSERT_EQ(1u, read_entries.size());
    ASSERT_TRUE(read_entries[0].key == binary_key);
    ASSERT_TRUE(read_entries[0].value == binary_value);

    CleanupTestDir();
}

void TestReadMetadata() {
    SetupTestDir();
    std::string filepath = kTestDataDir + "/meta.sst";

    std::vector<kvdb::KeyValuePair> entries;
    entries.push_back({"short", "v"});
    entries.push_back({"longer_key_here", "value"});
    entries.push_back({"k", "value_long"});

    kvdb::SSTable::Write(filepath, entries);
    ASSERT_TRUE(fs::exists(filepath));

    auto meta = kvdb::SSTable::ReadMetadata(filepath);
    ASSERT_EQ(3u, meta.entry_count);
    ASSERT_TRUE(meta.file_size > 0);
    ASSERT_TRUE(meta.min_key_len <= meta.max_key_len);

    CleanupTestDir();
}

void TestReadMetadataEmpty() {
    SetupTestDir();
    std::string filepath = kTestDataDir + "/empty_meta.sst";

    std::vector<kvdb::KeyValuePair> entries;
    kvdb::SSTable::Write(filepath, entries);

    auto meta = kvdb::SSTable::ReadMetadata(filepath);
    ASSERT_EQ(0u, meta.entry_count);

    CleanupTestDir();
}

void TestFileSizeAfterWrite() {
    SetupTestDir();
    std::string filepath = kTestDataDir + "/size.sst";

    std::vector<kvdb::KeyValuePair> entries;
    for (int i = 0; i < 100; ++i) {
        entries.push_back({"key_" + std::to_string(i), "value_" + std::to_string(i)});
    }

    kvdb::SSTable::Write(filepath, entries);

    auto meta = kvdb::SSTable::ReadMetadata(filepath);
    ASSERT_TRUE(meta.file_size > 0);

    auto actual_size = fs::file_size(filepath);
    ASSERT_EQ(actual_size, meta.file_size);

    CleanupTestDir();
}

void TestInvalidFileThrows() {
    SetupTestDir();
    std::string filepath = kTestDataDir + "/invalid.sst";

    std::ofstream file(filepath);
    file << "not a valid sstable";
    file.close();

    try {
        kvdb::SSTable::ReadAll(filepath);
        ASSERT_TRUE(false);
    } catch (const std::runtime_error&) {
        ASSERT_TRUE(true);
    }

    CleanupTestDir();
}

void TestPreserveOrder() {
    SetupTestDir();
    std::string filepath = kTestDataDir + "/order.sst";

    std::vector<kvdb::KeyValuePair> entries;
    entries.push_back({"a", "1"});
    entries.push_back({"b", "2"});
    entries.push_back({"c", "3"});
    entries.push_back({"d", "4"});
    entries.push_back({"e", "5"});

    kvdb::SSTable::Write(filepath, entries);

    auto read = kvdb::SSTable::ReadAll(filepath);
    ASSERT_EQ(5u, read.size());
    ASSERT_EQ(std::string("a"), read[0].key);
    ASSERT_EQ(std::string("b"), read[1].key);
    ASSERT_EQ(std::string("c"), read[2].key);
    ASSERT_EQ(std::string("d"), read[3].key);
    ASSERT_EQ(std::string("e"), read[4].key);

    CleanupTestDir();
}

void TestNonExistentFileThrows() {
    try {
        kvdb::SSTable::ReadAll("./nonexistent_file_xyz.sst");
        ASSERT_TRUE(false);
    } catch (const std::runtime_error&) {
        ASSERT_TRUE(true);
    }
}

void TestBloomFilterNoFalseNegatives() {
    std::filesystem::remove_all("./test_sstable_data");
    std::filesystem::create_directories("./test_sstable_data");

    std::vector<kvdb::KeyValuePair> entries;
    for (int i = 0; i < 100; ++i)
        entries.push_back({"key_" + std::to_string(i), "val_" + std::to_string(i), static_cast<uint64_t>(i)});

    kvdb::SSTable::Write("./test_sstable_data/bloom.sst", entries);
    auto meta = kvdb::SSTable::ReadMetadata("./test_sstable_data/bloom.sst");

    for (int i = 0; i < 100; ++i)
        ASSERT_TRUE(meta.bloom.MightContain("key_" + std::to_string(i)));
}

void TestBlockIndexLookup() {
    std::filesystem::remove_all("./test_sstable_data");
    std::filesystem::create_directories("./test_sstable_data");

    std::vector<kvdb::KeyValuePair> entries;
    for (int i = 0; i < 500; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "k%03d", i);
        entries.push_back({buf, "v" + std::to_string(i), static_cast<uint64_t>(i)});
    }

    kvdb::SSTable::Write("./test_sstable_data/blockidx.sst", entries);

    auto meta = kvdb::SSTable::ReadMetadata("./test_sstable_data/blockidx.sst");
    ASSERT_TRUE(meta.block_offsets.size() > 1);
    ASSERT_EQ(meta.block_offsets.size(), meta.FirstKeyCount());

    for (int i = 0; i < 500; ++i) {
        std::string v;
        char buf[16];
        std::snprintf(buf, sizeof(buf), "k%03d", i);
        ASSERT_TRUE(kvdb::SSTable::LookupKey("./test_sstable_data/blockidx.sst",
            buf, static_cast<uint64_t>(i), v));
        ASSERT_STREQ("v" + std::to_string(i), v);
    }

    std::string v;
    ASSERT_FALSE(kvdb::SSTable::LookupKey("./test_sstable_data/blockidx.sst", "nonexistent", 0, v));
}

void TestRangeFilter() {
    std::filesystem::remove_all("./test_sstable_data");
    std::filesystem::create_directories("./test_sstable_data");

    std::vector<kvdb::KeyValuePair> entries;
    entries.push_back({"a", "first", 1});
    entries.push_back({"m", "middle", 2});
    entries.push_back({"z", "last", 3});

    kvdb::SSTable::Write("./test_sstable_data/range.sst", entries);
    auto meta = kvdb::SSTable::ReadMetadata("./test_sstable_data/range.sst");

    ASSERT_STREQ("a", meta.min_key);
    ASSERT_STREQ("z", meta.max_key);
}

void TestSnappyRoundTrip() {
    std::string original = "The quick brown fox jumps over the lazy dog. ";
    for (int i = 0; i < 10; ++i) original += original;

    std::string compressed;
    ASSERT_TRUE(kvdb::Snappy::Compress(original.data(), original.size(), compressed));
    ASSERT_TRUE(compressed.size() < original.size());

    std::string decompressed;
    ASSERT_TRUE(kvdb::Snappy::Uncompress(compressed.data(), compressed.size(), decompressed));
    ASSERT_STREQ(original, decompressed);
}

void TestWriteFromWalkPicksNewestTS() {
    std::filesystem::remove_all("./test_sstable_data");
    std::filesystem::create_directories("./test_sstable_data");

    kvdb::BPlusTree tree;
    for (int i = 0; i < 100; ++i)
        tree.Insert("k1", "old_" + std::to_string(i), static_cast<uint64_t>(i + 1));
    tree.Insert("k2", "v2", 200);
    tree.Insert("k1", "NEWEST", 201);

    kvdb::BPlusTree::MemTableWalk walk(tree);
    std::string filepath = "./test_sstable_data/walk.sst";
    kvdb::SSTable::WriteFromWalk(filepath, walk, tree.Size());

    auto entries = kvdb::SSTable::ReadAll(filepath);
    ASSERT_EQ(2u, entries.size());

    std::string v;
    ASSERT_TRUE(kvdb::SSTable::LookupKey(filepath, "k1", 999, v));
    ASSERT_STREQ("NEWEST", v);

    ASSERT_TRUE(kvdb::SSTable::LookupKey(filepath, "k2", 999, v));
    ASSERT_STREQ("v2", v);
}

void TestWriteFromWalkWithBlobs() {
    std::filesystem::remove_all("./test_sstable_data");
    std::filesystem::create_directories("./test_sstable_data");

    kvdb::BPlusTree tree;
    std::string small_val("small");
    std::string blob1(3000, 'A');
    std::string blob2(10000, 'B');
    std::string blob3(5000, 'C');
    std::string huge_blob(1024 * 1024, 'Z');

    tree.Insert("alpha", small_val, 1);
    tree.Insert("bravo", blob1, 2);
    tree.Insert("charlie", blob2, 3);
    tree.Insert("delta", small_val, 5);
    tree.Insert("delta", blob3, 10);

    tree.Insert("echo", huge_blob, 20);

    kvdb::BPlusTree::MemTableWalk walk(tree);
    std::string filepath = "./test_sstable_data/blob.sst";
    kvdb::SSTable::WriteFromWalk(filepath, walk, tree.Size());

    auto entries = kvdb::SSTable::ReadAll(filepath);
    ASSERT_EQ(5u, entries.size());

    std::string v;
    ASSERT_TRUE(kvdb::SSTable::LookupKey(filepath, "alpha", 99, v));
    ASSERT_STREQ(small_val, v);

    ASSERT_TRUE(kvdb::SSTable::LookupKey(filepath, "bravo", 99, v));
    ASSERT_STREQ(blob1, v);

    ASSERT_TRUE(kvdb::SSTable::LookupKey(filepath, "charlie", 99, v));
    ASSERT_STREQ(blob2, v);

    ASSERT_TRUE(kvdb::SSTable::LookupKey(filepath, "delta", 99, v));
    ASSERT_STREQ(blob3, v);

    ASSERT_TRUE(kvdb::SSTable::LookupKey(filepath, "echo", 99, v));
    ASSERT_STREQ(huge_blob, v);
    ASSERT_EQ(1024u * 1024u, v.size());

    auto meta = kvdb::SSTable::ReadMetadata(filepath);
    ASSERT_EQ(5u, meta.entry_count);
}

void TestCompactBasic() {
    std::filesystem::remove_all("./test_sstable_data");
    std::filesystem::create_directories("./test_sstable_data");

    std::vector<kvdb::KeyValuePair> e1, e2;
    e1.push_back({"a", "first", 1, false});
    e1.push_back({"b", "second", 2, false});
    e2.push_back({"c", "third", 3, false});
    e2.push_back({"d", "fourth", 4, false});

    kvdb::SSTable::Write("./test_sstable_data/comp1.sst", e1);
    kvdb::SSTable::Write("./test_sstable_data/comp2.sst", e2);

    auto m1 = kvdb::SSTable::ReadMetadata("./test_sstable_data/comp1.sst");
    m1.level = 1; m1.filepath = "./test_sstable_data/comp1.sst";
    auto m2 = kvdb::SSTable::ReadMetadata("./test_sstable_data/comp2.sst");
    m2.level = 1; m2.filepath = "./test_sstable_data/comp2.sst";

    std::vector<kvdb::SSTable::Metadata> inputs = {m1, m2};
    std::vector<kvdb::SSTable::Metadata> outputs;
    std::vector<std::string> garbage;
    NullBlockReader nbr;

    kvdb::SSTable::Compact(inputs, "./test_sstable_data", 100, 2,
                           4 * 1024 * 1024, false, "", "", outputs, garbage, nbr);

    ASSERT_EQ(1u, outputs.size());
    ASSERT_EQ(2, outputs[0].level);
    ASSERT_EQ(4u, outputs[0].entry_count);

    auto result = kvdb::SSTable::ReadAll(outputs[0].filepath);
    ASSERT_EQ(4u, result.size());
    ASSERT_STREQ("a", result[0].key);
    ASSERT_STREQ("b", result[1].key);
    ASSERT_STREQ("c", result[2].key);
    ASSERT_STREQ("d", result[3].key);
}

void TestCompactTombstoneRemoval() {
    std::filesystem::remove_all("./test_sstable_data");
    std::filesystem::create_directories("./test_sstable_data");

    std::vector<kvdb::KeyValuePair> entries;
    entries.push_back({"k", "old", 5, false});
    entries.push_back({"k", "", 10, true});

    kvdb::SSTable::Write("./test_sstable_data/tomb.sst", entries);

    auto m = kvdb::SSTable::ReadMetadata("./test_sstable_data/tomb.sst");
    m.level = 0; m.filepath = "./test_sstable_data/tomb.sst";

    std::vector<kvdb::SSTable::Metadata> inputs = {m};
    std::vector<kvdb::SSTable::Metadata> outputs;
    std::vector<std::string> garbage;
    NullBlockReader nbr2;

    kvdb::SSTable::Compact(inputs, "./test_sstable_data", 200, 7,
                           4 * 1024 * 1024, true, "", "", outputs, garbage, nbr2);

    ASSERT_EQ(0u, outputs.size());
}

void TestCompactSplitting() {
    std::filesystem::remove_all("./test_sstable_data");
    std::filesystem::create_directories("./test_sstable_data");

    std::vector<kvdb::KeyValuePair> entries;
    for (int i = 0; i < 100; ++i)
        entries.push_back({"k" + std::to_string(i), std::string(500, 'X'),
                           static_cast<uint64_t>(i), false});

    kvdb::SSTable::Write("./test_sstable_data/big.sst", entries);
    auto m = kvdb::SSTable::ReadMetadata("./test_sstable_data/big.sst");
    m.level = 0; m.filepath = "./test_sstable_data/big.sst";

    std::vector<kvdb::SSTable::Metadata> inputs = {m};
    std::vector<kvdb::SSTable::Metadata> outputs;
    std::vector<std::string> garbage;
    NullBlockReader nbr3;

    kvdb::SSTable::Compact(inputs, "./test_sstable_data", 300, 1,
                           8 * 1024, false, "", "", outputs, garbage, nbr3);

    ASSERT_TRUE(outputs.size() > 1);
    size_t total = 0;
    for (auto& o : outputs) total += o.entry_count;
    ASSERT_EQ(100u, total);
    ASSERT_EQ(1, outputs[0].level);
}

void TestCacheHitMiss();
void TestCacheBloomOffsets();
void TestCacheInvalidate();
void TestCacheEviction();
void TestCacheZeroCopySurvival();
void TestCacheUpdate();

void RunTests() {
    std::cout << "Running SSTable Tests...\n\n";

    TestWriteAndReadEmpty();
    TestWriteAndReadSingle();
    TestWriteAndReadMultiple();
    TestWriteAndReadLargeValues();
    TestWriteAndReadBinaryData();
    TestReadMetadata();
    TestReadMetadataEmpty();
    TestFileSizeAfterWrite();
    TestInvalidFileThrows();
    TestPreserveOrder();
    TestNonExistentFileThrows();
    TestBloomFilterNoFalseNegatives();
    TestBlockIndexLookup();
    TestRangeFilter();
    TestSnappyRoundTrip();
    TestWriteFromWalkPicksNewestTS();
    TestWriteFromWalkWithBlobs();
    TestCompactBasic();
    TestCompactTombstoneRemoval();
    TestCompactSplitting();
    TestCacheHitMiss();
    TestCacheBloomOffsets();
    TestCacheInvalidate();
    TestCacheEviction();
    TestCacheZeroCopySurvival();
    TestCacheUpdate();
}

void TestCacheHitMiss() {
    kvdb::SSTableCache cache(4, 4, 1024, 1);

    cache.PutBlock(1, 0, "hello");
    auto sp = cache.GetBlock(1, 0);
    ASSERT_TRUE(sp != nullptr);
    ASSERT_TRUE(*sp == "hello");

    auto miss = cache.GetBlock(1, 1);
    ASSERT_TRUE(miss == nullptr);

    miss = cache.GetBlock(2, 0);
    ASSERT_TRUE(miss == nullptr);
}

void TestCacheBloomOffsets() {
    kvdb::SSTableCache cache(4, 4, 4096, 1);
    uint64_t seq = 42;

    kvdb::BloomFilter bf(100, 0.01);
    bf.Add("foo");

    std::vector<uint64_t> offsets = {100, 200, 300};
    std::string first_key_buf;
    first_key_buf.push_back('\x01'); first_key_buf.push_back('\0'); first_key_buf += "a";
    first_key_buf.push_back('\x01'); first_key_buf.push_back('\0'); first_key_buf += "m";
    first_key_buf.push_back('\x01'); first_key_buf.push_back('\0'); first_key_buf += "z";
    cache.PutHeavy(seq, bf, offsets, first_key_buf, {}, {});

    auto heavy = cache.GetHeavy(seq);
    ASSERT_TRUE(heavy != nullptr);
    ASSERT_TRUE(heavy->bloom.BitCount() > 0);
    ASSERT_EQ(3u, heavy->block_offsets.size());
    ASSERT_EQ(100u, heavy->block_offsets[0]);
}

void TestCacheInvalidate() {
    kvdb::SSTableCache cache(8, 8, 4096, 1);

    cache.PutBlock(1, 0, "a");
    cache.PutBlock(1, 1, "b");
    cache.PutBlock(2, 0, "c");

    cache.Invalidate(1);

    ASSERT_TRUE(cache.GetBlock(1, 0) == nullptr);
    ASSERT_TRUE(cache.GetBlock(1, 1) == nullptr);
    ASSERT_TRUE(cache.GetBlock(2, 0) != nullptr);
    ASSERT_TRUE(*cache.GetBlock(2, 0) == "c");
}

void TestCacheEviction() {
    kvdb::SSTableCache cache(2, 2, 256, 1);

    cache.PutBlock(1, 0, std::string(50, 'a'));
    cache.PutBlock(2, 0, std::string(50, 'b'));
    ASSERT_TRUE(cache.GetBlock(1, 0) != nullptr);
    ASSERT_TRUE(cache.GetBlock(2, 0) != nullptr);

    cache.PutBlock(3, 0, std::string(50, 'c'));
    ASSERT_TRUE(cache.GetBlock(1, 0) == nullptr);
    ASSERT_TRUE(cache.GetBlock(2, 0) != nullptr);
    ASSERT_TRUE(cache.GetBlock(3, 0) != nullptr);

    cache.PutBlock(4, 0, std::string(200, 'd'));
    ASSERT_TRUE(cache.GetBlock(2, 0) == nullptr);
    ASSERT_TRUE(cache.GetBlock(4, 0) != nullptr);
}

void TestCacheZeroCopySurvival() {
    kvdb::SSTableCache cache(1, 1, 64, 1);

    cache.PutBlock(1, 0, "persistent");
    auto sp = cache.GetBlock(1, 0);
    ASSERT_TRUE(sp != nullptr);
    ASSERT_TRUE(*sp == "persistent");

    cache.PutBlock(2, 0, "pusher");

    ASSERT_TRUE(cache.GetBlock(1, 0) == nullptr);

    ASSERT_TRUE(*sp == "persistent");
    ASSERT_EQ(10u, sp->size());
}

void TestCacheUpdate() {
    kvdb::SSTableCache cache(4, 4, 256, 1);

    cache.PutBlock(1, 0, "old");
    cache.PutBlock(1, 0, "new");

    auto sp = cache.GetBlock(1, 0);
    ASSERT_TRUE(sp != nullptr);
    ASSERT_TRUE(*sp == "new");

    cache.PutHeavy(5, kvdb::BloomFilter(10, 0.01), std::vector<uint64_t>{}, std::string{}, {}, {});
    kvdb::BloomFilter bf2(100, 0.01);
    bf2.Add("test");
    cache.PutHeavy(5, bf2, std::vector<uint64_t>{}, std::string{}, {}, {});

    auto heavy = cache.GetHeavy(5);
    ASSERT_TRUE(heavy != nullptr);
    ASSERT_TRUE(heavy->bloom.BitCount() > 0);
}

} // namespace kvdb_test

RUN_TESTS()
