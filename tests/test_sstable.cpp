#include "test_common.hpp"
#include "kvdb/sstable.hpp"
#include "kvdb/config.hpp"
#include "kvdb/snappy.hpp"

#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

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
    for (int i = 0; i < 500; ++i)
        entries.push_back({"k" + std::to_string(i), "v" + std::to_string(i), static_cast<uint64_t>(i)});

    kvdb::SSTable::Write("./test_sstable_data/blockidx.sst", entries);

    auto meta = kvdb::SSTable::ReadMetadata("./test_sstable_data/blockidx.sst");
    ASSERT_TRUE(meta.block_offsets.size() > 1);
    ASSERT_EQ(meta.block_offsets.size(), meta.block_first_keys.size());

    for (int i = 0; i < 500; ++i) {
        std::string v;
        ASSERT_TRUE(kvdb::SSTable::LookupKey("./test_sstable_data/blockidx.sst",
            "k" + std::to_string(i), static_cast<uint64_t>(i), v));
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
    TestRangeFilter();
    TestSnappyRoundTrip();
}

} // namespace kvdb_test

RUN_TESTS()
