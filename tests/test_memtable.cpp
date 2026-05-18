#include "test_common.hpp"
#include "kvdb/memtable.hpp"
#include <string>
#include <vector>

namespace kvdb_test {

void TestBasicInsert() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    ASSERT_EQ(0u, memtable.EntryCount());
    ASSERT_EQ(0u, memtable.ApproximateMemoryUsage());
    memtable.Insert("hello", "world");
    ASSERT_EQ(1u, memtable.EntryCount());
    ASSERT_TRUE(memtable.ApproximateMemoryUsage() > 0);
}

void TestMultipleInserts() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    for (int i = 0; i < 10; ++i) {
        memtable.Insert("key_" + std::to_string(i), "value_" + std::to_string(i));
    }
    ASSERT_EQ(10u, memtable.EntryCount());
}

void TestIsFull() {
    kvdb::MemTable memtable(0, 100);
    ASSERT_FALSE(memtable.IsFull());
    std::string big_value(200, 'X');
    memtable.Insert("a", big_value);
    ASSERT_TRUE(memtable.IsFull());
}

void TestFreeze() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    memtable.Insert("a", "b");
    ASSERT_FALSE(memtable.IsFrozen());
    memtable.Freeze();
    ASSERT_TRUE(memtable.IsFrozen());
    memtable.Insert("c", "d");
    ASSERT_EQ(1u, memtable.EntryCount());
}

void TestExport() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    memtable.Insert("z", "last");
    memtable.Insert("a", "first");
    auto entries = memtable.ExportEntries();
    ASSERT_EQ(2u, entries.size());
    ASSERT_STREQ("a", entries[0].key);
    ASSERT_STREQ("first", entries[0].value);
    ASSERT_STREQ("z", entries[1].key);
    ASSERT_STREQ("last", entries[1].value);
}

void TestExportEmpty() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    auto entries = memtable.ExportEntries();
    ASSERT_EQ(0u, entries.size());
}

void TestKeyUpdate() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    memtable.Insert("key", "old");
    ASSERT_EQ(1u, memtable.EntryCount());
    memtable.Insert("key", "new");
    ASSERT_EQ(1u, memtable.EntryCount());
    auto entries = memtable.ExportEntries();
    ASSERT_STREQ("new", entries[0].value);
}

void TestEntryCountAfterInserts() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    ASSERT_EQ(0u, memtable.EntryCount());
    memtable.Insert("a", "1");
    memtable.Insert("b", "2");
    memtable.Insert("c", "3");
    ASSERT_EQ(3u, memtable.EntryCount());
}

void TestConstructorMaxBytes() {
    kvdb::MemTable memtable(0, 5000);
    ASSERT_FALSE(memtable.IsFull());
    memtable.Insert("key", std::string(6000, 'X'));
    ASSERT_TRUE(memtable.IsFull());
}

void TestDuplicateKeys() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    memtable.Insert("dup", "v1");
    ASSERT_EQ(1u, memtable.EntryCount());
    memtable.Insert("dup", "v2");
    ASSERT_EQ(1u, memtable.EntryCount());
    memtable.Insert("dup", "v3");
    ASSERT_EQ(1u, memtable.EntryCount());
}

void TestMemoryGrowthMonotonic() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    size_t prev = memtable.ApproximateMemoryUsage();
    for (int i = 0; i < 10; ++i) {
        memtable.Insert("k" + std::to_string(i), "v" + std::to_string(i));
        ASSERT_TRUE(memtable.ApproximateMemoryUsage() > prev);
        prev = memtable.ApproximateMemoryUsage();
    }
}

void TestLookup() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    memtable.Insert("hello", "world", 10);
    std::string value;
    ASSERT_TRUE(memtable.Lookup("hello", 10, value));
    ASSERT_STREQ("world", value);
}

void TestLookupTimestamp() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    memtable.Insert("hello", "world", 10);
    std::string value;
    ASSERT_FALSE(memtable.Lookup("hello", 5, value));
    ASSERT_TRUE(memtable.Lookup("hello", 15, value));
}

void TestLookupNotFound() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    memtable.Insert("hello", "world", 10);
    std::string value;
    ASSERT_FALSE(memtable.Lookup("nope", 10, value));
}

void TestMVCCMultiVersion() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    memtable.Insert("k", "v1", 5);
    memtable.Insert("k", "v2", 10);
    memtable.Insert("k", "v3", 15);
    memtable.Insert("k", "v4", 20);

    std::string v;
    ASSERT_TRUE(memtable.Lookup("k", 5, v));
    ASSERT_STREQ("v1", v);

    ASSERT_TRUE(memtable.Lookup("k", 10, v));
    ASSERT_STREQ("v2", v);

    ASSERT_TRUE(memtable.Lookup("k", 12, v));
    ASSERT_STREQ("v2", v);

    ASSERT_TRUE(memtable.Lookup("k", 15, v));
    ASSERT_STREQ("v3", v);

    ASSERT_TRUE(memtable.Lookup("k", 20, v));
    ASSERT_STREQ("v4", v);

    ASSERT_TRUE(memtable.Lookup("k", 100, v));
    ASSERT_STREQ("v4", v);

    ASSERT_FALSE(memtable.Lookup("k", 3, v));
}

void TestMVCCMultiVersionInterleaved() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    memtable.Insert("k1", "a", 1);
    memtable.Insert("k2", "b", 2);
    memtable.Insert("k1", "A", 3);
    memtable.Insert("k2", "B", 4);
    memtable.Insert("k1", "@", 5);

    std::string v;
    ASSERT_TRUE(memtable.Lookup("k1", 1, v)); ASSERT_STREQ("a", v);
    ASSERT_TRUE(memtable.Lookup("k1", 3, v)); ASSERT_STREQ("A", v);
    ASSERT_TRUE(memtable.Lookup("k1", 5, v)); ASSERT_STREQ("@", v);

    ASSERT_TRUE(memtable.Lookup("k2", 2, v)); ASSERT_STREQ("b", v);
    ASSERT_TRUE(memtable.Lookup("k2", 4, v)); ASSERT_STREQ("B", v);
}

void TestLargeValue() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    std::string big_value(4000, 'X');
    memtable.Insert("big", big_value, 10);

    std::string v;
    ASSERT_TRUE(memtable.Lookup("big", 10, v));
    ASSERT_EQ(4000u, v.size());
    ASSERT_EQ('X', v[0]);
    ASSERT_EQ('X', v[3999]);
}

void TestLargeValueBlobRoundTrip() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    std::string big_value(4000, 'Y');
    memtable.Insert("blobkey", big_value, 42);

    std::string v;
    ASSERT_TRUE(memtable.Lookup("blobkey", 42, v));
    ASSERT_STREQ(big_value, v);

    auto entries = memtable.ExportEntries();
    ASSERT_EQ(1u, entries.size());
    ASSERT_STREQ("blobkey", entries[0].key);
    ASSERT_STREQ(big_value, entries[0].value);
    ASSERT_EQ(42u, entries[0].timestamp);
}

void TestMemoryGrowthOnUpdate() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    memtable.Insert("k", std::string(100, 'a'));
    size_t m1 = memtable.ApproximateMemoryUsage();
    ASSERT_TRUE(m1 > 0);
    memtable.Insert("k", std::string(200, 'b'));
    size_t m2 = memtable.ApproximateMemoryUsage();
    ASSERT_TRUE(m2 > m1);
}

void TestIsFullAfterUpdates() {
    kvdb::MemTable memtable(0, 600);
    memtable.Insert("k", std::string(400, 'x'));
    ASSERT_FALSE(memtable.IsFull());
    memtable.Insert("k", std::string(400, 'y'));
    ASSERT_TRUE(memtable.IsFull());
}

void TestExportAfterHeavyMVCC() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    for (int i = 0; i < 100; ++i)
        memtable.Insert("onlykey", "val_" + std::to_string(i), static_cast<uint64_t>(i + 1));
    auto entries = memtable.ExportEntries();
    ASSERT_TRUE(entries.size() > 0u);
    std::string v;
    ASSERT_TRUE(memtable.Lookup("onlykey", 200, v));
    ASSERT_STREQ("val_99", v);
}

void TestExportAfterSplittingMVCC() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    for (int i = 0; i < 50; ++i)
        memtable.Insert("k1", "first_"  + std::to_string(i), static_cast<uint64_t>(i + 1));
    for (int i = 0; i < 50; ++i)
        memtable.Insert("k2", "second_" + std::to_string(i), static_cast<uint64_t>(i + 51));
    auto entries = memtable.ExportEntries();
    ASSERT_TRUE(entries.size() >= 2u);
    std::string v;
    ASSERT_TRUE(memtable.Lookup("k1", 200, v));
    ASSERT_STREQ("first_49", v);
    ASSERT_TRUE(memtable.Lookup("k2", 200, v));
    ASSERT_STREQ("second_49", v);
}

void TestExportAfterManySplits() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    std::string big(1500, 'X');
    for (int i = 0; i < 100; ++i) {
        memtable.Insert("k" + std::to_string(i), big + static_cast<char>('a' + (i % 26)),
                        static_cast<uint64_t>(i + 1));
    }
    auto entries = memtable.ExportEntries();
    ASSERT_TRUE(entries.size() >= 50u);
    for (size_t i = 1; i < entries.size(); ++i)
        ASSERT_TRUE(entries[i - 1].key < entries[i].key);
}

void TestTombstoneDistinctFromEmptyValue() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    std::string v;
    memtable.Insert("k", "real", 10, false);
    ASSERT_TRUE(memtable.Lookup("k", 10, v));
    ASSERT_STREQ("real", v);
    memtable.Insert("k", "", 20, false);
    ASSERT_TRUE(memtable.Lookup("k", 20, v));
    ASSERT_STREQ("", v);
    memtable.Insert("k", "", 30, true);
    ASSERT_FALSE(memtable.Lookup("k", 30, v));
    ASSERT_TRUE(memtable.Lookup("k", 25, v));
    ASSERT_STREQ("", v);
}

void TestTombstoneViaMemTableInsert() {
    kvdb::MemTable memtable(0, 1024 * 1024);
    memtable.Insert("live", "ok");
    memtable.Insert("dead", "", 10, true);
    std::string v;
    ASSERT_TRUE(memtable.Lookup("live", 10, v));
    ASSERT_STREQ("ok", v);
    ASSERT_FALSE(memtable.Lookup("dead", 10, v));
}

void RunTests() {
    std::cout << "Running MemTable Tests...\n\n";
    TestBasicInsert();
    TestMultipleInserts();
    TestIsFull();
    TestFreeze();
    TestExport();
    TestExportEmpty();
    TestKeyUpdate();
    TestEntryCountAfterInserts();
    TestConstructorMaxBytes();
    TestDuplicateKeys();
    TestMemoryGrowthMonotonic();
    TestLookup();
    TestLookupTimestamp();
    TestLookupNotFound();
    TestMVCCMultiVersion();
    TestMVCCMultiVersionInterleaved();
    TestLargeValue();
    TestLargeValueBlobRoundTrip();
    TestMemoryGrowthOnUpdate();
    TestIsFullAfterUpdates();
    TestExportAfterHeavyMVCC();
    TestExportAfterSplittingMVCC();
    TestExportAfterManySplits();
    TestTombstoneDistinctFromEmptyValue();
    TestTombstoneViaMemTableInsert();
}

}
RUN_TESTS()
