#include "kvdb/memtable.hpp"
#include "kvdb/sstable.hpp"
#include <iostream>
#include <string>

int main() {
    int failures = 0;

    kvdb::MemTable mt(0, 4096);

    for (int i = 0; i < 500; ++i) {
        std::string key = "k" + std::to_string(i);
        std::string val = "v" + std::to_string(i);
        mt.Insert(key, val, static_cast<uint64_t>(i));
    }

    auto walk = kvdb::BPlusTree::MemTableWalk(mt.GetTree());
    kvdb::SSTable::WriteFromWalk("test_small.sst", walk, mt.EntryCount());

    try {
        auto meta = kvdb::SSTable::ReadMetadata("test_small.sst");
        std::cerr << "ReadMetadata OK: entry_count=" << meta.entry_count
                  << " blocks=" << meta.block_offsets.size() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "ReadMetadata FAIL: " << e.what() << std::endl;
        failures++;
    }

    auto r = kvdb::SSTable::ReadAll("test_small.sst");
    std::cerr << "ReadAll: " << r.size() << " entries" << std::endl;

    if (r.empty()) {
        std::cerr << "FAIL: no entries read" << std::endl;
        failures++;
    } else {
        for (size_t i = 1; i < r.size(); ++i) {
            if (r[i-1].key >= r[i].key) {
                std::cerr << "FAIL: out of order at " << i << std::endl;
                failures++;
                break;
            }
        }
    }

    if (failures == 0)
        std::cerr << "PASSED" << std::endl;
    else
        std::cerr << "FAILED with " << failures << " errors" << std::endl;
    return failures;
}
