#include "kvdb/memtable.hpp"
#include "kvdb/sstable.hpp"
#include <iostream>
#include <string>
#include <algorithm>

int main() {
    int failures = 0;

    kvdb::MemTable mt(0, 1024 * 1024);
    for (int i = 0; i < 100; ++i) {
        std::string key = "k" + std::to_string(i);
        std::string val = "v" + std::to_string(i);
        mt.Insert(key, val, static_cast<uint64_t>(i));
    }

    auto walk = kvdb::BPlusTree::MemTableWalk(mt.GetTree());
    kvdb::SSTable::WriteFromWalk("test_wfw.sst", walk, 100);

    try {
        auto meta = kvdb::SSTable::ReadMetadata("test_wfw.sst");
        std::cerr << "ReadMetadata OK: entry_count=" << meta.entry_count
                  << " blocks=" << meta.block_offsets.size()
                  << " min_key=" << meta.min_key << " max_key=" << meta.max_key << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "ReadMetadata FAIL: " << e.what() << std::endl;
        failures++;
    }

    auto r = kvdb::SSTable::ReadAll("test_wfw.sst");
    if (r.size() != 100) {
        std::cerr << "FAIL: expected 100 entries, got " << r.size() << std::endl;
        failures++;
    }

    for (size_t i = 1; i < r.size(); ++i) {
        if (r[i - 1].key >= r[i].key) {
            std::cerr << "FAIL: entries out of order at " << i << std::endl;
            failures++;
        }
    }

    for (auto& kv : r) {
        int n = std::stoi(kv.key.substr(1));
        std::string ev = "v" + std::to_string(n);
        if (kv.value != ev || kv.timestamp != static_cast<uint64_t>(n)) {
            std::cerr << "FAIL: bad entry " << kv.key << std::endl;
            failures++;
        }
    }

    if (failures == 0)
        std::cerr << "test_wfw PASSED" << std::endl;
    else
        std::cerr << "test_wfw FAILED with " << failures << " errors" << std::endl;
    return failures;
}
