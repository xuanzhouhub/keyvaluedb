#include "kvdb/engine.hpp"
#include "kvdb/bptree.hpp"
#include <iostream>
#include <filesystem>
#include <random>

namespace fs = std::filesystem;

int main() {
    std::string dir = "./test_debug_data";
    fs::remove_all(dir);

    std::cout << "=== Testing B+Tree walk directly ===" << std::endl;

    kvdb::BPlusTree tree;
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> key_dist(0, 9);
    std::uniform_int_distribution<int> len_dist(1, 1024);
    std::uniform_int_distribution<int> char_dist(32, 126);

    for (int i = 0; i < 100; ++i) {
        std::string key = "fuzz_" + std::to_string(key_dist(rng));
        int len = len_dist(rng);
        std::string value(len, '\0');
        for (int j = 0; j < len; ++j) value[j] = static_cast<char>(char_dist(rng));
        tree.Insert(key, value, i + 1);
    }

    std::cout << "Tree Size: " << tree.Size() << std::endl;
    std::cout << "Tree Memory: " << tree.MemoryUsage() << std::endl;

    kvdb::BPlusTree::MemTableWalk walk(tree);
    int walked = 0;
    int distinct = 0;
    std::string prev;
    while (walk.Valid()) {
        const std::string& k = walk.Key();
        if (k != prev) { distinct++; prev = k; }
        walked++;
        walk.Next();
    }
    std::cout << "Walked entries: " << walked << std::endl;
    std::cout << "Distinct keys: " << distinct << std::endl;

    // Now test through the engine
    std::cout << "\n=== Testing engine ===" << std::endl;
    {
        kvdb::LSMTreeEngine engine(dir, 4 * 1024 * 1024);
        for (int i = 0; i < 100; ++i) {
            std::string key = "fuzz_" + std::to_string(key_dist(rng));
            int len = len_dist(rng);
            std::string value(len, '\0');
            for (int j = 0; j < len; ++j) value[j] = static_cast<char>(char_dist(rng));
            engine.Insert(key, value);
        }
        std::cout << "EntryCount: " << engine.ActiveMemTableEntryCount() << std::endl;
        std::cout << "SSTableCount: " << engine.SSTableCount() << std::endl;

        // Export entries
        auto entries = engine.RangeScan();
        int scanned = 0;
        while (entries.Valid()) { scanned++; entries.Next(); }
        std::cout << "RangeScan entries: " << scanned << std::endl;
    }

    std::cout << "\n--- Restart ---" << std::endl;
    {
        kvdb::LSMTreeEngine engine(dir, 4 * 1024 * 1024);
        std::cout << "SSTableCount: " << engine.SSTableCount() << std::endl;
        auto metas = engine.GetSSTableMetadata();
        for (size_t i = 0; i < metas.size(); ++i)
            std::cout << "  SSTable[" << i << "] " << metas[i].filepath
                      << " entries=" << metas[i].entry_count << std::endl;
    }

    fs::remove_all(dir);
    return 0;
}
