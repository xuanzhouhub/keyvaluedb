// Deterministic chain order test — verify after every Insert
#include "kvdb/bptree.hpp"
#include <iostream>
#include <random>
#include <string>
#include <vector>

static int CheckChain(kvdb::BPlusTree* tree) {
    std::vector<kvdb::KeyValuePair> entries;
    tree->Export(entries);
    for (size_t i = 1; i < entries.size(); ++i) {
        if (entries[i].key < entries[i-1].key) {
            std::cerr << "OUT OF ORDER at entry " << i << ": "
                      << entries[i-1].key << " > " << entries[i].key << std::endl;
            return 1;
        }
    }
    return 0;
}

int main() {
    kvdb::BPlusTree tree;
    std::string big(3000, 'X');
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, 999);

    for (int i = 0; i < 500; ++i) {
        int n = dist(rng);
        std::string key = "key_" + std::to_string(n);
        tree.Insert(key, big + static_cast<char>('a' + (i % 26)), i + 1, false);
        if (CheckChain(&tree)) {
            std::cerr << "FAILED after insert " << i << " (key=" << key << ")" << std::endl;
            return 1;
        }
    }
    std::cout << "OK — 500 inserts, chain verified after each" << std::endl;
    return 0;
}
