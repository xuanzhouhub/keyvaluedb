// Deterministic chain order test — single threaded, no concurrency
#include "kvdb/bptree.hpp"
#include <iostream>
#include <string>
#include <vector>

int main() {
    kvdb::BPlusTree tree;
    std::vector<std::string> written_keys;

    // Insert with blob-like values to trigger heavy splitting
    std::string big(3000, 'X');
    for (int i = 0; i < 500; ++i) {
        std::string key = "key_" + std::to_string(i);
        tree.Insert(key, big + static_cast<char>('a' + (i % 26)), i + 1, false);
        written_keys.push_back(key);
    }

    // Export and verify chain order
    std::vector<kvdb::KeyValuePair> entries;
    tree.Export(entries);

    int issues = 0;
    for (size_t i = 1; i < entries.size(); ++i) {
        if (entries[i].key < entries[i - 1].key) {
            std::cerr << "OUT OF ORDER at " << i << ": "
                      << entries[i - 1].key << " > " << entries[i].key << std::endl;
            issues++;
            if (issues >= 20) break;
        }
    }

    std::cout << "Total entries: " << entries.size() << std::endl;
    std::cout << "Order issues: " << issues << std::endl;
    std::cout << "Tree size: " << tree.Size() << std::endl;

    // Also verify each written key can be found
    int missing = 0, mismatched = 0;
    for (size_t i = 0; i < written_keys.size(); ++i) {
        std::string actual;
        if (!tree.Lookup(written_keys[i], 999, actual)) {
            missing++;
        }
    }
    std::cout << "Missing keys: " << missing << std::endl;

    return issues > 0 ? 1 : 0;
}
