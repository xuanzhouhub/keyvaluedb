#include "kvdb/engine.hpp"
#include "kvdb/sstable.hpp"
#include <iostream>
#include <filesystem>
#include <map>
#include <random>

namespace fs = std::filesystem;

int main() {
    std::string dir = "./test_debug_data";
    fs::remove_all(dir);

    {
        kvdb::LSMTreeEngine engine(dir, 4 * 1024 * 1024);

        std::mt19937 rng(42);
        std::uniform_int_distribution<int> key_dist(0, 99);
        std::uniform_int_distribution<int> len_dist(1, 1024);
        std::uniform_int_distribution<int> char_dist('a', 'z');

        std::map<std::string, std::string> confirmed;

        for (int i = 0; i < 50000; ++i) {
            std::string key = "k" + std::to_string(key_dist(rng));
            int len = len_dist(rng);
            std::string value(len, '\0');
            for (int j = 0; j < len; ++j) value[j] = static_cast<char>(char_dist(rng));
            engine.Insert(key, value);
            confirmed[key] = value;
        }

        std::cout << "Writes: 50000, Unique: " << confirmed.size() << std::endl;

        int ok = 0, mismatch = 0;
        for (const auto& [k, v] : confirmed) {
            std::string a;
            engine.Lookup(k, a);
            if (v == a) ok++; else mismatch++;
        }
        std::cout << "Before restart: OK=" << ok << " MISMATCH=" << mismatch << std::endl;
    }

    std::cout << "\n--- SSTable entries ---" << std::endl;
    {
        std::string fpath = dir + "/sstable_0.sst";
        if (fs::exists(fpath)) {
            auto entries = kvdb::SSTable::ReadAll(fpath);
            std::cout << "Entries: " << entries.size() << std::endl;
            for (const auto& e : entries) {
                std::cout << "  " << e.key << " val_len=" << e.value.size() 
                          << " ts=" << e.timestamp << " val=" << e.value.substr(0,20) << std::endl;
            }
        } else {
            std::cout << "No SSTable found" << std::endl;
        }
    }

    fs::remove_all(dir);
    return 0;
}
