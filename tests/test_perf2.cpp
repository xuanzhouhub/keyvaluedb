// Compare tree vs engine vs WAL-sync-only
#include "kvdb/engine.hpp"
#include "kvdb/bptree.hpp"
#include "kvdb/wal.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>

using Clock = std::chrono::steady_clock;
using TP = Clock::time_point;
static TP now() { return Clock::now(); }
static double ms(TP s, TP e) { return std::chrono::duration<double,std::milli>(e-s).count(); }

int main() {
    int n = 5000;
    std::string val(16, 'V');

    // 1. Pure tree Insert (no WAL, no engine)
    {
        kvdb::BPlusTree tree;
        auto t0 = now();
        for (int i = 0; i < n; ++i)
            tree.Insert("k"+std::to_string(i), val, static_cast<uint64_t>(i+1), false);
        auto t1 = now();
        std::cout << "Tree only:  " << static_cast<int>(n/(ms(t0,t1)/1000.0)) << " ops/s\n";
    }

    // 2. WAL Buffer + Sync only (no tree)
    {
        std::remove("./pw.wal");
        kvdb::WAL wal("./pw.wal");
        auto t0 = now();
        for (int i = 0; i < n; ++i) {
            wal.Buffer("k"+std::to_string(i), val, static_cast<uint64_t>(i+1));
            wal.Sync();
        }
        auto t1 = now();
        std::cout << "WAL Buff+Sync: " << static_cast<int>(n/(ms(t0,t1)/1000.0)) << " ops/s\n";
        std::remove("./pw.wal");
    }

    // 3. Full engine Insert
    {
        std::filesystem::remove_all("./pe2");
        kvdb::LSMTreeEngine engine("./pe2", 1024ULL*1024*1024);
        auto t0 = now();
        for (int i = 0; i < n; ++i)
            engine.Insert("k"+std::to_string(i), val);
        auto t1 = now();
        std::cout << "Engine full: " << static_cast<int>(n/(ms(t0,t1)/1000.0)) << " ops/s\n";
        std::filesystem::remove_all("./pe2");
    }

    std::cout << "\nDONE.\n";
}
