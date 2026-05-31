// Engine insert perf — 1GB memtable, no flush, no cleanup between tests
#include "kvdb/engine.hpp"
#include <chrono>
#include <iostream>
#include <string>

using Clock = std::chrono::steady_clock;
using TP = Clock::time_point;
static TP now() { return Clock::now(); }
static double ms(TP s, TP e) { return std::chrono::duration<double,std::milli>(e-s).count(); }

int main() {
    std::cout << "=== Engine Insert Perf ===\n";
    std::string val(4096, 'X');

    // One engine, 1GB memtable — never flushes
    kvdb::LSMTreeEngine engine("./perf_eng2", 1ULL*1024*1024*1024);

    for (int val_len : {16, 512, 4096}) {
        int n = 5000;
        std::string v(static_cast<size_t>(val_len), 'X');
        auto t0 = now();
        for (int i = 0; i < n; ++i)
            engine.Insert("k"+std::to_string(i), v);
        auto t1 = now();
        std::cout << "  " << val_len << "B: " << n << " ins, " << ms(t0,t1)
                  << "ms  (" << static_cast<int>(n/(ms(t0,t1)/1000.0)) << " ops/s)\n";
    }

    std::cout << "DONE.\n";
}
