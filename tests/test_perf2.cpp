// Quick engine insert test
#include "kvdb/engine.hpp"
#include <chrono>
#include <filesystem>
#include <iostream>
#include <string>

using Clock = std::chrono::steady_clock;
using TP = Clock::time_point;
static TP now() { return Clock::now(); }
static double ms(TP s, TP e) { return std::chrono::duration<double,std::milli>(e-s).count(); }

int main() {
    std::filesystem::remove_all("./pe3");
    std::cout << "Creating engine...\n" << std::flush;
    kvdb::LSMTreeEngine engine("./pe3", 1024ULL*1024*1024);
    std::cout << "Engine ready.\n" << std::flush;

    std::string val(16, 'V');
    int n = 5000;

    std::cout << "Inserting " << n << "...\n" << std::flush;
    auto t0 = now();
    for (int i = 0; i < n; ++i)
        engine.Insert("k"+std::to_string(i), val);
    auto t1 = now();
    std::cout << "Engine full: " << static_cast<int>(n/(ms(t0,t1)/1000.0)) << " ops/s\n" << std::flush;

    std::cout << "Destroying engine...\n" << std::flush;
    std::filesystem::remove_all("./pe3");
    std::cout << "DONE.\n" << std::flush;
}
