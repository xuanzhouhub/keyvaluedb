#include "kvdb/engine.hpp"
#include "kvdb/bptree.hpp"
#include "kvdb/wal.hpp"
#include "kvdb/kv_cache.hpp"
#include "kvdb/snp_tracker.hpp"
#include <chrono>
#include <filesystem>
#include <functional>
#include <iostream>

using Clock = std::chrono::steady_clock;
using TP = Clock::time_point;
static TP now() { return Clock::now(); }
static double ns(TP s, TP e) { return std::chrono::duration<double,std::nano>(e-s).count(); }

static double bench(const char* label, int n, std::function<void(int)> fn) {
    auto t0 = now();
    for (int i = 0; i < n; ++i) fn(i);
    auto t1 = now();
    double per = ns(t0, t1) / n;
    std::cout << "  " << label << ": " << per << " ns\n";
    return per;
}

int main() {
    int N = 50000;
    std::string k("k"), v("value_16_bytes!");

    kvdb::BPlusTree tree;
    bench("Tree Insert", N, [&](int i){ tree.Insert(k, v, 1, false); });

    std::remove("./pw.wal");
    kvdb::WAL wal("./pw.wal");
    bench("WAL Buffer ", N, [&](int i){ wal.Buffer(k, v, 1); });
    std::remove("./pw.wal");

    kvdb::SnapshotTracker st;
    bench("MinActiveTS", N, [&](int i){ st.MinActiveTS(); });

    std::shared_mutex sm;
    bench("shared_lock", N, [&](int i){ std::shared_lock lk(sm); });
    bench("unique_lock", N, [&](int i){ std::unique_lock lk(sm); });

    kvdb::KVCache kv(10000, 16*1024*1024);
    bench("KVCache Put", N/10, [&](int i){ kv.Put(k, v, 1); });

    std::filesystem::remove_all("./pe3");
    kvdb::LSMTreeEngine engine("./pe3", 1024ULL*1024*1024);
    bench("Engine Insert", 2000, [&](int i){ engine.Insert(k, v); });
    std::filesystem::remove_all("./pe3");

    std::cout << "\nDONE.\n";
}
