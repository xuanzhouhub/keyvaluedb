// Performance benchmark — single-threaded + concurrent + mixed workloads
#include "kvdb/bptree.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;
using TimePoint = Clock::time_point;
static TimePoint now() { return Clock::now(); }
static double ms(TimePoint start, TimePoint end) {
    return std::chrono::duration<double, std::milli>(end - start).count();
}

static uint64_t g_ts{0};

// ---- Single-threaded insert ----
static void bench_insert(const char* label, size_t n, int val_len) {
    kvdb::BPlusTree tree;
    std::string val(static_cast<size_t>(val_len), 'X');
    auto t0 = now();
    for (size_t i = 0; i < n; ++i) {
        std::string k = "k" + std::to_string(i);
        tree.Insert(k, val, ++g_ts, (val_len > 2000));
    }
    auto t1 = now();
    double ops = n / (ms(t0, t1) / 1000.0);
    std::cout << label << ": " << n << " inserts, " << ms(t0, t1)
              << "ms  (" << static_cast<int>(ops) << " ops/s)"
              << "  tree_size=" << tree.Size()
              << "  memory=" << tree.MemoryUsage() / 1024 << "KB" << std::endl;
}

// ---- Concurrent read-only ----
static void bench_readers(const char* label, kvdb::BPlusTree* tree,
                          int readers, int sec) {
    std::atomic<uint64_t> total{0};
    std::atomic<bool> stop{false};

    auto reader_fn = [&]() {
        size_t n = tree->Size();
        uint64_t local = 0;
        while (!stop.load()) {
            std::string k = "k" + std::to_string(local % n), v;
            tree->Lookup(k, g_ts, v);
            local++;
        }
        total.fetch_add(local);
    };

    std::vector<std::thread> th;
    auto t0 = now();
    for (int i = 0; i < readers; ++i) th.emplace_back(reader_fn);
    std::this_thread::sleep_for(std::chrono::seconds(sec));
    stop = true;
    auto t1 = now();
    for (auto& t : th) t.join();

    double ops = total.load() / (ms(t0, t1) / 1000.0);
    std::cout << label << ": " << readers << " readers, " << sec << "s, "
              << total.load() << " lookups, " << static_cast<int>(ops) << " ops/s" << std::endl;
}

// ---- Mixed concurrent ----
static void bench_mixed(const char* label, size_t n, int val_len,
                        int readers, int sec) {
    kvdb::BPlusTree tree;
    std::atomic<uint64_t> inserts{0}, lookups{0};
    std::atomic<bool> stop{false};

    auto writer_fn = [&]() {
        std::string val(static_cast<size_t>(val_len), 'Y');
        while (!stop.load()) {
            std::string k = "k" + std::to_string(inserts.load() % n);
            tree.Insert(k, val, ++g_ts, (val_len > 2000));
            inserts++;
        }
    };
    auto reader_fn = [&]() {
        uint64_t local = 0;
        while (!stop.load()) {
            std::string k = "k" + std::to_string(local % n), v;
            tree.Lookup(k, g_ts, v);
            local++;
        }
        lookups.fetch_add(local);
    };

    std::vector<std::thread> th;
    th.emplace_back(writer_fn);
    for (int i = 0; i < readers; ++i) th.emplace_back(reader_fn);
    auto t0 = now();
    std::this_thread::sleep_for(std::chrono::seconds(sec));
    stop = true;
    auto t1 = now();
    for (auto& t : th) t.join();

    double total_ms = ms(t0, t1);
    std::cout << label << ": 1W+" << readers << "R, " << sec << "s, "
              << inserts.load() << " ins (" << static_cast<int>(inserts.load() / (total_ms/1000.0)) << "/s), "
              << lookups.load() << " lkup (" << static_cast<int>(lookups.load() / (total_ms/1000.0)) << "/s)"
              << "  tree_size=" << tree.Size() << std::endl;
}

// ---- Range scan / Export ----
static void bench_export(const char* label, kvdb::BPlusTree* tree) {
    std::vector<kvdb::KeyValuePair> entries;
    auto t0 = now();
    tree->Export(entries);
    auto t1 = now();
    std::cout << label << ": " << entries.size() << " entries exported in "
              << ms(t0, t1) << "ms" << std::endl;
}

int main() {
    std::cout << "=== B+-Tree Performance ===\n" << std::endl;

    // 1. Single-threaded inserts
    std::cout << "--- Single-threaded inserts ---" << std::endl;
    bench_insert("  Small (16B)  ", 20000, 16);
    bench_insert("  Medium (512B)", 20000, 512);
    // 2. Blob inserts (slower due to allocs)
    bench_insert("  Blob  (4096B)", 10000, 4096);

    // 3. Concurrent reads
    std::cout << "\n--- Concurrent reads ---" << std::endl;
    {
        kvdb::BPlusTree tree;
        for (size_t i = 0; i < 5000; ++i)
            tree.Insert("k" + std::to_string(i), "val_" + std::to_string(i), ++g_ts, false);
        bench_readers("  Small tree", &tree, 4, 3);
    }
    {
        kvdb::BPlusTree tree;
        std::string big(3000, 'B');
        for (size_t i = 0; i < 2000; ++i)
            tree.Insert("k" + std::to_string(i), big + std::to_string(i), ++g_ts, true);
        bench_readers("  Blob tree ", &tree, 4, 3);
    }

    // 4. Mixed concurrent
    std::cout << "\n--- Mixed 1W + N readers ---" << std::endl;
    bench_mixed("  Small (16B)  ", 2000, 16, 3, 3);
    std::cout << "  Chain check: ";
    {
        kvdb::BPlusTree tree;
        for (size_t i = 0; i < 5000; ++i)
            tree.Insert("k" + std::to_string(i), "val", ++g_ts, false);
        bench_export("  Export 5K   ", &tree);
    }

    // 5. Scale test
    std::cout << "\n--- Scale test (100K inserts, sequential) ---" << std::endl;
    {
        kvdb::BPlusTree tree;
        auto t0 = now();
        for (size_t i = 0; i < 100000; ++i) {
            std::string k = "key_" + std::to_string(i);
            tree.Insert(k, "v" + std::to_string(i % 100), ++g_ts, false);
        }
        auto t1 = now();
        std::cout << "  100K inserts: " << ms(t0, t1) << "ms  ("
                  << static_cast<int>(100000.0 / (ms(t0, t1) / 1000.0)) << " ops/s)"
                  << "  tree_size=" << tree.Size()
                  << "  memory=" << tree.MemoryUsage() / 1024 << "KB" << std::endl;

        // Verify chain
        std::vector<kvdb::KeyValuePair> e;
        auto t2 = now();
        tree.Export(e);
        auto t3 = now();
        int bad = 0;
        for (size_t i = 1; i < e.size(); ++i)
            if (e[i].key < e[i-1].key) bad++;
        std::cout << "  Export: " << e.size() << " entries, " << ms(t2, t3)
                  << "ms, chain issues: " << bad << std::endl;

        // Verify lookups
        int missing = 0;
        for (size_t i = 0; i < 100000; ++i) {
            std::string v;
            if (!tree.Lookup("key_" + std::to_string(i), g_ts, v)) missing++;
        }
        std::cout << "  100K lookups missing: " << missing << std::endl;
    }

    std::cout << "\nDONE." << std::endl;
    return 0;
}
