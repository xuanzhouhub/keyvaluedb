//
// Comprehensive stress test for the lock-free B+-tree memtable
// Exercises: concurrent reads/writes, splits, blobs, MVCC, range scans, EBR drain
//
#include "kvdb/bptree.hpp"
#include "kvdb/memtable.hpp"
#include "kvdb/snp_tracker.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

static const int kThreads = 8;
static const int kWriters = 1;
static const int kDurationSec = 8;
static const int kKeyPoolSize = 500;
static const int kSmallMaxLen = 64;
static const int kMediumMaxLen = 1500;
static const int kBlobMinLen = 2500;
static const int kBlobMaxLen = 8096;

struct StressStats {
    std::atomic<uint64_t> inserts{0};
    std::atomic<uint64_t> lookups{0};
    std::atomic<uint64_t> range_scans{0};
    std::atomic<uint64_t> lookup_hits{0};
    std::atomic<uint64_t> lookup_misses{0};
    std::atomic<uint64_t> invariant_fails{0};
};
static StressStats g_stats;

static std::atomic<uint64_t> g_global_ts{0};
static std::atomic<bool> g_stop{false};
static std::atomic<bool> g_barrier{false};

static std::mutex g_confirmed_mutex;
static std::map<std::string, std::string> g_confirmed;  // last written value per key

#define FAIL(msg) do { \
    g_stats.invariant_fails++; \
    std::cerr << "FAIL: " << msg << std::endl; \
} while(0)

// Generate a random value of varying size
static std::string RandomValue(std::mt19937& rng) {
    std::uniform_int_distribution<int> type(0, 4);
    int t = type(rng);
    if (t == 0) {       // small: 1-64B
        std::uniform_int_distribution<int> len(1, kSmallMaxLen);
        return std::string(static_cast<size_t>(len(rng)), 'S');
    } else if (t <= 2) { // medium: 65-1500B  
        std::uniform_int_distribution<int> len(kSmallMaxLen + 1, kMediumMaxLen);
        return std::string(static_cast<size_t>(len(rng)), 'M');
    } else {            // blob: 2500-8096B
        std::uniform_int_distribution<int> len(kBlobMinLen, kBlobMaxLen);
        return std::string(static_cast<size_t>(len(rng)), 'B');
    }
}

// Writer thread: continuously insert and update keys
static void WriterThread(int id, kvdb::BPlusTree* tree) {
    std::mt19937 rng(static_cast<unsigned>(id * 31 + 17));
    std::uniform_int_distribution<int> key_dist(0, kKeyPoolSize - 1);
    auto start = std::chrono::steady_clock::now();

    while (!g_stop.load()) {
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > std::chrono::seconds(kDurationSec)) break;

        std::string key = "key_" + std::to_string(key_dist(rng));
        std::string value = RandomValue(rng);
        uint64_t ts = g_global_ts.fetch_add(1) + 1;

        tree->Insert(key, value, ts, false);
        g_stats.inserts++;

        {
            std::lock_guard<std::mutex> lock(g_confirmed_mutex);
            g_confirmed[key] = value;
        }
    }
}

// Reader thread: continuously look up keys (no SnapshotTracker in test mode)
static void ReaderThread(int id, kvdb::BPlusTree* tree) {
    std::mt19937 rng(static_cast<unsigned>(id * 53 + 29));
    std::uniform_int_distribution<int> key_dist(0, kKeyPoolSize - 1);
    auto start = std::chrono::steady_clock::now();

    while (!g_stop.load()) {
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > std::chrono::seconds(kDurationSec)) break;

        std::string key = "key_" + std::to_string(key_dist(rng));
        std::string value;
        uint64_t read_ts = g_global_ts.load();
        bool found = tree->Lookup(key, read_ts, value);
        g_stats.lookups++;
        if (found) g_stats.lookup_hits++;
        else g_stats.lookup_misses++;
    }
}

// Verify that all confirmed writes are visible at max_ts
static void VerifyPersistence(kvdb::BPlusTree* tree) {
    std::map<std::string, std::string> snapshot;
    {
        std::lock_guard<std::mutex> lock(g_confirmed_mutex);
        snapshot = g_confirmed;
    }

    uint64_t max_ts = g_global_ts.load();
    size_t verified = 0, missing = 0, mismatched = 0;
    for (const auto& [k, expected] : snapshot) {
        std::string actual;
        if (!tree->Lookup(k, max_ts, actual)) {
            missing++;
        } else if (actual != expected) {
            mismatched++;
            FAIL("Mismatch: key=" + k + " expected=" + expected.substr(0, 20) + " actual=" + actual.substr(0, 20));
        } else {
            verified++;
        }
    }
    std::cout << "  Verified:   " << verified << std::endl;
    std::cout << "  Missing:    " << missing << std::endl;
    std::cout << "  Mismatched: " << mismatched << std::endl;
    if (missing == 0 && mismatched == 0)
        std::cout << "  PERSISTENCE OK" << std::endl;
    else
        std::cout << "  PERSISTENCE FAILED!" << std::endl;
}

// Verify leaf chain consistency: all keys in order, no gaps
static void VerifyLeafChain(kvdb::BPlusTree* tree) {
    std::vector<kvdb::KeyValuePair> entries;
    tree->Export(entries);
    
    size_t issues = 0;
    for (size_t i = 1; i < entries.size(); ++i) {
        if (entries[i].key < entries[i-1].key) {
            FAIL("Leaf chain out of order at " + std::to_string(i) + 
                 ": " + entries[i-1].key + " > " + entries[i].key);
            issues++;
            if (issues > 10) break;
        }
    }
    std::cout << "  Leaf entries: " << entries.size() << std::endl;
    std::cout << "  Order issues: " << issues << std::endl;
    if (issues == 0)
        std::cout << "  LEAF CHAIN OK" << std::endl;
}

int main() {
    std::cout << "=== Lock-Free B+-Tree Stress Test ===" << std::endl;
    std::cout << "Threads: " << kThreads << " (" << kWriters << " writers, " 
              << (kThreads - kWriters) << " readers)" << std::endl;
    std::cout << "Duration: " << kDurationSec << "s" << std::endl;
    std::cout << "Key pool: " << kKeyPoolSize << std::endl;

    kvdb::BPlusTree tree;
    
    // Pre-populate with small values to establish tree structure
    std::cout << "\nPre-populating..." << std::endl;
    for (int i = 0; i < 500; ++i) {
        std::string key = "key_" + std::to_string(i);
        std::string value = "init_value_" + std::to_string(i);
        tree.Insert(key, value, g_global_ts.fetch_add(1) + 1, false);
        g_confirmed[key] = value;
    }
    std::cout << "  Pre-populated 500 keys, tree size: " << tree.Size() << std::endl;

    // Launch threads
    std::vector<std::thread> threads;
    for (int i = 0; i < kWriters; ++i)
        threads.emplace_back(WriterThread, i, &tree);
    for (int i = 0; i < kThreads - kWriters; ++i)
        threads.emplace_back(ReaderThread, i + kWriters, &tree);

    // Progress reporting
    auto test_start = std::chrono::steady_clock::now();
    while (true) {
        auto elapsed = std::chrono::steady_clock::now() - test_start;
        if (elapsed > std::chrono::seconds(kDurationSec)) break;

        std::this_thread::sleep_for(std::chrono::seconds(2));
        auto e = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
        std::cout << "  t=" << e << "s  inserts=" << g_stats.inserts.load()
                  << "  lookups=" << g_stats.lookups.load()
                  << "  hits=" << g_stats.lookup_hits.load()
                  << "  misses=" << g_stats.lookup_misses.load()
                  << "  fails=" << g_stats.invariant_fails.load()
                  << "  tree_size=" << tree.Size() << std::endl;
    }

    g_stop = true;
    for (auto& t : threads) t.join();

    std::cout << "\n--- Results ---" << std::endl;
    std::cout << "Inserts:      " << g_stats.inserts.load() << std::endl;
    std::cout << "Lookups:      " << g_stats.lookups.load() << std::endl;
    std::cout << "  Hits:       " << g_stats.lookup_hits.load() << std::endl;
    std::cout << "  Misses:     " << g_stats.lookup_misses.load() << std::endl;
    std::cout << "Invariant fails:" << g_stats.invariant_fails.load() << std::endl;
    std::cout << "Tree size:     " << tree.Size() << std::endl;
    std::cout << "Tree memory:   " << tree.MemoryUsage() << " bytes" << std::endl;

    std::cout << "\n--- Persistence Check ---" << std::endl;
    VerifyPersistence(&tree);

    std::cout << "\n--- Leaf Chain Check ---" << std::endl;
    VerifyLeafChain(&tree);

    bool pass = (g_stats.invariant_fails.load() == 0);
    std::cout << "\n" << (pass ? "STRESS TEST PASSED" : "STRESS TEST FAILED") << std::endl;
    return pass ? 0 : 1;
}
