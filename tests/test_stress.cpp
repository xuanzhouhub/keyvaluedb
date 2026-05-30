// Minimal inline-only stress test
#include "kvdb/bptree.hpp"
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

std::atomic<uint64_t> g_ts{0};
std::atomic<bool> g_stop{false};
std::mutex g_mtx;
std::map<std::string, std::string> g_val;

void writer(kvdb::BPlusTree* t) {
    std::mt19937 rng(1);
    std::uniform_int_distribution<int> kd(0, 199);
    int n = 0;
    while (!g_stop.load() && n < 50000) {
        if (n >= 20000 && n < 20100) std::cerr << "W" << n << " ";
        std::string k = "k" + std::to_string(kd(rng));
        std::string v = std::to_string(g_ts.load());
        t->Insert(k, v, g_ts.fetch_add(1) + 1, false);
        std::lock_guard lk(g_mtx); g_val[k] = v;
        n++;
    }
    std::cerr << "writer done after " << n << std::endl;
}

void reader(kvdb::BPlusTree* t) {
    std::mt19937 rng(2);
    std::uniform_int_distribution<int> kd(0, 199);
    int n = 0;
    while (!g_stop.load() && n < 100000) {
        std::string k = "k" + std::to_string(kd(rng)), v;
        t->Lookup(k, g_ts.load(), v);
        n++;
    }
    std::cerr << "reader done after " << n << std::endl;
}

int main() {
    kvdb::BPlusTree tree;
    for (int i = 0; i < 100; ++i) {
        std::string k = "k" + std::to_string(i);
        tree.Insert(k, "x", g_ts.fetch_add(1)+1, false);
        g_val[k] = "x";
    }

    std::vector<std::thread> th;
    th.emplace_back(writer, &tree);
    for (int i = 0; i < 3; ++i) th.emplace_back(reader, &tree);

    std::cout << "RUNNING...\n" << std::flush;
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    std::cout << "STOPPING...\n" << std::flush;
    g_stop = true;
    std::cerr << "stopping, waiting for threads\n";
    for (auto& t : th) { std::cerr << "j"; t.join(); std::cerr << "J"; }
    std::cerr << "\nall joined\n";
    std::flush(std::cerr);

    // check chain
    std::vector<kvdb::KeyValuePair> e;
    tree.Export(e);
    int bad = 0;
    for (size_t i = 1; i < e.size(); ++i)
        if (e[i].key < e[i-1].key) bad++;
    std::cout << "Entries: " << e.size() << "  Chain bad: " << bad << "\n";

    // check persistence
    int mis = 0;
    for (auto& [k,expected] : g_val) {
        std::string got;
        if (!tree.Lookup(k, g_ts.load(), got)) mis++;
    }
    std::cout << "Keys: " << g_val.size() << "  Missing: " << mis << "\n";
    std::cout << (bad == 0 && mis == 0 ? "PASS" : "FAIL") << std::endl;
    return (bad == 0 && mis == 0) ? 0 : 1;
}
