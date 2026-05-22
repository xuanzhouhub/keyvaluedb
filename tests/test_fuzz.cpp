#include "kvdb/engine.hpp"
#include "kvdb/server.hpp"
#include "kvdb/client.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <map>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

static const std::string kTestDir = "./fuzz_test_data";
static const int kPort = 19666;
static const int kNumClients = 16;
static const int kDurationSec = 20;
static const int kKillIntervalSec = 4;
static const int kKeyPoolSize = 100;
static const int kValueMaxLen = 1024;
static const int kRangeScanEveryN = 50;

struct FuzzStats {
    std::atomic<uint64_t> writes_ok{0};
    std::atomic<uint64_t> writes_fail{0};
    std::atomic<uint64_t> reads_ok{0};
    std::atomic<uint64_t> reads_fail{0};
    std::atomic<uint64_t> connects_fail{0};
    std::atomic<uint64_t> server_kills{0};
    std::atomic<uint64_t> invariant_fail{0};
};

static FuzzStats g_stats;
static std::map<std::string, std::string> g_confirmed;
static std::mutex g_confirmed_mutex;
static std::atomic<bool> g_server_alive{false};
static std::atomic<bool> g_stop{false};

static thread_local std::map<std::string, std::string> t_last_written;

#define INVARIANT(cond, msg) do { \
    if (!(cond)) { g_stats.invariant_fail++; \
        std::cerr << "INVARIANT: " << msg << std::endl; } \
} while(0)

static std::string RandomValue(std::mt19937& rng) {
    std::uniform_int_distribution<int> len_dist(1, kValueMaxLen);
    std::uniform_int_distribution<int> char_dist(32, 126);
    int len = len_dist(rng);
    std::string s(len, '\0');
    for (int i = 0; i < len; ++i) s[i] = static_cast<char>(char_dist(rng));
    return s;
}

static void ClientThread(int id) {
    std::mt19937 rng(static_cast<unsigned>(id * 12345 + 67890));
    std::uniform_int_distribution<int> key_dist(0, kKeyPoolSize - 1);
    std::uniform_int_distribution<int> op_dist(0, 1);

    auto start = std::chrono::steady_clock::now();
    kvdb::Client client;
    int op_count = 0;

    while (!g_stop.load()) {
        auto elapsed = std::chrono::steady_clock::now() - start;
        if (elapsed > std::chrono::seconds(kDurationSec)) break;

        if (!client.IsConnected()) {
            if (!client.Connect("127.0.0.1", kPort)) {
                g_stats.connects_fail++;
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                continue;
            }
        }

        std::string key = "fuzz_" + std::to_string(key_dist(rng));
        ++op_count;

        if (op_dist(rng) == 0) {
            std::string value = RandomValue(rng);
            if (client.Write(key, value)) {
                g_stats.writes_ok++;
                t_last_written[key] = value;
                {
                    std::lock_guard<std::mutex> lock(g_confirmed_mutex);
                    g_confirmed[key] = value;
                }
            } else {
                g_stats.writes_fail++;
                client.Disconnect();
            }
        } else if (op_count % kRangeScanEveryN == 0) {
            std::vector<kvdb::KeyValuePair> results;
            std::string prefix = "fuzz_";
            client.PrefixScan(prefix, results);
        } else {
            std::string result;
            if (client.Read(key, result)) {
                g_stats.reads_ok++;
                auto it = t_last_written.find(key);
                if (it != t_last_written.end()) {
                    if (result != it->second) {
                        std::string check_val;
                        if (!client.Read(key, check_val) || check_val.empty()) {
                            INVARIANT(false, "wrote but read empty: key=" + key);
                        }
                    }
                }
            } else {
                g_stats.reads_fail++;
                if (g_server_alive.load() && t_last_written.count(key)) {
                    INVARIANT(false, "wrote but key missing: key=" + key);
                }
            }
        }
    }

    client.Disconnect();
}

static void VerifyPersistence(kvdb::LSMTreeEngine& engine) {
    std::map<std::string, std::string> snapshot;
    {
        std::lock_guard<std::mutex> lock(g_confirmed_mutex);
        snapshot = g_confirmed;
    }

    size_t verified = 0, missing = 0, mismatched = 0;
    for (const auto& [k, expected] : snapshot) {
        std::string actual;
        if (!engine.Lookup(k, actual)) missing++;
        else if (actual != expected) mismatched++;
        else verified++;
    }

    std::cout << "  Verified:   " << verified << std::endl;
    std::cout << "  Missing:    " << missing << std::endl;
    std::cout << "  Mismatched: " << mismatched << std::endl;
    std::cout << (missing == 0 && mismatched == 0
        ? "  PERSISTENCE OK" : "  PERSISTENCE FAILED!") << std::endl;
}

int main() {
    std::cout << "=== Fault-Injection Fuzz Test ===" << std::endl;
    std::cout << "Clients: " << kNumClients << std::endl;
    std::cout << "Duration: " << kDurationSec << "s" << std::endl;
    std::cout << "Kill interval: " << kKillIntervalSec << "s" << std::endl;
    std::cout << "Key pool: " << kKeyPoolSize << std::endl;
    std::cout << "Max value: " << kValueMaxLen << " bytes" << std::endl;

    fs::remove_all(kTestDir);

    {
        kvdb::LSMTreeEngine engine(kTestDir, 4 * 1024 * 1024);

        std::vector<std::thread> clients;
        for (int i = 0; i < kNumClients; ++i) {
            clients.emplace_back(ClientThread, i);
        }

        auto test_start = std::chrono::steady_clock::now();
        int kill_count = 0;

        while (true) {
            auto elapsed = std::chrono::steady_clock::now() - test_start;
            if (elapsed > std::chrono::seconds(kDurationSec)) break;

            kvdb::Server server(engine, kPort);
            server.Start();
            g_server_alive = true;
            std::cout << "  [server up]" << std::endl;

            auto cycle_start = std::chrono::steady_clock::now();
            while (true) {
                auto cycle_elapsed = std::chrono::steady_clock::now() - cycle_start;
                if (cycle_elapsed > std::chrono::seconds(kKillIntervalSec)) break;
                auto total_elapsed = std::chrono::steady_clock::now() - test_start;
                if (total_elapsed > std::chrono::seconds(kDurationSec)) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
            }

            g_server_alive = false;
            server.Stop();
            kill_count++;
            g_stats.server_kills++;
            std::cout << "  [server killed #" << kill_count << "]" << std::endl;

            std::this_thread::sleep_for(std::chrono::milliseconds(300));
        }

        g_stop = true;
        for (auto& t : clients) t.join();

        std::cout << "\n--- Results ---" << std::endl;
        std::cout << "Writes OK:    " << g_stats.writes_ok.load() << std::endl;
        std::cout << "Writes fail:  " << g_stats.writes_fail.load() << std::endl;
        std::cout << "Reads OK:     " << g_stats.reads_ok.load() << std::endl;
        std::cout << "Reads fail:   " << g_stats.reads_fail.load() << std::endl;
        std::cout << "Conn fails:   " << g_stats.connects_fail.load() << std::endl;
        std::cout << "Server kills:  " << g_stats.server_kills.load() << std::endl;
        std::cout << "Invariants fail:" << g_stats.invariant_fail.load() << std::endl;

        std::cout << "\n--- Persistence After Chaos ---" << std::endl;
        VerifyPersistence(engine);

        std::cout << "\n--- Cache Consistency Check ---" << std::endl;
        {
            engine.WaitForPendingFlushes();
            std::this_thread::sleep_for(std::chrono::seconds(4));

            auto iter1 = engine.RangeScan();
            std::map<std::string, std::string> scan1;
            while (iter1.Valid()) {
                if (!iter1.IsTombstone())
                    scan1[iter1.Key()] = iter1.Value();
                iter1.Next();
            }

            auto iter2 = engine.RangeScan();
            std::map<std::string, std::string> scan2;
            while (iter2.Valid()) {
                if (!iter2.IsTombstone())
                    scan2[iter2.Key()] = iter2.Value();
                iter2.Next();
            }

            size_t diff = 0;
            for (const auto& [k, v] : scan1) {
                auto it = scan2.find(k);
                if (it == scan2.end()) diff++;
                else if (it->second != v) diff++;
            }
            for (const auto& [k, v] : scan2) {
                if (scan1.find(k) == scan1.end()) diff++;
            }
            std::cout << "  Scan1 entries:  " << scan1.size() << std::endl;
            std::cout << "  Scan2 entries:  " << scan2.size() << std::endl;
            std::cout << "  Differences:    " << diff << std::endl;
            std::cout << (diff == 0 ? "  CACHE CONSISTENCY OK" : "  CACHE CONSISTENCY ISSUE!") << std::endl;
        }
    }

    std::cout << "\n--- Restart Recovery ---" << std::endl;
    {
        kvdb::LSMTreeEngine engine2(kTestDir, 4 * 1024 * 1024);
        VerifyPersistence(engine2);
    }

    fs::remove_all(kTestDir);
    std::cout << "\nFuzz test complete." << std::endl;
    return 0;
}
