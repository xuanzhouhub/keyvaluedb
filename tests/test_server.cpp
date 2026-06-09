#include "test_common.hpp"
#include "kvdb/engine.hpp"
#include "kvdb/server.hpp"
#include "kvdb/client.hpp"

#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

namespace kvdb_test {

namespace fs = std::filesystem;

static const std::string kTestDir = "./test_server_data";
static const int kTestPort = 19555;

void Cleanup() {
    if (fs::exists(kTestDir)) fs::remove_all(kTestDir);
}

void TestWriteAndRead() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client client;
        ASSERT_TRUE(client.Connect("127.0.0.1", kTestPort));

        ASSERT_TRUE(client.Write("hello", "world"));

        std::string value;
        ASSERT_TRUE(client.Read("hello", value));
        ASSERT_STREQ("world", value);

        client.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestMultipleWrites() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client client;
        ASSERT_TRUE(client.Connect("127.0.0.1", kTestPort));

        for (int i = 0; i < 50; ++i) {
            ASSERT_TRUE(client.Write("k" + std::to_string(i), "v" + std::to_string(i)));
        }

        for (int i = 0; i < 50; ++i) {
            std::string value;
            ASSERT_TRUE(client.Read("k" + std::to_string(i), value));
            ASSERT_STREQ("v" + std::to_string(i), value);
        }

        client.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestNotFound() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client client;
        ASSERT_TRUE(client.Connect("127.0.0.1", kTestPort));

        std::string value;
        ASSERT_FALSE(client.Read("nonexistent", value));

        client.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestOverwrite() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client client;
        ASSERT_TRUE(client.Connect("127.0.0.1", kTestPort));

        ASSERT_TRUE(client.Write("key", "old"));
        ASSERT_TRUE(client.Write("key", "new"));

        std::string value;
        ASSERT_TRUE(client.Read("key", value));
        ASSERT_STREQ("new", value);

        client.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestMultipleClients() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::vector<std::thread> threads;
        for (int c = 0; c < 4; ++c) {
            threads.emplace_back([c]() {
                kvdb::Client client;
                if (!client.Connect("127.0.0.1", kTestPort)) return;

                for (int i = 0; i < 10; ++i) {
                    std::string key = "c" + std::to_string(c) + "_k" + std::to_string(i);
                    client.Write(key, "val");
                }

                for (int i = 0; i < 10; ++i) {
                    std::string key = "c" + std::to_string(c) + "_k" + std::to_string(i);
                    std::string value;
                    if (client.Read(key, value)) {
                        ASSERT_STREQ("val", value);
                    }
                }

                client.Disconnect();
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        server.Stop();
    }
    Cleanup();
}

void TestBinaryData() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client client;
        ASSERT_TRUE(client.Connect("127.0.0.1", kTestPort));

        std::string key("\x00\x01\x02\xFF", 4);
        std::string value("\xAB\xCD\x00\x11", 4);
        ASSERT_TRUE(client.Write(key, value));

        std::string result;
        ASSERT_TRUE(client.Read(key, result));
        ASSERT_STREQ(value, result);

        client.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestClientDelete() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client client;
        ASSERT_TRUE(client.Connect("127.0.0.1", kTestPort));

        ASSERT_TRUE(client.Write("k", "v"));
        std::string val;
        ASSERT_TRUE(client.Read("k", val));
        ASSERT_STREQ("v", val);

        ASSERT_TRUE(client.Delete("k"));
        ASSERT_FALSE(client.Read("k", val));

        client.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestClientRangeScan() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client client;
        ASSERT_TRUE(client.Connect("127.0.0.1", kTestPort));

        ASSERT_TRUE(client.Write("a", "1"));
        ASSERT_TRUE(client.Write("b", "2"));
        ASSERT_TRUE(client.Write("c", "3"));
        ASSERT_TRUE(client.Write("d", "4"));

        std::vector<kvdb::KeyValuePair> results;
        ASSERT_TRUE(client.RangeScan(
            kvdb::RangeBound::Inclusive("b"),
            kvdb::RangeBound::Inclusive("c"), results));
        ASSERT_EQ(2u, results.size());
        ASSERT_STREQ("b", results[0].key);
        ASSERT_STREQ("c", results[1].key);

        client.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestClientPrefixScan() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client client;
        ASSERT_TRUE(client.Connect("127.0.0.1", kTestPort));

        ASSERT_TRUE(client.Write("user:1", "alice"));
        ASSERT_TRUE(client.Write("user:2", "bob"));
        ASSERT_TRUE(client.Write("other", "xxx"));

        std::vector<kvdb::KeyValuePair> results;
        ASSERT_TRUE(client.PrefixScan("user:", results));
        ASSERT_EQ(2u, results.size());
        ASSERT_STREQ("user:1", results[0].key);
        ASSERT_STREQ("user:2", results[1].key);

        client.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestClientRangeScanExclusive() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client client;
        ASSERT_TRUE(client.Connect("127.0.0.1", kTestPort));

        ASSERT_TRUE(client.Write("a", "1"));
        ASSERT_TRUE(client.Write("b", "2"));
        ASSERT_TRUE(client.Write("c", "3"));

        std::vector<kvdb::KeyValuePair> results;
        ASSERT_TRUE(client.RangeScan(
            kvdb::RangeBound::Exclusive("a"),
            kvdb::RangeBound::Exclusive("c"), results));
        ASSERT_EQ(1u, results.size());
        ASSERT_STREQ("b", results[0].key);

        client.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestClientRangeScanExclusive();
void TestClientCompareAndSwap();
void TestClientCASFail();
void TestClientCASOnNewKey();
void TestClientManualCompact();

void RunTests() {
    std::cout << "Running Server Tests...\n\n";

    TestWriteAndRead();
    TestMultipleWrites();
    TestNotFound();
    TestOverwrite();
    TestMultipleClients();
    TestBinaryData();
    TestClientDelete();
    TestClientRangeScan();
    TestClientPrefixScan();
    TestClientRangeScanExclusive();
    TestClientCompareAndSwap();
    TestClientCASFail();
    TestClientCASOnNewKey();
    TestClientManualCompact();
}

void TestClientCompareAndSwap() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client c;
        ASSERT_TRUE(c.Connect("127.0.0.1", kTestPort));
        ASSERT_TRUE(c.Write("cas_key", "v1"));
        ASSERT_TRUE(c.CompareAndSwap("cas_key", "v1", "v2"));

        std::string v;
        ASSERT_TRUE(c.Read("cas_key", v));
        ASSERT_STREQ("v2", v);

        c.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestClientCASFail() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client c;
        ASSERT_TRUE(c.Connect("127.0.0.1", kTestPort));
        ASSERT_TRUE(c.Write("cf_key", "val1"));
        ASSERT_FALSE(c.CompareAndSwap("cf_key", "wrong", "val2"));

        std::string v;
        ASSERT_TRUE(c.Read("cf_key", v));
        ASSERT_STREQ("val1", v);

        c.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestClientCASOnNewKey() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 1024 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client c;
        ASSERT_TRUE(c.Connect("127.0.0.1", kTestPort));
        ASSERT_FALSE(c.CompareAndSwap("newkey", "any", "val"));

        std::string v;
        ASSERT_FALSE(c.Read("newkey", v));

        c.Disconnect();
        server.Stop();
    }
    Cleanup();
}

void TestClientManualCompact() {
    Cleanup();
    {
        kvdb::LSMTreeEngine engine(kTestDir, 256 * 1024);
        kvdb::Server server(engine, kTestPort);
        server.Start();
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        kvdb::Client c;
        ASSERT_TRUE(c.Connect("127.0.0.1", kTestPort));
        std::string v(512, 'x');
        for (int i = 0; i < 15000; ++i) {
            char buf[32]; std::snprintf(buf, sizeof(buf), "k%08d", i);
            c.WriteAsync(buf, v);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        engine.Flush(); engine.WaitForPendingFlushes();

        auto before = c.LevelCounts();
        ASSERT_TRUE(before.size() > 0);
        ASSERT_TRUE(before[0] >= 8);

        int compacted = c.ManualCompact(4, 0, true);
        ASSERT_TRUE(compacted > 0);

        auto after = c.LevelCounts();
        ASSERT_TRUE(after.size() > 0);
        ASSERT_TRUE(after[1] > 0);

        c.Disconnect();
        server.Stop();
    }
    Cleanup();
}

} // namespace kvdb_test

RUN_TESTS()
