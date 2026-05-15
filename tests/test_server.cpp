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

void RunTests() {
    std::cout << "Running Server Tests...\n\n";

    TestWriteAndRead();
    TestMultipleWrites();
    TestNotFound();
    TestOverwrite();
    TestMultipleClients();
    TestBinaryData();
}

} // namespace kvdb_test

RUN_TESTS()
