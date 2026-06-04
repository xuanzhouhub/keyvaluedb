#include "kvdb/internal/flat_cache.hpp"
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#define CHECK(cond, msg) do { if (!(cond)) { std::fprintf(stderr, "FAIL %s\n", msg); return 1; } } while(0)

struct StrValue { std::string data; };
struct SizeStr { size_t operator()(const StrValue& v) const { return v.data.size(); } };

int main() {
    int failures = 0;

    {
        kvdb::internal::FlatCache<int, StrValue, std::hash<int>, std::equal_to<int>, SizeStr>
            cache(32, 1024, 4);
        for (int i = 0; i < 20; i++) cache.Put(i, std::make_shared<StrValue>(StrValue{"val-" + std::to_string(i)}));
        for (int i = 0; i < 20; i++) { auto v = cache.Get(i); CHECK(v && v->data == "val-" + std::to_string(i), "t1:wrong"); }
        CHECK(!cache.Get(999), "t1:miss");
        std::fprintf(stderr, "  Test 1: PASS\n");
    }
    {
        kvdb::internal::FlatCache<int, StrValue, std::hash<int>, std::equal_to<int>, SizeStr>
            cache(4, 4096, 1);
        for (int i = 0; i < 10; i++) cache.Put(i, std::make_shared<StrValue>(StrValue{"v" + std::to_string(i)}));
        int found = 0; for (int i = 0; i < 10; i++) if (cache.Get(i)) found++;
        CHECK(found == 4, "t2:count"); CHECK(!cache.Get(0), "t2:first"); CHECK(cache.Get(9) != nullptr, "t2:last");
        std::fprintf(stderr, "  Test 2: PASS\n");
    }
    {
        kvdb::internal::FlatCache<int, StrValue, std::hash<int>, std::equal_to<int>, SizeStr>
            cache(3, 4096, 1);
        cache.Put(1, std::make_shared<StrValue>(StrValue{"a"}));
        cache.Put(2, std::make_shared<StrValue>(StrValue{"b"}));
        cache.Put(3, std::make_shared<StrValue>(StrValue{"c"}));
        cache.Get(1);
        cache.Put(4, std::make_shared<StrValue>(StrValue{"d"}));
        CHECK(cache.Get(1), "t3:1"); CHECK(!cache.Get(2), "t3:2"); CHECK(cache.Get(3), "t3:3"); CHECK(cache.Get(4), "t3:4");
        std::fprintf(stderr, "  Test 3: PASS\n");
    }
    {
        struct BigVal { std::string big; };
        struct SizeBig { size_t operator()(const BigVal& v) const { return v.big.size(); } };
        kvdb::internal::FlatCache<int, BigVal, std::hash<int>, std::equal_to<int>, SizeBig>
            cache(8, 100, 1);
        cache.Put(1, std::make_shared<BigVal>(BigVal{std::string(30, 'x')}));
        cache.Put(2, std::make_shared<BigVal>(BigVal{std::string(30, 'y')}));
        cache.Put(3, std::make_shared<BigVal>(BigVal{std::string(30, 'z')}));
        CHECK(cache.Get(1), "t4:pre");
        cache.Put(4, std::make_shared<BigVal>(BigVal{std::string(30, 'w')}));
        int alive = 0; for (int i = 1; i <= 4; i++) if (cache.Get(i)) alive++;
        CHECK(alive <= 3, "t4:byte_evict");
        std::fprintf(stderr, "  Test 4: PASS\n");
    }
    {
        kvdb::internal::FlatCache<int, int> cache(16, 4096, 4);
        cache.Put(42, std::make_shared<int>(100));
        CHECK(cache.Get(42), "t5:pre"); cache.Erase(42); CHECK(!cache.Get(42), "t5:post");
        std::fprintf(stderr, "  Test 5: PASS\n");
    }
    {
        kvdb::internal::FlatCache<int, int> cache(16, 4096, 4);
        cache.Put(1, std::make_shared<int>(10));
        cache.Put(1, std::make_shared<int>(20));
        auto v = cache.Get(1); CHECK(v && *v == 20, "t6:update");
        std::fprintf(stderr, "  Test 6: PASS\n");
    }
    {
        kvdb::internal::FlatCache<int, int> cache(200, 4096, 8);
        for (int i = 0; i < 100; i++) cache.Put(i, std::make_shared<int>(i));
        for (int i = 0; i < 100; i++) CHECK(cache.Get(i), "t7:shard");
        std::fprintf(stderr, "  Test 7: PASS\n");
    }
    {
        kvdb::internal::FlatCache<std::string, StrValue> cache(16, 4096, 4);
        cache.Put("foo", std::make_shared<StrValue>(StrValue{"bar"}));
        cache.Put("hello", std::make_shared<StrValue>(StrValue{"world"}));
        CHECK(cache.Get("foo")->data == "bar", "t8:a");
        CHECK(cache.Get("hello")->data == "world", "t8:b");
        CHECK(!cache.Get("nope"), "t8:miss");
        std::fprintf(stderr, "  Test 8: PASS\n");
    }
    {
        kvdb::internal::FlatCache<int, int> cache(16, 4096, 4);
        cache.Put(1, std::make_shared<int>(42)); cache.Clear();
        CHECK(!cache.Get(1), "t9:clear");
        std::fprintf(stderr, "  Test 9: PASS\n");
    }
    {
        kvdb::internal::FlatCache<int, int> cache(200, 99999, 4);
        for (int i = 0; i < 100; i++) cache.Put(i, std::make_shared<int>(i * 2));
        std::atomic<int> errors{0};
        std::vector<std::thread> threads;
        for (int t = 0; t < 2; t++)
            threads.emplace_back([&]() {
                for (int i = 0; i < 1000; i++)
                    if (!cache.Get((i * 13) % 100)) errors++;
            });
        for (auto& th : threads) th.join();
        CHECK(errors == 0, "t10:thread");
        std::fprintf(stderr, "  Test 10: PASS\n");
    }
    {
        kvdb::internal::FlatCache<int, int> cache(4, 4096, 1);
        for (int i = 0; i < 5; i++) cache.Put(i, std::make_shared<int>(i));
        cache.Erase(0);
        cache.Put(100, std::make_shared<int>(100));
        CHECK(cache.Get(100), "t11:erase+reuse");
        std::fprintf(stderr, "  Test 11: PASS\n");
    }

    std::fprintf(stderr, "\nALL 11 TESTS PASSED\n");
    return 0;
}
