#pragma once

#include <iostream>
#include <mutex>
#include <string>
#include <cstdlib>
#include <sstream>

namespace kvdb_test {

static int g_total_tests = 0;
static int g_passed_tests = 0;
static int g_failed_tests = 0;
static std::string g_failure_details;

template<typename T, typename U>
inline void AssertEqual(const T& expected, const U& actual,
                        const std::string& file, int line) {
    g_total_tests++;
    if (expected == actual) {
        g_passed_tests++;
    } else {
        g_failed_tests++;
        std::ostringstream oss;
        oss << "  FAIL [" << file << ":" << line << "] "
            << "Expected: " << expected << ", Got: " << actual << "\n";
        g_failure_details += oss.str();
    }
}

inline void AssertStrEqual(const std::string& expected, const std::string& actual,
                           const std::string& file, int line) {
    g_total_tests++;
    if (expected == actual) {
        g_passed_tests++;
    } else {
        g_failed_tests++;
        std::ostringstream oss;
        oss << "  FAIL [" << file << ":" << line << "] "
            << "Expected: \"" << expected << "\", Got: \"" << actual << "\"\n";
        g_failure_details += oss.str();
    }
}

inline void AssertTrue(bool condition, const std::string& msg,
                       const std::string& file, int line) {
    g_total_tests++;
    if (condition) {
        g_passed_tests++;
    } else {
        g_failed_tests++;
        std::ostringstream oss;
        oss << "  FAIL [" << file << ":" << line << "] " << msg << "\n";
        g_failure_details += oss.str();
    }
}

inline int PrintSummary() {
    std::cout << "\n=== Test Summary ===\n";
    std::cout << "Total:  " << g_total_tests << "\n";
    std::cout << "Passed: " << g_passed_tests << "\n";
    std::cout << "Failed: " << g_failed_tests << "\n";
    if (!g_failure_details.empty()) {
        std::cout << "\nFailures:\n" << g_failure_details;
    }
    return g_failed_tests > 0 ? 1 : 0;
}

} // namespace kvdb_test

#define ASSERT_EQ(expected, actual) \
    kvdb_test::AssertEqual((expected), (actual), __FILE__, __LINE__)

#define ASSERT_STREQ(expected, actual) \
    kvdb_test::AssertStrEqual((expected), (actual), __FILE__, __LINE__)

#define ASSERT_TRUE(condition) \
    kvdb_test::AssertTrue((condition), #condition, __FILE__, __LINE__)

#define ASSERT_FALSE(condition) \
    kvdb_test::AssertTrue(!(condition), "!" #condition, __FILE__, __LINE__)

#define RUN_TESTS() \
    int main() { \
        try { \
            kvdb_test::RunTests(); \
        } catch (const std::exception& e) { \
            std::cerr << "FATAL: " << e.what() << "\n"; \
            return 1; \
        } \
        return kvdb_test::PrintSummary(); \
    }
