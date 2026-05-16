#pragma once

#include <cstdint>
#include <string>

namespace kvdb {

struct RangeBound {
    std::string key;
    bool inclusive = true;

    static RangeBound Inclusive(const std::string& k) { return {k, true}; }
    static RangeBound Exclusive(const std::string& k) { return {k, false}; }
    static RangeBound Unbounded() { return {"", true}; }
    bool IsUnbounded() const { return key.empty(); }
};

struct KeyValuePair {
    std::string key;
    std::string value;
    uint64_t timestamp = 0;
    bool is_tombstone = false;
};

} // namespace kvdb
