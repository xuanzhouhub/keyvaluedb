#pragma once

#include <cstdint>
#include <string>

namespace kvdb {

struct KeyValuePair {
    std::string key;
    std::string value;
    uint64_t timestamp = 0;
};

} // namespace kvdb
