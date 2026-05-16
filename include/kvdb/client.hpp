#pragma once

#include "protocol.hpp"
#include "types.hpp"

#include <string>
#include <vector>

namespace kvdb {

class Client {
public:
    Client();
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    bool Connect(const std::string& host, int port);
    void Disconnect();
    bool IsConnected() const { return sock_ != kInvalidSocket; }

    bool Write(const std::string& key, const std::string& value);
    bool Read(const std::string& key, std::string& value_out);
    bool Delete(const std::string& key);
    bool RangeScan(const RangeBound& lower, const RangeBound& upper,
                   std::vector<KeyValuePair>& results);
    bool PrefixScan(const std::string& prefix,
                    std::vector<KeyValuePair>& results);

private:
    socket_t sock_ = kInvalidSocket;
};

} // namespace kvdb
