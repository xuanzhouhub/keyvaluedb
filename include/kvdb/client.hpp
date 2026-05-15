#pragma once

#include "protocol.hpp"

#include <string>

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

private:
    socket_t sock_ = kInvalidSocket;
};

} // namespace kvdb
