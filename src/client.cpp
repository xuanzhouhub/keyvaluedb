#include "kvdb/client.hpp"

#include <stdexcept>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#endif

namespace kvdb {
namespace {

#ifdef _WIN32
struct WinSockInit {
    WinSockInit() {
        WSADATA wsa;
        WSAStartup(MAKEWORD(2, 2), &wsa);
    }
    ~WinSockInit() {
        WSACleanup();
    }
};
#endif

} // anonymous namespace

Client::Client() {
#ifdef _WIN32
    static WinSockInit wsa;
#endif
}

Client::~Client() {
    Disconnect();
}

bool Client::Connect(const std::string& host, int port) {
    Disconnect();

    sock_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock_ == kInvalidSocket) return false;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<u_short>(port));

#ifdef _WIN32
    addr.sin_addr.s_addr = inet_addr(host.c_str());
    if (addr.sin_addr.s_addr == INADDR_NONE) {
        CloseSocket(sock_);
        sock_ = kInvalidSocket;
        return false;
    }
#else
    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        CloseSocket(sock_);
        sock_ = kInvalidSocket;
        return false;
    }
#endif

    if (::connect(sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        CloseSocket(sock_);
        sock_ = kInvalidSocket;
        return false;
    }

    return true;
}

void Client::Disconnect() {
    if (sock_ != kInvalidSocket) {
        CloseSocket(sock_);
        sock_ = kInvalidSocket;
    }
}

bool Client::Write(const std::string& key, const std::string& value) {
    if (sock_ == kInvalidSocket) return false;

    unsigned char req = Protocol::kWriteReq;
    if (!SendAll(sock_, &req, 1)) return false;
    if (!SendString(sock_, key)) return false;
    if (!SendString(sock_, value)) return false;

    unsigned char resp;
    if (!RecvAll(sock_, &resp, 1)) return false;
    if (resp == Protocol::kOkResp) return true;

    if (resp == Protocol::kErrorResp) {
        std::string msg;
        RecvString(sock_, msg);
    }
    return false;
}

bool Client::Read(const std::string& key, std::string& value_out) {
    if (sock_ == kInvalidSocket) return false;

    unsigned char req = Protocol::kReadReq;
    if (!SendAll(sock_, &req, 1)) return false;
    if (!SendString(sock_, key)) return false;

    unsigned char resp;
    if (!RecvAll(sock_, &resp, 1)) return false;

    if (resp == Protocol::kValueResp) {
        return RecvString(sock_, value_out);
    }

    return false;
}

} // namespace kvdb
