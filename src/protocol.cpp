#include "kvdb/protocol.hpp"

#include <cstring>
#include <stdexcept>

#ifdef _WIN32
#include <winsock2.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace kvdb {

bool SendAll(socket_t sock, const void* data, size_t len) {
    const char* p = static_cast<const char*>(data);
    size_t remaining = len;
    while (remaining > 0) {
        int sent = ::send(sock, p, static_cast<int>(remaining), 0);
        if (sent <= 0) return false;
        remaining -= static_cast<size_t>(sent);
        p += sent;
    }
    return true;
}

bool RecvAll(socket_t sock, void* data, size_t len) {
    char* p = static_cast<char*>(data);
    size_t remaining = len;
    while (remaining > 0) {
        int received = ::recv(sock, p, static_cast<int>(remaining), 0);
        if (received <= 0) return false;
        remaining -= static_cast<size_t>(received);
        p += received;
    }
    return true;
}

bool SendUint32(socket_t sock, uint32_t v) {
    unsigned char buf[4];
    buf[0] = static_cast<unsigned char>(v & 0xFF);
    buf[1] = static_cast<unsigned char>((v >> 8) & 0xFF);
    buf[2] = static_cast<unsigned char>((v >> 16) & 0xFF);
    buf[3] = static_cast<unsigned char>((v >> 24) & 0xFF);
    return SendAll(sock, buf, 4);
}

bool RecvUint32(socket_t sock, uint32_t& v) {
    unsigned char buf[4];
    if (!RecvAll(sock, buf, 4)) return false;
    v = buf[0] | (static_cast<uint32_t>(buf[1]) << 8)
      | (static_cast<uint32_t>(buf[2]) << 16)
      | (static_cast<uint32_t>(buf[3]) << 24);
    return true;
}

bool SendString(socket_t sock, const std::string& s) {
    return SendUint32(sock, static_cast<uint32_t>(s.size()))
        && SendAll(sock, s.data(), s.size());
}

bool RecvString(socket_t sock, std::string& s) {
    uint32_t len;
    if (!RecvUint32(sock, len)) return false;
    s.resize(len);
    return RecvAll(sock, &s[0], len);
}

} // namespace kvdb
