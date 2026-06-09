#pragma once

#include "types.hpp"

#include <cstdint>
#include <string>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace kvdb {

struct Protocol {
    static const uint8_t kWriteReq      = 'W';
    static const uint8_t kReadReq       = 'R';
    static const uint8_t kDeleteReq     = 'D';
    static const uint8_t kRangeScanReq  = 'S';
    static const uint8_t kPrefixScanReq = 'P';
    static const uint8_t kBatchBeginReq = 'B';
    static const uint8_t kBatchWriteReq = 'b';
    static const uint8_t kBatchCommitReq = 'C';
    static const uint8_t kBatchAbortReq  = 'A';
    static const uint8_t kCompareAndSwapReq = 'Z';
    static const uint8_t kAsyncWriteReq  = 'w';
    static const uint8_t kAsyncDeleteReq = 'd';
    static const uint8_t kLevelCountsReq   = 'L';
    static const uint8_t kManualCompactReq = 'M';
    static const uint8_t kOkResp        = 'O';
    static const uint8_t kValueResp     = 'V';
    static const uint8_t kNotFoundResp  = 'N';
    static const uint8_t kErrorResp     = 'E';
    static const uint8_t kEndResp       = 'E';
};

#ifdef _WIN32
using socket_t = SOCKET;
const socket_t kInvalidSocket = INVALID_SOCKET;
const int kSocketError = SOCKET_ERROR;
inline void CloseSocket(socket_t s) { closesocket(s); }
#else
using socket_t = int;
const socket_t kInvalidSocket = -1;
const int kSocketError = -1;
inline void CloseSocket(socket_t s) { close(s); }
#endif

bool SendAll(socket_t sock, const void* data, size_t len);
bool RecvAll(socket_t sock, void* data, size_t len);

bool SendUint32(socket_t sock, uint32_t v);
bool RecvUint32(socket_t sock, uint32_t& v);

bool SendString(socket_t sock, const std::string& s);
bool RecvString(socket_t sock, std::string& s);

bool SendRangeBound(socket_t sock, const RangeBound& bound);
bool RecvRangeBound(socket_t sock, RangeBound& bound);

} // namespace kvdb
