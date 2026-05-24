#include "kvdb/server.hpp"
#include "kvdb/engine.hpp"

#include <iostream>
#include <stdexcept>

#ifdef _WIN32
#include <winsock2.h>
using socklen_t = int;
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

Server::Server(LSMTreeEngine& engine, int port)
    : engine_(engine)
    , port_(port) {
#ifdef _WIN32
    static WinSockInit wsa;
#endif
}

Server::~Server() {
    Stop();
}

void Server::Start() {
    if (running_.load()) return;

    listen_sock_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_sock_ == kInvalidSocket) {
        throw std::runtime_error("Failed to create socket");
    }

    int opt = 1;
    setsockopt(listen_sock_, SOL_SOCKET, SO_REUSEADDR,
#ifdef _WIN32
               reinterpret_cast<const char*>(&opt), sizeof(opt));
#else
               &opt, sizeof(opt));
#endif

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(static_cast<u_short>(port_));

    if (::bind(listen_sock_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        CloseSocket(listen_sock_);
        throw std::runtime_error("Failed to bind socket");
    }

    if (::listen(listen_sock_, SOMAXCONN) != 0) {
        CloseSocket(listen_sock_);
        throw std::runtime_error("Failed to listen");
    }

    running_ = true;
    should_stop_ = false;

    writer_thread_ = std::thread(&Server::WriterLoop, this);
    listener_thread_ = std::thread(&Server::ListenerLoop, this);
}

void Server::Stop() {
    should_stop_ = true;

    if (listen_sock_ != kInvalidSocket) {
        CloseSocket(listen_sock_);
        listen_sock_ = kInvalidSocket;
    }

    write_queue_cv_.notify_all();
    write_queue_not_full_cv_.notify_all();

    if (writer_thread_.joinable()) {
        writer_thread_.join();
    }
    if (listener_thread_.joinable()) {
        listener_thread_.join();
    }

    std::vector<std::thread> clients;
    {
        std::lock_guard<std::mutex> lock(client_threads_mutex_);
        clients.swap(client_threads_);
    }
    for (auto& t : clients) {
        if (t.joinable()) t.join();
    }

    running_ = false;
}

void Server::ListenerLoop() {
    while (!should_stop_.load()) {
        sockaddr_in client_addr{};
        socklen_t addr_len = sizeof(client_addr);

        socket_t client = ::accept(listen_sock_,
                                    reinterpret_cast<sockaddr*>(&client_addr),
                                    &addr_len);
        if (client == kInvalidSocket) {
            if (should_stop_.load()) break;
            continue;
        }

        std::thread t(&Server::HandleClient, this, client);
        {
            std::lock_guard<std::mutex> lock(client_threads_mutex_);
            client_threads_.push_back(std::move(t));
        }
    }
}

void Server::HandleClient(socket_t client_sock) {
    uint32_t key_len, value_len;
    std::string key, value;

    while (!should_stop_.load()) {
        unsigned char req_type;
        if (!RecvAll(client_sock, &req_type, 1)) break;

        if (req_type == Protocol::kWriteReq) {
            if (!RecvUint32(client_sock, key_len)) break;
            key.resize(key_len);
            if (!RecvAll(client_sock, &key[0], key_len)) break;

            if (!RecvUint32(client_sock, value_len)) break;
            value.resize(value_len);
            if (!RecvAll(client_sock, &value[0], value_len)) break;

            WriteRequest req;
            req.key = std::move(key);
            req.value = std::move(value);
            req.client_sock = client_sock;
            auto future = req.promise.get_future();

            {
                std::unique_lock<std::mutex> lock(write_queue_mutex_);
                size_t req_size = req.key.size() + req.value.size();
                write_queue_not_full_cv_.wait(lock, [this, req_size] {
                    return queue_bytes_ + req_size <= max_queue_bytes_
                        || should_stop_.load();
                });
                if (should_stop_.load()) break;
                queue_bytes_ += req_size;
                write_queue_.push(std::move(req));
            }
            write_queue_cv_.notify_one();

            bool result = future.get();
            if (result) {
                unsigned char resp = Protocol::kOkResp;
                SendAll(client_sock, &resp, 1);
            } else {
                unsigned char resp = Protocol::kErrorResp;
                SendAll(client_sock, &resp, 1);
                SendString(client_sock, "write failed");
            }

        } else if (req_type == Protocol::kReadReq) {
            if (!RecvUint32(client_sock, key_len)) break;
            key.resize(key_len);
            if (!RecvAll(client_sock, &key[0], key_len)) break;

            std::string result;
            bool found = engine_.Lookup(key, result);

            if (found) {
                unsigned char resp = Protocol::kValueResp;
                SendAll(client_sock, &resp, 1);
                SendString(client_sock, result);
            } else {
                unsigned char resp = Protocol::kNotFoundResp;
                SendAll(client_sock, &resp, 1);
            }

        } else if (req_type == Protocol::kDeleteReq) {
            if (!RecvUint32(client_sock, key_len)) break;
            key.resize(key_len);
            if (!RecvAll(client_sock, &key[0], key_len)) break;

            WriteRequest req;
            req.key = std::move(key);
            req.value.clear();
            req.is_delete = true;
            req.client_sock = client_sock;
            auto future = req.promise.get_future();

            {
                std::unique_lock<std::mutex> lock(write_queue_mutex_);
                write_queue_not_full_cv_.wait(lock, [this] {
                    return queue_bytes_ + 64 <= max_queue_bytes_
                        || should_stop_.load();
                });
                if (should_stop_.load()) break;
                queue_bytes_ += 64;
                write_queue_.push(std::move(req));
            }
            write_queue_cv_.notify_one();

            bool result = future.get();
            unsigned char resp = result ? Protocol::kOkResp : Protocol::kErrorResp;
            SendAll(client_sock, &resp, 1);
            if (!result) SendString(client_sock, "delete failed");

        } else if (req_type == Protocol::kBatchWriteReq) {
            if (!RecvUint32(client_sock, key_len)) break;
            key.resize(key_len);
            if (!RecvAll(client_sock, &key[0], key_len)) break;

            if (!RecvUint32(client_sock, value_len)) break;
            value.resize(value_len);
            if (!RecvAll(client_sock, &value[0], value_len)) break;

            {
                std::unique_lock<std::mutex> lock(write_queue_mutex_);
                size_t req_size = key.size() + value.size() + 64;
                write_queue_not_full_cv_.wait(lock, [this, req_size] {
                    return batch_queue_bytes_ + req_size <= max_queue_bytes_
                        || should_stop_.load();
                });
                if (should_stop_.load()) break;
                batch_queue_bytes_ += req_size;
                WriteRequest wreq;
                wreq.key = std::move(key);
                wreq.value = std::move(value);
                wreq.is_delete = value.empty();
                wreq.is_batch = true;
                wreq.client_sock = client_sock;
                batch_queue_.push(std::move(wreq));
            }
            write_queue_cv_.notify_one();

            unsigned char resp = Protocol::kOkResp;
            SendAll(client_sock, &resp, 1);

        } else if (req_type == Protocol::kBatchBeginReq) {
            bool started = engine_.StartBatch();
            unsigned char resp = started ? Protocol::kOkResp : Protocol::kErrorResp;
            SendAll(client_sock, &resp, 1);
            if (!started) SendString(client_sock, "batch already in progress");

        } else if (req_type == Protocol::kBatchCommitReq) {
            {
                std::lock_guard<std::mutex> lock(write_queue_mutex_);
                batch_commit_pending_ = true;
            }
            write_queue_cv_.notify_one();

            {
                std::unique_lock<std::mutex> lock(write_queue_mutex_);
                batch_commit_done_cv_.wait(lock, [this] {
                    return !batch_commit_pending_ || should_stop_.load();
                });
            }

            unsigned char resp = Protocol::kOkResp;
            SendAll(client_sock, &resp, 1);

        } else if (req_type == Protocol::kBatchAbortReq) {
            {
                std::lock_guard<std::mutex> lock(write_queue_mutex_);
                while (!batch_queue_.empty()) {
                    batch_queue_bytes_ -= (batch_queue_.front().key.size()
                                        + batch_queue_.front().value.size());
                    batch_queue_.pop();
                }
                batch_commit_pending_ = false;
            }
            write_queue_not_full_cv_.notify_all();

            engine_.AbortBatch();
            unsigned char resp = Protocol::kOkResp;
            SendAll(client_sock, &resp, 1);

        } else if (req_type == Protocol::kCompareAndSwapReq) {
            if (!RecvUint32(client_sock, key_len)) break;
            key.resize(key_len);
            if (!RecvAll(client_sock, &key[0], key_len)) break;

            std::string expected, desired;
            if (!RecvString(client_sock, expected)) break;
            if (!RecvString(client_sock, desired)) break;

            WriteRequest req;
            req.key = std::move(key);
            req.expected_value = std::move(expected);
            req.value = std::move(desired);
            req.is_cas = true;
            req.client_sock = client_sock;
            auto future = req.promise.get_future();

            {
                std::unique_lock<std::mutex> lock(write_queue_mutex_);
                size_t req_size = req.key.size() + req.value.size() + req.expected_value.size();
                write_queue_not_full_cv_.wait(lock, [this, req_size] {
                    return queue_bytes_ + req_size <= max_queue_bytes_
                        || should_stop_.load();
                });
                if (should_stop_.load()) break;
                queue_bytes_ += req_size;
                write_queue_.push(std::move(req));
            }
            write_queue_cv_.notify_one();

            bool result = future.get();
            unsigned char resp = result ? Protocol::kOkResp : Protocol::kErrorResp;
            SendAll(client_sock, &resp, 1);

        } else if (req_type == Protocol::kRangeScanReq) {
            RangeBound lower, upper;
            if (!RecvRangeBound(client_sock, lower)) break;
            if (!RecvRangeBound(client_sock, upper)) break;

            auto iter = engine_.RangeScan(lower, upper);
            while (iter.Valid()) {
                unsigned char resp = Protocol::kValueResp;
                if (!SendAll(client_sock, &resp, 1)) break;
                if (!SendString(client_sock, iter.Key())) break;
                if (!SendString(client_sock, iter.Value())) break;
                iter.Next();
            }
            unsigned char end = Protocol::kEndResp;
            SendAll(client_sock, &end, 1);

        } else if (req_type == Protocol::kPrefixScanReq) {
            if (!RecvUint32(client_sock, key_len)) break;
            key.resize(key_len);
            if (!RecvAll(client_sock, &key[0], key_len)) break;

            auto iter = engine_.PrefixScan(key);
            while (iter.Valid()) {
                unsigned char resp = Protocol::kValueResp;
                if (!SendAll(client_sock, &resp, 1)) break;
                if (!SendString(client_sock, iter.Key())) break;
                if (!SendString(client_sock, iter.Value())) break;
                iter.Next();
            }
            unsigned char end = Protocol::kEndResp;
            SendAll(client_sock, &end, 1);

        } else {
            break;
        }
    }

    CloseSocket(client_sock);
}

void Server::WriterLoop() {
    auto resolvePromise = [](WriteRequest& req, bool ok) {
        try { req.promise.set_value(ok); } catch (...) {}
    };

    while (true) {
        {
            std::unique_lock<std::mutex> lock(write_queue_mutex_);
            write_queue_cv_.wait(lock, [this] {
                return !write_queue_.empty() || !batch_queue_.empty()
                    || batch_commit_pending_ || should_stop_.load();
            });
        }

        bool stop = false;
        {
            std::lock_guard<std::mutex> lock(write_queue_mutex_);
            if (should_stop_.load() && write_queue_.empty()
                && batch_queue_.empty() && !batch_commit_pending_)
                stop = true;
        }
        if (stop) break;

        WriteRequest normal_req;
        bool has_normal = false;
        {
            std::lock_guard<std::mutex> lock(write_queue_mutex_);
            if (!write_queue_.empty() && !batch_commit_pending_) {
                normal_req = std::move(write_queue_.front());
                write_queue_.pop();
                queue_bytes_ -= (normal_req.key.size() + normal_req.value.size());
                has_normal = true;
            }
        }
        if (has_normal) {
            write_queue_not_full_cv_.notify_one();
            try {
                if (normal_req.is_cas)
                    resolvePromise(normal_req, engine_.CompareAndSwap(
                        normal_req.key, normal_req.expected_value, normal_req.value));
                else if (normal_req.is_delete)
                    engine_.Delete(normal_req.key);
                else
                    engine_.Insert(normal_req.key, normal_req.value);
                resolvePromise(normal_req, true);
            } catch (const std::exception&) {
                resolvePromise(normal_req, false);
            }
            continue;
        }

        bool trigger = false;
        bool commit_mode = false;
        std::vector<WriteRequest> mini;
        {
            std::lock_guard<std::mutex> lock(write_queue_mutex_);
            trigger = (batch_queue_.size() >= mini_batch_size_)
                   || (batch_queue_bytes_ >= max_queue_bytes_ / 2)
                   || batch_commit_pending_;
            if (!trigger) continue;

            commit_mode = batch_commit_pending_;
            size_t limit = commit_mode ? SIZE_MAX : mini_batch_size_;
            while (!batch_queue_.empty() && mini.size() < limit) {
                auto& front = batch_queue_.front();
                batch_queue_bytes_ -= (front.key.size() + front.value.size());
                mini.push_back(std::move(front));
                batch_queue_.pop();
            }
        }
        write_queue_not_full_cv_.notify_one();

        try {
            for (auto& r : mini) {
                if (r.is_delete)
                    engine_.BatchDelete(r.key);
                else
                    engine_.BatchInsert(r.key, r.value);
            }
        } catch (...) {
            for (auto& r : mini) resolvePromise(r, false);
            continue;
        }

        if (commit_mode) {
            engine_.CommitBatch();
            {
                std::lock_guard<std::mutex> lock(write_queue_mutex_);
                batch_commit_pending_ = false;
            }
            batch_commit_done_cv_.notify_all();
        }

        for (auto& r : mini) resolvePromise(r, true);
    }
}

} // namespace kvdb
