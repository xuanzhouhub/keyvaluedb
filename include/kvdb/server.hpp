#pragma once

#include "config.hpp"
#include "protocol.hpp"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace kvdb {

class LSMTreeEngine;

class Server {
public:
    Server(LSMTreeEngine& engine, int port);
    ~Server();

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    void Start();
    void Stop();

    int Port() const { return port_; }

private:
    void ListenerLoop();
    void HandleClient(socket_t client_sock);
    void WriterLoop();

    LSMTreeEngine& engine_;
    int port_;
    socket_t listen_sock_ = kInvalidSocket;

    std::thread listener_thread_;
    std::thread writer_thread_;
    std::vector<std::thread> client_threads_;
    std::mutex client_threads_mutex_;

    struct WriteRequest {
        std::string key;
        std::string value;
        std::string expected_value;
        std::promise<bool> promise;
        socket_t client_sock;
        bool is_delete = false;
        bool is_batch = false;
        bool is_cas = false;
    };

    std::queue<WriteRequest> write_queue_;
    std::queue<WriteRequest> batch_queue_;
    std::mutex write_queue_mutex_;
    std::condition_variable write_queue_cv_;
    std::condition_variable write_queue_not_full_cv_;
    std::condition_variable batch_commit_done_cv_;
    size_t max_queue_bytes_ = Config::kMaxWriteQueueBytes;
    size_t queue_bytes_ = 0;
    size_t batch_queue_bytes_ = 0;
    size_t mini_batch_size_ = Config::kDefaultMiniBatchSize;
    bool batch_commit_pending_ = false;

    std::atomic<bool> running_{false};
    std::atomic<bool> should_stop_{false};
};

} // namespace kvdb
