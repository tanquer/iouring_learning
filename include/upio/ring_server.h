#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <mutex>
#include <span>
#include <thread>
#include <vector>

#include "upio/files.h"
#include "upio/reactor.h"
#include "upio/task.h"

namespace upio {

// Design B: one ring on one server thread, shared by any number of caller threads.
// Callers queue a request, wake the server through an eventfd the ring is reading, and block.
class RingServer {
 public:
  RingServer(std::span<const int> fds, unsigned entries);
  // Stops the server after the reads already queued finish. No read() may be running.
  ~RingServer();
  RingServer(const RingServer&) = delete;
  RingServer& operator=(const RingServer&) = delete;

  // Blocking read, callable from any thread. Throws std::system_error on failure.
  std::size_t read(FileId file, std::uint64_t offset, std::span<std::byte> buf);

  std::uint64_t wakeups() const { return wakeups_.load(std::memory_order_relaxed); }
  std::uint64_t requests() const { return requests_.load(std::memory_order_relaxed); }

 private:
  struct Request {
    FileId file;
    std::uint64_t offset;
    std::span<std::byte> buf;
    std::promise<std::size_t> done;
  };

  void serve_thread(unsigned entries, std::promise<void>& started);
  Task<void> accept_loop(Reactor& reactor, RandomAccessFiles& files, std::promise<void>& started);
  Task<void> serve(RandomAccessFiles& files, Request* req);

  std::vector<int> fds_;
  int eventfd_ = -1;
  std::mutex mu_;
  std::vector<Request*> pending_;
  bool stopping_ = false;
  std::atomic<std::uint64_t> wakeups_{0};
  std::atomic<std::uint64_t> requests_{0};
  std::thread thread_;
};

}  // namespace upio
