#pragma once

// Optimized copy of upio/ring_server.h. See DESIGN.md (M4) for the measurements behind it.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <future>
#include <mutex>
#include <span>
#include <thread>
#include <vector>

#include "optimized/files.h"
#include "optimized/reactor.h"
#include "upio/task.h"

namespace upio::optimized {

struct RingServerOptions {
  // How long a caller spins on its result before sleeping on a futex. 0 = sleep at once.
  unsigned spin_ns = 0;
  // Send futex wakes as IORING_OP_FUTEX_WAKE in the next batched submit instead of a futex syscall each.
  // Measured about 15% more throughput than a futex syscall per wake at 32 callers.
  bool wake_through_ring = true;
};

// Design B: one ring on one server thread, shared by any number of caller threads.
// Callers queue a request, wake the server through an eventfd the ring is reading, and wait on a
// per-thread futex word that the server only wakes when the caller actually went to sleep.
class RingServer {
 public:
  RingServer(std::span<const int> fds, unsigned entries, RingServerOptions options = {});
  // Stops the server after the reads already queued finish. No read() may be running.
  ~RingServer();
  RingServer(const RingServer&) = delete;
  RingServer& operator=(const RingServer&) = delete;

  // Blocking read, callable from any thread. Throws std::system_error on failure.
  std::size_t read(FileId file, std::uint64_t offset, std::span<std::byte> buf);

  std::uint64_t wakeups() const { return wakeups_.load(std::memory_order_relaxed); }
  std::uint64_t requests() const { return requests_.load(std::memory_order_relaxed); }
  std::uint64_t futex_wakes() const { return futex_wakes_.load(std::memory_order_relaxed); }

 private:
  // One per caller thread. The server may still wake a Waiter after that thread's last read returned,
  // so Waiters are never freed: an exiting thread returns its Waiter to a process-wide free list for
  // the next thread. A late wake then lands on a live object and is just a spurious wakeup.
  struct Waiter {
    std::atomic<std::uint32_t> state{0};
  };

  struct Request {
    FileId file;
    std::uint64_t offset;
    std::span<std::byte> buf;
    Waiter* waiter;
    std::size_t bytes = 0;
    int error = 0;
  };

  static Waiter& this_thread_waiter();
  void serve_thread(unsigned entries, std::promise<void>& started);
  Task<void> accept_loop(Reactor& reactor, RandomAccessFiles& files, std::promise<void>& started);
  Task<void> serve(Reactor& reactor, RandomAccessFiles& files, Request* req);

  std::vector<int> fds_;
  RingServerOptions options_;
  int eventfd_ = -1;
  std::mutex mu_;
  std::vector<Request*> pending_;
  bool stopping_ = false;
  std::atomic<std::uint64_t> wakeups_{0};
  std::atomic<std::uint64_t> requests_{0};
  std::atomic<std::uint64_t> futex_wakes_{0};
  std::thread thread_;
};

}  // namespace upio::optimized
