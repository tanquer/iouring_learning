#pragma once

// Optimized copy of upio/reactor.h: adds post() for requests nobody waits for, and run() drains
// posted SQEs before it stops. Lives in upio::optimized so it can sit beside the M3/M4 version.

#include <liburing.h>

#include <coroutine>
#include <cstdint>
#include <deque>
#include <span>
#include <vector>

#include "upio/task.h"

namespace upio::optimized {

// One in-flight request. It lives inside the awaiting coroutine's frame, and its address is the
// SQE's user_data. The frame stays suspended until the CQE arrives, so `Op` outlives the request.
struct Op {
  std::coroutine_handle<> waiter;
  std::int32_t res = 0;
};

struct ReactorStats {
  std::uint64_t enter_calls = 0;     // io_uring_submit_and_wait calls made by run()
  std::uint64_t sqes_submitted = 0;  // SQEs those calls handed to the kernel
  std::uint64_t early_submits = 0;   // submits forced by a full SQ in get_sqe()
  std::uint64_t completions = 0;
};

// Owns one io_uring and runs coroutines on the calling thread. Not thread-safe.
class Reactor {
 public:
  // setup_flags are IORING_SETUP_* flags, e.g. SINGLE_ISSUER | DEFER_TASKRUN for a ring owned by one thread.
  explicit Reactor(unsigned entries, unsigned setup_flags = 0);
  ~Reactor();
  Reactor(const Reactor&) = delete;
  Reactor& operator=(const Reactor&) = delete;

  // Prepare one SQE with `prep(sqe)`, suspend, and resume with the CQE's res (bytes or -errno).
  template <typename Prep>
  auto submit(Prep prep) {
    struct Awaiter {
      Reactor& reactor;
      Prep prep;
      Op op;
      bool await_ready() noexcept { return false; }
      void await_suspend(std::coroutine_handle<> h) {
        op.waiter = h;
        io_uring_sqe* sqe = reactor.get_sqe();
        prep(sqe);
        reactor.arm(sqe, &op);
      }
      std::int32_t await_resume() noexcept { return op.res; }
    };
    return Awaiter{*this, std::move(prep), {}};
  }

  // Queue a request nobody waits for: no Op, no CQE on success (IOSQE_CQE_SKIP_SUCCESS).
  // It goes to the kernel with the next batch of submissions.
  template <typename Prep>
  void post(Prep prep) {
    io_uring_sqe* sqe = get_sqe();
    prep(sqe);
    io_uring_sqe_set_data(sqe, nullptr);
    sqe->flags |= IOSQE_CQE_SKIP_SUCCESS;
  }

  // Start `task` on the next run(). The reactor keeps it alive until it finishes.
  void spawn(Task<void> task);

  // Run until every spawned task has finished and nothing is in flight.
  // Rethrows the first exception a spawned task threw.
  void run();

  void register_files(std::span<const int> fds);

  const ReactorStats& stats() const { return stats_; }
  unsigned in_flight() const { return in_flight_; }

 private:
  io_uring_sqe* get_sqe();
  void arm(io_uring_sqe* sqe, Op* op);
  void reap();

  io_uring ring_;
  unsigned in_flight_ = 0;
  std::deque<std::coroutine_handle<>> ready_;
  std::vector<Task<void>> spawned_;
  ReactorStats stats_;
};

}  // namespace upio::optimized
