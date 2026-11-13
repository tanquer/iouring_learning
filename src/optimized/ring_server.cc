#include "optimized/ring_server.h"

#include <linux/futex.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <system_error>

namespace upio::optimized {

namespace {

enum : std::uint32_t { kPending = 0, kDone = 1, kSleeping = 2 };

void signal_eventfd(int fd) {
  std::uint64_t one = 1;
  if (::write(fd, &one, sizeof one) != static_cast<ssize_t>(sizeof one)) {
    throw std::system_error(errno, std::generic_category(), "write eventfd");
  }
}

std::uint32_t* futex_word(std::atomic<std::uint32_t>& a) { return reinterpret_cast<std::uint32_t*>(&a); }

void cpu_relax() { __builtin_ia32_pause(); }

}  // namespace

RingServer::Waiter& RingServer::this_thread_waiter() {
  struct FreeList {
    std::mutex mu;
    std::vector<Waiter*> waiters;
  };
  static FreeList* free_list = new FreeList;
  struct Slot {
    Waiter* waiter;
    Slot() {
      std::lock_guard lock(free_list->mu);
      if (free_list->waiters.empty()) {
        waiter = new Waiter;
      } else {
        waiter = free_list->waiters.back();
        free_list->waiters.pop_back();
      }
    }
    ~Slot() {
      std::lock_guard lock(free_list->mu);
      free_list->waiters.push_back(waiter);
    }
  };
  static thread_local Slot slot;
  return *slot.waiter;
}

RingServer::RingServer(std::span<const int> fds, unsigned entries, RingServerOptions options)
    : fds_(fds.begin(), fds.end()), options_(options) {
  eventfd_ = eventfd(0, EFD_CLOEXEC);
  if (eventfd_ < 0) throw std::system_error(errno, std::generic_category(), "eventfd");
  std::promise<void> started;
  std::future<void> ready = started.get_future();
  thread_ = std::thread([this, entries, p = std::move(started)]() mutable { serve_thread(entries, p); });
  try {
    ready.get();
  } catch (...) {
    thread_.join();
    close(eventfd_);
    throw;
  }
}

RingServer::~RingServer() {
  {
    std::lock_guard lock(mu_);
    stopping_ = true;
  }
  signal_eventfd(eventfd_);
  thread_.join();
  close(eventfd_);
}

void RingServer::serve_thread(unsigned entries, std::promise<void>& started) {
  try {
    Reactor reactor(entries, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
    RandomAccessFiles files(reactor, fds_);
    reactor.spawn(accept_loop(reactor, files, started));
    reactor.run();
  } catch (const std::exception& e) {
    try {
      started.set_exception(std::current_exception());
    } catch (const std::future_error&) {
      std::fprintf(stderr, "upio: RingServer thread failed: %s\n", e.what());
      std::abort();
    }
  }
}

std::size_t RingServer::read(FileId file, std::uint64_t offset, std::span<std::byte> buf) {
  Waiter& w = this_thread_waiter();
  w.state.store(kPending, std::memory_order_relaxed);
  Request req{file, offset, buf, &w};

  bool was_empty = false;
  {
    std::lock_guard lock(mu_);
    was_empty = pending_.empty();
    pending_.push_back(&req);
  }
  // A non-empty queue means an earlier caller's write is pending or about to happen, and the server
  // drains the whole queue on each wakeup, so this request rides on that wakeup.
  if (was_empty) signal_eventfd(eventfd_);

  if (options_.spin_ns > 0) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::nanoseconds(options_.spin_ns);
    for (unsigned i = 0;; ++i) {
      if (w.state.load(std::memory_order_acquire) == kDone) goto done;
      cpu_relax();
      if ((i & 63) == 63 && std::chrono::steady_clock::now() >= deadline) break;
    }
  }
  {
    std::uint32_t expected = kPending;
    if (w.state.compare_exchange_strong(expected, kSleeping, std::memory_order_acq_rel)) {
      while (w.state.load(std::memory_order_acquire) != kDone) {
        syscall(SYS_futex, futex_word(w.state), FUTEX_WAIT_PRIVATE, kSleeping, nullptr, nullptr, 0);
      }
    }
  }
done:
  if (req.error != 0) throw std::system_error(req.error, std::generic_category(), "read");
  return req.bytes;
}

Task<void> RingServer::accept_loop(Reactor& reactor, RandomAccessFiles& files, std::promise<void>& started) {
  started.set_value();
  for (;;) {
    std::uint64_t count = 0;
    std::int32_t res = co_await reactor.submit([&](io_uring_sqe* sqe) {
      io_uring_prep_read(sqe, eventfd_, &count, sizeof count, 0);
    });
    if (res != static_cast<std::int32_t>(sizeof count)) {
      throw std::system_error(res < 0 ? -res : EIO, std::generic_category(), "read eventfd");
    }
    wakeups_.fetch_add(1, std::memory_order_relaxed);

    std::vector<Request*> batch;
    bool stop = false;
    {
      std::lock_guard lock(mu_);
      batch.swap(pending_);
      stop = stopping_;
    }
    requests_.fetch_add(batch.size(), std::memory_order_relaxed);
    for (Request* req : batch) reactor.spawn(serve(reactor, files, req));
    if (stop) co_return;
  }
}

Task<void> RingServer::serve(Reactor& reactor, RandomAccessFiles& files, Request* req) {
  Waiter* w = req->waiter;
  try {
    req->bytes = co_await files.read(req->file, req->offset, req->buf);
  } catch (const std::system_error& e) {
    req->error = e.code().value();
  }
  // After this exchange the caller may return and reuse its stack, so *req is off limits.
  // The Waiter is per thread and never freed, so waking it stays safe.
  if (w->state.exchange(kDone, std::memory_order_acq_rel) != kSleeping) co_return;
  futex_wakes_.fetch_add(1, std::memory_order_relaxed);
  if (options_.wake_through_ring) {
    reactor.post([w](io_uring_sqe* sqe) {
      io_uring_prep_futex_wake(sqe, futex_word(w->state), 1, FUTEX_BITSET_MATCH_ANY, FUTEX2_SIZE_U32 | FUTEX2_PRIVATE, 0);
    });
  } else {
    syscall(SYS_futex, futex_word(w->state), FUTEX_WAKE_PRIVATE, 1, nullptr, nullptr, 0);
  }
}

}  // namespace upio::optimized
