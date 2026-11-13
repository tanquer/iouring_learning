// M4, design B. Fill in TODO B (server side), C (caller side), D (serving one request), in the order
// the test reaches them.

#include "upio/ring_server.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <system_error>

namespace upio {

namespace {

[[maybe_unused]] [[noreturn]] void todo(const char* step) {
  std::fprintf(stderr, "TODO %s is not implemented yet\n", step);
  std::exit(2);
}

void signal_eventfd(int fd) {
  std::uint64_t one = 1;
  if (::write(fd, &one, sizeof one) != static_cast<ssize_t>(sizeof one)) {
    throw std::system_error(errno, std::generic_category(), "write eventfd");
  }
}

}  // namespace

RingServer::RingServer(std::span<const int> fds, unsigned entries) : fds_(fds.begin(), fds.end()) {
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
  Request req{file, offset, buf, {}};
  std::future<std::size_t> result = req.done.get_future();
  // TODO C: hand `req` to the server thread and wake it.
  // Push &req onto pending_ while holding mu_, then call signal_eventfd(eventfd_) after unlocking.
  // Why must the push happen before the eventfd write and not after?
  {
    std::lock_guard lock(mu_);
    pending_.push_back(&req);
  }
  signal_eventfd(eventfd_);
  return result.get();
}

Task<void> RingServer::accept_loop(Reactor& reactor, RandomAccessFiles& files, std::promise<void>& started) {
  started.set_value();
  for (;;) {
    [[maybe_unused]] std::uint64_t count = 0;
    // TODO B: sleep on the eventfd through the ring.
    // co_await reactor.submit(...) with an 8-byte io_uring_prep_read of eventfd_ into `count`.
    // It is a plain fd, not in the registered table, so no IOSQE_FIXED_FILE. Anything but 8 bytes
    // back is an error: throw std::system_error.
    // The counter adds up every write since the last read, so a caller that writes before this read
    // is queued still wakes it. An eventfd with a zero counter can't complete inline, so the kernel
    // arms a poll on it and finishes the read when it becomes readable; no io-wq thread blocks.
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
    for (Request* req : batch) reactor.spawn(serve(files, req));
    if (stop) co_return;
  }
}

Task<void> RingServer::serve([[maybe_unused]] RandomAccessFiles& files, [[maybe_unused]] Request* req) {
  // TODO D: do the read on this ring and hand the result to the caller's thread.
  // First copy file, offset and buf into locals and move req->done into a local promise. The
  // caller's Request lives on the caller's stack, and the caller returns as soon as the promise
  // is fulfilled, so nothing may touch *req after that. Owning the promise here makes it safe.
  // Then co_await files.read(...) and set_value(n), or set_exception(std::current_exception()) if it threw.
  FileId file = req->file;
  std::uint64_t offset = req->offset;
  std::span<std::byte> buf = req->buf;
  std::promise<std::size_t> done = std::move(req->done);
  try {
    done.set_value(co_await files.read(file, offset, buf));
  } catch (...) {
    done.set_exception(std::current_exception());
  }
}

}  // namespace upio
