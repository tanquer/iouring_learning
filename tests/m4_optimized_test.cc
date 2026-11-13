// Behavior tests for upio::optimized::RingServer. Run from the build directory (ext4).

#include <atomic>
#include <random>
#include <system_error>
#include <thread>

#include "common.h"
#include "optimized/ring_server.h"

using namespace lab;
using upio::optimized::RingServer;
using upio::optimized::RingServerOptions;

namespace {

constexpr unsigned kFiles = 4;
constexpr std::size_t kBlocks = 256;

struct alignas(kBlock) Block {
  std::byte bytes[kBlock];
};

struct Fixture {
  std::vector<int> fds;
  upio::optimized::FileId direct = 0;

  Fixture() {
    for (unsigned i = 0; i < kFiles; ++i) fds.push_back(make_test_file(kBlocks));
    direct = static_cast<upio::optimized::FileId>(fds.size());
    fds.push_back(reopen(fds[0], O_RDONLY | O_DIRECT));
  }
  ~Fixture() {
    for (int fd : fds) close(fd);
  }
};

void server_reads(Fixture& fx, RingServerOptions opts) {
  constexpr unsigned kThreads = 16;
  constexpr unsigned kReadsPerThread = 200;
  RingServer server(fx.fds, 64, opts);
  std::atomic<unsigned> verified{0};
  std::vector<std::thread> callers;
  for (unsigned t = 0; t < kThreads; ++t) {
    callers.emplace_back([&, t] {
      std::mt19937 rng(t);
      Block buf;
      for (unsigned i = 0; i < kReadsPerThread; ++i) {
        std::size_t block = rng() % kBlocks;
        std::size_t n = server.read(rng() % kFiles, block * kBlock, buf.bytes);
        expect(n == kBlock, "server read returned a full block");
        expect(block_ok(buf.bytes, block), "server read returned the right bytes");
        verified.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (std::thread& t : callers) t.join();
  expect(verified == kThreads * kReadsPerThread, "every server read finished and verified");
  expect(server.requests() == kThreads * kReadsPerThread, "the server saw every request");
  expect(server.futex_wakes() <= server.requests(), "at most one futex wake per request");
  std::printf("server:  spin %5u ns, wake via %-7s %u threads x %u reads verified, %llu wakeups, %llu futex wakes\n",
              opts.spin_ns, opts.wake_through_ring ? "ring," : "syscall,", kThreads, kReadsPerThread,
              (unsigned long long)server.wakeups(), (unsigned long long)server.futex_wakes());
}

void server_error_propagates(Fixture& fx) {
  RingServer server(fx.fds, 8);
  Block buf;
  int code = 0;
  std::thread([&] {
    try {
      server.read(fx.direct, 1, buf.bytes);
    } catch (const std::system_error& e) {
      code = e.code().value();
    }
  }).join();
  expect(code == EINVAL, "a failed read is rethrown in the calling thread");
  std::printf("server:  failed read rethrown in the caller's thread as EINVAL\n");
}

void server_idle_shutdown(Fixture& fx) {
  { RingServer server(fx.fds, 8); }
  std::printf("server:  idle server shut down cleanly\n");
}

}  // namespace

int main() {
  Fixture fx;
  server_reads(fx, {.spin_ns = 0, .wake_through_ring = false});
  server_reads(fx, {.spin_ns = 0, .wake_through_ring = true});
  server_reads(fx, {.spin_ns = 20000, .wake_through_ring = false});
  server_reads(fx, {.spin_ns = 20000, .wake_through_ring = true});
  server_error_propagates(fx);
  server_idle_shutdown(fx);
  std::printf("OK\n");
  return 0;
}
