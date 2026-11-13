// M4 behavior tests for ShardedPool (design A) and RingServer (design B). Run from the build directory.

#include <liburing.h>

#include <atomic>
#include <random>
#include <system_error>
#include <thread>

#include "common.h"
#include "upio/pool.h"
#include "upio/ring_server.h"

using namespace lab;

namespace {

constexpr unsigned kFiles = 4;
constexpr std::size_t kBlocks = 256;

struct alignas(kBlock) Block {
  std::byte bytes[kBlock];
};

struct Fixture {
  std::vector<int> fds;
  upio::FileId direct = 0;

  Fixture() {
    for (unsigned i = 0; i < kFiles; ++i) fds.push_back(make_test_file(kBlocks));
    direct = static_cast<upio::FileId>(fds.size());
    fds.push_back(reopen(fds[0], O_RDONLY | O_DIRECT));
  }
  ~Fixture() {
    for (int fd : fds) close(fd);
  }
};

void single_issuer_is_enforced() {
  io_uring ring;
  if (int r = io_uring_queue_init(8, &ring, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN); r < 0) {
    die_res(r, "io_uring_queue_init");
  }
  io_uring_prep_nop(io_uring_get_sqe(&ring));
  expect(io_uring_submit(&ring) == 1, "the creating thread may submit");
  int other = 0;
  std::thread([&] {
    io_uring_prep_nop(io_uring_get_sqe(&ring));
    other = io_uring_submit(&ring);
  }).join();
  expect(other == -EEXIST, "a SINGLE_ISSUER ring rejects submits from another thread with -EEXIST");
  io_uring_queue_exit(&ring);
  std::printf("kernel:  SINGLE_ISSUER ring rejected another thread's submit with EEXIST\n");
}

void pool_reads(Fixture& fx) {
  constexpr unsigned kShards = 4;
  constexpr unsigned kTasks = 32;
  constexpr unsigned kReadsPerTask = 64;
  upio::ShardedPool pool(fx.fds, kShards, 64);
  std::atomic<unsigned> verified{0};
  auto stats = pool.run(kTasks, [&](upio::RandomAccessFiles& files, unsigned shard, unsigned task) -> upio::Task<> {
    std::mt19937 rng(shard * 1000 + task);
    std::vector<std::byte> buf(kBlock);
    for (unsigned i = 0; i < kReadsPerTask; ++i) {
      upio::FileId file = rng() % kFiles;
      std::size_t block = rng() % kBlocks;
      std::size_t n = co_await files.read(file, block * kBlock, buf);
      expect(n == kBlock, "pool read returned a full block");
      expect(block_ok(buf.data(), block), "pool read returned the right bytes");
      verified.fetch_add(1, std::memory_order_relaxed);
    }
  });
  expect(verified == kShards * kTasks * kReadsPerTask, "every pool read finished and verified");
  for (const auto& s : stats) expect(s.completions == kTasks * kReadsPerTask, "each shard completed its own reads");
  std::printf("pool:    %u shards x %u tasks x %u reads verified\n", kShards, kTasks, kReadsPerTask);
}

void pool_error_propagates(Fixture& fx) {
  upio::ShardedPool pool(fx.fds, 2, 8);
  int code = 0;
  try {
    pool.run(1, [&](upio::RandomAccessFiles& files, unsigned, unsigned) -> upio::Task<> {
      std::vector<std::byte> buf(kBlock);
      co_await files.read(fx.direct, 1, buf);
    });
  } catch (const std::system_error& e) {
    code = e.code().value();
  }
  expect(code == EINVAL, "an error in a shard is rethrown by ShardedPool::run");
  std::printf("pool:    shard error rethrown to the caller as EINVAL\n");
}

void server_reads(Fixture& fx) {
  constexpr unsigned kThreads = 16;
  constexpr unsigned kReadsPerThread = 200;
  upio::RingServer server(fx.fds, 64);
  std::atomic<unsigned> verified{0};
  std::vector<std::thread> callers;
  for (unsigned t = 0; t < kThreads; ++t) {
    callers.emplace_back([&, t] {
      std::mt19937 rng(t);
      Block buf;
      for (unsigned i = 0; i < kReadsPerThread; ++i) {
        upio::FileId file = rng() % kFiles;
        std::size_t block = rng() % kBlocks;
        std::size_t n = server.read(file, block * kBlock, buf.bytes);
        expect(n == kBlock, "server read returned a full block");
        expect(block_ok(buf.bytes, block), "server read returned the right bytes");
        verified.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (std::thread& t : callers) t.join();
  expect(verified == kThreads * kReadsPerThread, "every server read finished and verified");
  expect(server.requests() == kThreads * kReadsPerThread, "the server saw every request");
  std::printf("server:  %u threads x %u reads verified, %llu requests in %llu wakeups\n", kThreads, kReadsPerThread,
              (unsigned long long)server.requests(), (unsigned long long)server.wakeups());
}

void server_error_propagates(Fixture& fx) {
  upio::RingServer server(fx.fds, 8);
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
  { upio::RingServer server(fx.fds, 8); }
  std::printf("server:  idle server shut down cleanly\n");
}

}  // namespace

int main() {
  Fixture fx;
  single_issuer_is_enforced();
  pool_reads(fx);
  pool_error_propagates(fx);
  server_reads(fx);
  server_error_propagates(fx);
  server_idle_shutdown(fx);
  std::printf("OK\n");
  return 0;
}
