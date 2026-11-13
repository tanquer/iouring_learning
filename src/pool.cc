// M4, design A. Fill in TODO A. Run `./m4_test` from the build directory.

#include "upio/pool.h"

#include <cstdio>
#include <cstdlib>
#include <exception>
#include <thread>

namespace upio {

namespace {

[[maybe_unused]] [[noreturn]] void todo(const char* step) {
  std::fprintf(stderr, "TODO %s is not implemented yet\n", step);
  std::exit(2);
}

}  // namespace

ShardedPool::ShardedPool(std::span<const int> fds, unsigned shards, unsigned entries)
    : fds_(fds.begin(), fds.end()), shards_(shards), entries_(entries) {}

std::vector<ReactorStats> ShardedPool::run([[maybe_unused]] unsigned tasks_per_shard, [[maybe_unused]] const Body& body) {
  std::vector<ReactorStats> stats(shards_);
  std::vector<std::exception_ptr> errors(shards_);
  std::vector<std::thread> threads;
  for (unsigned s = 0; s < shards_; ++s) {
    threads.emplace_back([&, s] {
      try {
        // TODO A: build this shard's ring on this thread, spawn its tasks, and run them.
        // Create a Reactor with IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN here, inside
        // the thread. SINGLE_ISSUER binds the ring to the thread that creates it: a submit from any
        // other thread fails with -EEXIST. DEFER_TASKRUN holds completion work until this thread
        // enters the kernel to wait, which the run loop's submit_and_wait already does.
        // Then a RandomAccessFiles over fds_ (every ring registers its own table), spawn
        // body(files, s, t) for each t < tasks_per_shard, run(), and store reactor.stats() in stats[s].
        Reactor reactor(entries_, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
        RandomAccessFiles files(reactor, fds_);
        for (unsigned t = 0; t < tasks_per_shard; ++t) reactor.spawn(body(files, s, t));
        reactor.run();
        stats[s] = reactor.stats();
      } catch (...) {
        errors[s] = std::current_exception();
      }
    });
  }
  for (std::thread& t : threads) t.join();
  for (std::exception_ptr& e : errors) {
    if (e) std::rethrow_exception(e);
  }
  return stats;
}

}  // namespace upio
