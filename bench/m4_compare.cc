// M4: the same random 4 KiB cached reads through design A (ShardedPool), design B (RingServer),
// and plain pread threads, at the same total concurrency. Build it without sanitizers:
//   cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DUPIO_SANITIZE=OFF && cmake --build build-release -j
//   cd build-release && ./m4_compare [threads] [tasks_per_thread] [reads_per_task]
// Run it from an ext4 directory. It prints throughput and latency percentiles per design.

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <random>
#include <thread>

#include "common.h"
#include "upio/pool.h"
#include "upio/ring_server.h"

using namespace lab;
using Clock = std::chrono::steady_clock;

namespace {

constexpr unsigned kFiles = 4;
constexpr std::size_t kBlocks = 4096;

struct Result {
  double seconds = 0;
  std::vector<std::uint32_t> latencies_ns;
};

std::uint32_t since(Clock::time_point t0) {
  return static_cast<std::uint32_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count());
}

void report(const char* name, Result& r, const char* extra = "") {
  auto& v = r.latencies_ns;
  std::sort(v.begin(), v.end());
  auto pct = [&](double p) { return v[std::min(v.size() - 1, static_cast<std::size_t>(p * v.size()))] / 1000.0; };
  std::printf("%-8s %9.0f reads/s   p50 %7.1f us   p99 %7.1f us   p99.9 %7.1f us   max %8.1f us  %s\n", name,
              v.size() / r.seconds, pct(0.50), pct(0.99), pct(0.999), v.back() / 1000.0, extra);
}

void merge(Result& into, std::vector<std::vector<std::uint32_t>>& parts) {
  for (auto& p : parts) into.latencies_ns.insert(into.latencies_ns.end(), p.begin(), p.end());
}

Result run_pool(std::vector<int>& fds, unsigned threads, unsigned tasks, unsigned reads) {
  upio::ShardedPool pool(fds, threads, 256);
  std::vector<std::vector<std::uint32_t>> lat(threads * tasks);
  auto t0 = Clock::now();
  pool.run(tasks, [&](upio::RandomAccessFiles& files, unsigned shard, unsigned task) -> upio::Task<> {
    std::mt19937 rng(shard * 7919 + task);
    std::vector<std::byte> buf(kBlock);
    auto& mine = lat[shard * tasks + task];
    mine.reserve(reads);
    for (unsigned i = 0; i < reads; ++i) {
      auto t = Clock::now();
      co_await files.read(rng() % kFiles, (rng() % kBlocks) * kBlock, buf);
      mine.push_back(since(t));
    }
  });
  Result r;
  r.seconds = std::chrono::duration<double>(Clock::now() - t0).count();
  merge(r, lat);
  return r;
}

template <typename ReadFn>
Result run_threads(unsigned callers, unsigned reads, ReadFn read) {
  std::vector<std::vector<std::uint32_t>> lat(callers);
  std::vector<std::thread> ts;
  auto t0 = Clock::now();
  for (unsigned c = 0; c < callers; ++c) {
    ts.emplace_back([&, c] {
      std::mt19937 rng(c * 7919);
      std::vector<std::byte> buf(kBlock);
      lat[c].reserve(reads);
      for (unsigned i = 0; i < reads; ++i) {
        auto t = Clock::now();
        read(rng() % kFiles, (rng() % kBlocks) * kBlock, std::span<std::byte>(buf));
        lat[c].push_back(since(t));
      }
    });
  }
  for (auto& t : ts) t.join();
  Result r;
  r.seconds = std::chrono::duration<double>(Clock::now() - t0).count();
  merge(r, lat);
  return r;
}

}  // namespace

int main(int argc, char** argv) {
  unsigned threads = argc > 1 ? std::atoi(argv[1]) : 4;
  unsigned tasks = argc > 2 ? std::atoi(argv[2]) : 8;
  unsigned reads = argc > 3 ? std::atoi(argv[3]) : 20000;
  std::vector<int> fds;
  for (unsigned i = 0; i < kFiles; ++i) fds.push_back(make_test_file(kBlocks));
  unsigned concurrency = threads * tasks;
  std::printf("%u files x %zu MiB (page cache), concurrency %u, %u reads per worker\n", kFiles,
              kBlocks * kBlock >> 20, concurrency, reads);

  Result pool = run_pool(fds, threads, tasks, reads);
  report("pool", pool, "(design A: threads x coroutines, one ring each)");

  std::uint64_t wakeups = 0, requests = 0;
  Result server;
  {
    upio::RingServer srv(fds, 256);
    server = run_threads(concurrency, reads, [&](upio::FileId f, std::uint64_t off, std::span<std::byte> b) {
      srv.read(f, off, b);
    });
    wakeups = srv.wakeups();
    requests = srv.requests();
  }
  char extra[128];
  std::snprintf(extra, sizeof extra, "(design B: %u blocking callers, %.1f requests per wakeup)", concurrency,
                double(requests) / double(wakeups));
  report("server", server, extra);

  Result pread_r = run_threads(concurrency, reads, [&](upio::FileId f, std::uint64_t off, std::span<std::byte> b) {
    if (::pread(fds[f], b.data(), b.size(), off) != static_cast<ssize_t>(b.size())) die("pread");
  });
  report("pread", pread_r, "(baseline: blocking pread threads)");

  for (int fd : fds) close(fd);
  return 0;
}
