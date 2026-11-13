// The M4 RingServer (upio) against the optimized one (upio::optimized) and pread threads, on the
// same random 4 KiB cached reads at the same concurrency. Build it in the Release tree and run it
// from an ext4 directory:
//   cd build-release && ./m4_compare_optimized [callers] [reads_per_caller]
// SPIN_NS=<ns> and WAKE=ring|syscall set the optimized server's options.

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>

#include "common.h"
#include "optimized/ring_server.h"
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

void report(const char* name, Result& r, const std::string& extra) {
  auto& v = r.latencies_ns;
  std::sort(v.begin(), v.end());
  auto pct = [&](double p) { return v[std::min(v.size() - 1, static_cast<std::size_t>(p * v.size()))] / 1000.0; };
  std::printf("%-10s %9.0f reads/s   p50 %7.1f us   p99 %7.1f us   p99.9 %7.1f us   max %8.1f us  %s\n", name,
              v.size() / r.seconds, pct(0.50), pct(0.99), pct(0.999), v.back() / 1000.0, extra.c_str());
}

template <typename ReadFn>
Result run_callers(unsigned callers, unsigned reads, ReadFn read) {
  std::vector<std::vector<std::uint32_t>> lat(callers);
  std::vector<std::thread> ts;
  auto t0 = Clock::now();
  for (unsigned c = 0; c < callers; ++c) {
    ts.emplace_back([&, c] {
      std::uint32_t x = c * 7919 + 1;
      std::vector<std::byte> buf(kBlock);
      lat[c].reserve(reads);
      for (unsigned i = 0; i < reads; ++i) {
        x = x * 1103515245 + 12345;
        auto t = Clock::now();
        read(x % kFiles, ((x >> 8) % kBlocks) * kBlock, std::span<std::byte>(buf));
        lat[c].push_back(static_cast<std::uint32_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t).count()));
      }
    });
  }
  for (auto& t : ts) t.join();
  Result r;
  r.seconds = std::chrono::duration<double>(Clock::now() - t0).count();
  for (auto& p : lat) r.latencies_ns.insert(r.latencies_ns.end(), p.begin(), p.end());
  return r;
}

}  // namespace

int main(int argc, char** argv) {
  unsigned callers = argc > 1 ? std::atoi(argv[1]) : 32;
  unsigned reads = argc > 2 ? std::atoi(argv[2]) : 20000;
  std::vector<int> fds;
  for (unsigned i = 0; i < kFiles; ++i) fds.push_back(make_test_file(kBlocks));
  std::printf("%u files x %zu MiB (page cache), %u blocking callers, %u reads each\n", kFiles, kBlocks * kBlock >> 20,
              callers, reads);

  {
    upio::RingServer srv(fds, 256);
    Result r = run_callers(callers, reads, [&](std::uint32_t f, std::uint64_t off, std::span<std::byte> b) {
      srv.read(f, off, b);
    });
    report("m4", r, "(upio::RingServer)");
  }

  upio::optimized::RingServerOptions opts;
  if (const char* v = std::getenv("SPIN_NS")) opts.spin_ns = std::atoi(v);
  if (const char* v = std::getenv("WAKE")) opts.wake_through_ring = std::string(v) == "ring";
  {
    upio::optimized::RingServer srv(fds, 256, opts);
    Result r = run_callers(callers, reads, [&](std::uint32_t f, std::uint64_t off, std::span<std::byte> b) {
      srv.read(f, off, b);
    });
    char extra[160];
    std::snprintf(extra, sizeof extra, "(upio::optimized::RingServer: spin %u ns, wake via %s, %.2f futex wakes/read)",
                  opts.spin_ns, opts.wake_through_ring ? "ring" : "syscall",
                  double(srv.futex_wakes()) / double(srv.requests()));
    report("optimized", r, extra);
  }

  Result r = run_callers(callers, reads, [&](std::uint32_t f, std::uint64_t off, std::span<std::byte> b) {
    if (::pread(fds[f], b.data(), b.size(), off) != static_cast<ssize_t>(b.size())) die("pread");
  });
  report("pread", r, "(blocking pread threads)");
  for (int fd : fds) close(fd);
  return 0;
}
