// M5. Random 4 KiB reads over the gen dataset through one engine, for a fixed duration.
//
//   bench --engine pread|mmap|uring|upio [--threads 4] [--depth 32] [--duration 10] [--dir m5_data]
//         [--direct] [--sqpoll] [--fixed-buffers] [--verify] [--drop-cache]
//   bench --smoke     (ctest: tiny dataset, every engine and mode, verified, a fraction of a second each)
//
// pread and mmap run `threads` blocking workers. uring and upio run `threads` rings with `depth`
// reads in flight on each. `uring` is a raw liburing loop (TODO A and B below); `upio` is your
// ShardedPool. Comparing them shows what the coroutine layer costs.
// Run from an ext4 directory and use the Release build for numbers.

#include <liburing.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/uio.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>

#include "dataset.h"
#include "upio/pool.h"

using namespace lab;
using Clock = std::chrono::steady_clock;

namespace {

struct Config {
  std::string engine = "uring";
  std::string dir = "m5_data";
  unsigned threads = 4;
  unsigned depth = 32;
  double duration = 10;
  bool direct = false;
  bool sqpoll = false;
  bool fixed_buffers = false;
  bool verify = false;
  bool drop_cache = false;
};

// What one worker measured. Latency is submit-to-completion for one 4 KiB read, in ns.
struct WorkerStats {
  std::vector<std::uint32_t> latency_ns;
  std::uint64_t errors = 0;
  std::uint64_t enter_calls = 0;
};

struct Xorshift {
  std::uint64_t s;
  std::uint64_t next() {
    s ^= s << 13;
    s ^= s >> 7;
    s ^= s << 17;
    return s;
  }
};

// One random block of the dataset.
struct Pick {
  unsigned file;
  std::size_t block;
  std::uint64_t global;
};

Pick pick(const m5::Dataset& ds, Xorshift& rng) {
  std::uint64_t r = rng.next();
  unsigned file = static_cast<unsigned>(r % ds.nf);
  std::size_t block = (r >> 20) % ds.nb;
  return {file, block, ds.global_block(file, block)};
}

std::uint32_t elapsed_ns(Clock::time_point t0) {
  return static_cast<std::uint32_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - t0).count());
}

struct AlignedBuffers {
  std::byte* base;
  explicit AlignedBuffers(std::size_t n) : base(static_cast<std::byte*>(std::aligned_alloc(kBlock, n * kBlock))) {}
  ~AlignedBuffers() { std::free(base); }
  std::byte* operator[](std::size_t i) const { return base + i * kBlock; }
};

void check(WorkerStats& w, const Config& cfg, int res, const std::byte* buf, std::uint64_t global) {
  if (res != static_cast<int>(kBlock) || (cfg.verify && !block_ok(buf, global))) ++w.errors;
}

template <typename Fn>
std::vector<WorkerStats> run_threads(unsigned n, Fn fn) {
  std::vector<WorkerStats> out(n);
  std::vector<std::thread> ts;
  for (unsigned t = 0; t < n; ++t) ts.emplace_back([&, t] { fn(t, out[t]); });
  for (auto& t : ts) t.join();
  return out;
}

std::vector<WorkerStats> run_pread(const Config& cfg, const m5::Dataset& ds, const std::atomic<bool>& stop) {
  return run_threads(cfg.threads, [&](unsigned t, WorkerStats& w) {
    Xorshift rng{0x9E3779B97F4A7C15ull * (t + 1)};
    AlignedBuffers buf(1);
    while (!stop.load(std::memory_order_relaxed)) {
      Pick p = pick(ds, rng);
      auto t0 = Clock::now();
      int res = static_cast<int>(::pread(ds.fds[p.file], buf[0], kBlock, p.block * kBlock));
      w.latency_ns.push_back(elapsed_ns(t0));
      check(w, cfg, res, buf[0], p.global);
    }
  });
}

std::vector<WorkerStats> run_mmap(const Config& cfg, const m5::Dataset& ds, const std::atomic<bool>& stop) {
  std::vector<const std::byte*> maps;
  for (int fd : ds.fds) {
    void* p = mmap(nullptr, ds.nb * kBlock, PROT_READ, MAP_SHARED, fd, 0);
    if (p == MAP_FAILED) die("mmap");
    madvise(p, ds.nb * kBlock, MADV_RANDOM);
    maps.push_back(static_cast<const std::byte*>(p));
  }
  auto out = run_threads(cfg.threads, [&](unsigned t, WorkerStats& w) {
    Xorshift rng{0x9E3779B97F4A7C15ull * (t + 1)};
    AlignedBuffers buf(1);
    while (!stop.load(std::memory_order_relaxed)) {
      Pick p = pick(ds, rng);
      auto t0 = Clock::now();
      std::memcpy(buf[0], maps[p.file] + p.block * kBlock, kBlock);
      w.latency_ns.push_back(elapsed_ns(t0));
      check(w, cfg, static_cast<int>(kBlock), buf[0], p.global);
    }
  });
  for (const std::byte* m : maps) munmap(const_cast<std::byte*>(m), ds.nb * kBlock);
  return out;
}

void uring_worker(const Config& cfg, const m5::Dataset& ds, [[maybe_unused]] const std::atomic<bool>& stop, unsigned t,
                  [[maybe_unused]] WorkerStats& w) {
  struct Slot {
    Pick pick;
    Clock::time_point start;
  };
  const unsigned depth = cfg.depth;
  AlignedBuffers bufs(depth);
  std::vector<Slot> slots(depth);
  Xorshift rng{0x9E3779B97F4A7C15ull * (t + 1)};
  io_uring ring;

  // TODO A: set up this thread's ring with `depth` entries.
  // - flags: IORING_SETUP_SINGLE_ISSUER always. With cfg.sqpoll add IORING_SETUP_SQPOLL and set
  //   params.sq_thread_idle (1000 ms is fine); without it add IORING_SETUP_DEFER_TASKRUN.
  //   The kernel rejects DEFER_TASKRUN together with SQPOLL. Why do they conflict?
  // - register ds.fds with io_uring_register_files.
  // - with cfg.fixed_buffers, register `depth` iovecs {bufs[i], kBlock} with io_uring_register_buffers.
  // Use io_uring_queue_init_params and die_res on any negative return.
  // (The queue() helper below already copes with SQPOLL's SQ filling up; read its comment.)
  io_uring_params params{};
  params.flags = IORING_SETUP_SINGLE_ISSUER;
  if (cfg.sqpoll) {
    params.flags |= IORING_SETUP_SQPOLL;
    params.sq_thread_idle = 1000;
  } else {
    params.flags |= IORING_SETUP_DEFER_TASKRUN;
  }
  if (int r = io_uring_queue_init_params(depth, &ring, &params); r < 0) die_res(r, "io_uring_queue_init_params");
  if (int r = io_uring_register_files(&ring, ds.fds.data(), ds.nf); r < 0) die_res(r, "io_uring_register_files");
  if (cfg.fixed_buffers) {
    std::vector<iovec> iovs(depth);
    for (unsigned i = 0; i < depth; ++i) iovs[i] = {bufs[i], kBlock};
    if (int r = io_uring_register_buffers(&ring, iovs.data(), depth); r < 0) die_res(r, "io_uring_register_buffers");
  }

  // Queue a read of a fresh random block into slot `i`. user_data is the slot index.
  // With SQPOLL the kernel thread consumes SQEs on its own schedule, so the SQ can still be full when a
  // slot is re-queued: wait for it to make room. Without SQPOLL, submit_and_wait already emptied it.
  [[maybe_unused]] auto queue = [&](unsigned i) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    while (sqe == nullptr) {
      io_uring_submit(&ring);
      io_uring_sqring_wait(&ring);
      sqe = io_uring_get_sqe(&ring);
    }
    Slot& s = slots[i];
    s.pick = pick(ds, rng);
    if (cfg.fixed_buffers) {
      io_uring_prep_read_fixed(sqe, s.pick.file, bufs[i], kBlock, s.pick.block * kBlock, i);
    } else {
      io_uring_prep_read(sqe, s.pick.file, bufs[i], kBlock, s.pick.block * kBlock);
    }
    io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
    io_uring_sqe_set_data64(sqe, i);
    s.start = Clock::now();
  };

  // TODO B: keep `depth` reads in flight until `stop`, then drain.
  // Queue every slot once. Then loop: io_uring_submit_and_wait(&ring, 1) (count it in w.enter_calls,
  // retry on -EINTR), walk the ready CQEs with io_uring_for_each_cqe, and for each one take the slot
  // from user_data, push elapsed_ns(slot.start) onto w.latency_ns, call
  // check(w, cfg, cqe->res, bufs[slot], slot.pick.global), and re-queue that slot unless stopping.
  // io_uring_cq_advance once per batch. Stop when `stop` is set and nothing is left in flight.
  // This is the same shape as fio's io_uring engine at iodepth=depth.
  for (unsigned i = 0; i < depth; ++i) queue(i);
  unsigned in_flight = depth;
  while (in_flight > 0) {
    int r = io_uring_submit_and_wait(&ring, 1);
    if (r == -EINTR) continue;
    if (r < 0) die_res(r, "io_uring_submit_and_wait");
    ++w.enter_calls;
    bool stopping = stop.load(std::memory_order_relaxed);
    unsigned head;
    io_uring_cqe* cqe;
    unsigned n = 0;
    io_uring_for_each_cqe(&ring, head, cqe) {
      auto i = static_cast<unsigned>(io_uring_cqe_get_data64(cqe));
      w.latency_ns.push_back(elapsed_ns(slots[i].start));
      check(w, cfg, cqe->res, bufs[i], slots[i].pick.global);
      if (stopping) {
        --in_flight;
      } else {
        queue(i);
      }
      ++n;
    }
    io_uring_cq_advance(&ring, n);
  }

  io_uring_queue_exit(&ring);
}

std::vector<WorkerStats> run_uring(const Config& cfg, const m5::Dataset& ds, const std::atomic<bool>& stop) {
  return run_threads(cfg.threads, [&](unsigned t, WorkerStats& w) { uring_worker(cfg, ds, stop, t, w); });
}

std::vector<WorkerStats> run_upio(const Config& cfg, const m5::Dataset& ds, const std::atomic<bool>& stop) {
  std::vector<WorkerStats> per_task(cfg.threads * cfg.depth);
  AlignedBuffers bufs(cfg.threads * cfg.depth);
  upio::ShardedPool pool(ds.fds, cfg.threads, cfg.depth);
  auto shard_stats = pool.run(cfg.depth, [&](upio::RandomAccessFiles& files, unsigned shard,
                                             unsigned task) -> upio::Task<> {
    unsigned id = shard * cfg.depth + task;
    WorkerStats& w = per_task[id];
    std::span<std::byte> buf(bufs[id], kBlock);
    Xorshift rng{0x9E3779B97F4A7C15ull * (id + 1)};
    while (!stop.load(std::memory_order_relaxed)) {
      Pick p = pick(ds, rng);
      auto t0 = Clock::now();
      int res = 0;
      try {
        res = static_cast<int>(co_await files.read(p.file, p.block * kBlock, buf));
      } catch (const std::system_error& e) {
        res = -e.code().value();
      }
      w.latency_ns.push_back(elapsed_ns(t0));
      check(w, cfg, res, buf.data(), p.global);
    }
  });
  for (unsigned s = 0; s < cfg.threads; ++s) per_task[s * cfg.depth].enter_calls = shard_stats[s].enter_calls;
  return per_task;
}

struct EngineDef {
  const char* name;
  bool async;  // depth applies
  bool direct;
  bool sqpoll;
  bool fixed_buffers;
  std::vector<WorkerStats> (*run)(const Config&, const m5::Dataset&, const std::atomic<bool>&);
};

constexpr EngineDef kEngines[] = {
    {"pread", false, true, false, false, run_pread},
    {"mmap", false, false, false, false, run_mmap},
    {"uring", true, true, true, true, run_uring},
    {"upio", true, true, false, false, run_upio},
};

const EngineDef& find_engine(const Config& cfg) {
  for (const EngineDef& e : kEngines) {
    if (cfg.engine != e.name) continue;
    expect(!cfg.direct || e.direct, "this engine does not support --direct");
    expect(!cfg.sqpoll || e.sqpoll, "this engine does not support --sqpoll");
    expect(!cfg.fixed_buffers || e.fixed_buffers, "this engine does not support --fixed-buffers");
    return e;
  }
  std::fprintf(stderr, "unknown engine %s (pread, mmap, uring, upio)\n", cfg.engine.c_str());
  std::exit(2);
}

std::string label(const Config& cfg, const EngineDef& e) {
  std::string s = cfg.engine;
  if (cfg.direct) s += "+direct";
  if (cfg.sqpoll) s += "+sqpoll";
  if (cfg.fixed_buffers) s += "+fixed";
  s += " t=" + std::to_string(cfg.threads);
  if (e.async) s += " qd=" + std::to_string(cfg.depth);
  return s;
}

double cpu_seconds() {
  rusage ru{};
  getrusage(RUSAGE_SELF, &ru);
  return ru.ru_utime.tv_sec + ru.ru_stime.tv_sec + (ru.ru_utime.tv_usec + ru.ru_stime.tv_usec) / 1e6;
}

struct Summary {
  std::uint64_t ops = 0;
  std::uint64_t errors = 0;
};

Summary run_one(const Config& cfg, const m5::Dataset& ds) {
  const EngineDef& e = find_engine(cfg);
  if (cfg.drop_cache) m5::drop_cache(ds);
  double cached_before = m5::cached_fraction(ds);
  std::atomic<bool> stop{false};
  std::thread timer([&] {
    std::this_thread::sleep_for(std::chrono::duration<double>(cfg.duration));
    stop.store(true);
  });
  double cpu0 = cpu_seconds();
  auto t0 = Clock::now();
  std::vector<WorkerStats> workers = e.run(cfg, ds, stop);
  double wall = std::chrono::duration<double>(Clock::now() - t0).count();
  double cpu = cpu_seconds() - cpu0;
  timer.join();
  double cached_after = m5::cached_fraction(ds);

  std::vector<std::uint32_t> lat;
  Summary sum;
  std::uint64_t enters = 0;
  for (auto& w : workers) {
    lat.insert(lat.end(), w.latency_ns.begin(), w.latency_ns.end());
    sum.errors += w.errors;
    enters += w.enter_calls;
  }
  sum.ops = lat.size();
  if (lat.empty()) {
    std::printf("%-30s no reads completed\n", label(cfg, e).c_str());
    return sum;
  }
  std::sort(lat.begin(), lat.end());
  auto us = [&](double p) { return lat[std::min(lat.size() - 1, static_cast<std::size_t>(p * lat.size()))] / 1000.0; };
  std::printf("%-30s %9.0f ops/s %7.1f MiB/s | us p50 %7.1f p90 %7.1f p99 %7.1f p99.9 %8.1f p99.99 %8.1f max %8.1f"
              " | cpu %4.1f cores %5.2f us/op",
              label(cfg, e).c_str(), sum.ops / wall, sum.ops * double(kBlock) / wall / (1 << 20), us(0.50), us(0.90),
              us(0.99), us(0.999), us(0.9999), lat.back() / 1000.0, cpu / wall, cpu / sum.ops * 1e6);
  if (enters) std::printf(" | %.1f reads/enter", double(sum.ops) / double(enters));
  std::printf(" | cached %3.0f%%->%3.0f%%", 100 * cached_before, 100 * cached_after);
  if (sum.errors) std::printf(" | %llu ERRORS", (unsigned long long)sum.errors);
  std::printf("\n");
  return sum;
}

int smoke() {
  const std::string dir = "m5_smoke_data";
  m5::generate(dir, 2, 256);
  struct Mode {
    const char* engine;
    bool direct, sqpoll, fixed;
  };
  const Mode modes[] = {
      {"pread", false, false, false}, {"pread", true, false, false}, {"mmap", false, false, false},
      {"uring", false, false, false}, {"uring", true, false, false}, {"uring", true, false, true},
      {"uring", false, true, false},  {"upio", false, false, false}, {"upio", true, false, false},
  };
  for (const Mode& m : modes) {
    Config cfg;
    cfg.engine = m.engine;
    cfg.dir = dir;
    cfg.threads = 2;
    cfg.depth = 8;
    cfg.duration = 0.2;
    cfg.direct = m.direct;
    cfg.sqpoll = m.sqpoll;
    cfg.fixed_buffers = m.fixed;
    cfg.verify = true;
    m5::Dataset ds;
    m5::open_dataset(ds, dir, cfg.direct);
    Summary s = run_one(cfg, ds);
    expect(s.ops > 0, "every engine completed reads");
    expect(s.errors == 0, "every read returned a full, correct block");
  }
  std::filesystem::remove_all(dir);
  std::printf("OK\n");
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto value = [&]() -> std::string {
      expect(i + 1 < argc, "option needs a value");
      return argv[++i];
    };
    if (a == "--smoke") return smoke();
    else if (a == "--engine") cfg.engine = value();
    else if (a == "--dir") cfg.dir = value();
    else if (a == "--threads") cfg.threads = std::stoul(value());
    else if (a == "--depth") cfg.depth = std::stoul(value());
    else if (a == "--duration") cfg.duration = std::stod(value());
    else if (a == "--direct") cfg.direct = true;
    else if (a == "--sqpoll") cfg.sqpoll = true;
    else if (a == "--fixed-buffers") cfg.fixed_buffers = true;
    else if (a == "--verify") cfg.verify = true;
    else if (a == "--drop-cache") cfg.drop_cache = true;
    else {
      std::fprintf(stderr, "unknown option %s\n", a.c_str());
      return 2;
    }
  }
  m5::Dataset ds;
  m5::open_dataset(ds, cfg.dir, cfg.direct);
  run_one(cfg, ds);
  return 0;
}
