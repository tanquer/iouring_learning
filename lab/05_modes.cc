// M2b. Where requests actually run: inline, io-wq workers, the SQPOLL thread, and IOPOLL.
//
// Each mode is visible as kernel threads inside this process (/proc/self/task/*/comm):
// io-wq workers are named iou-wrk-<pid>, the SQPOLL thread iou-sqp-<pid>.
// /tmp is tmpfs and the build directory is ext4; the async mode compares them.
// Run one mode at a time to count its syscalls:
//   strace -f -c -e trace=io_uring_enter ./build/05_modes sqpoll
// Modes: async, sqpoll, iopoll. No argument runs all three. It passes when it prints "OK".
// Reading list: IORING_SETUP_SQPOLL and IORING_SETUP_IOPOLL in `man io_uring_setup`,
// IOSQE_ASYNC in `man io_uring_enter`.

#include <cerrno>
#include <dirent.h>
#include <liburing.h>

#include <chrono>
#include <fstream>
#include <string>
#include <thread>

#include "common.h"

using namespace lab;

namespace {

constexpr unsigned kReads = 16;

unsigned count_threads(const std::string& prefix) {
  unsigned n = 0;
  DIR* dir = opendir("/proc/self/task");
  if (dir == nullptr) die("opendir /proc/self/task");
  while (dirent* e = readdir(dir)) {
    if (e->d_name[0] == '.') continue;
    std::ifstream comm(std::string("/proc/self/task/") + e->d_name + "/comm");
    std::string name;
    std::getline(comm, name);
    if (name.rfind(prefix, 0) == 0) ++n;
  }
  closedir(dir);
  return n;
}

struct Reads {
  alignas(kBlock) std::byte bufs[kReads][kBlock];
};

void check_reads(const Reads& r) {
  for (unsigned i = 0; i < kReads; ++i) expect(block_ok(r.bufs[i], i), "read returned the right bytes");
}

// Queue kReads reads of blocks 0..kReads-1, each with `sqe_flags`, and submit them.
[[maybe_unused]] void queue_reads(io_uring& ring, int fd, Reads& r, unsigned sqe_flags) {
  for (unsigned i = 0; i < kReads; ++i) {
    io_uring_sqe* sqe = io_uring_get_sqe(&ring);
    expect(sqe != nullptr, "SQ has room for every read");
    io_uring_prep_read(sqe, fd, r.bufs[i], kBlock, i * kBlock);
    io_uring_sqe_set_flags(sqe, sqe_flags);
  }
  int n = io_uring_submit(&ring);
  if (n < 0) die_res(n, "io_uring_submit");
  expect(n == static_cast<int>(kReads), "every read was submitted");
}

[[maybe_unused]] void expect_full_block(int res) {
  if (res != static_cast<int>(kBlock)) {
    std::fprintf(stderr, "res=%d (%s)\n", res, res < 0 ? std::strerror(-res) : "short read");
    expect(false, "read returned a full block");
  }
}

void mode_async(int ext4, int tmpfs) {
  struct Case {
    const char* name;
    int fd;
    unsigned flags;
  };
  for (Case c : {Case{"ext4", ext4, 0u}, Case{"ext4+ASYNC", ext4, unsigned(IOSQE_ASYNC)}, Case{"tmpfs", tmpfs, 0u}}) {
    io_uring ring;
    if (int r = io_uring_queue_init(32, &ring, 0); r < 0) die_res(r, "io_uring_queue_init");
    static Reads r;
    queue_reads(ring, c.fd, r, c.flags);
    unsigned ready_at_submit = io_uring_cq_ready(&ring);
    // TODO A: wait for all kReads completions and consume them, checking each res.
    // io_uring_wait_cqe_nr(&ring, &cqe, kReads) waits for kReads in one syscall. Then walk
    // them with io_uring_for_each_cqe, call expect_full_block(cqe->res) on each, and
    // io_uring_cq_advance by the count, as in M1's 02_batch.
    io_uring_cqe* cqe = nullptr;
    if (int e = io_uring_wait_cqe_nr(&ring, &cqe, kReads); e < 0) die_res(e, "io_uring_wait_cqe_nr");
    std::uint32_t head = 0;
    unsigned n = 0;
    io_uring_for_each_cqe(&ring, head, cqe) {
      expect_full_block(cqe->res);
      ++n;
    }
    io_uring_cq_advance(&ring, n);

    check_reads(r);
    unsigned workers = count_threads("iou-wrk-");
    std::printf("async:  %-10s %2u/%u done when submit returned, io-wq workers alive: %u\n", c.name, ready_at_submit,
                kReads, workers);
    if (c.fd == ext4 && c.flags == 0) {
      expect(ready_at_submit == kReads, "cached ext4 reads completed inline during submit");
      expect(workers == 0, "inline completion started no io-wq worker");
    } else if (c.flags & IOSQE_ASYNC) {
      expect(workers > 0, "IOSQE_ASYNC reads ran on io-wq workers");
    }
    io_uring_queue_exit(&ring);
  }
}

void mode_sqpoll(int fd) {
  constexpr unsigned kIdleMs = 50;
  io_uring ring;
  [[maybe_unused]] io_uring_params p{};
  // TODO B: create a 32-entry ring with IORING_SETUP_SQPOLL whose thread sleeps after kIdleMs.
  // Set p.flags and p.sq_thread_idle, then io_uring_queue_init_params. Unprivileged since 5.11.
  p.flags = IORING_SETUP_SQPOLL;
  p.sq_thread_idle = kIdleMs;
  if (int r = io_uring_queue_init_params(32, &ring, &p); r < 0) die_res(r, "io_uring_queue_init_params");
  expect(count_threads("iou-sqp-") == 1, "SQPOLL started one iou-sqp kernel thread");

  static Reads r;
  queue_reads(ring, fd, r, 0);
  unsigned spins = 0;
  // TODO C: reap all kReads completions without any syscall.
  // io_uring_peek_cqe returns -EAGAIN when the CQ is empty instead of entering the kernel.
  // Spin on it (count iterations in `spins`), check and consume each CQE with io_uring_cqe_seen.
  // queue_reads' io_uring_submit made no syscall either: the iou-sqp thread is watching the SQ tail.
  for (unsigned got = 0; got < kReads;) {
    io_uring_cqe* cqe = nullptr;
    if (io_uring_peek_cqe(&ring, &cqe) == 0) {
      expect_full_block(cqe->res);
      io_uring_cqe_seen(&ring, cqe);
      ++got;
    } else {
      ++spins;
    }
  }
  check_reads(r);
  std::printf("sqpoll: %u reads reaped by spinning (%u empty peeks)\n", kReads, spins);

  std::this_thread::sleep_for(std::chrono::milliseconds(4 * kIdleMs));
  bool asleep = false;
  // TODO D: after the idle period the thread sleeps and raises IORING_SQ_NEED_WAKEUP in the
  // SQ ring's flags word. Read it with IO_URING_READ_ONCE(*ring.sq.kflags) into `asleep`.
  // io_uring_submit checks the same bit and only then calls io_uring_enter(IORING_ENTER_SQ_WAKEUP).
  asleep = IO_URING_READ_ONCE(*ring.sq.kflags) & IORING_SQ_NEED_WAKEUP;
  expect(asleep, "SQPOLL thread went to sleep and asked for a wakeup");
  queue_reads(ring, fd, r, 0);
  io_uring_cqe* cqe = nullptr;
  if (int e = io_uring_wait_cqe_nr(&ring, &cqe, kReads); e < 0) die_res(e, "io_uring_wait_cqe_nr");
  io_uring_cq_advance(&ring, kReads);
  check_reads(r);
  std::printf("sqpoll: thread slept after %u ms idle, submit woke it\n", kIdleMs);
  io_uring_queue_exit(&ring);
}

void mode_iopoll(int fd) {
  int direct = reopen(fd, O_RDONLY | O_DIRECT);
  [[maybe_unused]] io_uring ring;
  int res = 0;
  alignas(kBlock) static std::byte buf[kBlock];
  // TODO E: create an 8-entry ring with IORING_SETUP_IOPOLL, read block 3 of `direct` into buf,
  // and store the CQE's res in `res`.
  // IOPOLL requires O_DIRECT and a device with poll queues. Completions don't arrive by interrupt:
  // waiting makes the kernel poll the device queue. Check /sys/block/nvme0n1/queue/io_poll first
  // and predict res. Wait with io_uring_wait_cqe, not peek: on an IOPOLL ring nothing reaches the
  // CQ until someone polls, and wait_cqe keeps re-entering the kernel to poll. strace it.
  if (int r = io_uring_queue_init(8, &ring, IORING_SETUP_SQPOLL); r < 0) die_res(r, "io_uring_queue_init_params");
  io_uring_sqe* sqe = io_uring_get_sqe(&ring);
  io_uring_prep_read(sqe, direct, buf, kBlock, 3 * kBlock);
  io_uring_cqe* cqe = nullptr;
  if (int e = io_uring_submit(&ring); e < 0) die_res(e, "io_uring_submit");
  if (int e = io_uring_wait_cqe(&ring, &cqe); e < 0) die_res(e, "io_uring_wait_cqe");
  res = cqe->res;
  io_uring_cqe_seen(&ring, cqe);
  io_uring_queue_exit(&ring);
  if (res == static_cast<int>(kBlock)) {
    expect(block_ok(buf, 3), "polled read returned the right bytes");
    std::printf("iopoll: device supports polling, read completed by polling\n");
  } else {
    expect(res == -EOPNOTSUPP, "a device without poll queues rejects IOPOLL reads with -EOPNOTSUPP");
    std::printf("iopoll: device has no poll queues, read failed with EOPNOTSUPP\n");
  }
  close(direct);
}

}  // namespace

int main(int argc, char** argv) {
  std::string only = argc > 1 ? argv[1] : "";
  int ext4 = make_test_file(kReads);
  int tmpfs = make_test_file(kReads, "/tmp");
  if (only.empty() || only == "async") mode_async(ext4, tmpfs);
  if (only.empty() || only == "sqpoll") mode_sqpoll(ext4);
  if (only.empty() || only == "iopoll") mode_iopoll(ext4);
  close(ext4);
  close(tmpfs);
  std::printf("OK\n");
  return 0;
}
