// M1b. Many reads in flight, completions matched back by user_data.
//
// 64 reads go through a 16-entry ring, so you must handle a full SQ and reap in batches.
// The program runs the same loop twice: once plain, once with IOSQE_ASYNC.
// It passes when it prints "OK". The out-of-order counts are observations, not assertions.
// Reading list: `man io_uring_submit_and_wait`, `man io_uring_for_each_cqe`, `man io_uring_cq_advance`,
// and the IOSQE_ASYNC entry in `man io_uring_enter`.

#include <liburing.h>

#include <climits>

#include "common.h"

using namespace lab;

namespace {

constexpr std::size_t kBlocks = 64;
constexpr unsigned kDepth = 16;

// One in-flight read. Its address is the user_data, like UringTask in the Rust library.
struct Op {
  std::size_t seq = 0;
  std::size_t block = 0;
  int res = INT_MIN;
  alignas(kBlock) std::byte buf[kBlock];
};

struct Stats {
  unsigned submit_calls = 0;
  unsigned out_of_order = 0;
};

Stats run([[maybe_unused]] int file, [[maybe_unused]] bool async) {
  io_uring ring;
  if (int r = io_uring_queue_init(kDepth, &ring, 0); r < 0) die_res(r, "io_uring_queue_init");

  std::vector<Op> ops(kBlocks);
  for (std::size_t i = 0; i < ops.size(); ++i) {
    ops[i].seq = i;
    ops[i].block = (i * 37) % kBlocks;
  }

  Stats stats;
  [[maybe_unused]] std::size_t next = 0;
  std::size_t done = 0;
  std::size_t max_seq_seen = 0;
  [[maybe_unused]] auto complete = [&](Op* op, int res) {
    expect(op >= ops.data() && op < ops.data() + ops.size(), "user_data points at one of our Ops");
    expect(op->res == INT_MIN, "each Op completes exactly once");
    op->res = res;
    expect(res == static_cast<int>(kBlock), "read returned a full block");
    expect(block_ok(op->buf, op->block), "bytes match the block pattern");
    if (done > 0 && op->seq < max_seq_seen) ++stats.out_of_order;
    max_seq_seen = std::max(max_seq_seen, op->seq);
    ++done;
  };

  while (done < ops.size()) {
    // TODO A: queue reads until the SQ is full or every Op is queued.
    // io_uring_get_sqe returns nullptr when the SQ is full. That just means "submit first".
    // Prep a read of op.block into op.buf and pass &op with io_uring_sqe_set_data.
    // When `async` is set, add IOSQE_ASYNC with io_uring_sqe_set_flags. It skips the inline
    // non-blocking attempt and sends the read straight to an io-wq worker thread.
    while (next < ops.size()) {
      auto sqe = io_uring_get_sqe(&ring);
      if (sqe == nullptr) break;
      auto &op = ops[next++];
      io_uring_prep_read(sqe, file, op.buf, kBlock, op.block * kBlock);
      io_uring_sqe_set_data(sqe, &op);
      if (async) io_uring_sqe_set_flags(sqe, IOSQE_ASYNC);
    }

    // TODO B: submit everything queued and wait for at least one completion, in one syscall.
    // io_uring_submit_and_wait(&ring, 1) is M0's enter(n, 1). Count each call in stats.submit_calls.
    // Why is it safe to wait for 1 here even when nothing new was queued this iteration?
    if (int r = io_uring_submit_and_wait(&ring, 1); r < 0) die_res(r, "io_uring_submit_and_wait");
    ++stats.submit_calls;

    // TODO C: reap every completion that is ready, without another syscall.
    // io_uring_for_each_cqe(&ring, head, cqe) walks the ready CQEs without consuming them.
    // Call complete(op, cqe->res) for each, with op from io_uring_cqe_get_data.
    // Then io_uring_cq_advance(&ring, n) consumes all n with one release store,
    // where M1a's io_uring_cqe_seen did one store per CQE.
    std::uint32_t head = 0;
    io_uring_cqe* cqe = nullptr;
    unsigned n = 0;
    io_uring_for_each_cqe(&ring, head, cqe) {
      complete(static_cast<Op*>(io_uring_cqe_get_data(cqe)), cqe->res);
      ++n;
    }
    io_uring_cq_advance(&ring, n);
  }

  io_uring_queue_exit(&ring);
  return stats;
}

}  // namespace

int main() {
  int file = make_test_file(kBlocks);
  for (bool async : {false, true}) {
    Stats s = run(file, async);
    std::printf("%-6s %zu reads, %u submit calls, %u completed out of submission order\n",
                async ? "async" : "inline", kBlocks, s.submit_calls, s.out_of_order);
  }
  close(file);
  std::printf("OK\n");
  return 0;
}
