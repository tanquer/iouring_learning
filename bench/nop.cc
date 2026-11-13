// M5: what one request costs when the I/O itself costs nothing. Every request is IORING_OP_NOP.
//
//   nop [--ops 2000000]
//
// raw     : a liburing loop that submits `batch` NOPs per io_uring_enter and reaps them (TODO C).
// upio    : your Reactor, with `tasks` coroutines each awaiting reactor.submit(nop) in a loop.
// The gap between the two at the same batch size is the coroutine layer's cost. This is the
// experiment the Rust post planned ("test nop in Rust and C") but never ran.

#include <liburing.h>

#include <chrono>
#include <cstring>
#include <string>

#include "common.h"
#include "upio/reactor.h"

using namespace lab;
using Clock = std::chrono::steady_clock;

namespace {

struct Cost {
  double ns_per_op;
  double ops_per_enter;
};

Cost raw_nops([[maybe_unused]] std::uint64_t ops, unsigned batch) {
  io_uring ring;
  if (int r = io_uring_queue_init(batch, &ring, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN); r < 0) {
    die_res(r, "io_uring_queue_init");
  }
  std::uint64_t done = 0;
  std::uint64_t enters = 0;
  auto t0 = Clock::now();
  // TODO C: run `ops` NOPs, `batch` at a time.
  // Each round: get_sqe + io_uring_prep_nop `batch` times, one io_uring_submit_and_wait(&ring, batch)
  // (count it in `enters`), then consume all `batch` CQEs with io_uring_for_each_cqe +
  // io_uring_cq_advance, checking every res is 0. Add `batch` to `done` until done >= ops.
  while (done < ops) {
    for (unsigned i = 0; i < batch; ++i) io_uring_prep_nop(io_uring_get_sqe(&ring));
    if (int r = io_uring_submit_and_wait(&ring, batch); r < 0) die_res(r, "io_uring_submit_and_wait");
    ++enters;
    unsigned head;
    io_uring_cqe* cqe;
    unsigned n = 0;
    io_uring_for_each_cqe(&ring, head, cqe) {
      expect(cqe->res == 0, "a NOP completes with res 0");
      ++n;
    }
    expect(n == batch, "every NOP in the batch completed");
    io_uring_cq_advance(&ring, n);
    done += batch;
  }
  double secs = std::chrono::duration<double>(Clock::now() - t0).count();
  io_uring_queue_exit(&ring);
  return {secs * 1e9 / double(done), double(done) / double(enters)};
}

Cost upio_nops(std::uint64_t ops, unsigned tasks) {
  upio::Reactor reactor(tasks, IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_DEFER_TASKRUN);
  std::uint64_t per_task = ops / tasks;
  for (unsigned t = 0; t < tasks; ++t) {
    reactor.spawn([](upio::Reactor& r, std::uint64_t n) -> upio::Task<> {
      for (std::uint64_t i = 0; i < n; ++i) {
        std::int32_t res = co_await r.submit([](io_uring_sqe* sqe) { io_uring_prep_nop(sqe); });
        expect(res == 0, "a NOP completes with res 0");
      }
    }(reactor, per_task));
  }
  auto t0 = Clock::now();
  reactor.run();
  double secs = std::chrono::duration<double>(Clock::now() - t0).count();
  std::uint64_t done = per_task * tasks;
  expect(reactor.stats().completions == done, "the reactor completed every NOP");
  return {secs * 1e9 / double(done), double(done) / double(reactor.stats().enter_calls)};
}

}  // namespace

int main(int argc, char** argv) {
  std::uint64_t ops = 2000000;
  for (int i = 1; i + 1 < argc; i += 2) {
    if (!std::strcmp(argv[i], "--ops")) ops = std::stoull(argv[i + 1]);
  }
  std::printf("%llu NOPs per row\n", (unsigned long long)ops);
  for (unsigned batch : {1u, 8u, 32u, 128u}) {
    Cost raw = raw_nops(ops, batch);
    Cost co = upio_nops(ops, batch);
    std::printf("batch %3u   raw %7.1f ns/op (%5.1f ops/enter)   upio %7.1f ns/op (%5.1f ops/enter)   upio overhead %+6.1f ns/op\n",
                batch, raw.ns_per_op, raw.ops_per_enter, co.ns_per_op, co.ops_per_enter, co.ns_per_op - raw.ns_per_op);
  }
  std::printf("OK\n");
  return 0;
}
