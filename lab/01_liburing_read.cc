// M1a. The M0 loop again, through liburing.
//
// Same rules as M0: fill in each TODO and delete its todo() line. It passes when it prints "OK".
// Reading list: `man io_uring_queue_init`, `man io_uring_prep_read`, `man io_uring_wait_cqe`,
// and /usr/include/liburing.h, where most of these calls are short inline functions.

#include <cstddef>
#include <liburing.h>

#include "common.h"

using namespace lab;

int main() {
  constexpr std::size_t kBlocks = 64;
  int file = make_test_file(kBlocks);

  io_uring ring;
  io_uring_params params{};
  // TODO A: create the ring with 8 entries.
  // io_uring_queue_init_params(entries, &ring, &params) does io_uring_setup plus all of
  // your M0 TODO A and B. Like every liburing call it returns -errno, so use die_res.
  auto ret = io_uring_queue_init_params(8, &ring, &params);
  if (ret < 0) {
    die_res(ret, "io_uring_queue_init_params");
  }
  std::printf("sq_entries=%u cq_entries=%u features=0x%x\n", params.sq_entries, params.cq_entries, params.features);

  alignas(kBlock) static std::byte buf[kBlock];
  for (std::size_t i = 0; i < 3 * kBlocks; ++i) {
    std::size_t block = (i * 37) % kBlocks;
    std::uint64_t tag = 0xC0DE0000 + i;

    // TODO B: queue one read of `block` into `buf` tagged with `tag`, then submit it.
    // io_uring_get_sqe returns nullptr when the SQ is full (it can't be here; one op in flight).
    // io_uring_prep_read fills opcode/fd/addr/len/off. io_uring_sqe_set_data64 sets user_data.
    // io_uring_submit returns how many SQEs the kernel consumed, or -errno.
    // Which of your M0 steps (C and D) does each call replace? Where did sq_array[idx] = idx go?
    auto sqe = io_uring_get_sqe(&ring);
    expect(sqe != nullptr, "SQ has room for one SQE");
    io_uring_prep_read(sqe, file, buf, kBlock, block * kBlock);
    io_uring_sqe_set_data64(sqe, tag);
    int submitted = io_uring_submit(&ring);
    if (submitted < 0) die_res(submitted, "io_uring_submit");
    expect(submitted == 1, "kernel consumed exactly one SQE");

    int res = 0;
    std::uint64_t got_tag = 0;
    // TODO C: wait for the completion, copy res and user_data into the variables above, then
    // mark the CQE consumed.
    // io_uring_wait_cqe blocks until a CQE is ready and returns a pointer into the CQ ring.
    // io_uring_cqe_get_data64 reads user_data. io_uring_cqe_seen is your M0 store_release on cq_head,
    // so copy everything out before calling it.
    io_uring_cqe* cqe = nullptr;
    if (int r = io_uring_wait_cqe(&ring, &cqe); r < 0) die_res(r, "io_uring_wait_cqe");
    got_tag = io_uring_cqe_get_data64(cqe);
    res = cqe->res;
    io_uring_cqe_seen(&ring, cqe);

    expect(got_tag == tag, "user_data comes back unchanged");
    expect(res == static_cast<int>(kBlock), "read returned a full block");
    expect(block_ok(buf, block), "bytes match the block pattern");
  }

  io_uring_queue_exit(&ring);
  close(file);
  std::printf("OK %zu reads through liburing\n", 3 * kBlocks);
  return 0;
}
