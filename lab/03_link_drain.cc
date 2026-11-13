// M1c. Ordering between requests: IOSQE_IO_LINK and IOSQE_IO_DRAIN.
//
// Three scenarios, each a TODO. The checks after each one say what must hold.
// Reading list: IOSQE_IO_LINK and IOSQE_IO_DRAIN in `man io_uring_enter`, `man io_uring_prep_nop`.

#include <liburing.h>

#include "common.h"

using namespace lab;

namespace {

struct Done {
  std::uint64_t tag;
  int res;
};

std::vector<Done> reap(io_uring& ring, unsigned n) {
  std::vector<Done> out;
  while (out.size() < n) {
    io_uring_cqe* cqe = nullptr;
    if (int r = io_uring_wait_cqe(&ring, &cqe); r < 0) die_res(r, "io_uring_wait_cqe");
    out.push_back({io_uring_cqe_get_data64(cqe), cqe->res});
    io_uring_cqe_seen(&ring, cqe);
  }
  return out;
}

[[maybe_unused]] void submit_all(io_uring& ring, int expected) {
  int r = io_uring_submit(&ring);
  if (r < 0) die_res(r, "io_uring_submit");
  expect(r == expected, "kernel consumed every queued SQE");
}

[[maybe_unused]] io_uring_sqe* sqe_or_die(io_uring& ring) {
  io_uring_sqe* sqe = io_uring_get_sqe(&ring);
  expect(sqe != nullptr, "SQ has room");
  return sqe;
}

}  // namespace

int main() {
  constexpr std::size_t kBlocks = 16;
  int file = make_test_file(kBlocks);
  io_uring ring;
  if (int r = io_uring_queue_init(16, &ring, 0); r < 0) die_res(r, "io_uring_queue_init");

  {
    [[maybe_unused]] constexpr std::size_t kTarget = 5;
    alignas(kBlock) static std::byte wbuf[kBlock];
    alignas(kBlock) static std::byte rbuf[kBlock];
    std::memset(wbuf, 0xAB, kBlock);
    // TODO A: write wbuf to block kTarget, then read the same block into rbuf, as one linked chain.
    // Tag the write 1 and the read 2. Put IOSQE_IO_LINK on the write only: a chain ends at the
    // first SQE without the flag. Submit both in one call with submit_all(ring, 2).
    // Without the link, both run concurrently and the read may see the old bytes.
    auto w_sqe = sqe_or_die(ring);
    io_uring_prep_write(w_sqe, file, wbuf, kBlock, kTarget * kBlock);
    io_uring_sqe_set_data64(w_sqe, 1);
    io_uring_sqe_set_flags(w_sqe, IOSQE_IO_LINK);

    auto r_sqe = sqe_or_die(ring);
    io_uring_prep_read(r_sqe, file, rbuf, kBlock, kTarget * kBlock);
    io_uring_sqe_set_data64(r_sqe, 2);
    
    submit_all(ring, 2);

    auto done = reap(ring, 2);
    expect(done[0].tag == 1 && done[1].tag == 2, "linked write completes before the read");
    expect(done[0].res == static_cast<int>(kBlock) && done[1].res == static_cast<int>(kBlock), "both moved a full block");
    expect(std::memcmp(wbuf, rbuf, kBlock) == 0, "read sees the linked write's bytes");
    std::printf("link:  write -> read ordered, read saw the new data\n");
  }

  {
    [[maybe_unused]] alignas(kBlock) static std::byte buf[kBlock];
    // TODO B: a chain whose first request fails.
    // Read from fd -1 (tag 1, IOSQE_IO_LINK), then a linked NOP (tag 2, io_uring_prep_nop).
    // Submit both. Predict both res values before you run it.
    auto bad = sqe_or_die(ring);
    io_uring_prep_read(bad, -1, buf, kBlock, 0);
    io_uring_sqe_set_data64(bad, 1);
    io_uring_sqe_set_flags(bad, IOSQE_IO_LINK);

    auto nop = sqe_or_die(ring);
    io_uring_prep_nop(nop);
    io_uring_sqe_set_data64(nop, 2);
    submit_all(ring, 2);

    auto done = reap(ring, 2);
    expect(done[0].tag == 1 && done[0].res == -EBADF, "failing head of the chain reports -EBADF");
    expect(done[1].tag == 2 && done[1].res == -ECANCELED, "rest of a broken chain is cancelled");
    std::printf("link:  head failed with EBADF, follower got ECANCELED\n");
  }

  {
    constexpr unsigned kReads = 8;
    constexpr std::uint64_t kFlushTag = 999;
    alignas(kBlock) static std::byte bufs[kReads][kBlock];
    // TODO C: a flush barrier, the Rust library's flush().
    // Queue kReads reads (tags 0..kReads-1, block kReads + i into bufs[i], clear of block 5 from A) with IOSQE_ASYNC so they run on
    // io-wq workers in parallel. Then a NOP tagged kFlushTag with IOSQE_IO_DRAIN.
    // Submit all kReads + 1 in one call.
    // Experiment afterwards: drop IOSQE_IO_DRAIN and see where the NOP lands.
    for (unsigned i = 0; i < kReads; ++i) {
      auto sqe = sqe_or_die(ring);
      io_uring_prep_read(sqe, file, bufs[i], kBlock, (kReads + i) * kBlock);
      io_uring_sqe_set_data64(sqe, i);
      io_uring_sqe_set_flags(sqe, IOSQE_ASYNC);
    }
    auto flush = sqe_or_die(ring);
    io_uring_prep_nop(flush);
    io_uring_sqe_set_data64(flush, kFlushTag);
    io_uring_sqe_set_flags(flush, IOSQE_IO_DRAIN);
    submit_all(ring, kReads + 1);
    auto done = reap(ring, kReads + 1);
    expect(done.back().tag == kFlushTag, "drained NOP completes after every earlier request");
    for (unsigned i = 0; i < kReads; ++i) expect(block_ok(bufs[i], kReads + i), "drained reads returned the right blocks");
    std::printf("drain: NOP completed after all %u reads\n", kReads);
  }

  io_uring_queue_exit(&ring);
  close(file);
  std::printf("OK\n");
  return 0;
}
