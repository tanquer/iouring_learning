// M2a. Registered files and registered buffers.
//
// Registering moves per-request setup work to one io_uring_register call:
// fixed files skip the fd-table lookup and refcount per request, fixed buffers skip pinning the
// user pages per request. Both are what the Rust library and fio's io_uring engine use.
// The files live in the build directory (ext4) because /tmp is tmpfs and the buffers are used
// with O_DIRECT. It passes when it prints "OK".
// Reading list: `man io_uring_register_files`, `man io_uring_register_buffers`, `man io_uring_prep_read_fixed`.

#include <liburing.h>
#include <sys/uio.h>

#include "common.h"

using namespace lab;

namespace {

int run_one(io_uring& ring) {
  if (int r = io_uring_submit_and_wait(&ring, 1); r < 0) die_res(r, "io_uring_submit_and_wait");
  io_uring_cqe* cqe = nullptr;
  if (int r = io_uring_peek_cqe(&ring, &cqe); r < 0) die_res(r, "io_uring_peek_cqe");
  int res = cqe->res;
  io_uring_cqe_seen(&ring, cqe);
  return res;
}

io_uring_sqe* sqe_or_die(io_uring& ring) {
  io_uring_sqe* sqe = io_uring_get_sqe(&ring);
  expect(sqe != nullptr, "SQ has room");
  return sqe;
}

}  // namespace

int main() {
  constexpr unsigned kFiles = 4;
  constexpr std::size_t kBlocks = 16;
  std::vector<int> direct;
  for (unsigned i = 0; i < kFiles; ++i) {
    int fd = make_test_file(kBlocks, ".");
    direct.push_back(reopen(fd, O_RDONLY | O_DIRECT));
    close(fd);
  }

  io_uring ring;
  if (int r = io_uring_queue_init(16, &ring, 0); r < 0) die_res(r, "io_uring_queue_init");

  // TODO A: register the O_DIRECT fds as the ring's file table, then close every fd in `direct`.
  // io_uring_register_files(&ring, fds, count). The ring takes its own reference to each file,
  // so reads by index must keep working after the close. That is the point of this step.
  if (int r = io_uring_register_files(&ring, direct.data(), kFiles); r < 0) die_res(r, "io_uring_register_files");
  for (int fd : direct) close(fd);

  constexpr unsigned kBufs = 4;
  auto* arena = static_cast<std::byte*>(std::aligned_alloc(kBlock, kBufs * kBlock));
  std::vector<iovec> iovs(kBufs);
  for (unsigned i = 0; i < kBufs; ++i) iovs[i] = {arena + i * kBlock, kBlock};

  {
    std::byte* buf = arena;
    unsigned reads = 0;
    for (unsigned f = 0; f < kFiles; ++f) {
      for (std::size_t block = 0; block < kBlocks; block += 5) {
        [[maybe_unused]] io_uring_sqe* sqe = sqe_or_die(ring);
        // TODO B: prep a read of `block` from registered file index `f` into `buf`.
        // With IOSQE_FIXED_FILE set, sqe->fd is an index into the registered table, not an fd.
        // This is the Rust code's types::Fixed(id).
        io_uring_prep_read(sqe, f, buf, kBlock, block * kBlock);
        io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
        int res = run_one(ring);
        expect(res == static_cast<int>(kBlock), "fixed-file read returned a full block");
        expect(block_ok(buf, block), "fixed-file read returned the right bytes");
        ++reads;
      }
    }
    std::printf("fixed files:   %u reads by index after the fds were closed\n", reads);
  }

  // TODO C: register the kBufs buffers described by `iovs`.
  // io_uring_register_buffers(&ring, iovs, count). The kernel pins these pages once, up front.
  if (int r = io_uring_register_buffers(&ring, iovs.data(), kBufs); r < 0) die_res(r, "io_uring_register_buffers");

  {
    for (unsigned i = 0; i < kBufs; ++i) {
      [[maybe_unused]] io_uring_sqe* sqe = sqe_or_die(ring);
      // TODO D: read block i of registered file 0 into registered buffer i.
      // io_uring_prep_read_fixed(sqe, fd, buf, len, offset, buf_index). `buf` must lie inside
      // iovs[buf_index]. The file is still a registered index, so keep IOSQE_FIXED_FILE.
      io_uring_prep_read_fixed(sqe, 0, iovs[i].iov_base, kBlock, i * kBlock, i);
      io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
      int res = run_one(ring);
      expect(res == static_cast<int>(kBlock), "read_fixed returned a full block");
      expect(block_ok(static_cast<std::byte*>(iovs[i].iov_base), i), "read_fixed returned the right bytes");
    }
    std::printf("fixed buffers: %u O_DIRECT reads into registered buffers\n", kBufs);
  }

  {
    [[maybe_unused]] alignas(kBlock) static std::byte outside[kBlock];
    [[maybe_unused]] io_uring_sqe* sqe = sqe_or_die(ring);
    // TODO E: break the contract on purpose. read_fixed into `outside` (not registered) with
    // buf_index 0, from registered file 0. Predict the result before you run it.
    io_uring_prep_read_fixed(sqe, 0, outside, kBlock, 0, 0);
    io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
    int res = run_one(ring);
    expect(res == -EFAULT, "a buffer outside the registered range is rejected with -EFAULT");
    std::printf("fixed buffers: unregistered address rejected with EFAULT\n");
  }

  io_uring_queue_exit(&ring);
  std::free(arena);
  std::printf("OK\n");
  return 0;
}
