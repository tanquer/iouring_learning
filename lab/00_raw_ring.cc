// M0. An io_uring built by hand from the three syscalls, without liburing.
//
// Fill in TODO A to E in order. Each one is guarded by todo("..."), which stops
// the program and names the step. Delete the todo() line once you write the code.
// Run with `ctest --test-dir build --output-on-failure` or `./build/00_raw_ring`.
// It passes when it prints "OK".
//
// Reading list: `man 2 io_uring_setup`, `man 2 io_uring_enter`, and
// /usr/include/linux/io_uring.h for io_uring_params, io_uring_sqe and io_uring_cqe.

#include <fcntl.h>
#include <linux/io_uring.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <system_error>
#include <vector>

namespace {

[[noreturn]] void die(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

[[noreturn]] void todo(const char* step) {
  std::fprintf(stderr, "TODO %s is not implemented yet\n", step);
  std::exit(2);
}

int sys_io_uring_setup(unsigned entries, io_uring_params* p) {
  return static_cast<int>(syscall(__NR_io_uring_setup, entries, p));
}

[[maybe_unused]] int sys_io_uring_enter(int fd, unsigned to_submit, unsigned min_complete, unsigned flags) {
  return static_cast<int>(syscall(__NR_io_uring_enter, fd, to_submit, min_complete, flags, nullptr, 0));
}

// The kernel hands out byte offsets into each mapping (params.sq_off.*, params.cq_off.*).
template <typename T>
[[maybe_unused]] T* at(void* base, std::uint32_t off) {
  return reinterpret_cast<T*>(static_cast<char*>(base) + off);
}

// The kernel reads and writes head/tail concurrently with us.
// Use these for every head/tail access that crosses to the other side.
[[maybe_unused]] std::uint32_t load_acquire(const std::uint32_t* p) {
  return std::atomic_ref<const std::uint32_t>(*p).load(std::memory_order_acquire);
}

[[maybe_unused]] void store_release(std::uint32_t* p, std::uint32_t v) {
  std::atomic_ref<std::uint32_t>(*p).store(v, std::memory_order_release);
}

struct RawRing {
  int fd = -1;
  io_uring_params params{};

  void* sq_map = MAP_FAILED;
  std::size_t sq_map_len = 0;
  void* cq_map = MAP_FAILED;
  std::size_t cq_map_len = 0;
  io_uring_sqe* sqes = static_cast<io_uring_sqe*>(MAP_FAILED);
  std::size_t sqes_len = 0;

  // Submission side. We own the tail, the kernel owns the head.
  std::uint32_t* sq_head = nullptr;
  std::uint32_t* sq_tail = nullptr;
  std::uint32_t sq_mask = 0;
  std::uint32_t* sq_array = nullptr;  // ring of indices into sqes[]

  // Completion side. The kernel owns the tail, we own the head.
  std::uint32_t* cq_head = nullptr;
  std::uint32_t* cq_tail = nullptr;
  std::uint32_t cq_mask = 0;
  io_uring_cqe* cqes = nullptr;  // ring of whole entries

  explicit RawRing(unsigned entries) {
    fd = sys_io_uring_setup(entries, &params);
    if (fd < 0) die("io_uring_setup");

    // TODO A: map the three shared regions.
    //
    // 1. Compute the lengths:
    //      sq_map_len = where the SQ index array starts + sq_entries * sizeof(u32)
    //      cq_map_len = where the CQE array starts      + cq_entries * sizeof(io_uring_cqe)
    //      sqes_len   = sq_entries * sizeof(io_uring_sqe)
    // 2. mmap(nullptr, len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, OFFSET)
    //    with OFFSET = IORING_OFF_SQ_RING, IORING_OFF_CQ_RING, IORING_OFF_SQES.
    // 3. If params.features has IORING_FEAT_SINGLE_MMAP, the SQ and CQ rings share one
    //    mapping: map max(sq_map_len, cq_map_len) once and set cq_map = sq_map.
    //    The destructor already handles that case.
    // Question to answer for yourself: why are the SQEs a separate mapping from the SQ ring?
    sq_map_len = params.sq_off.array + params.sq_entries * sizeof(std::uint32_t);
    cq_map_len = params.cq_off.cqes + params.cq_entries * sizeof(io_uring_cqe);
    bool single = params.features & IORING_FEAT_SINGLE_MMAP;
    if (single) sq_map_len = cq_map_len = std::max(sq_map_len, cq_map_len);
    sq_map = mmap(nullptr, sq_map_len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQ_RING);

    if (sq_map == MAP_FAILED) {
      die("mmap sq ring");
    }
    if (single) {
      cq_map = sq_map;
    } else {
      cq_map = mmap(nullptr, cq_map_len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_CQ_RING);
      if (cq_map == MAP_FAILED) die("mmap cq ring");
    }

    sqes_len = params.sq_entries * sizeof(io_uring_sqe);
    sqes = static_cast<io_uring_sqe*>(mmap(nullptr, sqes_len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, IORING_OFF_SQES));
    if (sqes == MAP_FAILED) die("mmap sqes");
    

    // TODO B: point the members above into the mappings using at<T>(base, params.sq_off.X)
    // and params.cq_off.X. ring_mask is a value the kernel never changes, so read it once
    // into sq_mask / cq_mask. Everything else stays a pointer.
    sq_head = at<std::uint32_t>(sq_map, params.sq_off.head);
    sq_tail = at<std::uint32_t>(sq_map, params.sq_off.tail);
    sq_mask = *at<std::uint32_t>(sq_map, params.sq_off.ring_mask);
    sq_array = at<std::uint32_t>(sq_map, params.sq_off.array);

    cq_head = at<std::uint32_t>(cq_map, params.cq_off.head);
    cq_tail = at<std::uint32_t>(cq_map, params.cq_off.tail);
    cq_mask = *at<std::uint32_t>(cq_map, params.cq_off.ring_mask);
    cqes = at<io_uring_cqe>(cq_map, params.cq_off.cqes);
  }

  ~RawRing() {
    if (sqes != MAP_FAILED) munmap(sqes, sqes_len);
    if (cq_map != MAP_FAILED && cq_map != sq_map) munmap(cq_map, cq_map_len);
    if (sq_map != MAP_FAILED) munmap(sq_map, sq_map_len);
    if (fd >= 0) close(fd);
  }

  RawRing(const RawRing&) = delete;
  RawRing& operator=(const RawRing&) = delete;

  // Queue one IORING_OP_READ. Does not call into the kernel.
  void submit_read(int file_fd, void* buf, std::uint32_t len, std::uint64_t offset, std::uint64_t user_data) {
    // TODO C: produce one submission.
    //
    // 1. Read our tail. If tail - head == params.sq_entries the queue is full.
    //    Which of the two needs load_acquire, and which can be a plain read?
    // 2. idx = tail & sq_mask. Zero sqes[idx], then set opcode, fd, addr, len, off, user_data.
    // 3. sq_array[idx] = idx. This indirection is the "SQ holds indices" design from the article.
    // 4. Publish with store_release(sq_tail, tail + 1). The release orders the SQE writes
    //    before the kernel can observe the new tail.
    std::uint32_t tail = *sq_tail;
    if (tail - load_acquire(sq_head) == params.sq_entries) {
      errno = EBUSY;
      die("submission queue full");
    }

    std::uint32_t idx = tail & sq_mask;
    io_uring_sqe* sqe = &sqes[idx];
    std::memset(sqe, 0, sizeof(*sqe));
    sqe->fd = file_fd;
    sqe->opcode = IORING_OP_READ;
    sqe->addr = reinterpret_cast<std::uint64_t>(buf);
    sqe->len = len;
    sqe->off = offset;
    sqe->user_data = user_data;
    sq_array[idx] = idx;
    store_release(sq_tail, tail + 1);
  }

  // Tell the kernel about to_submit new SQEs and optionally wait for completions.
  void enter(unsigned to_submit, unsigned wait_for) {
    // TODO D: call sys_io_uring_enter. Waiting requires IORING_ENTER_GETEVENTS in flags.
    // A negative return means failure with errno set, so die("io_uring_enter").
    int ret = sys_io_uring_enter(fd, to_submit, wait_for, wait_for ? IORING_ENTER_GETEVENTS : 0);
    if (ret < 0) die("io_uring_enter");
  }

  // Pop one completion. Must not be called when the CQ is empty.
  io_uring_cqe reap_one() {
    // TODO E: consume one completion.
    //
    // 1. Read our head, load_acquire the kernel's tail. Equal means empty (errno = EAGAIN; die).
    // 2. Copy cqes[head & cq_mask] out.
    // 3. store_release(cq_head, head + 1) so the kernel may reuse that slot.
    //    Why must the copy in step 2 happen before this store?
    std::uint32_t head = *cq_head;
    if (head == load_acquire(cq_tail)) {
      errno = EAGAIN;
      die("completion queue empty");
    }
    io_uring_cqe cqe = cqes[head & cq_mask];
    store_release(cq_head, head + 1);
    return cqe;
  }
};

constexpr std::size_t kBlock = 4096;
constexpr std::size_t kBlocks = 64;

std::uint64_t pattern(std::size_t block, std::size_t word) {
  return (block * 0x9E3779B97F4A7C15ull) ^ (word * 48892056947ull);
}

int make_test_file() {
  char path[] = "/tmp/upio_m0_XXXXXX";
  int fd = mkstemp(path);
  if (fd < 0) die("mkstemp");
  unlink(path);
  std::vector<std::uint64_t> block(kBlock / sizeof(std::uint64_t));
  for (std::size_t b = 0; b < kBlocks; ++b) {
    for (std::size_t w = 0; w < block.size(); ++w) block[w] = pattern(b, w);
    if (pwrite(fd, block.data(), kBlock, b * kBlock) != static_cast<ssize_t>(kBlock)) die("pwrite");
  }
  return fd;
}

}  // namespace

int main() {
  int file = make_test_file();
  RawRing ring(8);
  std::printf("sq_entries=%u cq_entries=%u single_mmap=%s\n", ring.params.sq_entries, ring.params.cq_entries,
              (ring.params.features & IORING_FEAT_SINGLE_MMAP) ? "yes" : "no");

  alignas(kBlock) static std::byte got[kBlock];
  alignas(kBlock) static std::byte want[kBlock];
  std::size_t reads = 0;
  // 3x the block count on an 8-entry ring, so head/tail wrap many times and a missing mask shows up.
  for (std::size_t i = 0; i < 3 * kBlocks; ++i) {
    std::size_t block = (i * 37) % kBlocks;
    std::uint64_t tag = 0xC0DE0000 + i;
    ring.submit_read(file, got, kBlock, block * kBlock, tag);
    ring.enter(1, 1);
    io_uring_cqe cqe = ring.reap_one();
    if (cqe.user_data != tag) {
      std::fprintf(stderr, "FAIL user_data: got %llx want %llx\n", (unsigned long long)cqe.user_data,
                   (unsigned long long)tag);
      return 1;
    }
    if (cqe.res != static_cast<int>(kBlock)) {
      std::fprintf(stderr, "FAIL res=%d (%s)\n", cqe.res, cqe.res < 0 ? std::strerror(-cqe.res) : "short read");
      return 1;
    }
    if (pread(file, want, kBlock, block * kBlock) != static_cast<ssize_t>(kBlock)) die("pread");
    if (std::memcmp(got, want, kBlock) != 0) {
      std::fprintf(stderr, "FAIL bytes differ from pread at block %zu\n", block);
      return 1;
    }
    ++reads;
  }
  std::printf("OK %zu reads through a hand-built ring, wrapped the SQ %zu times\n", reads,
              reads / ring.params.sq_entries);
  close(file);
  return 0;
}
