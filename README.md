# iouring_learning

A C++20 rebuild of skyzh's [`uring-positioned-io`](https://github.com/skyzh/uring-positioned-io),
done as a sequence of small, self-checking milestones that walk through io_uring from the raw
syscalls up to a coroutine library and a benchmark.

The goal is the Rust library's one-call API:

```cpp
std::size_t n = co_await files.read(file_id, offset, buf);
```

and a benchmark that answers the blog's question with measurements: when does io_uring beat
`pread` and `mmap` for random 4 KiB reads, and why.

## Quick start

Requirements: a recent Linux kernel and liburing (the optimized RingServer uses `IORING_OP_FUTEX_WAKE`, which
needs kernel 6.7 or newer), a C++20 compiler, and CMake 3.20+. Developed with kernel 6.12, liburing 2.9 and g++ 14.2.

```
cmake -S . -B build                     # Debug, ASan + UBSan
cmake --build build -j
ctest --test-dir build --output-on-failure
```

Benchmarks need a Release build without sanitizers:

```
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release -DUPIO_SANITIZE=OFF
cmake --build build-release -j
cd build-release
./gen                                                   # m5_data/: 4 files x 16 MiB
./bench --engine uring --threads 4 --depth 8 --duration 10
./bench --engine pread --threads 32 --duration 10
./nop
./m4_compare 4 8 20000
```

Run tests and benchmarks from a directory on a real filesystem (the build directories are fine).
They create their data files in the current directory. On a machine where `/tmp` is tmpfs, reads
from there behave differently: they skip io_uring's inline fast path and go to io-wq workers.

## Layout

```
lab/                        one program per concept; each is a ctest that exits 0 when its checks pass
  common.h                  test files, block pattern, expect/die helpers
  00_raw_ring.cc            no liburing: io_uring_setup, mmap the three regions, io_uring_enter
  01_liburing_read.cc       get_sqe / prep_read / submit / wait_cqe, user_data round trip
  02_batch.cc               64 reads through a 16-entry ring, batched reaping, inline vs IOSQE_ASYNC
  03_link_drain.cc          IOSQE_IO_LINK (write then read), broken chains, IOSQE_IO_DRAIN as a barrier
  04_registered.cc          registered files (IOSQE_FIXED_FILE), registered buffers (read_fixed)
  05_modes.cc               inline vs io-wq, SQPOLL and NEED_WAKEUP, IOPOLL
include/upio/, src/         the library (namespace upio)
  task.h                    lazy Task<T> coroutine with symmetric transfer
  reactor.h                 one io_uring on one thread: submit/await, batched submit_and_wait, reap
  files.h                   RandomAccessFiles: registered file table, read(), flush()
  pool.h                    ShardedPool: one SINGLE_ISSUER | DEFER_TASKRUN ring per thread
  ring_server.h             RingServer: one ring shared by blocking callers through an eventfd
include/optimized/,         upio::optimized: RingServer whose callers sleep on a futex word that the
  src/optimized/            server wakes with IORING_OP_FUTEX_WAKE; Reactor gains post()
tests/                      m3_test, m4_test, m4_optimized_test
bench/
  gen.cc, dataset.h         dataset generator and helpers (cache residency, cache drop)
  bench.cc                  random reads via pread | mmap | uring (raw liburing) | upio, fixed duration
  nop.cc                    IORING_OP_NOP cost: raw liburing vs the upio coroutine layer
  m4_compare.cc             ShardedPool vs RingServer vs pread threads
  m4_compare_optimized.cc   RingServer vs optimized RingServer vs pread threads
```

## Milestones

| | What it builds | What it teaches |
|---|---|---|
| M0 | A ring from the three syscalls, no liburing | SQ ring of indices vs CQ ring of entries, offsets from `io_uring_params`, acquire/release on head and tail |
| M1 | liburing: one read, a batch, links and drain | `user_data` as a context pointer, batching, `IOSQE_IO_LINK` (a failed head cancels the chain with `-ECANCELED`), `IOSQE_IO_DRAIN` (waits for everything submitted earlier, ring-wide) |
| M2 | Registered files and buffers; execution modes | Fixed files survive `close()`; `read_fixed` outside the registered range is `-EFAULT`; where a request runs (inline, io-wq, SQPOLL thread); IOPOLL needs device poll queues |
| M3 | `Reactor` + `Task` + `RandomAccessFiles` | The Rust `UringContext` as a single-threaded coroutine reactor; one `submit_and_wait` per loop is where batching comes from; `-EINTR` is not an error |
| M4 | `ShardedPool` and `RingServer` | Thread-per-ring vs one shared ring; `SINGLE_ISSUER` (another thread's submit gets `-EEXIST`); waking a ring from other threads with an eventfd |
| M5 | `gen`, `bench`, `nop` | Measuring which regime a number came from: cached vs device, CPU per read, reads per syscall |

## Design

Every in-flight request is an `Op` inside the awaiting coroutine's frame, and the SQE's
`user_data` is its address, the same trick as the Rust `UringTask`:

```cpp
struct Op {
  std::coroutine_handle<> waiter;
  std::int32_t res;        // cqe->res: bytes, or -errno
};
```

The coroutine stays suspended until its CQE arrives, so the frame (and the caller's buffer) outlive
the kernel's use of them. Destroying a `Reactor` with requests in flight aborts instead of letting
the kernel write into freed frames. This is the Rust README's "the read future must be polled to
completion" rule, enforced.

Two deliberate departures from the Rust code:

- The Rust `UringPollFuture` re-wakes itself whenever the CQ is empty, so it spins a core. The
  reactor sleeps in `io_uring_submit_and_wait` instead. It knows at least one request is in flight,
  so a completion is guaranteed to arrive.
- The Rust code shares one ring across several poll futures. A liburing `io_uring` is not
  thread-safe, so here exactly one thread owns each ring.

## Results

Measured on a 16-vCPU cloud VM (kernel 6.12, liburing 2.9, 30 GiB RAM, 8 GiB EBS volume) in
Release builds. Treat them as one machine's numbers, not general truths.

**Cached random 4 KiB reads** (64 MiB dataset, fully in page cache, 4 s runs):

| Engine | reads/s | p50 | CPU per read |
|---|---|---|---|
| pread, 1 thread | 885k | 1.1 µs | 1.13 µs |
| uring, 1 thread × depth 32 | 1.17M | 27 µs | 0.86 µs |
| pread, 4 threads | 2.69M | 1.4 µs | 1.49 µs |
| uring, 4 × 8 | 3.67M | 8.6 µs | 1.09 µs |
| upio (ShardedPool), 4 × 8 | 3.52M | 8.9 µs | 1.14 µs |
| uring + SQPOLL, 4 × 8 | 2.43M | 12.9 µs | 2.02 µs |
| pread, 32 threads | 5.30M | 2.8 µs | 3.01 µs |
| mmap, 32 threads | 24.1M | 0.5 µs | 0.66 µs |

**O_DIRECT** reads flatten at about 3,000 reads/s for every engine and every depth. That is
consistent with the volume's provisioned IOPS (inferred, not checked against the volume's
settings), and latency follows Little's law: depth 32 / 3,000 per second ≈ 10.7 ms, measured
p50 10.9 ms.

**Cost of the coroutine layer** (`nop`, 2M NOPs per row): raw liburing 58.5 ns per NOP at batch 128,
upio 68.6 ns. The layer adds about 10 ns per request at every batch size.

**M4, 32 concurrent callers, cached reads:**

| Design | reads/s | p50 |
|---|---|---|
| ShardedPool, 16 threads × 2 | 5.9M | 5 µs |
| ShardedPool, 4 threads × 8 | 3.0–3.3M | 9 µs |
| RingServer (promise/future hand-off) | 225–237k | 136 µs |
| optimized RingServer (futex word, wake via ring) | 325–333k | 96 µs |

What the numbers say:

- On cached data a read is a memcpy from page cache. `mmap` wins because it needs no syscall at
  all, which matches the blog's result. io_uring does beat `pread` on CPU per read, because one
  `io_uring_enter` carries a whole batch.
- io_uring's real advantage is many reads in flight per thread on data that is not cached. This
  machine can't show it: its disk caps every engine at the same IOPS.
- SQPOLL slowed cached reads down. The SQPOLL kernel thread appears to execute each read inline
  while it submits, so the copying serializes on that thread.
- RingServer is limited by handing results between threads, not by io_uring. With 8 or more
  callers its server thread is 100% busy at about 4.4 µs per read. Cutting eventfd writes 16×
  changed throughput by 1%. Futex wake syscalls were about half of its time, and moving the wake
  into the ring (`IORING_OP_FUTEX_WAKE`) gave +40%. Thread-per-ring avoids the hand-off entirely.
- Reads from tmpfs are punted to io-wq instead of completing inline (0 of 16 done when submit
  returned, against 16 of 16 on ext4).

## This machine's limits

| Fact | Consequence |
|---|---|
| `/sys/block/nvme0n1/queue/io_poll` = 0 | IOPOLL reads fail with `-EOPNOTSUPP` (`lab/05_modes`) |
| 30 GiB RAM, small disk | The blog's dataset-larger-than-RAM setup can't be reproduced; `--direct` is the only way to reach the device |
| EBS volume, about 3,000 IOPS | All `--direct` rows measure the volume, not the engine |
| `/tmp` is tmpfs | Run from the build directory, not `/tmp` |

`bench --drop-cache` evicts the dataset with `posix_fadvise(DONTNEED)`, but it is back in cache
within seconds, so buffered "cold" rows mix disk and cache. Check the `cached` column on every row.

## Not covered

Writes beyond the link demo, networking ops, cancellation (`IORING_OP_ASYNC_CANCEL`), multishot
requests and provided-buffer rings, and a device fast enough to compare engines on uncached reads.
