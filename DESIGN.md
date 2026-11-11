# upio: positioned random reads on io_uring, in C++

A learning project that rebuilds skyzh's `uring-positioned-io` in C++20 and
works through the kernel concepts from "io_uring 的接口与实现" along the way.

Sources:
- https://www.skyzh.dev/blog/2021-06-14-deep-dive-io-uring/ (ring layout, SQE/CQE, io-wq, IOPOLL, SQPOLL, links)
- https://www.skyzh.dev/blog/2021-01-30-async-random-read-with-rust/ (the library and its benchmark)
- https://github.com/skyzh/uring-positioned-io (`src/context.rs`, `uring_bench/`)

## Goal

One call, the same shape as the Rust library:

```cpp
std::size_t n = co_await files.read(file_id, offset, buf);
```

Plus a benchmark that answers the blog's question honestly: when does io_uring
beat `pread` and `mmap` for random 4 KiB reads, and why.

## This machine (measured 2026-11-10)

| Fact | Value | Consequence |
|---|---|---|
| Kernel | 6.12 | Every feature here is available. SQPOLL works unprivileged (since 5.11). |
| liburing | 2.9 (`liburing-dev` installed) | Use it from milestone 1 on. |
| Compiler | g++ 14.2, cmake present | C++20 coroutines are fine. |
| RAM | 30 GiB | The blog's dataset (128 GiB on a 32 GiB box) cannot be reproduced. Anything we generate fits in page cache. |
| Free disk | 1.1 GiB on an 8 GiB EBS volume | Dataset is capped at about 512 MiB. |
| `/sys/block/nvme0n1/queue/io_poll` | 0, `poll_queues` = 0 | IOPOLL will fail with `-EOPNOTSUPP`. That failure is itself a lab exercise. |
| `ulimit -l` | 8 MiB | Enough for registered buffers in the benchmark. Kernel ≥5.12 charges rings to memcg, not memlock. |

So "cold" reads come from `O_DIRECT`, not from a dataset larger than RAM.

## Data shape

Everything hangs off one in-flight record, and `user_data` is a pointer to it.
This mirrors the Rust `UringTask`, whose address goes into `user_data` and is
recovered on completion.

```cpp
using FileId = std::uint32_t;            // index into the registered file table

struct ReadReq {
  FileId file;
  std::uint64_t offset;
  std::span<std::byte> buf;              // caller-owned, must outlive the op
};

struct Op {                              // lives in the awaiting coroutine's frame
  ReadReq req;
  std::coroutine_handle<> waiter;
  std::int32_t res;                      // cqe->res: bytes read or -errno
};
```

Ownership rule: the `Op` lives inside the awaiter, inside the coroutine frame.
The coroutine stays suspended until the CQE arrives, so the frame and the
buffer outlive the kernel's use of them. This is the same contract the Rust
README states ("the read future must be polled to completion"). In C++ the
failure mode is destroying a suspended coroutine. The reactor refuses to shut
down while `in_flight > 0`.

## Layout

```
iouring/
  CMakeLists.txt
  lab/                      one small program per concept, each self-checking
    common.h                test file, block pattern, expect/die/todo helpers shared by the labs
    00_raw_ring.cc          no liburing: io_uring_setup, mmap the 3 regions, io_uring_enter
    01_liburing_read.cc     get_sqe / prep_read / submit / wait_cqe, user_data round trip
    02_batch.cc             64 reads through a 16-entry ring, batched reaping, inline vs IOSQE_ASYNC
    03_link_drain.cc        IOSQE_IO_LINK (write then read), IOSQE_IO_DRAIN nop as a barrier
    04_registered.cc        register_files + IOSQE_FIXED_FILE, register_buffers + read_fixed
    05_modes.cc             inline vs io-wq (ext4, IOSQE_ASYNC, tmpfs), SQPOLL + NEED_WAKEUP, IOPOLL (-EOPNOTSUPP here)
  include/upio/
    task.h                  minimal Task<T> coroutine type
    reactor.h               owns one io_uring, runs the submit/reap loop
    files.h                 RandomAccessFiles: open + register, read(), flush()
  src/                      reactor.cc, files.cc
  bench/
    gen.cc                  write N files of 4 KiB blocks, each filled from hash(file, block)
    bench.cc                one binary, --engine=pread|mmap|uring, prints ops/s and p50/p90/p99.9/p99.99/max
    nop.cc                  NOP round trips: measures ring and wrapper overhead alone
  tests/                    ctest: read correctness, short read at EOF, error propagation, flush ordering
```

## Milestones

Each milestone ends in a check that runs. Do not start the next one until the
check passes.

**M0. Raw ring (lab/00).** Implement the ring by hand from the deep-dive
article. Call `io_uring_setup`, mmap the SQ ring, CQ ring, and SQE array using
`io_uring_params.sq_off/cq_off`. Write one SQE, store its index into the SQ
array, publish the tail with a release store, call `io_uring_enter`, then read
the CQE with an acquire load on the CQ tail. Learn why the SQ ring holds indices
and the CQ ring holds whole entries. Check that the bytes read match `pread`.

**M1. liburing basics (lab/01 to 03).** The same read through liburing, then a
batch of 64, then links and drain. Check that `user_data` maps each CQE back to
its request when completions arrive out of order. Check that a linked read sees
the preceding write's data.

**M2. Registered resources and modes (lab/04, 05).** Register the file table and
use `IOSQE_FIXED_FILE`, which is what the Rust code does with `types::Fixed(id)`.
Register buffers and use `read_fixed` with `O_DIRECT`. Turn on SQPOLL and watch
the `iou-sqp-*` kernel thread in `top`. See `IORING_SQ_NEED_WAKEUP` after the
idle timeout. Try IOPOLL and read the error.

**M3. Single-thread reactor + coroutines (include/upio).** This is the C++
port of `UringContext`.

```
loop:
  run every ready coroutine; each read() grabs an SQE and suspends
  if nothing is runnable: io_uring_submit_and_wait(ring, 1)
  else:                   io_uring_submit(ring)
  for each CQE: op = (Op*)user_data; op->res = cqe->res; resume op->waiter
```

Batching falls out of the loop. Everything queued since the last iteration goes
in one submit. Record the submit batch size, since the Rust post saw about 20.
`flush()` is a NOP with `IOSQE_IO_DRAIN`, same as the Rust code. When
`io_uring_get_sqe` returns null, submit and retry.

Two deliberate departures from the Rust code:
- The Rust `UringPollFuture` calls `wake_by_ref()` whenever the CQ is empty, so
  each poll future spins a core. The reactor blocks in `submit_and_wait`
  instead. Measure both and see the CPU cost in `top`.
- The Rust code shares one `concurrent` ring across several poll futures. A
  liburing `io_uring` is not thread-safe, so one thread owns each ring.

Check with tests/: correct bytes, short read at EOF returns the short count,
a bad offset on an `O_DIRECT` file returns `-EINVAL`, and `flush()` completes
after earlier reads.

**M4. Many threads.** Callers on several threads need reads. Build two designs
and let the benchmark pick:
- A. Thread-per-ring. N reactors, each with its own ring and its own
  registered files. Callers run as coroutines on a reactor. No locks.
- B. Shared ring. One reactor thread, and other threads hand it `Op`s through
  an MPSC queue and wake it with an eventfd registered on the ring.

Expect A to win on throughput and B to show the handoff cost in p50. That is a
guess until M5 measures it.

**M5. Benchmark (bench/).** Mirror `uring_bench`. Generate 16 files of 32 MiB
(512 MiB, which fits the free disk). Every block is filled from
`hash(file, block)` so every read is verified, like `verify_buf` in Rust.

Matrix, with a fixed 30 s duration:

| Engine | Knob | Notes |
|---|---|---|
| pread | threads 8, 32, 512 | blocking baseline |
| mmap + MADV_RANDOM | threads 8, 32, 512 | the blog's winner |
| uring | reactors 8 × in-flight 8, 32, 512 | the blog's uring_8/32/512 |
| uring + SQPOLL | same | fewer syscalls, one burned core |
| uring + O_DIRECT | same, ± registered buffers | the only way to hit the device here |
| nop | in-flight 1 and 32 | the blog's unfinished "Rust vs C nop" idea |

Run every row twice, cached and `O_DIRECT`. Cached runs measure software
overhead (syscalls, wakeups, copies). `O_DIRECT` runs measure EBS, which has a
fixed IOPS cap, so every engine should flatten at the same ceiling. If they
don't, the benchmark is measuring something else.

Before trusting a number, check:
- that buffered "random" reads were actually served from cache (`vmtouch` or
  `fincore`), so you know which regime you measured;
- the submit batch-size histogram from M3;
- `perf stat -e syscalls:sys_enter_io_uring_enter`, so syscalls per op is a
  measured value and not an assumed one.

## What each milestone teaches

| Concept from the posts | Where you touch it |
|---|---|
| SQ ring of indices, CQ ring of entries, shared `io_rings` | M0 |
| Memory ordering on head/tail | M0 |
| `user_data` as a context pointer | M0, M3 |
| Inline non-blocking attempt, then io-wq punt; `IOSQE_ASYNC` | M2 (watch `iou-wrk-*` threads appear on cache misses) |
| IOPOLL needs `O_DIRECT` and device poll queues | M2 (fails here, read why) |
| SQPOLL thread, idle sleep, `NEED_WAKEUP` | M2, M5 |
| Links and drain | M1, `flush()` in M3 |
| Registered files and buffers | M2, M5 |
| Batching vs latency tradeoff | M3 histogram, M5 p50 |
| Buffer lifetime and cancellation | M3 ownership rule |
| Is io_uring faster than mmap for cached data? | M5 cached rows |

## Out of scope

Writes beyond the link demo, networking ops, cancellation (`IORING_OP_ASYNC_CANCEL`),
and multishot or provided-buffer rings. Each is a reasonable M6 once M5 has
numbers.

## Build

```
cmake -S . -B build          # Debug with ASan and UBSan by default; -DUPIO_SANITIZE=OFF for benchmarks
cmake --build build
ctest --test-dir build --output-on-failure
```

Add a lab with `add_lab(01_liburing_read URING)` in `CMakeLists.txt`. The `URING`
flag links liburing. Each lab is also a ctest that passes when it exits 0.
