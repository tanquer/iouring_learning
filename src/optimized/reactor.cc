#include "optimized/reactor.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <system_error>

namespace upio::optimized {

namespace {

[[noreturn]] void die_res(int res, const char* what) {
  throw std::system_error(-res, std::generic_category(), what);
}

}  // namespace

Reactor::Reactor(unsigned entries, unsigned setup_flags) {
  if (int r = io_uring_queue_init(entries, &ring_, setup_flags); r < 0) die_res(r, "io_uring_queue_init");
}

Reactor::~Reactor() {
  if (in_flight_ > 0) {
    // The kernel would write into coroutine frames that are about to be freed.
    std::fprintf(stderr, "upio: Reactor destroyed with %u requests in flight\n", in_flight_);
    std::abort();
  }
  spawned_.clear();
  io_uring_queue_exit(&ring_);
}

void Reactor::register_files(std::span<const int> fds) {
  if (int r = io_uring_register_files(&ring_, fds.data(), fds.size()); r < 0) die_res(r, "io_uring_register_files");
}

void Reactor::spawn(Task<void> task) {
  ready_.push_back(task.handle());
  spawned_.push_back(std::move(task));
}

io_uring_sqe* Reactor::get_sqe() {
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    if (int r = io_uring_submit(&ring_); r < 0) die_res(r, "io_uring_submit");
    ++stats_.early_submits;
    sqe = io_uring_get_sqe(&ring_);
    if (sqe == nullptr) {
      // Without SQPOLL the kernel consumes SQEs inside io_uring_submit, so this means it took none.
      std::fprintf(stderr, "upio: SQ still full after submitting\n");
      std::abort();
    }
  }
  return sqe;
}

void Reactor::arm(io_uring_sqe* sqe, Op* op) {
  io_uring_sqe_set_data(sqe, op);
  ++in_flight_;
}

void Reactor::reap() {
  unsigned head;
  io_uring_cqe* cqe;
  unsigned n = 0;
  io_uring_for_each_cqe(&ring_, head, cqe) {
    auto* op = static_cast<Op*>(io_uring_cqe_get_data(cqe));
    ++n;
    if (op == nullptr) {
      std::fprintf(stderr, "upio: posted request failed: %s\n", std::strerror(-cqe->res));
      continue;
    }
    op->res = cqe->res;
    ready_.push_back(op->waiter);
    --in_flight_;
  }
  io_uring_cq_advance(&ring_, n);
  stats_.completions += n;
}

void Reactor::run() {
  for (;;) {
    while (!ready_.empty()) {
      std::coroutine_handle<> h = ready_.front();
      ready_.pop_front();
      h.resume();
    }
    // Drop finished tasks so a long-running reactor (RingServer) doesn't grow or rescan them forever.
    std::erase_if(spawned_, [](Task<void>& t) {
      if (!t.done()) return false;
      t.result();
      return true;
    });
    if (in_flight_ == 0 && io_uring_sq_ready(&ring_) == 0) break;

    int submitted = io_uring_submit_and_wait(&ring_, in_flight_ > 0 ? 1 : 0);
    if (submitted < 0 && submitted != -EINTR) die_res(submitted, "io_uring_submit_and_wait");
    ++stats_.enter_calls;
    if (submitted > 0) stats_.sqes_submitted += submitted;

    reap();
  }
  spawned_.clear();
}

}  // namespace upio::optimized
