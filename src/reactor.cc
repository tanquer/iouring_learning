// M3. The reactor: the C++ port of the Rust library's UringContext and UringPollFuture.
//
// The letters follow the order the test reaches them: A in src/files.cc (read), B to E here
// (get_sqe, arm, the run loop's submit, reap), then F in src/files.cc (flush). Each todo() stops
// the test and names the step. Run `./build/m3_test` from the build directory.

#include "upio/reactor.h"

#include <cstdio>
#include <cstdlib>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <system_error>

namespace upio {

namespace {

[[noreturn]] void die_res(int res, const char* what) {
  throw std::system_error(-res, std::generic_category(), what);
}

[[maybe_unused]] [[noreturn]] void todo(const char* step) {
  std::fprintf(stderr, "TODO %s is not implemented yet\n", step);
  std::exit(2);
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
  // TODO B: return a free SQE, making room if the SQ is full.
  // io_uring_get_sqe returns nullptr when every slot is queued but not yet submitted. Then call
  // io_uring_submit to hand them to the kernel (count it in stats_.early_submits) and retry.
  // Why is one retry always enough on a single-threaded ring?
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (sqe == nullptr) {
    if (int r = io_uring_submit(&ring_); r < 0) die_res(r, "io_uring_submit");
    ++stats_.early_submits;
    sqe = io_uring_get_sqe(&ring_);
  }
  return sqe;
}

void Reactor::arm([[maybe_unused]] io_uring_sqe* sqe, [[maybe_unused]] Op* op) {
  // TODO C: make the CQE find its way back to `op`, and count the request as in flight.
  // io_uring_sqe_set_data stores the pointer as user_data, like UringTask in the Rust code.
  io_uring_sqe_set_data(sqe, op);
  ++in_flight_;
}

void Reactor::reap() {
  // TODO E: move every ready CQE's result into its Op and queue the waiter, without resuming it.
  // Walk with io_uring_for_each_cqe, set op->res, push op->waiter onto ready_, decrement
  // in_flight_, count stats_.completions, then io_uring_cq_advance once.
  // Why queue instead of calling op->waiter.resume() right here inside the loop?
  unsigned n = 0;
  std::uint32_t head = 0;
  io_uring_cqe* cqe = nullptr;
  io_uring_for_each_cqe(&ring_, head, cqe) {
    auto op = static_cast<Op*>(io_uring_cqe_get_data(cqe));
    op->res = cqe->res;
    ready_.push_back(op->waiter);
    --in_flight_;
    ++n;
  }
  stats_.completions += n;
  io_uring_cq_advance(&ring_, n);
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
    if (in_flight_ == 0) break;

    // TODO D: hand every queued SQE to the kernel and sleep until at least one completion,
    // in one syscall. io_uring_submit_and_wait returns how many SQEs it submitted.
    // Count the call in stats_.enter_calls and the SQEs in stats_.sqes_submitted.
    // Everything the coroutines queued since the last iteration goes in this one call:
    // that is where batching comes from.
    int submitted = io_uring_submit_and_wait(&ring_, 1);
    if (submitted < 0 && submitted != -EINTR) die_res(submitted, "io_uring_submit_and_wait");
    if (submitted > 0) stats_.sqes_submitted += submitted;
    ++stats_.enter_calls;
    reap();
  }
  spawned_.clear();
}

}  // namespace upio
