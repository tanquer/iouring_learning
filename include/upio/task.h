#pragma once

#include <coroutine>
#include <exception>
#include <optional>
#include <utility>

namespace upio {

// A lazy coroutine. It starts when awaited (or when a Reactor spawns it) and resumes its awaiter
// when it finishes. Awaiting a Task transfers control directly into it (symmetric transfer).
template <typename T>
class Task;

namespace detail {

// On finishing, jump straight to whoever awaited this task, or back to the reactor if nobody did.
struct FinalAwaiter {
  bool await_ready() noexcept { return false; }
  template <typename P>
  std::coroutine_handle<> await_suspend(std::coroutine_handle<P> h) noexcept {
    auto next = h.promise().continuation;
    return next ? next : std::noop_coroutine();
  }
  void await_resume() noexcept {}
};

struct PromiseBase {
  std::coroutine_handle<> continuation;
  std::exception_ptr error;

  std::suspend_always initial_suspend() noexcept { return {}; }
  FinalAwaiter final_suspend() noexcept { return {}; }

  void unhandled_exception() { error = std::current_exception(); }

  void rethrow_if_failed() {
    if (error) std::rethrow_exception(error);
  }
};

template <typename T>
struct Promise : PromiseBase {
  std::optional<T> value;
  Task<T> get_return_object();
  void return_value(T v) { value = std::move(v); }
  T take() {
    rethrow_if_failed();
    return std::move(*value);
  }
};

template <>
struct Promise<void> : PromiseBase {
  Task<void> get_return_object();
  void return_void() {}
  void take() { rethrow_if_failed(); }
};

}  // namespace detail

template <typename T = void>
class [[nodiscard]] Task {
 public:
  using promise_type = detail::Promise<T>;
  using Handle = std::coroutine_handle<promise_type>;

  explicit Task(Handle h) : h_(h) {}
  Task(Task&& o) noexcept : h_(std::exchange(o.h_, {})) {}
  Task& operator=(Task&& o) noexcept {
    if (this != &o) {
      if (h_) h_.destroy();
      h_ = std::exchange(o.h_, {});
    }
    return *this;
  }
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  ~Task() {
    if (h_) h_.destroy();
  }

  bool done() const { return h_.done(); }
  Handle handle() const { return h_; }

  // The result of a finished task. Rethrows if the coroutine threw.
  T result() { return h_.promise().take(); }

  auto operator co_await() noexcept {
    struct Awaiter {
      Handle h;
      bool await_ready() noexcept { return false; }
      std::coroutine_handle<> await_suspend(std::coroutine_handle<> caller) noexcept {
        h.promise().continuation = caller;
        return h;
      }
      T await_resume() { return h.promise().take(); }
    };
    return Awaiter{h_};
  }

 private:
  Handle h_;
};

namespace detail {
template <typename T>
Task<T> Promise<T>::get_return_object() {
  return Task<T>(std::coroutine_handle<Promise<T>>::from_promise(*this));
}
inline Task<void> Promise<void>::get_return_object() {
  return Task<void>(std::coroutine_handle<Promise<void>>::from_promise(*this));
}
}  // namespace detail

}  // namespace upio
