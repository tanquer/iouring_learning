// M3 behavior tests for Reactor + RandomAccessFiles. Run from the build directory (ext4).

#include <random>
#include <stdexcept>
#include <system_error>

#include "common.h"
#include "upio/files.h"
#include "upio/reactor.h"

using namespace lab;
using upio::RandomAccessFiles;
using upio::Reactor;
using upio::Task;

namespace {

constexpr unsigned kFiles = 4;
constexpr std::size_t kBlocks = 64;

struct Fixture {
  std::vector<int> fds;
  upio::FileId direct = 0;
  upio::FileId tmpfs = 0;

  Fixture() {
    for (unsigned i = 0; i < kFiles; ++i) fds.push_back(make_test_file(kBlocks));
    direct = static_cast<upio::FileId>(fds.size());
    fds.push_back(reopen(fds[0], O_RDONLY | O_DIRECT));
    tmpfs = static_cast<upio::FileId>(fds.size());
    fds.push_back(make_test_file(kBlocks, "/tmp"));
  }
  ~Fixture() {
    for (int fd : fds) close(fd);
  }
};

struct alignas(kBlock) Block {
  std::byte bytes[kBlock];
};

void concurrent_reads(Fixture& fx) {
  constexpr unsigned kTasks = 256;
  Reactor reactor(64);
  RandomAccessFiles files(reactor, fx.fds);
  std::vector<Block> bufs(kTasks);
  unsigned verified = 0;
  std::mt19937 rng(42);
  for (unsigned i = 0; i < kTasks; ++i) {
    upio::FileId file = rng() % kFiles;
    std::size_t block = rng() % kBlocks;
    reactor.spawn([](RandomAccessFiles& files, upio::FileId file, std::size_t block, std::span<std::byte> buf,
                     unsigned& verified) -> Task<> {
      std::size_t n = co_await files.read(file, block * kBlock, buf);
      expect(n == kBlock, "concurrent read returned a full block");
      expect(block_ok(buf.data(), block), "concurrent read returned the right bytes");
      ++verified;
    }(files, file, block, bufs[i].bytes, verified));
  }
  reactor.run();
  const auto& s = reactor.stats();
  expect(verified == kTasks, "every spawned read finished and verified");
  expect(reactor.in_flight() == 0, "nothing left in flight after run()");
  expect(s.completions == kTasks, "one completion per read");
  expect(s.enter_calls + s.early_submits < kTasks, "reads were batched into fewer submits than reads");
  std::printf("concurrent: %u reads, %llu submit_and_wait + %llu early submits, %llu SQEs via submit_and_wait\n",
              kTasks, (unsigned long long)s.enter_calls, (unsigned long long)s.early_submits,
              (unsigned long long)s.sqes_submitted);
}

void short_read_at_eof(Fixture& fx) {
  Reactor reactor(8);
  RandomAccessFiles files(reactor, fx.fds);
  static Block buf;
  std::size_t got = 0;
  reactor.spawn([](RandomAccessFiles& files, std::size_t& got) -> Task<> {
    got = co_await files.read(0, kBlocks * kBlock - 100, buf.bytes);
  }(files, got));
  reactor.run();
  expect(got == 100, "a read crossing EOF returns the bytes that exist");
  std::printf("eof:        read across EOF returned %zu bytes\n", got);
}

void error_propagates(Fixture& fx) {
  Reactor reactor(8);
  RandomAccessFiles files(reactor, fx.fds);
  static Block buf;
  int code = 0;
  reactor.spawn([](RandomAccessFiles& files, upio::FileId direct, int& code) -> Task<> {
    try {
      co_await files.read(direct, 1, buf.bytes);
    } catch (const std::system_error& e) {
      code = e.code().value();
    }
  }(files, fx.direct, code));
  reactor.run();
  expect(code == EINVAL, "an unaligned O_DIRECT read throws system_error(EINVAL)");
  std::printf("error:      unaligned O_DIRECT read threw EINVAL\n");
}

void flush_waits_for_earlier_reads(Fixture& fx) {
  constexpr unsigned kReads = 32;
  Reactor reactor(64);
  RandomAccessFiles files(reactor, fx.fds);
  std::vector<Block> bufs(kReads);
  std::vector<int> order;
  for (unsigned i = 0; i < kReads; ++i) {
    reactor.spawn([](RandomAccessFiles& files, upio::FileId tmpfs, unsigned i, std::span<std::byte> buf,
                     std::vector<int>& order) -> Task<> {
      co_await files.read(tmpfs, i * kBlock, buf);
      expect(block_ok(buf.data(), i), "read before flush returned the right bytes");
      order.push_back(static_cast<int>(i));
    }(files, fx.tmpfs, i, bufs[i].bytes, order));
  }
  reactor.spawn([](RandomAccessFiles& files, std::vector<int>& order) -> Task<> {
    co_await files.flush();
    order.push_back(-1);
  }(files, order));
  reactor.run();
  expect(order.size() == kReads + 1, "every read and the flush finished");
  expect(order.back() == -1, "flush finished after every read submitted before it");
  std::printf("flush:      completed after all %u io-wq reads\n", kReads);
}

void task_exception_reaches_run() {
  Reactor reactor(8);
  reactor.spawn([]() -> Task<> {
    throw std::runtime_error("boom");
    co_return;
  }());
  bool caught = false;
  try {
    reactor.run();
  } catch (const std::runtime_error&) {
    caught = true;
  }
  expect(caught, "an exception escaping a spawned task is rethrown by run()");
  std::printf("exception:  spawned task's exception rethrown by run()\n");
}

}  // namespace

int main() {
  Fixture fx;
  concurrent_reads(fx);
  short_read_at_eof(fx);
  error_propagates(fx);
  flush_waits_for_earlier_reads(fx);
  task_exception_reaches_run();
  std::printf("OK\n");
  return 0;
}
