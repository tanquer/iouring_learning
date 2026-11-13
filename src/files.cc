#include "upio/files.h"

#include <cstdio>
#include <cstdlib>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <system_error>

namespace upio {

namespace {

[[maybe_unused]] [[noreturn]] void todo(const char* step) {
  std::fprintf(stderr, "TODO %s is not implemented yet\n", step);
  std::exit(2);
}

}  // namespace

RandomAccessFiles::RandomAccessFiles(Reactor& reactor, std::span<const int> fds) : reactor_(reactor) {
  reactor_.register_files(fds);
}

Task<std::size_t> RandomAccessFiles::read([[maybe_unused]] FileId file, [[maybe_unused]] std::uint64_t offset,
                                          [[maybe_unused]] std::span<std::byte> buf) {
  // TODO A: read through the registered file table.
  // co_await reactor_.submit(prep) where prep is a lambda taking io_uring_sqe*: prep a read of
  // buf at offset from registered index `file` and set IOSQE_FIXED_FILE.
  // The result is bytes read or -errno. Throw std::system_error(-res, std::generic_category(), "read")
  // on failure, otherwise co_return the byte count.
  std::int32_t res = co_await reactor_.submit([&](io_uring_sqe* sqe) {
    io_uring_prep_read(sqe, file, buf.data(), static_cast<unsigned>(buf.size()), offset);
    io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
  });
  if (res < 0) throw std::system_error(-res, std::generic_category(), "read");
  co_return static_cast<std::size_t>(res);
}

Task<void> RandomAccessFiles::flush() {
  // TODO F: the Rust library's flush(). Submit a NOP with IOSQE_IO_DRAIN and wait for it.
  // A NOP's res is 0. Anything else is an error worth throwing.
  std::int32_t res = co_await reactor_.submit([](io_uring_sqe* sqe) {
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_flags(sqe, IOSQE_IO_DRAIN);
  });
  if (res < 0) throw std::system_error(-res, std::generic_category(), "flush");
}

}  // namespace upio
