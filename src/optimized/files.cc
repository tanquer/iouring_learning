#include "optimized/files.h"

#include <cstdio>
#include <cstdlib>
#include <system_error>

namespace upio::optimized {

RandomAccessFiles::RandomAccessFiles(Reactor& reactor, std::span<const int> fds) : reactor_(reactor) {
  reactor_.register_files(fds);
}

Task<std::size_t> RandomAccessFiles::read(FileId file, std::uint64_t offset,
                                          std::span<std::byte> buf) {
  std::int32_t res = co_await reactor_.submit([&](io_uring_sqe* sqe) {
    io_uring_prep_read(sqe, file, buf.data(), static_cast<unsigned>(buf.size()), offset);
    io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
  });
  if (res < 0) throw std::system_error(-res, std::generic_category(), "read");
  co_return static_cast<std::size_t>(res);
}

Task<void> RandomAccessFiles::flush() {
  std::int32_t res = co_await reactor_.submit([](io_uring_sqe* sqe) {
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_flags(sqe, IOSQE_IO_DRAIN);
  });
  if (res < 0) throw std::system_error(-res, std::generic_category(), "flush");
}

}  // namespace upio::optimized
