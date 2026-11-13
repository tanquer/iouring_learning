#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "upio/reactor.h"
#include "upio/task.h"

namespace upio {

using FileId = std::uint32_t;  // index into the registered file table

// The C++ port of the Rust library's UringRandomAccessFiles.
class RandomAccessFiles {
 public:
  // Registers `fds` on the reactor's ring. FileId i means fds[i]. The caller may close the fds after.
  RandomAccessFiles(Reactor& reactor, std::span<const int> fds);

  // Read up to buf.size() bytes at `offset`. Returns the byte count (short at EOF).
  // Throws std::system_error on failure. `buf` must stay alive until the returned task finishes.
  Task<std::size_t> read(FileId file, std::uint64_t offset, std::span<std::byte> buf);

  // Completes after every request submitted before it has completed.
  Task<void> flush();

 private:
  Reactor& reactor_;
};

}  // namespace upio
