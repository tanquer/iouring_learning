#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <system_error>
#include <vector>

namespace lab {

inline constexpr std::size_t kBlock = 4096;

[[noreturn]] inline void die(const char* what) {
  throw std::system_error(errno, std::generic_category(), what);
}

// liburing returns -errno instead of setting errno.
[[noreturn]] inline void die_res(int res, const char* what) {
  errno = -res;
  die(what);
}

[[noreturn]] inline void todo(const char* step) {
  std::fprintf(stderr, "TODO %s is not implemented yet\n", step);
  std::exit(2);
}

inline void expect(bool ok, const char* what) {
  if (!ok) {
    std::fprintf(stderr, "FAIL %s\n", what);
    std::exit(1);
  }
}

inline std::uint64_t pattern(std::size_t block, std::size_t word) {
  return (block * 0x9E3779B97F4A7C15ull) ^ (word * 48892056947ull);
}

inline bool block_ok(const std::byte* buf, std::size_t block) {
  for (std::size_t w = 0; w < kBlock / sizeof(std::uint64_t); ++w) {
    std::uint64_t v;
    std::memcpy(&v, buf + w * sizeof(v), sizeof(v));
    if (v != pattern(block, w)) return false;
  }
  return true;
}

// An unlinked temp file of `blocks` 4 KiB blocks, block b filled from pattern(b, word).
// The default is the current directory, so run labs from the ext4 build directory. /tmp is tmpfs:
// its reads often miss the inline fast path and go to io-wq, and it has no device for IOPOLL.
inline int make_test_file(std::size_t blocks, const char* dir = ".") {
  std::string path = std::string(dir) + "/upio_lab_XXXXXX";
  int fd = mkstemp(path.data());
  if (fd < 0) die("mkstemp");
  unlink(path.c_str());
  std::vector<std::uint64_t> buf(kBlock / sizeof(std::uint64_t));
  for (std::size_t b = 0; b < blocks; ++b) {
    for (std::size_t w = 0; w < buf.size(); ++w) buf[w] = pattern(b, w);
    if (pwrite(fd, buf.data(), kBlock, b * kBlock) != static_cast<ssize_t>(kBlock)) die("pwrite");
  }
  return fd;
}

// Reopen an fd (even an unlinked one) with different flags, e.g. O_RDONLY | O_DIRECT.
inline int reopen(int fd, int flags) {
  std::string proc = "/proc/self/fd/" + std::to_string(fd);
  int out = open(proc.c_str(), flags);
  if (out < 0) die("reopen");
  return out;
}

}  // namespace lab
