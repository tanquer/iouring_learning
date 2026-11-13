#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "common.h"

namespace m5 {

// A directory of files named file_000.dat ... Block b of file f holds lab::pattern(f * nb + b, word),
// so every 4 KiB block in the dataset is distinct and a read can be verified on its own.
struct Dataset {
  std::string dir;
  unsigned nf = 0;
  std::size_t nb = 0;  // blocks per file
  std::vector<int> fds;

  std::uint64_t global_block(unsigned file, std::size_t block) const { return std::uint64_t(file) * nb + block; }

  ~Dataset() {
    for (int fd : fds) close(fd);
  }
};

inline std::string file_path(const std::string& dir, unsigned f) {
  char name[32];
  std::snprintf(name, sizeof name, "file_%03u.dat", f);
  return dir + "/" + name;
}

inline void generate(const std::string& dir, unsigned nf, std::size_t nb) {
  std::filesystem::create_directories(dir);
  std::vector<std::uint64_t> buf(lab::kBlock / sizeof(std::uint64_t));
  for (unsigned f = 0; f < nf; ++f) {
    int fd = open(file_path(dir, f).c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) lab::die("open for generate");
    for (std::size_t b = 0; b < nb; ++b) {
      for (std::size_t w = 0; w < buf.size(); ++w) buf[w] = lab::pattern(std::uint64_t(f) * nb + b, w);
      if (pwrite(fd, buf.data(), lab::kBlock, b * lab::kBlock) != static_cast<ssize_t>(lab::kBlock)) lab::die("pwrite");
    }
    if (fsync(fd) != 0) lab::die("fsync");
    close(fd);
  }
}

// Opens every file_NNN.dat in `dir`. All files must have the same whole number of blocks.
inline void open_dataset(Dataset& ds, const std::string& dir, bool direct) {
  ds.dir = dir;
  for (unsigned f = 0;; ++f) {
    std::string path = file_path(dir, f);
    if (!std::filesystem::exists(path)) break;
    int fd = open(path.c_str(), O_RDONLY | (direct ? O_DIRECT : 0));
    if (fd < 0) lab::die("open dataset file");
    struct stat st{};
    fstat(fd, &st);
    std::size_t nb = st.st_size / lab::kBlock;
    lab::expect(nb > 0 && st.st_size % lab::kBlock == 0, "dataset files are whole 4 KiB blocks");
    lab::expect(ds.nb == 0 || ds.nb == nb, "dataset files all have the same size");
    ds.nb = nb;
    ds.fds.push_back(fd);
  }
  ds.nf = static_cast<unsigned>(ds.fds.size());
  lab::expect(ds.nf > 0, "dataset directory has file_000.dat (run gen first)");
}

// Fraction of the dataset's pages currently in the page cache.
inline double cached_fraction(const Dataset& ds) {
  std::size_t resident = 0, total = 0;
  long page = sysconf(_SC_PAGESIZE);
  for (unsigned f = 0; f < ds.nf; ++f) {
    int fd = open(file_path(ds.dir, f).c_str(), O_RDONLY);
    std::size_t len = ds.nb * lab::kBlock;
    void* p = mmap(nullptr, len, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (p == MAP_FAILED) lab::die("mmap for mincore");
    std::vector<unsigned char> vec((len + page - 1) / page);
    if (mincore(p, len, vec.data()) != 0) lab::die("mincore");
    for (unsigned char v : vec) resident += v & 1;
    total += vec.size();
    munmap(p, len);
  }
  return total ? double(resident) / double(total) : 0;
}

// Ask the kernel to drop this dataset's clean pages from the page cache. Needs no root.
inline void drop_cache(const Dataset& ds) {
  for (unsigned f = 0; f < ds.nf; ++f) {
    int fd = open(file_path(ds.dir, f).c_str(), O_RDONLY);
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
    close(fd);
  }
}

}  // namespace m5
