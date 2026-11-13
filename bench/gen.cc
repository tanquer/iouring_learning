// M5: write the benchmark dataset. Usage: gen [--dir m5_data] [--nf 4] [--nb 4096]
// The default is 4 files x 16 MiB. Put it on ext4 (not /tmp) so O_DIRECT reads hit the disk.

#include <cstring>
#include <string>

#include "dataset.h"

int main(int argc, char** argv) {
  std::string dir = "m5_data";
  unsigned nf = 4;
  std::size_t nb = 4096;
  for (int i = 1; i + 1 < argc; i += 2) {
    if (!std::strcmp(argv[i], "--dir")) dir = argv[i + 1];
    else if (!std::strcmp(argv[i], "--nf")) nf = std::stoul(argv[i + 1]);
    else if (!std::strcmp(argv[i], "--nb")) nb = std::stoul(argv[i + 1]);
    else {
      std::fprintf(stderr, "unknown option %s\n", argv[i]);
      return 2;
    }
  }
  m5::generate(dir, nf, nb);
  std::printf("wrote %u files x %zu blocks (%zu MiB total) to %s\n", nf, nb, nf * nb * lab::kBlock >> 20, dir.c_str());
  return 0;
}
