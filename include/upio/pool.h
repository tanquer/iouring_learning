#pragma once

#include <functional>
#include <span>
#include <vector>

#include "upio/files.h"
#include "upio/reactor.h"
#include "upio/task.h"

namespace upio {

// Design A: one ring per thread. Each shard thread owns a Reactor and its own registered copy of
// the file table and runs its coroutines with no locks. Work is partitioned before it starts.
class ShardedPool {
 public:
  using Body = std::function<Task<void>(RandomAccessFiles& files, unsigned shard, unsigned task)>;

  ShardedPool(std::span<const int> fds, unsigned shards, unsigned entries);

  // Run `tasks_per_shard` coroutines from `body` on every shard in parallel and wait for all of them.
  // Rethrows the first exception from any shard. Returns each shard's reactor stats.
  std::vector<ReactorStats> run(unsigned tasks_per_shard, const Body& body);

 private:
  std::vector<int> fds_;
  unsigned shards_;
  unsigned entries_;
};

}  // namespace upio
