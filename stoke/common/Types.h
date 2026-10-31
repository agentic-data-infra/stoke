#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "kiln/common/ChunkId.h"

namespace stoke {

// A consumer incarnation that leases chunks: a trainer job and how often it
// started. A restarted trainer takes with a new incarnation and releases
// the chunks of the old one (ShardRpc::co_release).
struct Epoch {
  std::string job;
  uint64_t incarnation{0};

  std::string toString() const {
    return job + "#" + std::to_string(incarnation);
  }

  bool operator==(const Epoch&) const = default;
};

struct TakeResult {
  // Leased to the epoch until acked or released.
  std::vector<kiln::ChunkId> chunks;
  // The stream is sealed and this shard has no more chunks for anyone.
  bool end{false};
};

// A shard's view of one stream; summed over shards, depth is the backlog and
// waiters tells whether consumers starve.
struct ShardStats {
  // Chunks queued, not leased.
  uint64_t depth{0};
  // Chunks leased and not yet acked.
  uint64_t leased{0};
  // Takes parked until a chunk comes.
  uint64_t waiters{0};
  // Distinct chunks put; duplicate puts are counted apart.
  uint64_t put{0};
  uint64_t duplicates{0};
  // Distinct chunks taken at least once.
  uint64_t taken{0};
  uint64_t acked{0};
  // Leases returned to the queue, by co_release or because they timed out.
  uint64_t released{0};
  uint64_t expired{0};
  bool sealed{false};
  bool ended{false};

  ShardStats& operator+=(const ShardStats& other);
};

} // namespace stoke
