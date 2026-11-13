#pragma once

#include <memory>
#include <string>
#include <vector>

#include <folly/coro/Task.h>

#include "kiln/common/Types.h"
#include "stoke/client/Registry.h"
#include "stoke/rpc/ShardRpc.h"

namespace kiln::client {
class Client;
} // namespace kiln::client

namespace stoke {

// A stream's shards, for producers (co_put) and consumers (shard()). Thread
// safe; calls fail with kiln::Error.
class StreamClient {
 public:
  // Reads the stream's registry entry and connects to its shards on first
  // use. Throws kiln::Error(kNotFound) if there is no such stream.
  static folly::coro::Task<std::unique_ptr<StreamClient>> co_open(
      std::shared_ptr<Registry> registry,
      std::string stream);

  // The stream over shards already connected (e.g. in this process), in
  // the order of info.shards.
  StreamClient(
      std::string stream,
      StreamInfo info,
      std::vector<std::shared_ptr<rpc::ShardRpc>> shards);

  const std::string& stream() const {
    return stream_;
  }

  // The registry entry as of co_open.
  const StreamInfo& info() const {
    return info_;
  }

  uint32_t numShards() const {
    return static_cast<uint32_t>(shards_.size());
  }

  rpc::ShardRpc& shard(uint32_t index) const {
    return *shards_.at(index);
  }

  // Queues 'chunk' at its shard (shardOf). Returns false for a duplicate.
  folly::coro::Task<bool> co_put(kiln::ChunkId chunk, std::string host = {});

  // Whether 'chunk' was consumed, waiting up to 'wait' at its shard (see
  // ShardRpc::co_awaitConsumed).
  folly::coro::Task<bool> co_awaitConsumed(
      kiln::ChunkId chunk,
      std::chrono::milliseconds wait);

  // Each shard's stats, in shard order.
  folly::coro::Task<std::vector<ShardStats>> co_stats();

 private:
  const std::string stream_;
  const StreamInfo info_;
  const std::vector<std::shared_ptr<rpc::ShardRpc>> shards_;
};

// The host a committed chunk is stored on, as shards queue it: that of its
// first block (all of them unless its server filled up), in Kiln's terms
// (kiln::localHostName() there); "" if not known.
folly::coro::Task<std::string> co_chunkHost(
    kiln::client::Client& kiln,
    const kiln::ChunkInfo& chunk);

} // namespace stoke
