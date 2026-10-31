#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

#include <folly/coro/Task.h>

#include "kiln/common/ChunkId.h"
#include "stoke/common/Types.h"

namespace stoke::rpc {

// The operations of a shard server (stoke/shard/Shard.h), served and called
// over RPC (Rpc.h). Calls name a stream registered with `stoke create`
// (Registry) and fail with kiln::Error: kNotFound for an unknown stream,
// kInvalidArgument for bad arguments, kUnavailable if the shard cannot be
// reached.
class ShardRpc {
 public:
  virtual ~ShardRpc() = default;

  // Queues 'chunk', or hands it straight to a waiting take. Returns false
  // for a chunk the shard had already, which it drops.
  virtual folly::coro::Task<bool> co_put(
      std::string stream,
      kiln::ChunkId chunk) = 0;

  // Up to 'max' chunks, leased to 'epoch'. With none queued, waits up to
  // 'wait' for one to be put. Returns no chunks if none came in time, or
  // with 'end' once the stream ended at this shard.
  virtual folly::coro::Task<TakeResult> co_take(
      std::string stream,
      Epoch epoch,
      uint32_t max,
      std::chrono::milliseconds wait) = 0;

  // Ends the leases of 'epoch' on 'chunks': they were consumed. Others are
  // ignored.
  virtual folly::coro::Task<void> co_ack(
      std::string stream,
      Epoch epoch,
      std::vector<kiln::ChunkId> chunks) = 0;

  // Whether 'chunk', put at this shard, was consumed (acked). If not, waits
  // up to 'wait' for its ack. Producers call it to stay within their
  // StreamInfo::taskBudget. kNotFound for a chunk the shard does not have.
  virtual folly::coro::Task<bool> co_awaitConsumed(
      std::string stream,
      kiln::ChunkId chunk,
      std::chrono::milliseconds wait) = 0;

  // Returns every chunk leased to 'epoch' to the queue; returns how many.
  virtual folly::coro::Task<uint64_t> co_release(
      std::string stream,
      Epoch epoch) = 0;

  virtual folly::coro::Task<ShardStats> co_stats(std::string stream) = 0;
};

} // namespace stoke::rpc
