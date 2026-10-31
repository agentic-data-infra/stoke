#pragma once

#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "stoke/rpc/ShardRpc.h"

namespace stoke {

class Registry;

struct ShardOptions {
  // How long a taken chunk stays leased without an ack before it goes back
  // to the queue, for consumers that died without co_release.
  std::chrono::milliseconds leaseTimeout{std::chrono::minutes(5)};
  // How often the shard re-reads its streams' registry entries (to see
  // seals and removals) and expires leases; 0 to do it only on co_refresh.
  std::chrono::milliseconds refreshInterval{std::chrono::seconds(1)};
  // Bounds on co_take's arguments. A server stops after the takes in
  // flight, so waits stay short.
  uint32_t maxTake{4096};
  std::chrono::milliseconds maxWait{std::chrono::seconds(5)};
};

// One shard of the stoke queue: for each stream it serves, the chunk ids
// put here, queued in DRAM until consumers take them. A chunk is queued
// once (duplicates are dropped), leased to the epoch that took it until
// acked, and goes back to the queue if released or its lease times out.
// Takes on an empty queue park until a chunk comes or their wait is over;
// there is no polling. A sealed stream ends at this shard once it handed
// out StreamInfo::stopFraction of its chunks, or all of them.
//
// Streams are learned from the registry on their first call. Thread safe.
class Shard : public rpc::ShardRpc {
 public:
  Shard(std::shared_ptr<Registry> registry, ShardOptions options = {});

  ~Shard() override;

  folly::coro::Task<bool> co_put(std::string stream, kiln::ChunkId chunk)
      override;

  folly::coro::Task<TakeResult> co_take(
      std::string stream,
      Epoch epoch,
      uint32_t max,
      std::chrono::milliseconds wait) override;

  folly::coro::Task<void> co_ack(
      std::string stream,
      Epoch epoch,
      std::vector<kiln::ChunkId> chunks) override;

  folly::coro::Task<uint64_t> co_release(std::string stream, Epoch epoch)
      override;

  folly::coro::Task<ShardStats> co_stats(std::string stream) override;

  // Re-reads the registry entries of the streams served and expires
  // leases, as the shard does every refreshInterval.
  folly::coro::Task<void> co_refresh();

 private:
  struct Stream;

  // The stream's state, from the registry on first use. Throws
  // kiln::Error(kNotFound) if it has no entry.
  folly::coro::Task<std::shared_ptr<Stream>> co_stream(const std::string& name);

  void refreshLoop();

  const std::shared_ptr<Registry> registry_;
  const ShardOptions options_;

  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<Stream>> streams_;

  std::mutex stopMutex_;
  std::condition_variable stopCondition_;
  bool stopping_{false};
  std::thread refresher_;
};

} // namespace stoke
