#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "kiln/common/Buffer.h"
#include "stoke/client/StreamClient.h"
#include "stoke/format/Batch.h"

namespace kiln::client {
class Client;
}

namespace stoke {

struct LoaderOptions {
  // The consumer: a trainer job's rank and incarnation. Every rank needs a
  // job name of its own.
  Epoch epoch;
  // On start, return the chunks leased to the epoch's previous incarnation
  // to the queue (ShardRpc::co_release).
  bool releasePrevious{true};
  // Threads that take and read chunks.
  uint32_t fetchers{2};
  // Fetchers take chunks while fewer batches than this are buffered.
  size_t bufferedBatches{64};
  uint32_t chunksPerTake{1};
  // How long a take waits at a shard once two others came back empty.
  std::chrono::milliseconds wait{100};
  // Remove each chunk from Kiln once read: streams are consumed once.
  bool removeChunks{true};
  // Seeds the choice of shards; 0 for a random seed.
  uint64_t seed{0};
};

// A batch in a chunk's memory, which it keeps alive.
struct Batch {
  format::BatchView view;
  std::shared_ptr<const kiln::Buffer> chunk;
};

struct LoaderStats {
  uint64_t batches{0};
  uint64_t chunks{0};
  uint64_t bytes{0};
  uint64_t takes{0};
  // Takes that returned no chunks.
  uint64_t emptyTakes{0};
  // Time in takes, including the waits of parked ones.
  std::chrono::nanoseconds takeTime{0};
  // Failed takes or reads, retried; chunks that could not be read are lost.
  uint64_t errors{0};
  uint64_t lostChunks{0};
  // Time next() spent waiting for a batch: the trainer starving.
  std::chrono::nanoseconds waitTime{0};
};

// The consumer side of a stream, one per rank: fetcher threads take chunks
// from random shards, read them from Kiln into memory and split them into
// batches, which next() hands out without copying. A take first tries two
// shards without waiting, then waits at a third, so that idle consumers
// park at shards instead of polling them. Chunks are acked once read:
// those read but not consumed when the trainer stops are lost
// (at-most-once).
class Loader {
 public:
  Loader(
      std::shared_ptr<StreamClient> stream,
      std::shared_ptr<kiln::client::Client> kiln,
      LoaderOptions options);

  // Opens 'stream' in the Kiln cluster at 'metadata' (kiln::MetadataAddress
  // form).
  static std::unique_ptr<Loader>
  open(const std::string& metadata, std::string stream, LoaderOptions options);

  // Stops the fetchers.
  ~Loader();

  Loader(const Loader&) = delete;
  Loader& operator=(const Loader&) = delete;

  // The next batch, waiting for one if need be; nullopt once every shard
  // ended the stream and all batches were handed out.
  std::optional<Batch> next();

  LoaderStats stats() const;

 private:
  void fetchLoop(uint32_t index);

  folly::coro::Task<void> co_fetch(std::mt19937_64& random);

  // A random shard that has not ended the stream, or nullopt if all have.
  std::optional<uint32_t> pickShard(std::mt19937_64& random);

  const std::shared_ptr<StreamClient> stream_;
  const std::shared_ptr<kiln::client::Client> kiln_;
  const LoaderOptions options_;

  mutable std::mutex mutex_;
  std::condition_variable batchesReady_;
  std::condition_variable spaceFree_;
  std::deque<Batch> batches_;
  std::vector<bool> ended_;
  uint32_t endedShards_{0};
  uint32_t runningFetchers_{0};
  bool stopping_{false};
  LoaderStats stats_;
  std::vector<std::thread> fetchers_;
};

} // namespace stoke
