#pragma once

#include <chrono>
#include <deque>
#include <memory>
#include <mutex>
#include <string_view>

#include <folly/coro/Task.h>

#include "kiln/common/Types.h"
#include "stoke/client/StreamClient.h"

namespace kiln::client {
class Client;
}

namespace stoke {

// Writes chunks of a stream: each into the stream's Kiln bucket, then puts
// it at its shard. Ember's stoke connector writes through it, one per task,
// and so do tests, benchmarks and Python feeds.
//
// With a StreamInfo::taskBudget of N (as of the stream's co_open), a
// producer has at most N chunks put and not yet consumed: before writing
// the next one it waits until a consumer acks its oldest. That is the
// stream's backpressure; the caller just sees co_put take longer. Thread
// safe; concurrent callers share the budget.
class Producer {
 public:
  Producer(
      std::shared_ptr<StreamClient> stream,
      std::shared_ptr<kiln::client::Client> kiln,
      kiln::Placement placement = {});

  const StreamClient& stream() const {
    return *stream_;
  }

  // Writes 'chunk' (framed batches, see stoke/format/Batch.h) as one Kiln
  // chunk and puts it, once within the budget. Returns its id.
  folly::coro::Task<kiln::ChunkId> co_put(std::string_view chunk);

  // Chunks put and not yet seen consumed; counted only with a budget.
  size_t unconsumed() const;

  // Time co_put spent waiting for the budget, summed over callers.
  std::chrono::nanoseconds waited() const;

 private:
  // Waits until fewer than the budget's chunks are unconsumed.
  folly::coro::Task<void> co_awaitBudget();

  const std::shared_ptr<StreamClient> stream_;
  const std::shared_ptr<kiln::client::Client> kiln_;
  const kiln::Placement placement_;
  const uint32_t budget_;

  mutable std::mutex mutex_;
  // Oldest first.
  std::deque<kiln::ChunkId> unconsumed_;
  std::chrono::nanoseconds waited_{0};
};

} // namespace stoke
