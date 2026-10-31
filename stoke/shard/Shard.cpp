#include "stoke/shard/Shard.h"

#include <cmath>
#include <deque>
#include <list>

#include <folly/container/F14Map.h>
#include <folly/coro/Baton.h>
#include <folly/coro/BlockingWait.h>
#include <folly/futures/Future.h>
#include <glog/logging.h>

#include "kiln/common/Error.h"
#include "stoke/client/Registry.h"

namespace stoke {

namespace {

using Clock = std::chrono::steady_clock;

// A take parked on an empty queue, until a put, a seal or its timeout.
struct Waiter {
  Epoch epoch;
  TakeResult result;
  bool done{false};
  folly::coro::Baton baton;
};

using Waiters = std::vector<std::shared_ptr<Waiter>>;

// A producer parked until a chunk's ack or its timeout.
struct ConsumedWaiter {
  bool done{false};
  folly::coro::Baton baton;
};

using ConsumedWaiters = std::vector<std::shared_ptr<ConsumedWaiter>>;

template <typename T>
void wake(const std::vector<std::shared_ptr<T>>& waiters) {
  for (const auto& waiter : waiters) {
    waiter->baton.post();
  }
}

} // namespace

struct Shard::Stream {
  struct Lease {
    Epoch epoch;
    Clock::time_point deadline;
  };

  struct Chunk {
    bool taken{false};
    bool consumed{false};
  };

  std::mutex mutex;
  StreamInfo info;
  // Every chunk put.
  folly::F14FastMap<kiln::ChunkId, Chunk> chunks;
  std::deque<kiln::ChunkId> queue;
  folly::F14FastMap<kiln::ChunkId, Lease> leases;
  std::list<std::shared_ptr<Waiter>> waiters;
  // Producers waiting for chunks' acks.
  folly::F14FastMap<kiln::ChunkId, ConsumedWaiters> consumedWaiters;
  ShardStats stats;
  // No longer in the registry.
  bool removed{false};

  bool ended() const {
    if (removed) {
      return true;
    }
    if (info.state != StreamState::kSealed) {
      return false;
    }
    auto stop = static_cast<uint64_t>(
        std::ceil(info.stopFraction * static_cast<double>(stats.put)));
    return stats.taken >= stop || (queue.empty() && leases.empty());
  }

  void lease(kiln::ChunkId chunk, const Epoch& epoch, Clock::duration timeout) {
    auto& entry = chunks[chunk];
    if (!entry.taken) {
      entry.taken = true;
      ++stats.taken;
    }
    leases[chunk] = Lease{epoch, Clock::now() + timeout};
  }

  // Hands queued chunks to parked takes, one each, or ends them all once
  // the stream ended. Returns the takes to wake, outside the lock.
  Waiters dispatch(Clock::duration timeout) {
    Waiters done;
    if (ended()) {
      for (auto& waiter : waiters) {
        waiter->result.end = true;
        waiter->done = true;
        done.push_back(std::move(waiter));
      }
      waiters.clear();
      return done;
    }
    while (!waiters.empty() && !queue.empty()) {
      auto waiter = std::move(waiters.front());
      waiters.pop_front();
      auto chunk = queue.front();
      queue.pop_front();
      lease(chunk, waiter->epoch, timeout);
      waiter->result.chunks.push_back(chunk);
      waiter->done = true;
      done.push_back(std::move(waiter));
    }
    return done;
  }

  ShardStats snapshot() const {
    auto result = stats;
    result.depth = queue.size();
    result.leased = leases.size();
    result.waiters = waiters.size();
    result.sealed = info.state == StreamState::kSealed;
    result.ended = ended();
    return result;
  }
};

Shard::Shard(std::shared_ptr<Registry> registry, ShardOptions options)
    : registry_{std::move(registry)}, options_{options} {
  if (options_.refreshInterval.count() > 0) {
    refresher_ = std::thread([this] { refreshLoop(); });
  }
}

Shard::~Shard() {
  {
    std::lock_guard lock(stopMutex_);
    stopping_ = true;
  }
  stopCondition_.notify_all();
  if (refresher_.joinable()) {
    refresher_.join();
  }
}

void Shard::refreshLoop() {
  std::unique_lock lock(stopMutex_);
  while (!stopCondition_.wait_for(
      lock, options_.refreshInterval, [this] { return stopping_; })) {
    lock.unlock();
    try {
      folly::coro::blockingWait(co_refresh());
    } catch (const std::exception& e) {
      LOG(WARNING) << "Cannot refresh streams: " << e.what();
    }
    lock.lock();
  }
}

folly::coro::Task<std::shared_ptr<Shard::Stream>> Shard::co_stream(
    const std::string& name) {
  {
    std::lock_guard lock(mutex_);
    auto it = streams_.find(name);
    if (it != streams_.end()) {
      co_return it->second;
    }
  }
  auto info = co_await registry_->co_get(name);
  if (!info) {
    throw kiln::Error(kiln::ErrorCode::kNotFound, "no stream " + name);
  }
  auto stream = std::make_shared<Stream>();
  stream->info = std::move(*info);
  std::lock_guard lock(mutex_);
  // Another call may have added it meanwhile.
  auto [it, added] = streams_.emplace(name, std::move(stream));
  co_return it->second;
}

folly::coro::Task<void> Shard::co_refresh() {
  std::vector<std::pair<std::string, std::shared_ptr<Stream>>> streams;
  {
    std::lock_guard lock(mutex_);
    streams.assign(streams_.begin(), streams_.end());
  }
  const auto now = Clock::now();
  const auto timeout = options_.leaseTimeout;
  for (auto& [name, stream] : streams) {
    auto info = co_await registry_->co_get(name);
    if (!info) {
      std::lock_guard lock(mutex_);
      streams_.erase(name);
    }
    Waiters done;
    {
      std::lock_guard lock(stream->mutex);
      if (info) {
        stream->info = std::move(*info);
      } else {
        stream->removed = true;
      }
      for (auto it = stream->leases.begin(); it != stream->leases.end();) {
        if (it->second.deadline <= now) {
          stream->queue.push_front(it->first);
          ++stream->stats.expired;
          it = stream->leases.erase(it);
        } else {
          ++it;
        }
      }
      done = stream->dispatch(timeout);
    }
    wake(done);
  }
}

folly::coro::Task<bool> Shard::co_put(std::string stream, kiln::ChunkId chunk) {
  auto state = co_await co_stream(stream);
  Waiters done;
  {
    std::lock_guard lock(state->mutex);
    if (state->removed) {
      throw kiln::Error(kiln::ErrorCode::kNotFound, "no stream " + stream);
    }
    if (!state->chunks.emplace(chunk, Stream::Chunk{}).second) {
      ++state->stats.duplicates;
      co_return false;
    }
    ++state->stats.put;
    state->queue.push_back(chunk);
    done = state->dispatch(options_.leaseTimeout);
  }
  wake(done);
  co_return true;
}

folly::coro::Task<TakeResult> Shard::co_take(
    std::string stream,
    Epoch epoch,
    uint32_t max,
    std::chrono::milliseconds wait) {
  if (max == 0 || max > options_.maxTake) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument,
        "take needs 1 to " + std::to_string(options_.maxTake) + " chunks");
  }
  if (wait.count() < 0 || wait > options_.maxWait) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument,
        "take waits at most " + std::to_string(options_.maxWait.count()) +
            " ms");
  }
  auto state = co_await co_stream(stream);
  auto waiter = std::make_shared<Waiter>();
  {
    std::lock_guard lock(state->mutex);
    TakeResult result;
    if (state->ended()) {
      result.end = true;
      co_return result;
    }
    while (result.chunks.size() < max && !state->queue.empty()) {
      auto chunk = state->queue.front();
      state->queue.pop_front();
      state->lease(chunk, epoch, options_.leaseTimeout);
      result.chunks.push_back(chunk);
    }
    if (!result.chunks.empty() || wait.count() == 0) {
      co_return result;
    }
    waiter->epoch = std::move(epoch);
    state->waiters.push_back(waiter);
  }
  // Ends the wait unless a put or a seal did first. Holds only the stream
  // and the waiter, which outlive the shard if need be.
  folly::futures::sleep(wait).toUnsafeFuture().thenValue(
      [state, waiter](auto&&) {
        {
          std::lock_guard lock(state->mutex);
          if (waiter->done) {
            return;
          }
          waiter->done = true;
          state->waiters.remove(waiter);
        }
        waiter->baton.post();
      });
  co_await waiter->baton;
  co_return std::move(waiter->result);
}

folly::coro::Task<void> Shard::co_ack(
    std::string stream,
    Epoch epoch,
    std::vector<kiln::ChunkId> chunks) {
  auto state = co_await co_stream(stream);
  Waiters done;
  ConsumedWaiters consumed;
  {
    std::lock_guard lock(state->mutex);
    for (const auto& chunk : chunks) {
      auto it = state->leases.find(chunk);
      if (it != state->leases.end() && it->second.epoch == epoch) {
        state->leases.erase(it);
        ++state->stats.acked;
        state->chunks[chunk].consumed = true;
        auto waiting = state->consumedWaiters.find(chunk);
        if (waiting != state->consumedWaiters.end()) {
          for (auto& waiter : waiting->second) {
            waiter->done = true;
            consumed.push_back(std::move(waiter));
          }
          state->consumedWaiters.erase(waiting);
        }
      }
    }
    // The last acks may end a sealed stream.
    done = state->dispatch(options_.leaseTimeout);
  }
  wake(done);
  wake(consumed);
}

folly::coro::Task<bool> Shard::co_awaitConsumed(
    std::string stream,
    kiln::ChunkId chunk,
    std::chrono::milliseconds wait) {
  if (wait.count() < 0 || wait > options_.maxWait) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument,
        "awaitConsumed waits at most " +
            std::to_string(options_.maxWait.count()) + " ms");
  }
  auto state = co_await co_stream(stream);
  auto waiter = std::make_shared<ConsumedWaiter>();
  {
    std::lock_guard lock(state->mutex);
    if (state->removed) {
      throw kiln::Error(kiln::ErrorCode::kNotFound, "no stream " + stream);
    }
    auto it = state->chunks.find(chunk);
    if (it == state->chunks.end()) {
      throw kiln::Error(
          kiln::ErrorCode::kNotFound,
          "no chunk " + chunk.toString() + " in stream " + stream);
    }
    if (it->second.consumed || wait.count() == 0) {
      co_return it->second.consumed;
    }
    state->consumedWaiters[chunk].push_back(waiter);
  }
  // As in co_take: ends the wait unless the ack did first.
  folly::futures::sleep(wait).toUnsafeFuture().thenValue(
      [state, waiter, chunk](auto&&) {
        {
          std::lock_guard lock(state->mutex);
          if (waiter->done) {
            return;
          }
          waiter->done = true;
          auto it = state->consumedWaiters.find(chunk);
          if (it != state->consumedWaiters.end()) {
            std::erase(it->second, waiter);
            if (it->second.empty()) {
              state->consumedWaiters.erase(it);
            }
          }
        }
        waiter->baton.post();
      });
  co_await waiter->baton;
  std::lock_guard lock(state->mutex);
  co_return state->chunks[chunk].consumed;
}

folly::coro::Task<uint64_t> Shard::co_release(std::string stream, Epoch epoch) {
  auto state = co_await co_stream(stream);
  Waiters done;
  uint64_t released = 0;
  {
    std::lock_guard lock(state->mutex);
    for (auto it = state->leases.begin(); it != state->leases.end();) {
      if (it->second.epoch == epoch) {
        state->queue.push_front(it->first);
        ++released;
        it = state->leases.erase(it);
      } else {
        ++it;
      }
    }
    state->stats.released += released;
    done = state->dispatch(options_.leaseTimeout);
  }
  wake(done);
  co_return released;
}

folly::coro::Task<ShardStats> Shard::co_stats(std::string stream) {
  auto state = co_await co_stream(stream);
  std::lock_guard lock(state->mutex);
  co_return state->snapshot();
}

} // namespace stoke
