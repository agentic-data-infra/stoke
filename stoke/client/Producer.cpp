#include "stoke/client/Producer.h"

#include <cstring>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"

namespace stoke {

namespace {

// One wait at a shard, within ShardOptions::maxWait; co_awaitBudget loops.
constexpr std::chrono::seconds kConsumedWait{1};

} // namespace

Producer::Producer(
    std::shared_ptr<StreamClient> stream,
    std::shared_ptr<kiln::client::Client> kiln,
    kiln::Placement placement)
    : stream_{std::move(stream)},
      kiln_{std::move(kiln)},
      placement_{placement},
      budget_{stream_->info().taskBudget} {}

folly::coro::Task<void> Producer::co_awaitBudget() {
  if (budget_ == 0) {
    co_return;
  }
  const auto start = std::chrono::steady_clock::now();
  while (true) {
    kiln::ChunkId oldest;
    {
      std::lock_guard lock(mutex_);
      if (unconsumed_.size() < budget_) {
        break;
      }
      oldest = unconsumed_.front();
    }
    bool consumed = false;
    try {
      consumed = co_await stream_->co_awaitConsumed(oldest, kConsumedWait);
    } catch (const kiln::Error& e) {
      // A shard that restarted lost its queue, so the chunk will never be
      // consumed: stop counting it. A removed stream fails the next put.
      if (e.code() != kiln::ErrorCode::kNotFound) {
        throw;
      }
      consumed = true;
    }
    if (consumed) {
      std::lock_guard lock(mutex_);
      // Another caller may have dropped it already.
      if (!unconsumed_.empty() && unconsumed_.front() == oldest) {
        unconsumed_.pop_front();
      }
    }
  }
  std::lock_guard lock(mutex_);
  waited_ += std::chrono::steady_clock::now() - start;
}

folly::coro::Task<kiln::ChunkId> Producer::co_put(std::string_view chunk) {
  co_await co_awaitBudget();
  auto writer = co_await kiln_->co_create(
      stream_->info().bucket, placement_, chunk.size());
  if (!chunk.empty()) {
    auto buffer = kiln_->allocateBuffer(chunk.size());
    std::memcpy(buffer.data(), chunk.data(), chunk.size());
    buffer.setSize(chunk.size());
    co_await writer->co_append(std::move(buffer));
  }
  auto id = co_await writer->co_commit();
  co_await stream_->co_put(id);
  if (budget_ > 0) {
    std::lock_guard lock(mutex_);
    unconsumed_.push_back(id);
  }
  co_return id;
}

size_t Producer::unconsumed() const {
  std::lock_guard lock(mutex_);
  return unconsumed_.size();
}

std::chrono::nanoseconds Producer::waited() const {
  std::lock_guard lock(mutex_);
  return waited_;
}

} // namespace stoke
