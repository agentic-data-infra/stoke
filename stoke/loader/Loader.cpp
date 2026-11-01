#include "stoke/loader/Loader.h"

#include <folly/coro/BlockingWait.h>
#include <glog/logging.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"

namespace stoke {

namespace {

using Clock = std::chrono::steady_clock;

// How long a fetcher waits after a failed take or read.
constexpr auto kRetryDelay = std::chrono::milliseconds(100);

} // namespace

Loader::Loader(
    std::shared_ptr<StreamClient> stream,
    std::shared_ptr<kiln::client::Client> kiln,
    LoaderOptions options)
    : stream_{std::move(stream)},
      kiln_{std::move(kiln)},
      options_{std::move(options)},
      ended_(stream_->numShards(), false) {
  if (options_.fetchers == 0 || options_.minBufferedBatches == 0 ||
      options_.chunksPerTake == 0) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument,
        "a loader needs fetchers, buffered batches and chunks per take");
  }
  if (options_.maxBufferedBatches != 0 &&
      options_.maxBufferedBatches < options_.minBufferedBatches) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument,
        "maxBufferedBatches must be at least minBufferedBatches");
  }
  if (options_.releasePrevious && options_.epoch.incarnation > 0) {
    const Epoch previous{options_.epoch.job, options_.epoch.incarnation - 1};
    for (uint32_t i = 0; i < stream_->numShards(); ++i) {
      folly::coro::blockingWait(
          stream_->shard(i).co_release(stream_->stream(), previous));
    }
  }
  runningFetchers_ = options_.fetchers;
  for (uint32_t i = 0; i < options_.fetchers; ++i) {
    fetchers_.emplace_back([this, i] { fetchLoop(i); });
  }
}

std::unique_ptr<Loader> Loader::open(
    const std::string& metadata,
    std::string stream,
    LoaderOptions options) {
  auto kiln = std::make_shared<kiln::client::Client>(
      kiln::client::ClientOptions{
          .metadata = kiln::MetadataAddress::parse(metadata)});
  auto registry = std::make_shared<Registry>(kiln);
  std::shared_ptr<StreamClient> client = folly::coro::blockingWait(
      StreamClient::co_open(std::move(registry), std::move(stream)));
  return std::make_unique<Loader>(
      std::move(client), std::move(kiln), std::move(options));
}

Loader::~Loader() {
  {
    std::lock_guard lock(mutex_);
    stopping_ = true;
  }
  spaceFree_.notify_all();
  for (auto& fetcher : fetchers_) {
    fetcher.join();
  }
}

std::optional<Batch> Loader::next() {
  std::unique_lock lock(mutex_);
  if (batches_.empty()) {
    const auto start = Clock::now();
    batchesReady_.wait(lock, [this] {
      return !batches_.empty() || runningFetchers_ == 0;
    });
    stats_.waitTime += Clock::now() - start;
  }
  if (batches_.empty()) {
    return std::nullopt;
  }
  auto batch = std::move(batches_.front());
  batches_.pop_front();
  ++stats_.batches;
  lock.unlock();
  spaceFree_.notify_one();
  return batch;
}

LoaderStats Loader::stats() const {
  std::lock_guard lock(mutex_);
  auto result = stats_;
  result.buffered = batches_.size();
  return result;
}

bool Loader::wantsChunks() const {
  if (batches_.size() >= options_.minBufferedBatches) {
    return false;
  }
  if (options_.maxBufferedBatches == 0) {
    return true;
  }
  // Before the first chunk the batches per chunk are unknown: one take at a
  // time.
  if (stats_.chunks == 0) {
    return fetching_ == 0;
  }
  // The batches the takes in flight will likely add.
  const double perChunk =
      static_cast<double>(batchesRead_) / static_cast<double>(stats_.chunks);
  const double expected =
      static_cast<double>(fetching_ * options_.chunksPerTake) * perChunk;
  return static_cast<double>(batches_.size()) + expected <
      static_cast<double>(options_.maxBufferedBatches);
}

std::optional<uint32_t> Loader::pickShard(std::mt19937_64& random) {
  std::lock_guard lock(mutex_);
  const auto open = stream_->numShards() - endedShards_;
  if (open == 0) {
    return std::nullopt;
  }
  // The n-th shard that has not ended.
  auto n = std::uniform_int_distribution<uint32_t>(0, open - 1)(random);
  for (uint32_t shard = 0; shard < ended_.size(); ++shard) {
    if (!ended_[shard] && n-- == 0) {
      return shard;
    }
  }
  return std::nullopt;
}

void Loader::fetchLoop(uint32_t index) {
  std::mt19937_64 random(
      options_.seed != 0 ? options_.seed + index : std::random_device{}());
  while (true) {
    {
      std::unique_lock lock(mutex_);
      spaceFree_.wait(lock, [this] {
        return stopping_ || endedShards_ == ended_.size() || wantsChunks();
      });
      if (stopping_ || endedShards_ == ended_.size()) {
        break;
      }
      ++fetching_;
    }
    bool failed = false;
    try {
      folly::coro::blockingWait(co_fetch(random));
    } catch (const std::exception& e) {
      LOG(WARNING) << "stoke loader: " << e.what();
      failed = true;
    }
    {
      std::lock_guard lock(mutex_);
      --fetching_;
      if (failed) {
        ++stats_.errors;
      }
    }
    // Fewer takes in flight may let another fetcher go.
    spaceFree_.notify_all();
    if (failed) {
      std::this_thread::sleep_for(kRetryDelay);
    }
  }
  {
    std::lock_guard lock(mutex_);
    --runningFetchers_;
  }
  batchesReady_.notify_all();
}

folly::coro::Task<void> Loader::co_fetch(std::mt19937_64& random) {
  const auto& name = stream_->stream();
  TakeResult result;
  uint32_t shard = 0;
  // Two shards without waiting, then a wait at a third.
  for (int attempt = 0; attempt < 3; ++attempt) {
    auto picked = pickShard(random);
    if (!picked) {
      co_return;
    }
    shard = *picked;
    const auto wait =
        attempt < 2 ? std::chrono::milliseconds(0) : options_.wait;
    const auto start = Clock::now();
    result = co_await stream_->shard(shard).co_take(
        name, options_.epoch, options_.chunksPerTake, wait);
    std::lock_guard lock(mutex_);
    ++stats_.takes;
    stats_.takeTime += Clock::now() - start;
    if (result.end && !ended_[shard]) {
      ended_[shard] = true;
      ++endedShards_;
    }
    if (!result.chunks.empty()) {
      break;
    }
    ++stats_.emptyTakes;
  }
  if (result.end) {
    // The last shard's end lets every fetcher and next() finish.
    spaceFree_.notify_all();
    batchesReady_.notify_all();
  }

  for (const auto& chunk : result.chunks) {
    try {
      auto info = co_await kiln_->co_stat(chunk);
      std::shared_ptr<const kiln::Buffer> data;
      std::vector<format::BatchView> views;
      if (info.size > 0) {
        auto buffer = kiln_->allocateBuffer(info.size);
        buffer =
            co_await kiln_->co_read(chunk, 0, info.size, std::move(buffer));
        // A read may return fewer bytes than asked for.
        for (uint64_t done = buffer.size(); done < info.size;) {
          auto rest = kiln_->registerMemory(
              buffer.data() + done, info.size - done);
          rest = co_await kiln_->co_read(
              chunk, done, info.size - done, std::move(rest));
          if (rest.size() == 0) {
            throw kiln::Error(
                kiln::ErrorCode::kInternal,
                "chunk " + chunk.toString() + " is cut short");
          }
          done += rest.size();
        }
        buffer.setSize(info.size);
        data = std::make_shared<const kiln::Buffer>(std::move(buffer));
        views = format::parseChunk({data->data(), data->size()});
      }
      if (options_.removeChunks) {
        co_await kiln_->co_remove(chunk);
      }
      {
        std::lock_guard lock(mutex_);
        ++stats_.chunks;
        stats_.bytes += info.size;
        batchesRead_ += views.size();
        for (auto& view : views) {
          batches_.push_back(Batch{std::move(view), data});
        }
        stats_.peakBuffered =
            std::max<uint64_t>(stats_.peakBuffered, batches_.size());
      }
      batchesReady_.notify_all();
    } catch (const kiln::Error& e) {
      // Acked below with the others: at most once.
      LOG(WARNING) << "stoke loader: lost chunk " << chunk.toString() << ": "
                   << e.what();
      std::lock_guard lock(mutex_);
      ++stats_.lostChunks;
    } catch (const std::invalid_argument& e) {
      LOG(WARNING) << "stoke loader: malformed chunk " << chunk.toString()
                   << ": " << e.what();
      std::lock_guard lock(mutex_);
      ++stats_.lostChunks;
    }
  }
  if (!result.chunks.empty()) {
    co_await stream_->shard(shard).co_ack(name, options_.epoch, result.chunks);
  }
}

} // namespace stoke
