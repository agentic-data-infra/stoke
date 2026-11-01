#include "stoke/loader/Loader.h"

#include <cstring>
#include <set>
#include <thread>

#include <folly/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"
#include "kiln/local/LocalCluster.h"
#include "stoke/client/Producer.h"
#include "stoke/shard/Shard.h"

namespace stoke {
namespace {

using folly::coro::blockingWait;
using std::chrono::milliseconds;

constexpr uint32_t kShards = 2;
constexpr uint64_t kBatchRows = 4;
constexpr uint64_t kSequence = 16;

// A producer puts chunks of framed int32 batches; batch b holds b in every
// token.
class LoaderTest : public testing::Test {
 protected:
  LoaderTest()
      : cluster_{kiln::local::LocalClusterOptions{
            .dramServers = 1, .localFsServers = 0, .regionSize = 64 << 20}},
        kiln_{std::make_shared<kiln::client::Client>(
            kiln::client::ClientOptions{
                .metadata = cluster_.metadataEndpoint()})},
        registry_{std::make_shared<Registry>(kiln_)} {
    std::vector<std::string> names;
    std::vector<std::shared_ptr<rpc::ShardRpc>> shards;
    for (uint32_t i = 0; i < kShards; ++i) {
      names.push_back("shard" + std::to_string(i) + ":1");
      shards_.push_back(std::make_shared<Shard>(
          registry_, ShardOptions{.refreshInterval = milliseconds(20)}));
      shards.push_back(shards_.back());
    }
    auto info = blockingWait(registry_->co_create("s", names, 1.0));
    stream_ = std::make_shared<StreamClient>("s", info, shards);
    producer_ = std::make_shared<Producer>(stream_, kiln_);
  }

  // Puts a chunk of 'batches' batches, numbered from 'first'.
  kiln::ChunkId produce(int32_t first, int batches) {
    std::string chunk;
    for (int b = 0; b < batches; ++b) {
      std::vector<int32_t> tokens(kBatchRows * kSequence, first + b);
      std::span<const uint8_t> payload(
          reinterpret_cast<const uint8_t*>(tokens.data()),
          tokens.size() * sizeof(int32_t));
      format::appendFrame(
          chunk,
          format::encodeBatch(
              {.dtype = format::DType::kInt32,
               .shape = {kBatchRows, kSequence}},
              payload));
    }
    auto id = blockingWait(producer_->co_put(chunk));
    return id;
  }

  std::unique_ptr<Loader> makeLoader(Epoch epoch, uint32_t fetchers = 2) {
    return std::make_unique<Loader>(
        stream_,
        kiln_,
        LoaderOptions{
            .epoch = std::move(epoch),
            .fetchers = fetchers,
            .minBufferedBatches = 8,
            .wait = milliseconds(50),
            .seed = 1});
  }

  kiln::local::LocalCluster cluster_;
  std::shared_ptr<kiln::client::Client> kiln_;
  std::shared_ptr<Registry> registry_;
  std::vector<std::shared_ptr<Shard>> shards_;
  std::shared_ptr<StreamClient> stream_;
  std::shared_ptr<Producer> producer_;
};

TEST_F(LoaderTest, readsEveryBatchOnceThenEnds) {
  constexpr int kChunks = 20;
  constexpr int kBatchesPerChunk = 5;
  auto loader = makeLoader({"rank0", 0});
  std::thread producer([&] {
    for (int c = 0; c < kChunks; ++c) {
      produce(c * kBatchesPerChunk, kBatchesPerChunk);
      if (c % 5 == 0) {
        std::this_thread::sleep_for(milliseconds(20));
      }
    }
    blockingWait(registry_->co_seal("s"));
  });

  std::set<int32_t> seen;
  while (auto batch = loader->next()) {
    EXPECT_EQ(batch->view.header.dtype, format::DType::kInt32);
    EXPECT_EQ(
        batch->view.header.shape,
        (std::vector<uint64_t>{kBatchRows, kSequence}));
    const auto* tokens =
        reinterpret_cast<const int32_t*>(batch->view.payload.data());
    EXPECT_EQ(reinterpret_cast<uintptr_t>(tokens) % alignof(int32_t), 0);
    EXPECT_EQ(tokens[0], tokens[kBatchRows * kSequence - 1]);
    EXPECT_TRUE(seen.insert(tokens[0]).second);
  }
  producer.join();
  EXPECT_EQ(seen.size(), kChunks * kBatchesPerChunk);
  // next() keeps returning the end.
  EXPECT_FALSE(loader->next());

  auto stats = loader->stats();
  EXPECT_EQ(stats.chunks, kChunks);
  EXPECT_EQ(stats.batches, kChunks * kBatchesPerChunk);
  EXPECT_EQ(stats.lostChunks, 0);
  // Read chunks were removed from Kiln and acked at the shards.
  ShardStats total;
  for (const auto& shardStats : blockingWait(stream_->co_stats())) {
    total += shardStats;
  }
  EXPECT_EQ(total.acked, kChunks);
  EXPECT_EQ(total.leased, 0);
}

// With a maximum, the buffer stays under it but for part of a chunk, even
// with many fetchers and a slow trainer.
TEST_F(LoaderTest, keepsTheBufferUnderTheMaximum) {
  constexpr int kChunks = 30;
  constexpr int kBatchesPerChunk = 4;
  constexpr size_t kMax = 12;
  for (int c = 0; c < kChunks; ++c) {
    produce(c * kBatchesPerChunk, kBatchesPerChunk);
  }
  blockingWait(registry_->co_seal("s"));
  Loader loader(
      stream_,
      kiln_,
      LoaderOptions{
          .epoch = {"rank0", 0},
          .fetchers = 4,
          .minBufferedBatches = 8,
          .maxBufferedBatches = kMax,
          .wait = milliseconds(50),
          .seed = 1});
  int batches = 0;
  while (loader.next()) {
    ++batches;
    std::this_thread::sleep_for(milliseconds(1));
  }
  EXPECT_EQ(batches, kChunks * kBatchesPerChunk);
  EXPECT_LT(loader.stats().peakBuffered, kMax + kBatchesPerChunk);
  EXPECT_EQ(loader.stats().buffered, 0);
}

TEST_F(LoaderTest, rejectsAMaximumUnderTheMinimum) {
  EXPECT_THROW(
      Loader(
          stream_,
          kiln_,
          LoaderOptions{
              .epoch = {"rank0", 0},
              .minBufferedBatches = 8,
              .maxBufferedBatches = 4}),
      kiln::Error);
}

TEST_F(LoaderTest, batchesOutliveTheLoader) {
  auto id = produce(42, 1);
  std::optional<Batch> batch;
  {
    auto loader = makeLoader({"rank0", 0}, 1);
    batch = loader->next();
  }
  ASSERT_TRUE(batch);
  EXPECT_EQ(
      reinterpret_cast<const int32_t*>(batch->view.payload.data())[5], 42);
  EXPECT_THROW(blockingWait(kiln_->co_stat(id)), kiln::Error);
}

TEST_F(LoaderTest, restartReleasesThePreviousIncarnation) {
  for (int c = 0; c < 4; ++c) {
    produce(c, 1);
  }
  // A crashed incarnation leaves chunks leased and unread.
  for (uint32_t i = 0; i < kShards; ++i) {
    blockingWait(stream_->shard(i).co_take("s", {"rank0", 3}, 10, {}));
  }
  blockingWait(registry_->co_seal("s"));

  auto loader = makeLoader({"rank0", 4});
  int batches = 0;
  while (loader->next()) {
    ++batches;
  }
  EXPECT_EQ(batches, 4);
}

TEST_F(LoaderTest, skipsChunksThatAreGone) {
  auto id = produce(1, 2);
  blockingWait(kiln_->co_remove(id));
  produce(10, 2);
  blockingWait(registry_->co_seal("s"));
  auto loader = makeLoader({"rank0", 0});
  int batches = 0;
  while (loader->next()) {
    ++batches;
  }
  EXPECT_EQ(batches, 2);
  EXPECT_EQ(loader->stats().lostChunks, 1);
}

} // namespace
} // namespace stoke
