#include "stoke/client/Producer.h"

#include <algorithm>
#include <atomic>
#include <thread>

#include <folly/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "kiln/client/Client.h"
#include "kiln/local/LocalCluster.h"
#include "stoke/shard/Shard.h"

namespace stoke {
namespace {

using folly::coro::blockingWait;
using std::chrono::milliseconds;

constexpr uint32_t kShards = 2;
const Epoch kEpoch{"consumer", 1};

// A stream over in-process shards, with a task budget. Kiln stores each
// chunk in a whole 1 MiB block and nothing here removes chunks, so the
// region holds every chunk the tests put.
class ProducerTest : public testing::Test {
 protected:
  ProducerTest()
      : cluster_{kiln::local::LocalClusterOptions{
            .dramServers = 1, .localFsServers = 0, .regionSize = 64 << 20}},
        kiln_{std::make_shared<kiln::client::Client>(
            kiln::client::ClientOptions{
                .metadata = cluster_.metadataEndpoint()})},
        registry_{std::make_shared<Registry>(kiln_)} {}

  std::shared_ptr<Producer> makeProducer(uint32_t budget) {
    std::vector<std::string> names;
    std::vector<std::shared_ptr<rpc::ShardRpc>> shards;
    for (uint32_t i = 0; i < kShards; ++i) {
      names.push_back("shard" + std::to_string(i) + ":1");
      shards_.push_back(std::make_shared<Shard>(
          registry_, ShardOptions{.refreshInterval = milliseconds(0)}));
      shards.push_back(shards_.back());
    }
    auto info = blockingWait(registry_->co_create("s", names, 1.0, budget));
    stream_ = std::make_shared<StreamClient>("s", info, shards);
    return std::make_shared<Producer>(stream_, kiln_);
  }

  // Takes 'chunk' from its shard and acks it.
  void consume(const kiln::ChunkId& chunk) {
    auto& shard = stream_->shard(shardOf(chunk, kShards));
    while (true) {
      auto taken = blockingWait(shard.co_take("s", kEpoch, 64, milliseconds(0)));
      blockingWait(shard.co_ack("s", kEpoch, {chunk}));
      // Others go back for later.
      blockingWait(shard.co_release("s", kEpoch));
      if (std::find(taken.chunks.begin(), taken.chunks.end(), chunk) !=
          taken.chunks.end()) {
        return;
      }
    }
  }

  // Chunks put and not acked, over the shards.
  uint64_t unacked() {
    uint64_t total = 0;
    for (const auto& stats : blockingWait(stream_->co_stats())) {
      total += stats.put - stats.acked;
    }
    return total;
  }

  kiln::local::LocalCluster cluster_;
  std::shared_ptr<kiln::client::Client> kiln_;
  std::shared_ptr<Registry> registry_;
  std::vector<std::shared_ptr<Shard>> shards_;
  std::shared_ptr<StreamClient> stream_;
};

TEST_F(ProducerTest, withoutABudgetNeverWaits) {
  auto producer = makeProducer(0);
  for (int i = 0; i < 10; ++i) {
    blockingWait(producer->co_put("chunk" + std::to_string(i)));
  }
  EXPECT_EQ(producer->unconsumed(), 0);
  EXPECT_EQ(producer->waited().count(), 0);
  EXPECT_EQ(unacked(), 10);
}

TEST_F(ProducerTest, waitsAtTheBudgetUntilTheOldestIsConsumed) {
  auto producer = makeProducer(2);
  auto oldest = blockingWait(producer->co_put("chunk0"));
  blockingWait(producer->co_put("chunk1"));
  EXPECT_EQ(producer->unconsumed(), 2);

  std::atomic<bool> done{false};
  std::thread third([&] {
    blockingWait(producer->co_put("chunk2"));
    done = true;
  });
  std::this_thread::sleep_for(milliseconds(200));
  EXPECT_FALSE(done);
  EXPECT_EQ(unacked(), 2);

  consume(oldest);
  third.join();
  EXPECT_EQ(producer->unconsumed(), 2);
  EXPECT_EQ(unacked(), 2);
  EXPECT_GE(producer->waited(), milliseconds(150));
}

// A producer far faster than its consumer never has more than the budget
// put and not acked.
TEST_F(ProducerTest, boundHoldsWithASlowConsumer) {
  constexpr uint32_t kBudget = 3;
  constexpr int kChunks = 30;
  auto producer = makeProducer(kBudget);
  std::thread feed([&] {
    for (int i = 0; i < kChunks; ++i) {
      blockingWait(producer->co_put("chunk" + std::to_string(i)));
    }
  });

  int consumed = 0;
  uint64_t most = 0;
  uint32_t next = 0;
  while (consumed < kChunks) {
    auto& shard = stream_->shard(next++ % kShards);
    auto taken = blockingWait(shard.co_take("s", kEpoch, 1, milliseconds(20)));
    most = std::max(most, unacked());
    if (taken.chunks.empty()) {
      continue;
    }
    std::this_thread::sleep_for(milliseconds(5));
    blockingWait(shard.co_ack("s", kEpoch, taken.chunks));
    ++consumed;
  }
  feed.join();
  EXPECT_LE(most, kBudget);
  EXPECT_GT(producer->waited().count(), 0);
}

} // namespace
} // namespace stoke
