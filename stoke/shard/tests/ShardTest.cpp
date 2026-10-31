#include "stoke/shard/Shard.h"

#include <thread>

#include <folly/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"
#include "kiln/local/LocalCluster.h"
#include "stoke/client/Registry.h"

namespace stoke {
namespace {

using folly::coro::blockingWait;
using std::chrono::milliseconds;

const Epoch kEpoch{"job", 1};

kiln::ChunkId chunk(uint64_t i) {
  return kiln::ChunkId{0, i};
}

class ShardTest : public testing::Test {
 protected:
  ShardTest()
      : cluster_{kiln::local::LocalClusterOptions{
            .dramServers = 1, .localFsServers = 0, .regionSize = 16 << 20}},
        registry_{std::make_shared<Registry>(
            std::make_shared<kiln::client::Client>(kiln::client::ClientOptions{
                .metadata = cluster_.metadataEndpoint()}))} {
    blockingWait(registry_->co_create("s", {"localhost:1"}, 0.5));
  }

  std::unique_ptr<Shard> makeShard(milliseconds leaseTimeout = {}) {
    ShardOptions options{.refreshInterval = milliseconds(0)};
    if (leaseTimeout.count() > 0) {
      options.leaseTimeout = leaseTimeout;
    }
    return std::make_unique<Shard>(registry_, options);
  }

  kiln::local::LocalCluster cluster_;
  std::shared_ptr<Registry> registry_;
};

TEST_F(ShardTest, putsAndTakesOnce) {
  auto shard = makeShard();
  for (uint64_t i = 0; i < 5; ++i) {
    EXPECT_TRUE(blockingWait(shard->co_put("s", chunk(i))));
  }
  EXPECT_FALSE(blockingWait(shard->co_put("s", chunk(3))));

  auto first = blockingWait(shard->co_take("s", kEpoch, 3, milliseconds(0)));
  EXPECT_EQ(first.chunks, (std::vector{chunk(0), chunk(1), chunk(2)}));
  EXPECT_FALSE(first.end);
  auto second = blockingWait(shard->co_take("s", kEpoch, 3, milliseconds(0)));
  EXPECT_EQ(second.chunks, (std::vector{chunk(3), chunk(4)}));
  auto empty = blockingWait(shard->co_take("s", kEpoch, 3, milliseconds(0)));
  EXPECT_TRUE(empty.chunks.empty());
  EXPECT_FALSE(empty.end);

  blockingWait(shard->co_ack("s", kEpoch, first.chunks));
  auto stats = blockingWait(shard->co_stats("s"));
  EXPECT_EQ(stats.put, 5);
  EXPECT_EQ(stats.duplicates, 1);
  EXPECT_EQ(stats.taken, 5);
  EXPECT_EQ(stats.acked, 3);
  EXPECT_EQ(stats.leased, 2);
  EXPECT_EQ(stats.depth, 0);
}

TEST_F(ShardTest, rejectsUnknownStreamsAndBadArguments) {
  auto shard = makeShard();
  try {
    blockingWait(shard->co_put("nope", chunk(1)));
    FAIL();
  } catch (const kiln::Error& e) {
    EXPECT_EQ(e.code(), kiln::ErrorCode::kNotFound);
  }
  EXPECT_THROW(
      blockingWait(shard->co_take("s", kEpoch, 0, milliseconds(0))),
      kiln::Error);
  EXPECT_THROW(
      blockingWait(shard->co_take("s", kEpoch, 1, std::chrono::minutes(1))),
      kiln::Error);
}

TEST_F(ShardTest, waitingTakeGetsTheNextPut) {
  auto shard = makeShard();
  std::thread producer([&] {
    std::this_thread::sleep_for(milliseconds(100));
    blockingWait(shard->co_put("s", chunk(7)));
  });
  auto start = std::chrono::steady_clock::now();
  auto result =
      blockingWait(shard->co_take("s", kEpoch, 10, std::chrono::seconds(5)));
  auto waited = std::chrono::steady_clock::now() - start;
  producer.join();
  EXPECT_EQ(result.chunks, std::vector{chunk(7)});
  EXPECT_LT(waited, std::chrono::seconds(4));
  EXPECT_EQ(blockingWait(shard->co_stats("s")).waiters, 0);
}

TEST_F(ShardTest, waitingTakeTimesOut) {
  auto shard = makeShard();
  auto start = std::chrono::steady_clock::now();
  auto result =
      blockingWait(shard->co_take("s", kEpoch, 10, milliseconds(100)));
  EXPECT_GE(std::chrono::steady_clock::now() - start, milliseconds(100));
  EXPECT_TRUE(result.chunks.empty());
  EXPECT_FALSE(result.end);
  // The next put is queued, not handed to the take that gave up.
  blockingWait(shard->co_put("s", chunk(1)));
  EXPECT_EQ(blockingWait(shard->co_stats("s")).depth, 1);
}

TEST_F(ShardTest, releaseReturnsAnEpochsLeases) {
  auto shard = makeShard();
  for (uint64_t i = 0; i < 4; ++i) {
    blockingWait(shard->co_put("s", chunk(i)));
  }
  auto taken = blockingWait(shard->co_take("s", kEpoch, 3, milliseconds(0)));
  blockingWait(shard->co_ack("s", kEpoch, {taken.chunks[0]}));
  // Acks of another epoch do nothing.
  Epoch next{"job", 2};
  blockingWait(shard->co_ack("s", next, {taken.chunks[1]}));

  EXPECT_EQ(blockingWait(shard->co_release("s", kEpoch)), 2);
  auto retaken = blockingWait(shard->co_take("s", next, 10, milliseconds(0)));
  EXPECT_EQ(retaken.chunks.size(), 3);
  auto stats = blockingWait(shard->co_stats("s"));
  EXPECT_EQ(stats.released, 2);
  EXPECT_EQ(stats.taken, 4);
}

TEST_F(ShardTest, expiredLeasesGoBackToTheQueue) {
  auto shard = makeShard(milliseconds(50));
  blockingWait(shard->co_put("s", chunk(1)));
  blockingWait(shard->co_take("s", kEpoch, 1, milliseconds(0)));
  std::this_thread::sleep_for(milliseconds(100));
  blockingWait(shard->co_refresh());
  auto stats = blockingWait(shard->co_stats("s"));
  EXPECT_EQ(stats.expired, 1);
  EXPECT_EQ(stats.depth, 1);
  // A late ack of the expired lease does nothing.
  blockingWait(shard->co_ack("s", kEpoch, {chunk(1)}));
  EXPECT_EQ(blockingWait(shard->co_stats("s")).acked, 0);
}

TEST_F(ShardTest, sealedStreamEndsAfterStopFraction) {
  auto shard = makeShard();
  for (uint64_t i = 0; i < 4; ++i) {
    blockingWait(shard->co_put("s", chunk(i)));
  }
  blockingWait(registry_->co_seal("s"));
  blockingWait(shard->co_refresh());
  // stopFraction 0.5: two of the four chunks, then the end.
  auto taken = blockingWait(shard->co_take("s", kEpoch, 2, milliseconds(0)));
  EXPECT_EQ(taken.chunks.size(), 2);
  EXPECT_FALSE(taken.end);
  auto end = blockingWait(shard->co_take("s", kEpoch, 2, milliseconds(0)));
  EXPECT_TRUE(end.chunks.empty());
  EXPECT_TRUE(end.end);
  EXPECT_TRUE(blockingWait(shard->co_stats("s")).ended);
}

TEST_F(ShardTest, sealEndsWaitingTakes) {
  auto shard = makeShard();
  std::thread sealer([&] {
    std::this_thread::sleep_for(milliseconds(100));
    blockingWait(registry_->co_seal("s"));
    blockingWait(shard->co_refresh());
  });
  auto result =
      blockingWait(shard->co_take("s", kEpoch, 1, std::chrono::seconds(5)));
  sealer.join();
  EXPECT_TRUE(result.end);
}

TEST_F(ShardTest, removedStreamIsForgotten) {
  auto shard = makeShard();
  blockingWait(shard->co_put("s", chunk(1)));
  EXPECT_TRUE(blockingWait(registry_->co_remove("s")));
  blockingWait(shard->co_refresh());
  EXPECT_THROW(blockingWait(shard->co_stats("s")), kiln::Error);
}

TEST_F(ShardTest, refreshesOnItsOwn) {
  auto shard = std::make_shared<Shard>(
      registry_, ShardOptions{.refreshInterval = milliseconds(20)});
  blockingWait(shard->co_put("s", chunk(1)));
  blockingWait(registry_->co_seal("s"));
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!blockingWait(shard->co_stats("s")).sealed &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(milliseconds(10));
  }
  EXPECT_TRUE(blockingWait(shard->co_stats("s")).sealed);
}

} // namespace
} // namespace stoke
