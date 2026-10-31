// Producers and consumers of a stream over shard servers reached over RPC.

#include "stoke/client/StreamClient.h"

#include <atomic>
#include <mutex>
#include <set>
#include <thread>

#include <folly/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"
#include "kiln/local/LocalCluster.h"
#include "stoke/rpc/Rpc.h"
#include "stoke/shard/Shard.h"

namespace stoke {
namespace {

using folly::coro::blockingWait;
using std::chrono::milliseconds;

constexpr int kShards = 3;

class StreamClientTest : public testing::Test {
 protected:
  StreamClientTest()
      : cluster_{kiln::local::LocalClusterOptions{
            .dramServers = 1, .localFsServers = 0, .regionSize = 16 << 20}},
        registry_{std::make_shared<Registry>(
            std::make_shared<kiln::client::Client>(kiln::client::ClientOptions{
                .metadata = cluster_.metadataEndpoint()}))} {
    std::vector<std::string> endpoints;
    for (int i = 0; i < kShards; ++i) {
      shards_.push_back(std::make_shared<Shard>(
          registry_, ShardOptions{.refreshInterval = milliseconds(20)}));
      servers_.push_back(rpc::serveShard(shards_.back(), "127.0.0.1", 0));
      endpoints.push_back(
          "127.0.0.1:" + std::to_string(servers_.back()->port()));
    }
    blockingWait(registry_->co_create("s", endpoints, 1.0));
  }

  kiln::local::LocalCluster cluster_;
  std::shared_ptr<Registry> registry_;
  std::vector<std::shared_ptr<Shard>> shards_;
  std::vector<std::unique_ptr<rpc::RpcServer>> servers_;
};

TEST_F(StreamClientTest, rejectsUnknownStreams) {
  try {
    blockingWait(StreamClient::co_open(registry_, "nope"));
    FAIL();
  } catch (const kiln::Error& e) {
    EXPECT_EQ(e.code(), kiln::ErrorCode::kNotFound);
  }
  auto client = blockingWait(StreamClient::co_open(registry_, "s"));
  try {
    blockingWait(client->shard(0).co_stats("nope"));
    FAIL();
  } catch (const kiln::Error& e) {
    // Over RPC too.
    EXPECT_EQ(e.code(), kiln::ErrorCode::kNotFound);
  }
}

// A producer waits at the chunk's shard, over RPC, until a consumer acks
// it.
TEST_F(StreamClientTest, awaitsConsumptionAtTheChunksShard) {
  auto client = blockingWait(StreamClient::co_open(registry_, "s"));
  const kiln::ChunkId chunk{0, 42};
  blockingWait(client->co_put(chunk));
  EXPECT_FALSE(blockingWait(client->co_awaitConsumed(chunk, milliseconds(0))));

  auto& shard = client->shard(shardOf(chunk, kShards));
  Epoch epoch{"consumer", 1};
  std::thread consumer([&] {
    std::this_thread::sleep_for(milliseconds(50));
    auto taken = blockingWait(shard.co_take("s", epoch, 1, milliseconds(0)));
    blockingWait(shard.co_ack("s", epoch, taken.chunks));
  });
  EXPECT_TRUE(
      blockingWait(client->co_awaitConsumed(chunk, std::chrono::seconds(5))));
  consumer.join();
}

// Producers put chunks (each twice, as retried tasks would) while consumers
// take from random shards, as the dataloader does; then the stream is
// sealed. Every chunk is consumed exactly once and every consumer sees the
// end.
TEST_F(StreamClientTest, consumersGetEveryChunkOnce) {
  constexpr uint64_t kChunks = 600;
  constexpr int kProducers = 3;
  constexpr int kConsumers = 4;
  auto client = blockingWait(StreamClient::co_open(registry_, "s"));
  ASSERT_EQ(client->numShards(), kShards);

  std::mutex mutex;
  std::multiset<kiln::ChunkId> consumed;
  std::vector<std::thread> consumers;
  for (int c = 0; c < kConsumers; ++c) {
    consumers.emplace_back([&, c] {
      Epoch epoch{"consumer" + std::to_string(c), 1};
      std::vector<bool> ended(kShards);
      uint32_t next = c;
      while (std::count(ended.begin(), ended.end(), true) < kShards) {
        auto shard = next++ % kShards;
        if (ended[shard]) {
          continue;
        }
        auto result = blockingWait(
            client->shard(shard).co_take("s", epoch, 8, milliseconds(50)));
        if (result.end) {
          ended[shard] = true;
          continue;
        }
        {
          std::lock_guard lock(mutex);
          consumed.insert(result.chunks.begin(), result.chunks.end());
        }
        blockingWait(client->shard(shard).co_ack("s", epoch, result.chunks));
      }
    });
  }

  std::atomic<uint64_t> fresh{0};
  std::vector<std::thread> producers;
  for (int p = 0; p < kProducers; ++p) {
    producers.emplace_back([&, p] {
      for (uint64_t i = p; i < kChunks; i += kProducers) {
        auto bytes = std::to_string(i);
        auto id = kiln::ChunkId::of(folly::StringPiece(bytes));
        fresh += blockingWait(client->co_put(id)) ? 1 : 0;
        blockingWait(client->co_put(id));
      }
    });
  }
  for (auto& producer : producers) {
    producer.join();
  }
  EXPECT_EQ(fresh, kChunks);
  blockingWait(registry_->co_seal("s"));
  for (auto& consumer : consumers) {
    consumer.join();
  }

  EXPECT_EQ(consumed.size(), kChunks);
  EXPECT_EQ(std::set(consumed.begin(), consumed.end()).size(), kChunks);
  ShardStats total;
  for (const auto& stats : blockingWait(client->co_stats())) {
    // Chunks spread over the shards.
    EXPECT_GT(stats.put, kChunks / kShards / 2);
    total += stats;
  }
  EXPECT_EQ(total.put, kChunks);
  EXPECT_EQ(total.duplicates, kChunks);
  EXPECT_EQ(total.acked, kChunks);
  EXPECT_EQ(total.depth, 0);
  EXPECT_TRUE(total.ended);
}

} // namespace
} // namespace stoke
