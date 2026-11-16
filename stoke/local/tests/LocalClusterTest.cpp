#include "stoke/local/LocalCluster.h"

#include <cstring>
#include <future>

#include <folly/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"
#include "stoke/client/Producer.h"
#include "stoke/loader/Loader.h"

namespace stoke {
namespace {

using folly::coro::blockingWait;

// A chunk of one int32 batch holding 'value'.
std::string makeChunk(int32_t value) {
  std::string chunk;
  format::appendFrame(
      chunk,
      format::encodeBatch(
          {.dtype = format::DType::kInt32, .shape = {1}},
          {reinterpret_cast<const uint8_t*>(&value), sizeof(value)}));
  return chunk;
}

class LocalClusterTest : public testing::Test {
 protected:
  LocalClusterTest()
      : cluster_{LocalClusterOptions{.memory = 64 << 20}},
        kiln_{std::make_shared<kiln::client::Client>(
            kiln::client::ClientOptions{.metadata = cluster_.metadata()})},
        registry_{std::make_shared<Registry>(kiln_)} {}

  std::shared_ptr<Producer> makeProducer(uint32_t budget) {
    blockingWait(registry_->co_create("s", {cluster_.shard()}, 1.0, budget));
    std::shared_ptr<StreamClient> stream =
        blockingWait(StreamClient::co_open(registry_, "s"));
    return std::make_shared<Producer>(std::move(stream), kiln_);
  }

  LocalCluster cluster_;
  std::shared_ptr<kiln::client::Client> kiln_;
  std::shared_ptr<Registry> registry_;
};

TEST_F(LocalClusterTest, loaderReadsWhatProducerPut) {
  auto producer = makeProducer(0);
  for (int32_t i = 0; i < 10; ++i) {
    blockingWait(producer->co_put(makeChunk(i)));
  }
  blockingWait(registry_->co_seal("s"));

  auto loader = Loader::open(
      cluster_.metadata().toString(), "s", LoaderOptions{.epoch = {"job", 0}});
  int32_t sum = 0;
  int batches = 0;
  while (auto batch = loader->next()) {
    int32_t value;
    std::memcpy(&value, batch->view.payload.data(), sizeof(value));
    sum += value;
    ++batches;
  }
  EXPECT_EQ(batches, 10);
  EXPECT_EQ(sum, 45);
}

// A trainer that stops early removes the stream: a producer waiting for
// its budget then fails rather than waiting forever.
TEST_F(LocalClusterTest, removingStreamFailsWaitingProducer) {
  auto producer = makeProducer(1);
  blockingWait(producer->co_put(makeChunk(0)));
  auto waiting = std::async(std::launch::async, [&] {
    blockingWait(producer->co_put(makeChunk(1)));
  });
  EXPECT_EQ(
      waiting.wait_for(std::chrono::milliseconds(500)),
      std::future_status::timeout);

  ASSERT_TRUE(blockingWait(registry_->co_remove("s")));
  ASSERT_EQ(
      waiting.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  EXPECT_THROW(waiting.get(), kiln::Error);
}

} // namespace
} // namespace stoke
