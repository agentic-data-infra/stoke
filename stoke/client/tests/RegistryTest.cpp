#include "stoke/client/Registry.h"

#include <folly/coro/BlockingWait.h>
#include <gtest/gtest.h>

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"
#include "kiln/local/LocalCluster.h"

namespace stoke {
namespace {

using folly::coro::blockingWait;

class RegistryTest : public testing::Test {
 protected:
  RegistryTest()
      : cluster_{kiln::local::LocalClusterOptions{
            .dramServers = 1, .localFsServers = 0, .regionSize = 16 << 20}},
        registry_{std::make_shared<kiln::client::Client>(
            kiln::client::ClientOptions{
                .metadata = cluster_.metadataEndpoint()})} {}

  kiln::local::LocalCluster cluster_;
  Registry registry_;
};

TEST_F(RegistryTest, createsSealsAndRemoves) {
  auto created =
      blockingWait(registry_.co_create("s", {"a:1", "b:2"}, 0.9));
  EXPECT_NE(created.bucket, kiln::kDefaultBucket);
  EXPECT_EQ(created.state, StreamState::kOpen);
  EXPECT_EQ(created.taskBudget, 0);
  EXPECT_EQ(blockingWait(registry_.co_get("s")), created);
  EXPECT_EQ(blockingWait(registry_.co_list()), std::vector<std::string>{"s"});

  blockingWait(registry_.co_seal("s"));
  blockingWait(registry_.co_seal("s"));
  EXPECT_EQ(blockingWait(registry_.co_get("s"))->state, StreamState::kSealed);

  EXPECT_TRUE(blockingWait(registry_.co_remove("s")));
  EXPECT_FALSE(blockingWait(registry_.co_remove("s")));
  EXPECT_FALSE(blockingWait(registry_.co_get("s")));
  // The bucket went with it.
  EXPECT_FALSE(
      blockingWait(registry_.kiln().co_deleteBucket(created.bucket)));
}

TEST_F(RegistryTest, keepsTheTaskBudget) {
  blockingWait(registry_.co_create("s", {"a:1"}, 0.95, 8));
  EXPECT_EQ(blockingWait(registry_.co_get("s"))->taskBudget, 8);
}

TEST_F(RegistryTest, rejectsBadStreams) {
  blockingWait(registry_.co_create("s", {"a:1"}));
  EXPECT_THROW(blockingWait(registry_.co_create("s", {"a:1"})), kiln::Error);
  EXPECT_THROW(blockingWait(registry_.co_create("t", {})), kiln::Error);
  EXPECT_THROW(
      blockingWait(registry_.co_create("t", {"a:1"}, 1.5)), kiln::Error);
  EXPECT_THROW(blockingWait(registry_.co_create("a/b", {"a:1"})), kiln::Error);
  try {
    blockingWait(registry_.co_seal("t"));
    FAIL();
  } catch (const kiln::Error& e) {
    EXPECT_EQ(e.code(), kiln::ErrorCode::kNotFound);
  }
}

} // namespace
} // namespace stoke
