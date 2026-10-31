#include "stoke/common/Stream.h"

#include <gtest/gtest.h>

#include "kiln/common/Error.h"

namespace stoke {
namespace {

TEST(StreamTest, encodesAndDecodes) {
  StreamInfo info{
      .bucket = 42,
      .shards = {"host-a:7480", "host-b:7480"},
      .state = StreamState::kSealed,
      .stopFraction = 0.9};
  EXPECT_EQ(StreamInfo::decode(info.encode()), info);
  EXPECT_EQ(StreamInfo::decode(StreamInfo{}.encode()), StreamInfo{});
}

TEST(StreamTest, rejectsMalformedInfo) {
  EXPECT_THROW(StreamInfo::decode("not json"), kiln::Error);
  EXPECT_THROW(StreamInfo::decode("{}"), kiln::Error);
  EXPECT_THROW(
      StreamInfo::decode(
          R"({"bucket":1,"shards":[],"state":"closed","stopFraction":1})"),
      kiln::Error);
}

TEST(StreamTest, rejectsInfoTooLargeForARef) {
  StreamInfo info;
  info.shards.assign(100, "a-rather-long-host-name.example.com:7480");
  EXPECT_THROW(info.encode(), kiln::Error);
}

TEST(StreamTest, checksNames) {
  EXPECT_EQ(streamRefName("fineweb-edu_v1.2"), "stream/fineweb-edu_v1.2");
  EXPECT_THROW(streamRefName(""), kiln::Error);
  EXPECT_THROW(streamRefName("a/b"), kiln::Error);
  EXPECT_THROW(streamRefName(std::string(129, 'a')), kiln::Error);
}

TEST(StreamTest, spreadsChunksOverShards) {
  std::vector<int> counts(4);
  for (int i = 0; i < 4000; ++i) {
    auto bytes = std::to_string(i);
    auto id = kiln::ChunkId::of(folly::StringPiece(bytes));
    auto shard = shardOf(id, 4);
    ASSERT_LT(shard, 4u);
    EXPECT_EQ(shardOf(id, 4), shard);
    ++counts[shard];
  }
  for (int count : counts) {
    EXPECT_GT(count, 800);
  }
  EXPECT_THROW(shardOf(kiln::ChunkId{}, 0), kiln::Error);
}

} // namespace
} // namespace stoke
