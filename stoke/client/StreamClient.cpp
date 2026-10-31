#include "stoke/client/StreamClient.h"

#include <folly/coro/Collect.h>

#include "kiln/common/Error.h"
#include "stoke/rpc/Rpc.h"

namespace stoke {

folly::coro::Task<std::unique_ptr<StreamClient>> StreamClient::co_open(
    std::shared_ptr<Registry> registry,
    std::string stream) {
  auto info = co_await registry->co_get(stream);
  if (!info) {
    throw kiln::Error(kiln::ErrorCode::kNotFound, "no stream " + stream);
  }
  std::vector<std::shared_ptr<rpc::ShardRpc>> shards;
  for (const auto& shard : info->shards) {
    shards.push_back(rpc::connectShard(kiln::Endpoint::parse(shard)));
  }
  co_return std::make_unique<StreamClient>(
      std::move(stream), std::move(*info), std::move(shards));
}

StreamClient::StreamClient(
    std::string stream,
    StreamInfo info,
    std::vector<std::shared_ptr<rpc::ShardRpc>> shards)
    : stream_{std::move(stream)},
      info_{std::move(info)},
      shards_{std::move(shards)} {
  if (shards_.empty()) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument, "a stream needs at least one shard");
  }
}

folly::coro::Task<bool> StreamClient::co_put(kiln::ChunkId chunk) {
  auto& shard = *shards_[shardOf(chunk, numShards())];
  co_return co_await shard.co_put(stream_, chunk);
}

folly::coro::Task<std::vector<ShardStats>> StreamClient::co_stats() {
  std::vector<folly::coro::Task<ShardStats>> calls;
  calls.reserve(shards_.size());
  for (const auto& shard : shards_) {
    calls.push_back(shard->co_stats(stream_));
  }
  co_return co_await folly::coro::collectAllRange(std::move(calls));
}

} // namespace stoke
