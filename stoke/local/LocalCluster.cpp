#include "stoke/local/LocalCluster.h"

#include <algorithm>

#include "kiln/client/Client.h"
#include "stoke/client/Registry.h"
#include "stoke/rpc/Rpc.h"
#include "stoke/shard/Shard.h"

namespace stoke {

namespace {

constexpr uint64_t kRegionSize = 64 << 20;

kiln::local::LocalClusterOptions kilnOptions(
    const LocalClusterOptions& options) {
  const uint64_t regions = (options.memory + kRegionSize - 1) / kRegionSize;
  return {
      .dramServers = 1,
      .localFsServers = 0,
      .regionsPerServer = static_cast<uint32_t>(std::max<uint64_t>(1, regions)),
      .regionSize = kRegionSize,
      .metadata = {.gracePeriod = options.gracePeriod}};
}

} // namespace

LocalCluster::LocalCluster(LocalClusterOptions options)
    : kiln_{kilnOptions(options)} {
  auto registry = std::make_shared<Registry>(
      std::make_shared<kiln::client::Client>(
          kiln::client::ClientOptions{.metadata = kiln_.metadataAddress()}));
  shard_ = rpc::serveShard(
      std::make_shared<Shard>(std::move(registry)), "127.0.0.1", 0);
  shardEndpoint_ = "127.0.0.1:" + std::to_string(shard_->port());
}

LocalCluster::~LocalCluster() = default;

} // namespace stoke
