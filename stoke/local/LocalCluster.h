#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "kiln/local/LocalCluster.h"

namespace stoke {

namespace rpc {
class RpcServer;
}

struct LocalClusterOptions {
  // Kiln's DRAM for chunks, rounded up to whole regions.
  uint64_t memory{uint64_t{1} << 30};
  // Kiln's grace period: blocks of removed chunks are reused after it.
  // Short, as loaders remove chunks once read.
  std::chrono::milliseconds gracePeriod{std::chrono::seconds(2)};
};

// Kiln (a metadata server and one DRAM server) and one stoke shard in this
// process, so that a stream runs on one machine without servers to start:
// stoke.LocalStream, and tests. Both serve on 127.0.0.1, so producers and
// loaders reach them as they would a real cluster. Stops them when
// destroyed.
class LocalCluster {
 public:
  explicit LocalCluster(LocalClusterOptions options = {});

  ~LocalCluster();

  // Kiln's metadata server, for ClientOptions::metadata, the loader and
  // Ember's kiln_metadata option.
  const kiln::MetadataAddress& metadata() const {
    return kiln_.metadataAddress();
  }

  // The shard's endpoint, for Registry::co_create.
  const std::string& shard() const {
    return shardEndpoint_;
  }

 private:
  kiln::local::LocalCluster kiln_;
  // Stopped before Kiln.
  std::unique_ptr<rpc::RpcServer> shard_;
  std::string shardEndpoint_;
};

} // namespace stoke
