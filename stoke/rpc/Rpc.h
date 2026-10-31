// stoke's RPC over fbthrift: serving a shard's operations (ShardRpc.h) and
// calling them at a remote shard.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "kiln/common/Types.h"
#include "stoke/rpc/ShardRpc.h"

namespace apache::thrift {
class AsyncProcessorFactory;
class ThriftServer;
} // namespace apache::thrift

namespace stoke::rpc {

// A server answering calls in its own threads until destroyed.
class RpcServer {
 public:
  ~RpcServer();

  RpcServer(const RpcServer&) = delete;
  RpcServer& operator=(const RpcServer&) = delete;

  // The port it listens on; the one it was given, or the one the OS chose
  // for port 0.
  uint16_t port() const;

 private:
  friend std::unique_ptr<RpcServer>
  serveShard(std::shared_ptr<ShardRpc>, const std::string&, uint16_t);

  RpcServer(
      std::shared_ptr<apache::thrift::AsyncProcessorFactory> service,
      const std::string& host,
      uint16_t port);

  void stop();

  // Serves on thread_ until stopped.
  const std::shared_ptr<apache::thrift::ThriftServer> server_;
  std::thread thread_;
  uint16_t port_{0};
};

// Serves 'shard' on 'host' (empty for all interfaces) and 'port' (0 for any
// free one). Throws kiln::Error(kUnavailable) if it cannot listen there.
std::unique_ptr<RpcServer> serveShard(
    std::shared_ptr<ShardRpc> shard,
    const std::string& host,
    uint16_t port);

// The operations of the shard at 'endpoint'. Throws
// kiln::Error(kUnavailable) if its host name does not resolve. Connects on
// the first call, and again after the connection broke; calls fail with
// kiln::Error(kUnavailable) if the shard cannot be reached. Safe to use from
// any thread.
std::shared_ptr<ShardRpc> connectShard(const kiln::Endpoint& endpoint);

} // namespace stoke::rpc
