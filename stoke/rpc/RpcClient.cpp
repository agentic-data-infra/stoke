#include <fmt/format.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/io/async/AsyncSocket.h>
#include <thrift/lib/cpp2/async/PooledRequestChannel.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>

#include "stoke/rpc/Convert.h"
#include "stoke/rpc/Rpc.h"
#include "stoke/rpc/gen-cpp2/ShardServiceAsyncClient.h"

namespace stoke::rpc {

namespace {

constexpr int kConnectTimeoutMs = 10'000;

// The event base threads of every connection in the process.
std::weak_ptr<folly::IOExecutor> ioExecutor() {
  static auto* pool = new std::shared_ptr<folly::IOThreadPoolExecutor>(
      std::make_shared<folly::IOThreadPoolExecutor>(
          4, std::make_shared<folly::NamedThreadFactory>("StokeRpcIO")));
  return *pool;
}

// A generated client of 'Service' at 'endpoint', usable from any thread: its
// channel runs on the IO pool and connects (again) when needed.
template <typename Service>
std::unique_ptr<apache::thrift::Client<Service>> makeClient(
    const kiln::Endpoint& endpoint) {
  folly::SocketAddress address;
  try {
    address.setFromHostPort(endpoint.host, endpoint.port);
  } catch (const std::exception& e) {
    throw kiln::Error(
        kiln::ErrorCode::kUnavailable,
        fmt::format("Cannot resolve {}: {}", endpoint.toString(), e.what()));
  }
  auto channel = apache::thrift::PooledRequestChannel::newSyncChannel(
      ioExecutor(), [address](folly::EventBase& eventBase) {
        auto socket =
            folly::AsyncSocket::newSocket(&eventBase, address, kConnectTimeoutMs);
        auto channel =
            apache::thrift::RocketClientChannel::newChannel(std::move(socket));
        // Calls take as long as they take; callers bound their own waits.
        channel->setTimeout(0);
        return channel;
      });
  return std::make_unique<apache::thrift::Client<Service>>(std::move(channel));
}

// A generated client of 'Service' at 'endpoint' that connects again after
// its connection broke, e.g. when the server restarted.
template <typename Service>
class Connection {
 public:
  using Client = apache::thrift::Client<Service>;

  explicit Connection(const kiln::Endpoint& endpoint)
      : endpoint_{endpoint}, client_{makeClient<Service>(endpoint)} {}

  const kiln::Endpoint& endpoint() const {
    return endpoint_;
  }

  std::shared_ptr<Client> client() {
    std::lock_guard lock(mutex_);
    return client_;
  }

  // Awaits a call of 'client', turning its failures into kiln::Error.
  template <typename T>
  folly::coro::Task<T> translated(
      std::shared_ptr<Client> client,
      folly::coro::Task<T> call) {
    try {
      co_return co_await std::move(call);
    } catch (const thrift::StokeException& e) {
      throw fromThrift(e);
    } catch (const apache::thrift::transport::TTransportException& e) {
      reconnect(client);
      throw kiln::Error(
          kiln::ErrorCode::kUnavailable,
          fmt::format("Cannot reach {}: {}", endpoint_.toString(), e.what()));
    } catch (const kiln::Error&) {
      throw;
    } catch (const std::exception& e) {
      throw kiln::Error(
          kiln::ErrorCode::kInternal,
          fmt::format("Call to {} failed: {}", endpoint_.toString(), e.what()));
    }
  }

 private:
  // Replaces a client whose connection broke, unless another call did.
  void reconnect(const std::shared_ptr<Client>& broken) {
    std::lock_guard lock(mutex_);
    if (client_ == broken) {
      client_ = makeClient<Service>(endpoint_);
    }
  }

  const kiln::Endpoint endpoint_;
  std::mutex mutex_;
  std::shared_ptr<Client> client_;
};

class RemoteShard : public ShardRpc {
 public:
  explicit RemoteShard(const kiln::Endpoint& endpoint)
      : connection_{endpoint} {}

  folly::coro::Task<bool> co_put(std::string stream, kiln::ChunkId chunk)
      override {
    const auto request = toThrift(chunk);
    auto client = connection_.client();
    co_return co_await connection_.translated(
        client, client->co_put(stream, request));
  }

  folly::coro::Task<TakeResult> co_take(
      std::string stream,
      Epoch epoch,
      uint32_t max,
      std::chrono::milliseconds wait) override {
    const auto request = toThrift(epoch);
    auto client = connection_.client();
    co_return fromThrift(co_await connection_.translated(
        client,
        client->co_take(
            stream, request, static_cast<int32_t>(max), wait.count())));
  }

  folly::coro::Task<void> co_ack(
      std::string stream,
      Epoch epoch,
      std::vector<kiln::ChunkId> chunks) override {
    const auto request = toThrift(epoch);
    const auto ids = toThrift(chunks);
    auto client = connection_.client();
    co_await connection_.translated(
        client, client->co_ack(stream, request, ids));
  }

  folly::coro::Task<uint64_t> co_release(std::string stream, Epoch epoch)
      override {
    const auto request = toThrift(epoch);
    auto client = connection_.client();
    co_return static_cast<uint64_t>(co_await connection_.translated(
        client, client->co_release(stream, request)));
  }

  folly::coro::Task<ShardStats> co_stats(std::string stream) override {
    auto client = connection_.client();
    co_return fromThrift(co_await connection_.translated(
        client, client->co_stats(stream)));
  }

 private:
  Connection<thrift::ShardService> connection_;
};

} // namespace

std::shared_ptr<ShardRpc> connectShard(const kiln::Endpoint& endpoint) {
  return std::make_shared<RemoteShard>(endpoint);
}

} // namespace stoke::rpc
