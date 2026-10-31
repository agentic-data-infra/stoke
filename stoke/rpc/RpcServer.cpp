#include <atomic>
#include <future>

#include <fmt/format.h>
#include <folly/ExceptionString.h>
#include <folly/SocketAddress.h>
#include <glog/logging.h>
#include <thrift/lib/cpp/server/TServerEventHandler.h>
#include <thrift/lib/cpp2/server/ThriftServer.h>

#include "stoke/rpc/Convert.h"
#include "stoke/rpc/Rpc.h"
#include "stoke/rpc/gen-cpp2/ShardService.h"

namespace stoke::rpc {

namespace {

// Awaits a handler's call, turning kiln::Error into the exception clients
// turn back into it.
template <typename T>
folly::coro::Task<T> guarded(folly::coro::Task<T> call) {
  try {
    co_return co_await std::move(call);
  } catch (const kiln::Error& e) {
    throw toThrift(e);
  }
}

template <typename T>
std::unique_ptr<T> boxed(T value) {
  return std::make_unique<T>(std::move(value));
}

// The generated service, answered by a ShardRpc.
class ShardService
    : public apache::thrift::ServiceHandler<thrift::ShardService> {
 public:
  explicit ShardService(std::shared_ptr<ShardRpc> shard)
      : shard_{std::move(shard)} {}

  folly::coro::Task<bool> co_put(
      std::unique_ptr<std::string> stream,
      std::unique_ptr<thrift::ChunkId> chunk) override {
    co_return co_await guarded(
        shard_->co_put(std::move(*stream), fromThrift(*chunk)));
  }

  folly::coro::Task<std::unique_ptr<thrift::TakeResult>> co_take(
      std::unique_ptr<std::string> stream,
      std::unique_ptr<thrift::Epoch> epoch,
      int32_t max,
      int64_t waitMs) override {
    if (max < 0 || waitMs < 0) {
      throw toThrift(kiln::Error(
          kiln::ErrorCode::kInvalidArgument, "Negative max or wait"));
    }
    auto result = co_await guarded(shard_->co_take(
        std::move(*stream),
        fromThrift(*epoch),
        static_cast<uint32_t>(max),
        std::chrono::milliseconds(waitMs)));
    co_return boxed(toThrift(result));
  }

  folly::coro::Task<void> co_ack(
      std::unique_ptr<std::string> stream,
      std::unique_ptr<thrift::Epoch> epoch,
      std::unique_ptr<std::vector<thrift::ChunkId>> chunks) override {
    co_await guarded(shard_->co_ack(
        std::move(*stream), fromThrift(*epoch), fromThrift(*chunks)));
  }

  folly::coro::Task<bool> co_awaitConsumed(
      std::unique_ptr<std::string> stream,
      std::unique_ptr<thrift::ChunkId> chunk,
      int64_t waitMs) override {
    if (waitMs < 0) {
      throw toThrift(
          kiln::Error(kiln::ErrorCode::kInvalidArgument, "Negative wait"));
    }
    co_return co_await guarded(shard_->co_awaitConsumed(
        std::move(*stream),
        fromThrift(*chunk),
        std::chrono::milliseconds(waitMs)));
  }

  folly::coro::Task<int64_t> co_release(
      std::unique_ptr<std::string> stream,
      std::unique_ptr<thrift::Epoch> epoch) override {
    co_return static_cast<int64_t>(co_await guarded(
        shard_->co_release(std::move(*stream), fromThrift(*epoch))));
  }

  folly::coro::Task<std::unique_ptr<thrift::ShardStats>> co_stats(
      std::unique_ptr<std::string> stream) override {
    auto result = co_await guarded(shard_->co_stats(std::move(*stream)));
    co_return boxed(toThrift(result));
  }

 private:
  const std::shared_ptr<ShardRpc> shard_;
};

constexpr int kIoThreads = 4;
constexpr int kHandlerThreads = 4;
constexpr auto kStopTimeout = std::chrono::seconds(10);

// Tells RpcServer once its server listens, or why it cannot.
class Started : public apache::thrift::server::TServerEventHandler {
 public:
  void preServe(const folly::SocketAddress* address) override {
    port_ = address->getPort();
    done_ = true;
    promise_.set_value();
  }

  void failed(std::exception_ptr error) {
    if (done_.exchange(true)) {
      LOG(ERROR) << "RPC server failed: "
                 << folly::exceptionStr(error).toStdString();
      return;
    }
    promise_.set_exception(std::move(error));
  }

  // The port served; throws why the server could not start.
  uint16_t wait() {
    promise_.get_future().get();
    return port_;
  }

 private:
  std::promise<void> promise_;
  std::atomic<bool> done_{false};
  uint16_t port_{0};
};

} // namespace

RpcServer::RpcServer(
    std::shared_ptr<apache::thrift::AsyncProcessorFactory> service,
    const std::string& host,
    uint16_t port)
    : server_{std::make_shared<apache::thrift::ThriftServer>()} {
  // "::" accepts IPv4 connections too.
  const auto bindHost = host.empty() ? std::string("::") : host;
  server_->setInterface(std::move(service));
  // stoke's RPC is plaintext.
  server_->setSSLPolicy(apache::thrift::SSLPolicy::DISABLED);
  server_->setNumIOWorkerThreads(kIoThreads);
  server_->setNumCPUWorkerThreads(kHandlerThreads);
  // Calls wait for a handler thread as long as it takes, rather than failing
  // after fbthrift's 100 ms queue timeout.
  server_->setQueueTimeout(std::chrono::milliseconds(0));
  // Clients keep their connections between calls.
  server_->setIdleTimeout(std::chrono::milliseconds(0));
  // Stopping waits this long for calls in flight; fbthrift aborts the process
  // if one still runs then.
  server_->setWorkersJoinTimeout(kStopTimeout);
  auto started = std::make_shared<Started>();
  server_->addServerEventHandler(started);
  try {
    server_->setAddress(folly::SocketAddress(bindHost, port));
    thread_ = std::thread([server = server_, started] {
      try {
        server->serve();
      } catch (...) {
        started->failed(std::current_exception());
      }
    });
    port_ = started->wait();
  } catch (const std::exception& e) {
    stop();
    throw kiln::Error(
        kiln::ErrorCode::kUnavailable,
        fmt::format(
            "Cannot serve on {}: {}",
            kiln::Endpoint{bindHost, port}.toString(),
            e.what()));
  }
}

RpcServer::~RpcServer() {
  stop();
}

void RpcServer::stop() {
  if (thread_.joinable()) {
    server_->stop();
    thread_.join();
  }
}

uint16_t RpcServer::port() const {
  return port_;
}

std::unique_ptr<RpcServer> serveShard(
    std::shared_ptr<ShardRpc> shard,
    const std::string& host,
    uint16_t port) {
  return std::unique_ptr<RpcServer>(new RpcServer(
      std::make_shared<ShardService>(std::move(shard)), host, port));
}

} // namespace stoke::rpc
