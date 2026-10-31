#include "stoke/client/Producer.h"

#include <cstring>

#include "kiln/client/Client.h"

namespace stoke {

Producer::Producer(
    std::shared_ptr<StreamClient> stream,
    std::shared_ptr<kiln::client::Client> kiln,
    kiln::Placement placement)
    : stream_{std::move(stream)},
      kiln_{std::move(kiln)},
      placement_{placement} {}

folly::coro::Task<kiln::ChunkId> Producer::co_put(std::string_view chunk) {
  auto writer = co_await kiln_->co_create(
      stream_->info().bucket, placement_, chunk.size());
  if (!chunk.empty()) {
    auto buffer = kiln_->allocateBuffer(chunk.size());
    std::memcpy(buffer.data(), chunk.data(), chunk.size());
    buffer.setSize(chunk.size());
    co_await writer->co_append(std::move(buffer));
  }
  auto id = co_await writer->co_commit();
  co_await stream_->co_put(id);
  co_return id;
}

} // namespace stoke
