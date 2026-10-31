#pragma once

#include <memory>
#include <string_view>

#include <folly/coro/Task.h>

#include "kiln/common/Types.h"
#include "stoke/client/StreamClient.h"

namespace kiln::client {
class Client;
}

namespace stoke {

// Writes chunks of a stream: each into the stream's Kiln bucket, then puts
// it at its shard. For producers that are not Ember jobs (which write
// stoke:// files), e.g. tests, benchmarks and Python feeds.
class Producer {
 public:
  Producer(
      std::shared_ptr<StreamClient> stream,
      std::shared_ptr<kiln::client::Client> kiln,
      kiln::Placement placement = {});

  const StreamClient& stream() const {
    return *stream_;
  }

  // Writes 'chunk' (framed batches, see stoke/format/Batch.h) as one Kiln
  // chunk and puts it. Returns its id.
  folly::coro::Task<kiln::ChunkId> co_put(std::string_view chunk);

 private:
  const std::shared_ptr<StreamClient> stream_;
  const std::shared_ptr<kiln::client::Client> kiln_;
  const kiln::Placement placement_;
};

} // namespace stoke
