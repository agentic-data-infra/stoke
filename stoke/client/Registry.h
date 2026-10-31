#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <folly/coro/Task.h>

#include "stoke/common/Stream.h"

namespace kiln::client {
class Client;
}

namespace stoke {

// The streams' registry entries, as Kiln refs stream/<name> (StreamInfo).
// Calls fail with kiln::Error.
class Registry {
 public:
  explicit Registry(std::shared_ptr<kiln::client::Client> kiln);

  ~Registry();

  kiln::client::Client& kiln() const {
    return *kiln_;
  }

  // Creates an open stream over 'shards' ("host:port"), with a new Kiln
  // bucket for its chunks. Throws kiln::Error(kInvalidArgument) if the name
  // is taken or there are no shards.
  folly::coro::Task<StreamInfo> co_create(
      std::string name,
      std::vector<std::string> shards,
      double stopFraction = 0.95);

  // The stream's entry, or nullopt if there is none.
  folly::coro::Task<std::optional<StreamInfo>> co_get(std::string name);

  // The names of all streams.
  folly::coro::Task<std::vector<std::string>> co_list();

  // Marks the stream sealed: no more chunks come. Throws
  // kiln::Error(kNotFound) if there is no such stream.
  folly::coro::Task<void> co_seal(std::string name);

  // Removes the stream's entry and its bucket with all its chunks; returns
  // whether it existed. Shards drop the stream when they next read the
  // registry.
  folly::coro::Task<bool> co_remove(std::string name);

 private:
  const std::shared_ptr<kiln::client::Client> kiln_;
};

} // namespace stoke
