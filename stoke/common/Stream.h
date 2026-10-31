#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "kiln/common/ChunkId.h"
#include "kiln/common/Types.h"

namespace stoke {

enum class StreamState {
  // Producers may still put chunks.
  kOpen,
  // No more chunks come: shards end the stream once it drains (see
  // StreamInfo::stopFraction).
  kSealed,
};

// A stream's entry in the registry: the Kiln ref streamRefName(name), whose
// value is encode()'s JSON. `stoke create` writes it, `stoke seal` sets
// kSealed, and shards and consumers read it.
struct StreamInfo {
  // The Kiln bucket the stream's chunks go into.
  kiln::BucketId bucket{kiln::kDefaultBucket};
  // The shards' "host:port" endpoints; a chunk goes to shards[shardOf(id)].
  std::vector<std::string> shards;
  StreamState state{StreamState::kOpen};
  // Once sealed, a shard ends the stream for its consumers when it has
  // handed out this fraction of its chunks, so that ranks stop together
  // rather than wait for the last stragglers.
  double stopFraction{0.95};

  // Throws kiln::Error(kInvalidArgument) if the JSON does not fit a Kiln
  // ref value (kiln::kMaxRefValueSize, a few dozen shards).
  std::string encode() const;

  // Throws kiln::Error(kInvalidArgument) if 'value' is not encode()'s form.
  static StreamInfo decode(std::string_view value);

  bool operator==(const StreamInfo&) const = default;
};

// Throws kiln::Error(kInvalidArgument) unless 'name' is 1 to 128 of
// [A-Za-z0-9._-].
void checkStreamName(std::string_view name);

// "stream/<name>", after checkStreamName.
std::string streamRefName(std::string_view name);

// The shard, in [0, numShards), that queues chunk 'id'. Every attempt of a
// task writes the same bytes, hence the same id, so retries reach the same
// shard and it drops the duplicates.
uint32_t shardOf(const kiln::ChunkId& id, uint32_t numShards);

} // namespace stoke
