#include "stoke/common/Stream.h"

#include <folly/dynamic.h>
#include <folly/json.h>

#include "kiln/common/Error.h"

namespace stoke {

namespace {

constexpr size_t kMaxStreamName = 128;

const char* stateName(StreamState state) {
  return state == StreamState::kOpen ? "open" : "sealed";
}

StreamState parseState(const std::string& name) {
  if (name == "open") {
    return StreamState::kOpen;
  }
  if (name == "sealed") {
    return StreamState::kSealed;
  }
  throw kiln::Error(
      kiln::ErrorCode::kInvalidArgument, "unknown stream state: " + name);
}

} // namespace

std::string StreamInfo::encode() const {
  folly::dynamic shardList = folly::dynamic::array;
  for (const auto& shard : shards) {
    shardList.push_back(shard);
  }
  folly::dynamic json = folly::dynamic::object("bucket", bucket)(
      "shards", std::move(shardList))("state", stateName(state))(
      "stopFraction", stopFraction);
  auto value = folly::toJson(json);
  if (value.size() > kiln::kMaxRefValueSize) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument,
        "stream info too large for a Kiln ref (" +
            std::to_string(shards.size()) + " shards)");
  }
  return value;
}

StreamInfo StreamInfo::decode(std::string_view value) {
  try {
    auto json = folly::parseJson(value);
    StreamInfo info;
    info.bucket = json["bucket"].asInt();
    for (const auto& shard : json["shards"]) {
      info.shards.push_back(shard.asString());
    }
    info.state = parseState(json["state"].asString());
    info.stopFraction = json["stopFraction"].asDouble();
    return info;
  } catch (const kiln::Error&) {
    throw;
  } catch (const std::exception& e) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument,
        std::string("malformed stream info: ") + e.what());
  }
}

void checkStreamName(std::string_view name) {
  if (name.empty() || name.size() > kMaxStreamName) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument,
        "stream name must have 1 to 128 characters: " + std::string(name));
  }
  for (char c : name) {
    bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
    if (!ok) {
      throw kiln::Error(
          kiln::ErrorCode::kInvalidArgument,
          "stream name may only have [A-Za-z0-9._-]: " + std::string(name));
    }
  }
}

std::string streamRefName(std::string_view name) {
  checkStreamName(name);
  return "stream/" + std::string(name);
}

uint32_t shardOf(const kiln::ChunkId& id, uint32_t numShards) {
  if (numShards == 0) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument, "a stream needs at least one shard");
  }
  // The id is a hash already.
  return static_cast<uint32_t>(id.low % numShards);
}

} // namespace stoke
