#include "stoke/client/Registry.h"

#include "kiln/client/Client.h"
#include "kiln/common/Error.h"

namespace stoke {

namespace {

constexpr std::string_view kRefPrefix = "stream/";

} // namespace

Registry::Registry(std::shared_ptr<kiln::client::Client> kiln)
    : kiln_{std::move(kiln)} {}

Registry::~Registry() = default;

folly::coro::Task<StreamInfo> Registry::co_create(
    std::string name,
    std::vector<std::string> shards,
    double stopFraction,
    uint32_t taskBudget) {
  auto refName = streamRefName(name);
  if (shards.empty()) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument, "a stream needs at least one shard");
  }
  if (!(stopFraction > 0 && stopFraction <= 1)) {
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument,
        "stopFraction must be in (0, 1]");
  }
  StreamInfo info{
      .shards = std::move(shards),
      .stopFraction = stopFraction,
      .taskBudget = taskBudget};
  // Checks the size before making a bucket.
  info.encode();
  info.bucket = co_await kiln_->co_createBucket();
  auto value = info.encode();
  auto created =
      co_await kiln_->co_casRef(std::move(refName), std::nullopt, value);
  if (!created) {
    co_await kiln_->co_deleteBucket(info.bucket);
    throw kiln::Error(
        kiln::ErrorCode::kInvalidArgument, "stream exists: " + name);
  }
  co_return info;
}

folly::coro::Task<std::optional<StreamInfo>> Registry::co_get(
    std::string name) {
  auto ref = co_await kiln_->co_getRef(streamRefName(name));
  if (!ref) {
    co_return std::nullopt;
  }
  co_return StreamInfo::decode(ref->value);
}

folly::coro::Task<std::vector<std::string>> Registry::co_list() {
  auto refs = co_await kiln_->co_listRefs(std::string(kRefPrefix));
  std::vector<std::string> names;
  names.reserve(refs.size());
  for (const auto& ref : refs) {
    names.push_back(ref.name.substr(kRefPrefix.size()));
  }
  co_return names;
}

folly::coro::Task<void> Registry::co_seal(std::string name) {
  auto refName = streamRefName(name);
  while (true) {
    auto ref = co_await kiln_->co_getRef(refName);
    if (!ref) {
      throw kiln::Error(kiln::ErrorCode::kNotFound, "no stream " + name);
    }
    auto info = StreamInfo::decode(ref->value);
    if (info.state == StreamState::kSealed) {
      co_return;
    }
    info.state = StreamState::kSealed;
    auto value = info.encode();
    auto updated = co_await kiln_->co_casRef(refName, ref->version, value);
    if (updated) {
      co_return;
    }
  }
}

folly::coro::Task<bool> Registry::co_remove(std::string name) {
  auto refName = streamRefName(name);
  while (true) {
    auto ref = co_await kiln_->co_getRef(refName);
    if (!ref) {
      co_return false;
    }
    auto info = StreamInfo::decode(ref->value);
    // At the version read, so that only one of concurrent removes deletes
    // the bucket; retries if the ref changed meanwhile, e.g. was sealed.
    if (co_await kiln_->co_removeRef(refName, ref->version)) {
      co_await kiln_->co_deleteBucket(info.bucket);
      co_return true;
    }
  }
}

} // namespace stoke
