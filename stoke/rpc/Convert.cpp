#include "stoke/rpc/Convert.h"

namespace stoke::rpc {

thrift::ChunkId toThrift(const kiln::ChunkId& id) {
  thrift::ChunkId result;
  result.high() = static_cast<int64_t>(id.high);
  result.low() = static_cast<int64_t>(id.low);
  return result;
}

kiln::ChunkId fromThrift(const thrift::ChunkId& id) {
  return kiln::ChunkId{
      static_cast<uint64_t>(*id.high()), static_cast<uint64_t>(*id.low())};
}

std::vector<thrift::ChunkId> toThrift(const std::vector<kiln::ChunkId>& ids) {
  std::vector<thrift::ChunkId> result;
  result.reserve(ids.size());
  for (const auto& id : ids) {
    result.push_back(toThrift(id));
  }
  return result;
}

std::vector<kiln::ChunkId> fromThrift(const std::vector<thrift::ChunkId>& ids) {
  std::vector<kiln::ChunkId> result;
  result.reserve(ids.size());
  for (const auto& id : ids) {
    result.push_back(fromThrift(id));
  }
  return result;
}

thrift::Epoch toThrift(const Epoch& epoch) {
  thrift::Epoch result;
  result.job() = epoch.job;
  result.incarnation() = static_cast<int64_t>(epoch.incarnation);
  return result;
}

Epoch fromThrift(const thrift::Epoch& epoch) {
  return Epoch{*epoch.job(), static_cast<uint64_t>(*epoch.incarnation())};
}

thrift::TakeResult toThrift(const TakeResult& result) {
  thrift::TakeResult converted;
  converted.chunks() = toThrift(result.chunks);
  converted.end() = result.end;
  return converted;
}

TakeResult fromThrift(const thrift::TakeResult& result) {
  return TakeResult{fromThrift(*result.chunks()), *result.end()};
}

thrift::ShardStats toThrift(const ShardStats& stats) {
  thrift::ShardStats result;
  result.depth() = static_cast<int64_t>(stats.depth);
  result.leased() = static_cast<int64_t>(stats.leased);
  result.waiters() = static_cast<int64_t>(stats.waiters);
  result.put() = static_cast<int64_t>(stats.put);
  result.duplicates() = static_cast<int64_t>(stats.duplicates);
  result.taken() = static_cast<int64_t>(stats.taken);
  result.acked() = static_cast<int64_t>(stats.acked);
  result.released() = static_cast<int64_t>(stats.released);
  result.expired() = static_cast<int64_t>(stats.expired);
  result.sealed() = stats.sealed;
  result.ended() = stats.ended;
  return result;
}

ShardStats fromThrift(const thrift::ShardStats& stats) {
  return ShardStats{
      .depth = static_cast<uint64_t>(*stats.depth()),
      .leased = static_cast<uint64_t>(*stats.leased()),
      .waiters = static_cast<uint64_t>(*stats.waiters()),
      .put = static_cast<uint64_t>(*stats.put()),
      .duplicates = static_cast<uint64_t>(*stats.duplicates()),
      .taken = static_cast<uint64_t>(*stats.taken()),
      .acked = static_cast<uint64_t>(*stats.acked()),
      .released = static_cast<uint64_t>(*stats.released()),
      .expired = static_cast<uint64_t>(*stats.expired()),
      .sealed = *stats.sealed(),
      .ended = *stats.ended()};
}

thrift::StokeException toThrift(const kiln::Error& error) {
  thrift::StokeException result;
  result.code() = static_cast<thrift::ErrorCode>(error.code());
  result.message() = error.what();
  return result;
}

kiln::Error fromThrift(const thrift::StokeException& error) {
  return kiln::Error(
      static_cast<kiln::ErrorCode>(*error.code()), *error.message());
}

} // namespace stoke::rpc
