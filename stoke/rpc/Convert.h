// Internal to stoke/rpc: stoke's types to and from the generated ones.

#pragma once

#include <vector>

#include "kiln/common/Error.h"
#include "stoke/common/Types.h"
#include "stoke/rpc/gen-cpp2/stoke_types.h"

namespace stoke::rpc {

thrift::ChunkId toThrift(const kiln::ChunkId& id);
kiln::ChunkId fromThrift(const thrift::ChunkId& id);

std::vector<thrift::ChunkId> toThrift(const std::vector<kiln::ChunkId>& ids);
std::vector<kiln::ChunkId> fromThrift(const std::vector<thrift::ChunkId>& ids);

thrift::Epoch toThrift(const Epoch& epoch);
Epoch fromThrift(const thrift::Epoch& epoch);

thrift::TakeResult toThrift(const TakeResult& result);
TakeResult fromThrift(const thrift::TakeResult& result);

thrift::ShardStats toThrift(const ShardStats& stats);
ShardStats fromThrift(const thrift::ShardStats& stats);

thrift::StokeException toThrift(const kiln::Error& error);
kiln::Error fromThrift(const thrift::StokeException& error);

} // namespace stoke::rpc
