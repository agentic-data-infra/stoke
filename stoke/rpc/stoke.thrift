// The stoke shard servers' service. Compiled by thrift1 into
// stoke/rpc/gen-cpp2 (see BUCK). The types mirror stoke/common/Types.h.

package "stoke.dev/rpc"

namespace cpp2 stoke.rpc.thrift

include "thrift/annotation/thrift.thrift"

/// kiln::ErrorCode, which stoke's calls fail with.
enum ErrorCode {
  NOT_FOUND = 0,
  OUT_OF_SPACE = 1,
  INVALID_ARGUMENT = 2,
  UNAVAILABLE = 3,
  INTERNAL = 4,
  TOO_LARGE = 5,
}

/// A kiln::Error thrown by a handler.
exception StokeException {
  1: ErrorCode code;
  @thrift.ExceptionMessage
  2: string message;
}

struct ChunkId {
  1: i64 high;
  2: i64 low;
}

struct Epoch {
  1: string job;
  2: i64 incarnation;
}

struct TakeResult {
  1: list<ChunkId> chunks;
  2: bool end;
}

struct ShardStats {
  1: i64 depth;
  2: i64 leased;
  3: i64 waiters;
  4: i64 put;
  5: i64 duplicates;
  6: i64 taken;
  7: i64 acked;
  8: i64 released;
  9: i64 expired;
  10: bool sealed;
  11: bool ended;
}

/// See stoke/rpc/ShardRpc.h. ('stream' is a Thrift keyword.)
service ShardService {
  bool put(1: string streamName, 2: ChunkId chunk) throws (
    1: StokeException e,
  );

  TakeResult take(
    1: string streamName,
    2: Epoch epoch,
    3: i32 max,
    4: i64 waitMs,
  ) throws (1: StokeException e);

  void ack(
    1: string streamName,
    2: Epoch epoch,
    3: list<ChunkId> chunks,
  ) throws (1: StokeException e);

  i64 release(1: string streamName, 2: Epoch epoch) throws (
    1: StokeException e,
  );

  ShardStats stats(1: string streamName) throws (1: StokeException e);
}
