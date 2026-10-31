#include "stoke/common/Types.h"

namespace stoke {

ShardStats& ShardStats::operator+=(const ShardStats& other) {
  depth += other.depth;
  leased += other.leased;
  waiters += other.waiters;
  put += other.put;
  duplicates += other.duplicates;
  taken += other.taken;
  acked += other.acked;
  released += other.released;
  expired += other.expired;
  sealed = sealed || other.sealed;
  ended = ended || other.ended;
  return *this;
}

} // namespace stoke
