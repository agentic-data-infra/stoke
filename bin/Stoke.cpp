// stoke: manages streams.
//
//   stoke [--metadata=URLS] create STREAM --shards=HOST:PORT,...
//         [--stop_fraction=F]
//   stoke [--metadata=URLS] seal STREAM
//   stoke [--metadata=URLS] stats STREAM
//   stoke [--metadata=URLS] ls
//   stoke [--metadata=URLS] rm STREAM
//
// create registers an open stream over the shard servers (stoke_shard) and
// a new Kiln bucket for its chunks. seal tells the shards that no more
// chunks come, so that consumers see the end once they took --stop_fraction
// of them. stats prints each shard's queue: the backlog (depth), the
// consumers waiting, and chunks put, taken and acked. rm removes the stream
// and its chunks.
//
// --metadata is the Kiln metadata server's URLs, as kiln_meta prints them;
// it defaults to $KILN_METADATA, else localhost:7470.

#include <fmt/format.h>
#include <folly/String.h>
#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "bin/Metadata.h"
#include "kiln/common/Error.h"
#include "stoke/client/StreamClient.h"

DEFINE_string(
    metadata,
    "",
    "The Kiln metadata server's URLs; default $KILN_METADATA or "
    "localhost:7470.");
DEFINE_string(shards, "", "create: the shards' HOST:PORT, comma separated.");
DEFINE_double(
    stop_fraction,
    0.95,
    "create: the fraction of a shard's chunks after which a sealed stream "
    "ends there.");

namespace stoke {
namespace {

constexpr const char* kUsage =
    "Usage: stoke create STREAM --shards=HOST:PORT,... | seal STREAM | "
    "stats STREAM | ls | rm STREAM\n";

void printStats(const std::string& name, const ShardStats& stats) {
  fmt::print(
      "{:<24} {:>8} {:>8} {:>7} {:>10} {:>10} {:>10} {:>6} {:>8} {:>7}  {}\n",
      name,
      stats.depth,
      stats.leased,
      stats.waiters,
      stats.put,
      stats.taken,
      stats.acked,
      stats.duplicates,
      stats.released,
      stats.expired,
      stats.ended ? "ended" : (stats.sealed ? "sealed" : "open"));
}

folly::coro::Task<void> co_stats(
    std::shared_ptr<Registry> registry,
    std::string stream) {
  auto client = co_await StreamClient::co_open(registry, stream);
  auto stats = co_await client->co_stats();
  fmt::print(
      "{:<24} {:>8} {:>8} {:>7} {:>10} {:>10} {:>10} {:>6} {:>8} {:>7}  {}\n",
      "shard",
      "depth",
      "leased",
      "waiters",
      "put",
      "taken",
      "acked",
      "dups",
      "released",
      "expired",
      "state");
  ShardStats total;
  for (size_t i = 0; i < stats.size(); ++i) {
    printStats(client->info().shards[i], stats[i]);
    total += stats[i];
  }
  // Ended only once every shard did.
  total.ended = std::all_of(
      stats.begin(), stats.end(), [](const auto& s) { return s.ended; });
  printStats("total", total);
}

folly::coro::Task<int> co_main(std::vector<std::string> args) {
  auto registry = std::make_shared<Registry>(kilnFromFlags());
  const auto& command = args[0];
  if (command == "create" && args.size() == 2) {
    std::vector<std::string> shards;
    folly::split(',', FLAGS_shards, shards, true);
    auto info =
        co_await registry->co_create(args[1], shards, FLAGS_stop_fraction);
    fmt::print("{}\n", info.encode());
  } else if (command == "seal" && args.size() == 2) {
    co_await registry->co_seal(args[1]);
  } else if (command == "stats" && args.size() == 2) {
    co_await co_stats(registry, args[1]);
  } else if (command == "ls" && args.size() == 1) {
    for (const auto& name : co_await registry->co_list()) {
      fmt::print("{}\n", name);
    }
  } else if (command == "rm" && args.size() == 2) {
    if (!co_await registry->co_remove(args[1])) {
      fmt::print(stderr, "No stream {}\n", args[1]);
      co_return 1;
    }
  } else {
    fmt::print(stderr, kUsage);
    co_return 2;
  }
  co_return 0;
}

} // namespace
} // namespace stoke

int main(int argc, char** argv) {
  gflags::SetUsageMessage(stoke::kUsage);
  folly::Init init(&argc, &argv);
  if (argc < 2) {
    fmt::print(stderr, stoke::kUsage);
    return 2;
  }
  const std::vector<std::string> args(argv + 1, argv + argc);
  try {
    return folly::coro::blockingWait(stoke::co_main(args));
  } catch (const kiln::Error& e) {
    fmt::print(stderr, "stoke: {} ({})\n", e.what(), kiln::toString(e.code()));
    return 1;
  }
}
