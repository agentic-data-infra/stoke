// stoke_shard: a shard server of the stoke queue.
//
//   stoke_shard [--port=P] [--bind_host=name] [--metadata=URLS]
//               [--lease_timeout_s=S]
//
// Serves the streams whose registry entries (in the Kiln cluster at
// --metadata) list it. Its state is in memory. It logs its endpoint, for
// `stoke create --shards`. Stops on SIGINT or SIGTERM.

#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "bin/Metadata.h"
#include "bin/Signals.h"
#include "kiln/common/Types.h"
#include "stoke/client/Registry.h"
#include "stoke/rpc/Rpc.h"
#include "stoke/shard/Shard.h"

DEFINE_int32(port, 7480, "Port to serve on.");
DEFINE_string(bind_host, "", "Interface to serve on; empty for all.");
DEFINE_string(
    metadata,
    "",
    "The Kiln metadata server's URLs; default $KILN_METADATA or "
    "localhost:7470.");
DEFINE_int32(
    lease_timeout_s,
    300,
    "Seconds before chunks taken and not acked are queued again.");

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  folly::Init init(&argc, &argv);
  const auto signals = stoke::blockStopSignals();

  auto registry = std::make_shared<stoke::Registry>(stoke::kilnFromFlags());
  auto shard = std::make_shared<stoke::Shard>(
      registry,
      stoke::ShardOptions{
          .leaseTimeout = std::chrono::seconds(FLAGS_lease_timeout_s)});
  auto server = stoke::rpc::serveShard(
      shard, FLAGS_bind_host, static_cast<uint16_t>(FLAGS_port));
  kiln::Endpoint endpoint{
      .host = FLAGS_bind_host.empty() ? kiln::localHostName() : FLAGS_bind_host,
      .port = server->port()};
  LOG(INFO) << "Shard at " << endpoint.toString();

  const int signal = stoke::waitForStopSignal(signals);
  LOG(INFO) << "Stopping on signal " << signal;
  return 0;
}
