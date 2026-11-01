// stoke_bench: synthetic producers and consumers of a stream.
//
//   stoke_bench [--consumers=1,2,4,8] [--producers=4] [--shards=4]
//               [--chunks=500] [--chunk_kb=4096] [--batches_per_chunk=4]
//               [--step_us=0] [--fetchers=2] [--wait_ms=100]
//               [--produce_rate=0] [--dram_mb=0] [--task_budget=0]
//               [--min_buffered_batches=64] [--max_buffered_batches=0]
//
// For each number of consumers, producers put --chunks chunks of int32
// batches as fast as they can (or --produce_rate chunks/s in all), the
// consumers (one dataloader each, as ranks would have) read every batch,
// spending --step_us on each as a training step would, and the stream is
// sealed once the producers are done. It runs Kiln (one DRAM server) and
// --shards shard servers in this process, reached over TCP like remote
// ones. Kiln's grace period is short (--grace_ms), as consumers remove
// chunks once read and DRAM would otherwise hold them that much longer.
// Each producer thread has a Producer of its own, as each Ember task does:
// with --task_budget N it has at most N chunks put and not yet consumed.
// Prints per run: chunks/s and GB/s consumed, mean take time
// (including parked waits), the share of empty takes, the fraction of
// time consumers starved in next(), on average and at worst, the share of
// time producers waited for their budget, and the most chunks the stream
// held unconsumed (put and not acked, sampled every 10 ms).

#include <atomic>
#include <cstring>
#include <thread>

#include <fmt/format.h>
#include <folly/String.h>
#include <folly/coro/BlockingWait.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "kiln/client/Client.h"
#include "kiln/local/LocalCluster.h"
#include "stoke/client/Producer.h"
#include "stoke/loader/Loader.h"
#include "stoke/rpc/Rpc.h"
#include "stoke/shard/Shard.h"

DEFINE_string(consumers, "1,2,4,8", "Numbers of consumers to run with.");
DEFINE_int32(producers, 4, "Producer threads.");
DEFINE_int32(shards, 4, "Shard servers.");
DEFINE_int32(chunks, 500, "Chunks per run.");
DEFINE_int32(chunk_kb, 4096, "KiB per chunk.");
DEFINE_int32(batches_per_chunk, 4, "Batches per chunk.");
DEFINE_int32(step_us, 0, "Microseconds a consumer spends on each batch.");
DEFINE_int32(fetchers, 2, "Fetcher threads per consumer.");
DEFINE_int32(wait_ms, 100, "How long takes park at a shard.");
DEFINE_int32(produce_rate, 0, "Chunks per second in all; 0 for no limit.");
DEFINE_int32(dram_mb, 0, "Kiln DRAM; 0 for room for all chunks of a run.");
DEFINE_uint32(
    task_budget,
    0,
    "Chunks each producer may have put and not yet consumed; 0 for no "
    "limit.");
DEFINE_uint64(min_buffered_batches, 64, "Each consumer's minimum buffer.");
DEFINE_uint64(
    max_buffered_batches,
    0,
    "Each consumer's maximum buffer; 0 for none.");
DEFINE_int32(
    grace_ms,
    1000,
    "Kiln's grace period: blocks of removed chunks are reused after it.");

namespace stoke {
namespace {

using Clock = std::chrono::steady_clock;
using folly::coro::blockingWait;

constexpr uint64_t kRegionMb = 256;

struct RunResult {
  double seconds{0};
  uint64_t chunks{0};
  uint64_t bytes{0};
  uint64_t lost{0};
  uint64_t takes{0};
  uint64_t emptyTakes{0};
  std::chrono::nanoseconds takeTime{0};
  double meanStarved{0};
  double maxStarved{0};
  // Share of the producers' time spent waiting for their budget.
  double producersWaited{0};
  // Most chunks put and not acked.
  uint64_t peakUnconsumed{0};
};

// A chunk of int32 batches whose tokens all hold 'index', so that every
// chunk has its own id.
std::string makeChunk(uint32_t index) {
  const uint64_t batchBytes = (static_cast<uint64_t>(FLAGS_chunk_kb) << 10) /
      FLAGS_batches_per_chunk;
  const uint64_t tokens =
      std::max<uint64_t>(1, (batchBytes - format::kHeaderSize) / 4);
  std::vector<int32_t> values(tokens, static_cast<int32_t>(index));
  std::span<const uint8_t> payload(
      reinterpret_cast<const uint8_t*>(values.data()), tokens * 4);
  const auto batch = format::encodeBatch(
      {.dtype = format::DType::kInt32, .shape = {tokens}}, payload);
  std::string chunk;
  for (int b = 0; b < FLAGS_batches_per_chunk; ++b) {
    format::appendFrame(chunk, batch);
  }
  return chunk;
}

class Bench {
 public:
  Bench() {
    uint64_t dramMb = FLAGS_dram_mb;
    if (dramMb == 0) {
      // Room for a run's chunks (consumers may fall behind) and slack. Kiln
      // stores chunks in whole 1 MiB blocks.
      const uint64_t chunkMb = (FLAGS_chunk_kb + 1023) / 1024;
      dramMb = FLAGS_chunks * chunkMb * 5 / 4 + kRegionMb;
    }
    cluster_ = std::make_unique<kiln::local::LocalCluster>(
        kiln::local::LocalClusterOptions{
            .dramServers = 1,
            .localFsServers = 0,
            .regionsPerServer =
                static_cast<uint32_t>((dramMb + kRegionMb - 1) / kRegionMb),
            .regionSize = kRegionMb << 20,
            .metadata = kiln::meta::MetadataOptions{
                .gracePeriod = std::chrono::milliseconds(FLAGS_grace_ms)}});
    // Chunk infos are cached for less than the grace period.
    kiln_ = std::make_shared<kiln::client::Client>(kiln::client::ClientOptions{
        .metadata = cluster_->metadataEndpoint(),
        .chunkCacheTtl = std::chrono::milliseconds(FLAGS_grace_ms / 2)});
    registry_ = std::make_shared<Registry>(kiln_);
    for (int i = 0; i < FLAGS_shards; ++i) {
      // Sees the seal soon after the producers are done.
      auto shard = std::make_shared<Shard>(
          registry_,
          ShardOptions{.refreshInterval = std::chrono::milliseconds(100)});
      shards_.push_back(rpc::serveShard(std::move(shard), "127.0.0.1", 0));
      endpoints_.push_back(
          "127.0.0.1:" + std::to_string(shards_.back()->port()));
    }
  }

  RunResult run(int consumers) {
    const auto name = "bench-" + std::to_string(consumers);
    blockingWait(
        registry_->co_create(name, endpoints_, 1.0, FLAGS_task_budget));
    std::shared_ptr<StreamClient> stream =
        blockingWait(StreamClient::co_open(registry_, name));

    std::vector<std::unique_ptr<Loader>> loaders;
    for (int c = 0; c < consumers; ++c) {
      loaders.push_back(std::make_unique<Loader>(
          stream,
          kiln_,
          LoaderOptions{
              .epoch = {"bench-consumer" + std::to_string(c), 0},
              .fetchers = static_cast<uint32_t>(FLAGS_fetchers),
              .minBufferedBatches = FLAGS_min_buffered_batches,
              .maxBufferedBatches = FLAGS_max_buffered_batches,
              .wait = std::chrono::milliseconds(FLAGS_wait_ms)}));
    }
    const auto start = Clock::now();
    std::vector<double> busy(consumers);
    std::vector<std::thread> threads;
    for (int c = 0; c < consumers; ++c) {
      threads.emplace_back([&, c] {
        while (loaders[c]->next()) {
          if (FLAGS_step_us > 0) {
            std::this_thread::sleep_for(
                std::chrono::microseconds(FLAGS_step_us));
          }
        }
        busy[c] = std::chrono::duration<double>(Clock::now() - start).count();
      });
    }

    std::atomic<bool> done{false};
    std::atomic<uint64_t> peakUnconsumed{0};
    std::thread sampler([&] {
      while (!done) {
        uint64_t unconsumed = 0;
        for (const auto& stats : blockingWait(stream->co_stats())) {
          unconsumed += stats.put - stats.acked;
        }
        peakUnconsumed = std::max<uint64_t>(peakUnconsumed, unconsumed);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    });

    std::atomic<uint32_t> next{0};
    std::vector<std::shared_ptr<Producer>> producerOf;
    for (int p = 0; p < FLAGS_producers; ++p) {
      producerOf.push_back(std::make_shared<Producer>(stream, kiln_));
    }
    std::vector<std::thread> producers;
    for (int p = 0; p < FLAGS_producers; ++p) {
      producers.emplace_back([&, producer = producerOf[p]] {
        uint32_t index;
        while ((index = next++) < static_cast<uint32_t>(FLAGS_chunks)) {
          if (FLAGS_produce_rate > 0) {
            std::this_thread::sleep_until(
                start + std::chrono::microseconds(
                            uint64_t{index} * 1'000'000 / FLAGS_produce_rate));
          }
          const auto chunk = makeChunk(index);
          blockingWait(producer->co_put(chunk));
        }
      });
    }
    for (auto& thread : producers) {
      thread.join();
    }
    blockingWait(registry_->co_seal(name));
    for (auto& thread : threads) {
      thread.join();
    }
    done = true;
    sampler.join();

    RunResult result;
    result.seconds =
        std::chrono::duration<double>(Clock::now() - start).count();
    result.peakUnconsumed = peakUnconsumed;
    for (const auto& producer : producerOf) {
      result.producersWaited +=
          std::chrono::duration<double>(producer->waited()).count() /
          (result.seconds * FLAGS_producers);
    }
    for (int c = 0; c < consumers; ++c) {
      const auto stats = loaders[c]->stats();
      result.chunks += stats.chunks;
      result.bytes += stats.bytes;
      result.lost += stats.lostChunks;
      result.takes += stats.takes;
      result.emptyTakes += stats.emptyTakes;
      result.takeTime += stats.takeTime;
      const double starved =
          std::chrono::duration<double>(stats.waitTime).count() / busy[c];
      result.meanStarved += starved / consumers;
      result.maxStarved = std::max(result.maxStarved, starved);
    }
    loaders.clear();
    blockingWait(registry_->co_remove(name));
    // The next run gets the blocks of this one's chunks.
    std::this_thread::sleep_for(std::chrono::milliseconds(FLAGS_grace_ms));
    return result;
  }

 private:
  std::unique_ptr<kiln::local::LocalCluster> cluster_;
  std::shared_ptr<kiln::client::Client> kiln_;
  std::shared_ptr<Registry> registry_;
  std::vector<std::unique_ptr<rpc::RpcServer>> shards_;
  std::vector<std::string> endpoints_;
};

int main() {
  std::vector<int> counts;
  folly::split(',', FLAGS_consumers, counts, true);
  Bench bench;
  fmt::print(
      "{} chunks of {} KiB ({} batches), {} producers, {} shards, "
      "step {} us, task budget {}\n",
      FLAGS_chunks,
      FLAGS_chunk_kb,
      FLAGS_batches_per_chunk,
      FLAGS_producers,
      FLAGS_shards,
      FLAGS_step_us,
      FLAGS_task_budget);
  fmt::print(
      "{:>9} {:>9} {:>9} {:>8} {:>11} {:>8} {:>13} {:>12} {:>5} {:>13} "
      "{:>11}\n",
      "consumers",
      "seconds",
      "chunks/s",
      "GB/s",
      "take ms",
      "empty %",
      "starved mean",
      "starved max",
      "lost",
      "prod waited",
      "unconsumed");
  for (int consumers : counts) {
    const auto r = bench.run(consumers);
    const double takeMs = r.takes == 0
        ? 0
        : std::chrono::duration<double, std::milli>(r.takeTime).count() /
            r.takes;
    fmt::print(
        "{:>9} {:>9.2f} {:>9.0f} {:>8.2f} {:>11.3f} {:>8.1f} {:>12.1f}% "
        "{:>11.1f}% {:>5} {:>12.1f}% {:>11}\n",
        consumers,
        r.seconds,
        r.chunks / r.seconds,
        r.bytes / r.seconds / 1e9,
        takeMs,
        r.takes == 0 ? 0.0 : 100.0 * r.emptyTakes / r.takes,
        100 * r.meanStarved,
        100 * r.maxStarved,
        r.lost,
        100 * r.producersWaited,
        r.peakUnconsumed);
  }
  return 0;
}

} // namespace
} // namespace stoke

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  return stoke::main();
}
