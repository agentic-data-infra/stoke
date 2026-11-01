# stoke

A distributed PyTorch dataloader for data preprocessed by
[Ember](https://github.com/agentic-data-infra/ember). You write
preprocessing as an Ember Dataframe pipeline, and stoke feeds its output to
the trainers while the pipeline runs.

Producers (e.g. the workers running an
[Ember](https://github.com/agentic-data-infra/ember) job's tasks) write
batches of training data into chunks of
[Kiln](https://github.com/agentic-data-infra/kiln), and trainers read them,
at the same time. stoke is the queue between them: its messages are just
chunk ids (16 bytes), and the bytes of the batches only ever move between
the producers, Kiln and the trainers.

- **One unordered queue per stream, sharded.** Any consumer gets any
  available chunk, from any shard. A chunk goes to shard
  `hash(chunk id) mod S`, so retried producer tasks, which write the same
  bytes and hence the same chunk id, reach the same shard, and it drops the
  duplicates.
- **Decoupled.** Producers only know a stream name; trainers only know the
  stoke dataloader.
- **Kiln is the data plane.** Consumers read a chunk with one Kiln read.
  stoke adds one Kiln ref per stream and no other load on Kiln's metadata
  server.

Status: v1. There are shard servers, the `stoke` CLI, the batch format,
the dataloader (C++, with a PyTorch `IterableDataset`), a benchmark, and
an end-to-end example where an Ember job feeds a nanoGPT-style trainer.
Ember writes streams with `Dataframe.write.stream` (in Ember, built with
`-c ember.stoke=true`).

## Streams

A stream's registry entry is the Kiln ref `stream/<name>`, whose value is
JSON: the Kiln bucket of its chunks, its shards' endpoints, its state
(`open` or `sealed`) and the fraction of chunks after which shards end a
sealed stream (`stopFraction`, so that ranks stop together), and the
chunks each producer may have put and not yet seen consumed (`taskBudget`,
`stoke create --task_budget`; 0 for no limit). See
[stoke/common/Stream.h](stoke/common/Stream.h).

## Shards

A shard server (`stoke_shard`) keeps, for each stream that lists it, the
chunk ids put there, in DRAM ([stoke/shard/Shard.h](stoke/shard/Shard.h);
the API is [stoke/rpc/ShardRpc.h](stoke/rpc/ShardRpc.h), over fbthrift):

- `co_put(stream, chunk)` queues a chunk, or hands it straight to a waiting
  take; a chunk the shard already has is dropped.
- `co_take(stream, epoch, max, wait)` returns up to `max` chunks, leased to
  the consumer's epoch (job, incarnation). On an empty queue it parks for
  up to `wait`, so consumers never poll.
- `co_ack(stream, epoch, chunks)` ends leases; `co_release(stream, epoch)`
  queues all of an epoch's leased chunks again, for a restarted consumer.
  Leases not acked within `--lease_timeout_s` are queued again too.
- `co_awaitConsumed(stream, chunk, wait)` says whether a chunk was acked,
  parking for up to `wait` until it is. A producer
  ([stoke/client/Producer.h](stoke/client/Producer.h), one per Ember task)
  with `taskBudget` chunks put and not yet acked waits on its oldest before
  writing the next: that is the stream's backpressure, and the writer just
  sees its put take longer.
- Once a stream is sealed, a shard ends it (`take` returns `end`) when it
  handed out `stopFraction` of its chunks, or all of them.
- `co_stats(stream)`: depth, leased, waiting takes, chunks put, taken and
  acked. Summed over the shards, depth is the backlog and waiters says
  whether consumers starve.

Shards learn streams from the registry on first use and re-read their
entries every second, to see seals and removals. A producer puts each
chunk at shard `shardOf(id)`; a consumer takes from shards at random
([stoke/client/StreamClient.h](stoke/client/StreamClient.h)).

```
third-party/kiln/scripts/start_local_cluster.sh -d 1 -l 0 -r 4   # prints localhost:7470
buck2 run //bin:stoke_shard -- --port=7480 &
buck2 run //bin:stoke_shard -- --port=7481 &
buck2 run //bin:stoke -- create demo --shards=localhost:7480,localhost:7481
buck2 run //bin:stoke -- stats demo
buck2 run //bin:stoke -- seal demo
buck2 run //bin:stoke -- rm demo
```

## Batch format

A chunk holds framed batches: `[u64 length][batch]...`, where a batch is a
64-byte header (dtype, up to 6 dims of shape, payload length) followed by
the tensor's elements, padded to 8 bytes so that tensors in a chunk read
into aligned memory can be used in place. Batches in a chunk may differ in
shape. See [stoke/format/Batch.h](stoke/format/Batch.h), which has no
dependencies, for producers outside stoke.

## Dataloader

Each rank runs a loader ([stoke/loader/Loader.h](stoke/loader/Loader.h)).
Its fetcher threads keep a buffer of batches filled: while fewer than
`minBufferedBatches` (64) are buffered, and, with `maxBufferedBatches`,
while the buffer plus the batches expected from the takes in flight stay
under that maximum (it may be passed by part of a chunk), a fetcher takes a
chunk from a random shard, tries a second one if that was empty, and then
waits at a third (100 ms by default); it reads the whole chunk from Kiln,
splits it into batches, removes the chunk from Kiln and acks it. `next()`
hands out batches that point into the chunk's memory; once every shard has
ended the stream it returns the end. A restarted rank (a higher
incarnation) first returns the chunks leased to its previous incarnation;
chunks read but not trained on before a crash are lost (at most once).

In Python (`python/stoke`), batches are tensors without copies:

```python
import stoke, torch

dataset = stoke.StokeDataset("demo", job=f"run1-rank{rank}", metadata="localhost:7470")
for batch in torch.utils.data.DataLoader(dataset, batch_size=None):
    ...  # e.g. an int32 [B, T+1] tensor as the producer packed it
print(dataset.stats())  # batches, chunks, wait_seconds (starving), ...
```

Ranks reach the end at about the same time, not at the same step: stop
them together with `stoke.all_ranks_have_data(has_batch)` every step (an
`all_reduce(MIN)`). `stoke.Producer` puts chunks of NumPy arrays, for feeds
that are not Ember jobs; `stoke.create_stream`, `seal_stream` and
`remove_stream` do what the CLI does.

```
uv venv --python 3.10 .venv
uv pip install --python .venv/bin/python pybind11 numpy pytest
uv pip install --python .venv/bin/python torch --index-url https://download.pytorch.org/whl/cpu
scripts/gen_third_party.sh   # again, for the Python flags
buck2 build //python:_stoke --out python/stoke/_stoke.so
(cd python && PYTHONPATH=. ../.venv/bin/python -m pytest tests)
```

## Benchmark

`examples/bench` runs synthetic producers and consumers (one dataloader
each) against shards and Kiln in one process, over TCP, and prints for 1,
2, 4 and 8 consumers: chunks/s and GB/s, mean take time, the share of
empty takes, and how much of the time consumers starved. `--step_us`
simulates a training step per batch, `--produce_rate` limits producers.

```
buck2 run //examples/bench:stoke_bench -- --step_us=2000
```

On velox-dev (16 vCPUs, 4 shards, 4 producers, 500 chunks of 4 MiB with 4
batches each, a 2 ms step per batch):

| consumers | chunks/s | GB/s | take ms | empty takes | starved (mean) |
|---:|---:|---:|---:|---:|---:|
| 1 | 118 | 0.50 | 0.23 | 3% | 0.6% |
| 2 | 236 | 0.99 | 0.23 | 10% | 0.4% |
| 4 | 468 | 1.96 | 1.5 | 38% | 1.0% |
| 8 | 648 | 2.72 | 7.3 | 67% | 31% |

Consumers scale linearly until the producers (about 700 chunks/s here)
can no longer keep up; then they wait parked at shards, not polling. Kiln
stores chunks in whole 1 MiB blocks and reuses the blocks of removed
chunks only after its grace period (30 s by default), so DRAM needs room
for that much throughput beyond the backlog; the benchmark uses 1 s.

`--task_budget` bounds each producer (a Producer per producer thread, as
per Ember task) to that many chunks put and not yet consumed, and the
benchmark prints the most chunks the stream held unconsumed and how long
producers waited. With producers far faster than 2 consumers (300 chunks
of 1 MiB, 4 batches each, a 5 ms step, `--max_buffered_batches=16`):

| task budget | chunks/s | most unconsumed | producers waited | starved (mean) |
|---:|---:|---:|---:|---:|
| none | 98 | 286 | 0% | 0.7% |
| 2 | 95 | 8 | 91% | 2.8% |
| 4 | 97 | 16 | 90% | 0.7% |

The stream never holds more than producers × budget chunks, at the
consumers' pace; a budget of 2 is too small to keep 2 consumers fed, 4 is
enough.

## Example: Ember feeds nanoGPT

`examples/nanogpt/run.sh` starts a Kiln cluster and two shards, creates a
stream, and runs at the same time:

- the feed (`feed.py`), an Ember job over FineWeb-Edu:
  `read.parquet → filter(int_score >= 4) → stoke_pack(stoke_tokenize(text), B, T)
  → write.stream("fineweb")`. `stoke_tokenize` makes byte tokens
  (vocabulary 257); `stoke_pack` makes one int32 `[b ≤ B, T + 1]` batch per
  document. Each scan task puts a chunk into the stream every 4 MiB of
  batches (`--chunk_mb`), while the job runs;
- the trainer (`train.py`), a small GPT under `torchrun` (CPU, gloo), each
  rank reading its batches with `StokeDataset`.

The stream has a task budget (`TASK_BUDGET`, 4): each feed task waits
once it has 4 chunks not yet consumed, so the feed runs at the trainer's
pace. The feed seals the stream when its job commits; the ranks stop together
when it ends (or after `MAX_STEPS`) and print how long each waited for
data.

```
# Ember's Python module, with stoke:
(cd ~/source/ember && buck2 build -c ember.stoke=true //python:_ember --out python/ember/_ember.so)
buck2 build //python:_stoke --out python/stoke/_stoke.so
examples/nanogpt/run.sh                     # RANKS=4 MAX_STEPS=1000 ...
```

## Building

stoke builds with [Buck2](https://buck2.build), like Kiln and Ember, with
GCC 12 against the folly and fbthrift Velox's setup scripts build into
`/opt/deps-gcc12` (see Kiln's `scripts/build_deps_gcc12.sh`). Kiln is a git
submodule and the Buck cell `kiln//`; stoke uses its dependency setup.

```
git submodule update --init
scripts/gen_third_party.sh
buck2 build //...
buck2 test //...
```

## License

Apache 2.0, see [LICENSE](LICENSE).
