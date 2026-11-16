# stoke's design

How stoke's streams, shards, batches and dataloader work. The
[README](../README.md) says how to build and run it.

## Streams

A stream's registry entry is the Kiln ref `stream/<name>`, whose value is
JSON: the Kiln bucket of its chunks, its shards' endpoints, its state
(`open` or `sealed`) and the fraction of chunks after which shards end a
sealed stream (`stopFraction`, so that ranks stop together), and the
chunks each producer may have put and not yet seen consumed (`taskBudget`,
`stoke create --task_budget`; 0 for no limit), and whether producers write
chunks to their own host's Kiln server first (`localWrites`,
`stoke create --local_writes`; see Locality). See
[stoke/common/Stream.h](../stoke/common/Stream.h).

## Shards

A shard server (`stoke_shard`) keeps, for each stream that lists it, the
chunk ids put there, in DRAM ([stoke/shard/Shard.h](../stoke/shard/Shard.h);
the API is [stoke/rpc/ShardRpc.h](../stoke/rpc/ShardRpc.h), over fbthrift):

- `co_put(stream, chunk, host)` queues a chunk with the host it is stored
  on, or hands it straight to a waiting take (one on that host first); a
  chunk the shard already has is dropped.
- `co_take(stream, epoch, max, wait, host, hostOnly)` returns up to `max`
  chunks, leased to the consumer's epoch (job, incarnation): those stored
  on the consumer's `host` first, then, unless `hostOnly`, those of the
  host with the most queued. On an empty queue it parks for up to `wait`,
  so consumers never poll.
- `co_ack(stream, epoch, chunks)` ends leases; `co_release(stream, epoch)`
  queues all of an epoch's leased chunks again, for a restarted consumer.
  Leases not acked within `--lease_timeout_s` are queued again too.
- `co_awaitConsumed(stream, chunk, wait)` says whether a chunk was acked,
  parking for up to `wait` until it is. A producer
  ([stoke/client/Producer.h](../stoke/client/Producer.h), one per Ember task)
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
([stoke/client/StreamClient.h](../stoke/client/StreamClient.h)).

### Locality

Producers write each chunk sticky (Kiln's `Placement::sticky`): its blocks
stay on one Kiln server, picked round robin, and the producer puts the
chunk with that server's host. Shards queue chunks per host, and loaders
take their own host's chunks first, so that most reads are local (Kiln's
shared-memory path) without leaving any host's chunks behind: a loader
with no local chunk takes another host's rather than wait. With a stream's
`localWrites`, producers also ask for their own host's Kiln server first
(`Placement::preferredHost`), so that writes stay local too; for
deployments with a Kiln server on the producers' hosts (a full server, or
a host without one, falls back to round robin).

## Batch format

A chunk holds framed batches: `[u64 length][batch]...`, where a batch is a
64-byte header (dtype, up to 6 dims of shape, payload length) followed by
the tensor's elements, padded to 8 bytes so that tensors in a chunk read
into aligned memory can be used in place. Batches in a chunk may differ in
shape. See [stoke/format/Batch.h](../stoke/format/Batch.h), which has no
dependencies, for producers outside stoke.

[stoke/format/Packer.h](../stoke/format/Packer.h) packs tokens into such
batches for language model training: int32 `[b, T + 1]` windows (inputs and
shifted targets of T tokens each), filled across documents without padding,
with text tokenized by bytes (0 to 255, then the end-of-text token 256).

## Dataloader

Each rank runs a loader ([stoke/loader/Loader.h](../stoke/loader/Loader.h)).
Its fetcher threads keep a buffer of batches filled: while fewer than
`minBufferedBatches` (64) are buffered, and, with `maxBufferedBatches`,
while the buffer plus the batches expected from the takes in flight stay
under that maximum (it may be passed by part of a chunk), a fetcher asks
every shard for chunks stored on its own host, then takes any chunk from a
random shard, tries a second one if that was empty, and then waits at a
third (100 ms by default); it reads the whole chunk from Kiln,
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
`remove_stream` do what the CLI does. `stoke.LocalStream`
([python/stoke/local.py](../python/stoke/local.py)) runs Kiln, one shard
([stoke/local/LocalCluster.h](../stoke/local/LocalCluster.h)) and the feed
in the trainer's process.
