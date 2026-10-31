# stoke

A queue that feeds training data to PyTorch while it is being made.

Producers (e.g. [Ember](https://github.com/agentic-data-infra/ember) jobs)
write batches of training data into chunks of
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

Status: in development. Plan: stoke shard servers (`co_put`, `co_take`,
`co_ack`, `co_release`, `co_stats`), the `stoke` CLI, a batch format, a C++
dataloader with a PyTorch `IterableDataset`, a `stoke://` file system in
Ember, and examples.

## Streams

A stream's registry entry is the Kiln ref `stream/<name>`, whose value is
JSON: the Kiln bucket of its chunks, its shards' endpoints, its state
(`open` or `sealed`) and the fraction of chunks after which shards end a
sealed stream (`stopFraction`, so that ranks stop together). See
[stoke/common/Stream.h](stoke/common/Stream.h).

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
