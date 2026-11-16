# stoke

A distributed PyTorch dataloader for data preprocessed by
[Ember](https://github.com/agentic-data-infra/ember). You write
preprocessing as an Ember Dataframe pipeline, and stoke feeds its output to
the trainers while the pipeline runs.

## Overview

Producers (the workers running an Ember job's tasks) write batches of
training data into chunks of [Kiln](https://github.com/agentic-data-infra/kiln),
and trainers read them, at the same time. stoke is the queue between them:
its messages are just chunk ids (16 bytes), and the bytes of the batches
only ever move between the producers, Kiln and the trainers.

- **One unordered queue per stream, sharded.** Any consumer gets any
  available chunk, from any shard. A chunk goes to shard
  `hash(chunk id) mod S`, so retried producer tasks, which write the same
  bytes and hence the same chunk id, reach the same shard, and it drops the
  duplicates.
- **Backpressure.** With a stream's task budget, each producer task waits
  once it has that many chunks not yet consumed, so the pipeline runs at
  the trainers' pace.
- **Decoupled.** Producers only know a stream name; trainers only know the
  stoke dataloader.
- **Kiln is the data plane.** Consumers read a chunk with one Kiln read,
  from their own host's Kiln server where they can. stoke adds one Kiln ref
  per stream and no other load on Kiln's metadata server.

Status: v1. There are shard servers, the `stoke` CLI, the batch format,
the dataloader (C++, with a PyTorch `IterableDataset`), a local mode that
runs everything in the trainer's process, Python wheels, a benchmark, and
an example where an Ember job feeds a nanoGPT-style trainer. Ember writes
streams through its stoke connector:
`df.write.format("stoke").option("batch", B).option("sequence", T).save(stream)`
packs a column of text or tokens into int32 `[B, T + 1]` batches.

- [docs/design.md](docs/design.md): streams, shards, locality, the batch
  format and the dataloader.
- [docs/benchmark.md](docs/benchmark.md): throughput and backpressure.
- [docs/comparison.md](docs/comparison.md): stoke and other data loaders
  for training.

## Building

stoke builds with [Buck2](https://buck2.build), like Kiln and Ember, with
GCC 12 against the folly and fbthrift in `/opt/deps-gcc12` (see Kiln's
[Building](https://github.com/agentic-data-infra/kiln#building)). Kiln is a
git submodule and the Buck cell `kiln//`; stoke uses its dependency setup.

```
git submodule update --init
scripts/gen_third_party.sh
buck2 build //...
buck2 test //...
```

The Python module (`python/stoke`) needs a Python 3.10 `.venv` with
pybind11, and torch for the tests:

```
uv venv --python 3.10 .venv
uv pip install --python .venv/bin/python pybind11 numpy pytest
uv pip install --python .venv/bin/python torch --index-url https://download.pytorch.org/whl/cpu
scripts/gen_third_party.sh   # again, for the Python flags and the module's own gflags
buck2 build //python:_stoke --out python/stoke/_stoke.so
(cd python && PYTHONPATH=. ../.venv/bin/python -m pytest tests)
```

`scripts/build_wheel.sh` builds a wheel for Linux x86-64 (glibc 2.35 or
newer) and CPython 3.10 into `dist/`, with the shared libraries the module
links bundled in. Ember's `scripts/build_wheel.sh` builds its wheel the
same way.

## Running locally

Install Ember's and stoke's wheels (torch is not pinned, so a CPU build
installed first stays):

```
pip install torch --index-url https://download.pytorch.org/whl/cpu
pip install ember-0.1.0-*.whl stoke-0.1.0-*.whl
```

`stoke.LocalStream` runs a stream on this machine with nothing to start
beforehand: Kiln (one DRAM server holding `memory`) and a shard run in the
trainer's process, and `feed()` runs the Ember job on a thread of it.

```python
import stoke
from ember import Session
from ember.functions import col, length

ember = Session.builder.appName("local").master("local[4]").getOrCreate()
docs = (
    ember.read.parquet("fineweb-edu/")
    .filter((col("int_score") >= 4) & (col("language") == "en"))
    .filter(length(col("text")) > 200)
    .select("text")
)
with stoke.LocalStream("fineweb", task_budget=4, memory="1g") as stream:
    stream.feed(docs.write.format("stoke").option("batch", 16).option("sequence", 128))
    dataset = stream.dataset()
    for batch in dataset:          # int32 [16, 129] tensors
        ...                        # a training step
    print(dataset.stats())
```

The loop ends when the job is done; a failed job ends it and raises the
job's error. Leaving the `with` early (e.g. after a number of steps) ends
the job. `examples/local/train.py` is this program with a small
Transformer:

```
python examples/local/train.py path/to/fineweb-edu/parquet --max_steps 1000
```

## Running on a cluster

On a cluster, Kiln, the shards, the Ember job and the trainers run as
separate processes, on any hosts.

1. **Kiln.** Start a metadata server (`kiln_meta`) and a storage server
   (`kiln_server --metadata=...`) on each host that runs producers or
   trainers; see [Kiln's README](https://github.com/agentic-data-infra/kiln).
   Removed chunks' blocks are reused after Kiln's grace period
   (`kiln_meta --grace_period_s`, 30 s), so DRAM needs room for that much
   throughput beyond the backlog. `export KILN_METADATA=` the URLs
   `kiln_meta` prints, for the commands below.
2. **Shards.** Start shard servers; each keeps its streams' queues in DRAM.
   ```
   stoke_shard --port=7480     # on each shard host; logs its endpoint
   ```
3. **The stream.** Create it over the shards, with a task budget:
   ```
   stoke create fineweb --shards=host1:7480,host2:7480 --task_budget=4
   stoke stats fineweb         # backlog and waiting consumers per shard
   ```
4. **The feed.** An Ember program on an Ember cluster (Ember's
   [Running on a cluster](https://github.com/agentic-data-infra/ember#running-on-a-cluster))
   writes the stream; its tasks write chunks as
   they go, and the job seals the stream when it commits:
   ```python
   ember = (
       Session.builder.appName("feed").master("ember://driver-host:7077")
       .config("ember.cluster.workers", "8")
       .config("ember.kiln.metadata", kiln_metadata)
       .getOrCreate()
   )
   docs.write.format("stoke").option("batch", 16).option("sequence", 128) \
       .option("seal", True).save("fineweb")
   ```
5. **The trainers.** Each rank (e.g. under `torchrun`) reads with its own
   job name, and the ranks stop together once the stream ends:
   ```python
   dataset = stoke.StokeDataset("fineweb", job=f"run1-rank{rank}")
   batches = iter(dataset)
   while stoke.all_ranks_have_data((batch := next(batches, None)) is not None):
       ...                     # a training step
   ```

**Locality.** Each chunk stays on one Kiln server, and loaders take the
chunks on their own host's server first, so that most reads are local
(Kiln's shared-memory path). With `stoke create --local_writes`, producers
also write to their own host's Kiln server first: for deployments with a
Kiln server on the producers' hosts. See
[docs/design.md](docs/design.md#locality).

**Example.** `examples/nanogpt/run.sh` runs all of this on one machine:
a Kiln cluster, two shards, the stream, an Ember job over FineWeb-Edu
(`feed.py`) and a nanoGPT-style trainer under `torchrun` (`train.py`, CPU
ranks with gloo), at the same time. The feed runs at the trainer's pace
(`TASK_BUDGET`, 4) and seals the stream when its job commits; the ranks
print how long each waited for data.

```
examples/nanogpt/run.sh path/to/fineweb-edu.parquet    # RANKS=4 MAX_STEPS=1000 ...
```

It uses the Python modules built in place: stoke's (above) and Ember's,
built with the stoke connector, in `$EMBER_HOME` (see the script).

## License

Apache 2.0, see [LICENSE](LICENSE).
