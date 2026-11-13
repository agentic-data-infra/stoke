# Dataloader benchmark

Compares stoke with [Grain](https://github.com/google/grain) and Zephon as
the dataloader of one trainer.

**Experiment 1** is pure data movement. The dataset is 20 GB of GPT-2
tokens, pre-batched: each Parquet row is one batch of 32 sequences of
4097 tokens. Each loader reads all of it, in shuffled order, into one
consumer that drops the batches. For stoke, an Ember job reads the files
and writes the batches into a stoke stream (`feed.py`), and stoke's
loader reads them. A run reports samples/s (overall, and in steady state
between 10% and 90% of the batches), time to the first batch, the CPU
used, and the peak memory, all of the loader's processes included (for
stoke: Kiln, the shards and the Ember feed).

## Equal cores

Each run gets a budget of C CPUs (2, 4, 8 and 12), as whole cores: CPU k
and its hyperthread k+8, for a machine of 8 cores and 16 CPUs. Every
thread of the run is pinned to the budget, except the consumer, which
runs on CPU 7. Each run gets a systemd scope of its own, whose cgroup
gives the CPU time and the peak memory, with a memory cap (24G). The page
cache is warm. Each loader's knob N (Grain's workers, Zephon's workers
and fetch parallelism, the Ember job's `local[N]`) is set to C and 2C.

## Setup

- A venv with `requirements.txt` (Python 3.12).
- For stoke, an Ember checkout with its Python venv (`.venv`) and its
  third-party deps generated. Build its stoke Python module against the
  benchmark's Python, then everything the runs need into a build dir:

  ```
  (cd $EMBER_HOME && PYTHON=$VENV/bin/python third-party/stoke/scripts/gen_third_party.sh)
  scripts/benchmark/make_build.sh $EMBER_HOME $STOKE_BUILD
  ```

- The data, from FineWeb-Edu Parquet files (column `text`):

  ```
  python prep_data.py ~/data/dlbench-exp1 fineweb-edu/*.parquet --target_gb 20
  ```

## Running

One cell (here stoke, 2 CPUs, N=2):

```
systemd-run --user --scope -p MemoryMax=24G python bench.py stoke ~/data/dlbench-exp1 \
    --dop=2 --cpus=0,8 --consumer_cpu=7 --ember_home=$EMBER_HOME --build=$STOKE_BUILD
```

The full matrix (3 runs per cell, one run at a time), into `OUT_DIR`:

```
PYTHON=$VENV/bin/python EMBER_HOME=... STOKE_BUILD=... ./run_exp1.sh OUT_DIR [DATA_DIR]
```

`LOADERS`, `BUDGETS`, `FACTORS`, `REPS` and `MAX_BATCHES` pick a part of
it. `OUT_DIR` gets one JSON line per run (`results.jsonl`), each run's
log, and `versions.txt` (commits, build, packages, machine).
`profile_faults.sh` profiles one stoke run with perf.

Kiln runs as one local DRAM server, sized for what the stream may hold
unconsumed plus 4 GiB for the chunks removed within Kiln's grace period
(1 s), whose blocks it reuses only after it (`Stoke.kiln_gib` in
`bench.py`).
