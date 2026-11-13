"""Experiment 2 harness: RANKS synthetic trainers, one process each, take
batches from one loader at a fixed rate. Each loader filters the tokenized
documents (prep_tokenized.py), packs them into batches of [32, 4097] and
shuffles them (exp2_common.py):

- stoke: the Ember feed job (feed_exp2.py) filters, and its stream writer
  packs; each rank runs a stoke Loader on the stream.
- Grain: one pipeline per rank over every RANKS-th file (in shuffled order):
  ParquetIterDataset, interleaved, filter, ConcatThenSplitIterDataset,
  batch, WindowShuffleIterDataset over 64 batches (as in Experiment 1),
  mp_prefetch with DOP / RANKS workers.
- Zephon: one pipeline per rank (world_size RANKS, its rank): documents in
  shuffled shard order, a filter (map_transform), pack_flat (wrap, no
  positions), batch, with DOP / RANKS workers.

No loader shuffles single documents: order comes from shuffled files or
shards (Grain, Zephon; Zephon's in-shard shuffle reads rows at random and
cost it 8x on the smoke slice) and the Ember tasks and random chunk takes
of stoke; Grain also shuffles batches in a window.

    systemd-run --user --scope -p MemoryMax=24G \\
        python exp2.py {grain,zephon,stoke} DATA_DIR --dop N --step_ms S [--steps M]

A trainer step takes the next batch (the wait), then "computes" for
--step_ms (sleeps; 0: takes the next batch at once), so a rank asks for
1000 / step_ms batches/s. Each rank reports its wait fraction (time
waiting / time) over steady state, between 10% and 90% of its steps. A
rate is sustained if every rank waits less than 1% of the time.

Run it in a scope of its own: CPU is the scope's cgroup (user + sys of
every process in it, exited ones included), and with --cpus every thread
of every process runs on those CPUs (the budget), except each rank's main
thread (the trainer), which runs on its --trainer_cpus once it has its
first batch. CPU is also reported over the steady window common to all
ranks.

Prints and writes (--out) one JSON line.
"""

import argparse
import glob
import json
import multiprocessing as mp
import os
import shutil
import sys
import tempfile
import threading
import time

import numpy as np

import bench
from exp2_common import BATCH, END_OF_TEXT, SEQ, keep

HERE = os.path.dirname(os.path.abspath(__file__))


def grain_batches(data_dir, rank, ranks, workers, work):
    import grain
    import grain.experimental as ge

    files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    ds = grain.MapDataset.source(files).shuffle(seed=0)[rank::ranks]
    ds = ds.map(lambda path: ge.ParquetIterDataset(path))
    ds = ge.InterleaveIterDataset(ds, cycle_length=workers)
    ds = ds.filter(keep).map(lambda doc: {"tokens": doc["tokens"]})
    ds = ge.ConcatThenSplitIterDataset(ds, length_struct={"tokens": SEQ})
    ds = ds.map(lambda seq: seq["tokens"])
    ds = ds.batch(BATCH, drop_remainder=True)
    ds = ge.WindowShuffleIterDataset(ds, window_size=64, seed=rank)
    ds = ds.mp_prefetch(grain.MultiprocessingOptions(num_workers=workers))
    yield from ds


def zephon_batches(data_dir, rank, ranks, workers, work):
    import zephon

    dataset = zephon.Dataset.from_path("docs", data_dir, fmt="parquet")
    source = zephon.StaticMixtureWorkSource(
        [dataset],
        {"docs": 1.0},
        chunk_size=1024,
        shuffle_within_shard=False,
        stop_after_passes=1,
    )
    pipeline = (
        zephon.Pipeline(source)
        .fetch(parallelism=workers)
        .map_transform(lambda doc: {"tokens": doc["tokens"]} if keep(doc) else None)
        .pack_flat(SEQ, algorithm="wrap", tokens_field="tokens", emit_positions=False)
        .batch(BATCH)
        .options(max_workers=workers, world_size=ranks, global_rank=rank, aggregate_dir=work)
    )
    for batch in pipeline:
        yield batch.to_training(tokens_field="tokens", dtype=np.int32)["input_ids"]


def stoke_batches(build, metadata, stream, rank, fetchers):
    sys.path.insert(0, os.path.join(build, "bin"))
    import _stoke

    loader = _stoke.Loader(metadata, stream, f"bench-rank{rank}", fetchers=fetchers, seed=rank)
    for tensor in loader:
        yield np.from_dlpack(tensor)


def rank_main(args, rank, stream, results):
    try:
        results.put((rank, *train(args, rank, stream)))
    except BaseException as e:  # noqa: BLE001
        results.put((rank, f"{type(e).__name__}: {e}"))
        raise


def train(args, rank, stream):
    """One trainer: takes args.steps batches (0: all), one per step."""
    if args.loader == "stoke":
        batches = stoke_batches(args.build, *stream, rank, args.fetchers)
    else:
        workers = args.dop // args.ranks
        batches = {"grain": grain_batches, "zephon": zephon_batches}[args.loader](
            args.data_dir, rank, args.ranks, workers, args.work
        )
    step = args.step_ms / 1000
    start = asked = time.monotonic()
    taken, got, rows = [], [], []
    for batch in batches:
        now = time.monotonic()
        batch = np.asarray(batch)
        if not got:
            if batch.dtype != np.int32 or not (batch == END_OF_TEXT).any():
                raise RuntimeError(f"first batch: {batch.dtype}, no end-of-text token")
            if args.cpus:
                os.sched_setaffinity(0, {args.trainer_cpus[rank]})
        if batch.ndim != 2 or batch.shape[1] != SEQ or not 0 < batch.shape[0] <= BATCH:
            raise RuntimeError(f"batch of shape {batch.shape}")
        taken.append(asked)
        got.append(now)
        rows.append(batch.shape[0])
        if len(got) == args.steps:
            break
        # The step's compute, then the loop asks for the next batch.
        while (left := now + step - time.monotonic()) > 0:
            time.sleep(left)
        asked = time.monotonic()
    return start, np.array(taken), np.array(got), np.array(rows)


def rank_summary(start, taken, got, rows, step):
    n = len(got)
    lo, hi = int(n * 0.1), max(int(n * 0.9), 1)
    wait = float(np.sum(got[lo:hi] - taken[lo:hi]))
    span = float(got[hi - 1] - taken[lo]) + step
    tokens = int(np.sum(rows[lo:hi])) * SEQ
    return {
        "batches": n,
        "tokens": int(np.sum(rows)) * SEQ,
        "first_batch_s": round(float(got[0] - start), 3) if n else None,
        "wait_fraction": round(wait / span, 4) if n > 1 else None,
        "steady_tokens_per_s": round(tokens / span) if n > 1 else None,
        "window": (float(taken[lo]), float(got[hi - 1])),
    }


def sample_cpu(cgroup, samples, done):
    while not done.is_set():
        samples.append((time.monotonic(), sum(bench.cpu_seconds(cgroup))))
        time.sleep(0.2)


def pin(cgroup, cpus, skip):
    """Keeps every thread in the cgroup but those in 'skip' on 'cpus'."""
    while True:
        for pid in open(os.path.join(cgroup, "cgroup.procs")).read().split():
            try:
                tids = os.listdir(f"/proc/{pid}/task")
            except FileNotFoundError:
                continue
            for tid in map(int, tids):
                if tid in skip:
                    continue
                try:
                    if os.sched_getaffinity(tid) != cpus:
                        os.sched_setaffinity(tid, cpus)
                except OSError:  # exited
                    pass
        time.sleep(0.1)


class Stoke(bench.Stoke):
    def start_feed(self):
        self.feed = bench.subprocess.Popen(
            [
                os.path.join(self.args.ember_home, ".venv/bin/python"), os.path.join(HERE, "feed_exp2.py"),
                self.args.data_dir, self.stream, f"--metadata={self.metadata}",
                f"--master=local[{self.args.dop}]", f"--chunk_mb={self.args.chunk_mb}",
            ],
            env=dict(os.environ, PYTHONPATH=os.path.join(self.args.build, "python")),
            stdout=open(self.args.feed_log, "w"),
            stderr=bench.subprocess.STDOUT,
        )
        self.procs.append(self.feed)
        threading.Thread(target=self._watch_feed, daemon=True).start()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("loader", choices=["grain", "zephon", "stoke"])
    parser.add_argument("data_dir")
    parser.add_argument("--dop", type=int, required=True)
    parser.add_argument("--step_ms", type=float, required=True, help="a trainer step; 0: no wait")
    parser.add_argument("--steps", type=int, default=0, help="per rank; 0: until the data ends")
    parser.add_argument("--ranks", type=int, default=4)
    parser.add_argument("--out", help="append the JSON line to this file")
    parser.add_argument("--cpus", help="the core budget, e.g. 0,1,8,9; default: no pinning")
    parser.add_argument("--trainer_cpus", default="6,7,14,15", help="one per rank")
    stoke = parser.add_argument_group("stoke")
    stoke.add_argument("--ember_home", help="the Ember checkout the build is from")
    stoke.add_argument("--build", help="the stoke build of Ember (make_build.sh)")
    stoke.add_argument("--kiln_port", type=int, default=7670)
    stoke.add_argument("--shard_port", type=int, default=7680)
    stoke.add_argument("--fetchers", type=int, default=2, help="loader threads per rank")
    stoke.add_argument("--task_budget", type=int, default=4)
    stoke.add_argument("--feed_log", default="feed.log", help="the Ember feed's output")
    stoke.add_argument("--chunk_mb", type=int, default=16)
    args = parser.parse_args()
    args.trainer_cpus = [int(c) for c in args.trainer_cpus.split(",")]
    if len(args.trainer_cpus) < args.ranks:
        parser.error("--trainer_cpus needs one CPU per rank")
    if args.loader != "stoke" and args.dop % args.ranks:
        parser.error("--dop must be a multiple of --ranks (workers per rank)")

    cgroup = bench.cgroup_dir()
    cpus = {int(c) for c in args.cpus.split(",")} if args.cpus else None
    skip = set()  # the trainers' main threads
    if cpus:
        os.sched_setaffinity(0, cpus)
        threading.Thread(target=pin, args=(cgroup, cpus, skip), daemon=True).start()
    samples, done = [], threading.Event()
    threading.Thread(target=sample_cpu, args=(cgroup, samples, done), daemon=True).start()

    source = stream = None
    if args.loader == "stoke":
        if not args.ember_home or not args.build:
            parser.error("stoke needs --ember_home and --build")
        sys.path.insert(0, os.path.join(args.build, "bin"))
        source = Stoke(args)
        stream = (source.metadata, source.stream)
        source.start_feed()

    args.work = tempfile.mkdtemp(prefix="dlbench-exp2-")  # Zephon's checkpoints
    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    user0, sys0 = bench.cpu_seconds(cgroup)
    start = time.monotonic()
    ranks = []
    for rank in range(args.ranks):
        p = ctx.Process(target=rank_main, args=(args, rank, stream, results))
        p.start()
        skip.add(p.pid)
        ranks.append(p)
    try:
        got = [results.get() for _ in ranks]
        failed = [r for r in got if isinstance(r[1], str)]
        if failed:
            raise RuntimeError(f"rank {failed[0][0]} failed: {failed[0][1]}")
    finally:
        elapsed = time.monotonic() - start
        user1, sys1 = bench.cpu_seconds(cgroup)
        done.set()
        for p in ranks:
            p.join(timeout=30)
            if p.is_alive():
                p.kill()
        if source is not None:
            source.close()
        shutil.rmtree(args.work, ignore_errors=True)

    step = args.step_ms / 1000
    per_rank = [rank_summary(*r[1:], step) for r in sorted(got, key=lambda r: r[0])]
    lo = max(r["window"][0] for r in per_rank)
    hi = min(r["window"][1] for r in per_rank)
    window = [(t, c) for t, c in samples if lo <= t <= hi]
    steady_cores = (
        (window[-1][1] - window[0][1]) / (window[-1][0] - window[0][0]) if len(window) > 1 else None
    )
    waits = [r["wait_fraction"] for r in per_rank if r["wait_fraction"] is not None]
    for r in per_rank:
        del r["window"]
    cpu = (user1 - user0) + (sys1 - sys0)
    result = {
        "loader": args.loader,
        "dop": args.dop,
        "budget_cpus": len(cpus) if cpus else None,
        "ranks": args.ranks,
        "step_ms": args.step_ms,
        "rank_tokens_per_s_asked": round(BATCH * SEQ / step) if step else None,
        "max_wait_fraction": max(waits) if waits else None,
        "sustained": bool(waits) and max(waits) < 0.01,
        "steady_tokens_per_s": sum(r["steady_tokens_per_s"] or 0 for r in per_rank),
        "seconds": round(elapsed, 3),
        "cpu_user_s": round(user1 - user0, 1),
        "cpu_sys_s": round(sys1 - sys0, 1),
        "cpu_cores": round(cpu / elapsed, 2),
        "steady_cpu_cores": round(steady_cores, 2) if steady_cores is not None else None,
        "memory_peak_gib": round(int(open(os.path.join(cgroup, "memory.peak")).read()) / 2**30, 2),
        "per_rank": per_rank,
    }
    if source is not None:
        result["kiln_gib"] = source.kiln_gib()
        result["fetchers"] = args.fetchers
    line = json.dumps(result)
    print(line)
    if args.out:
        with open(args.out, "a") as f:
            f.write(line + "\n")


if __name__ == "__main__":
    main()
