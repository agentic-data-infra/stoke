"""Experiment 1 harness: one loader reads the pre-batched files (one row =
32 sequences of 4097 tokens) in shuffled order into one consumer that
drops the batches.

    systemd-run --user --scope -p MemoryMax=24G \
        python bench.py {grain,zephon,stoke} DATA_DIR --dop N [--max_batches M] [--out FILE]

Run it in a scope of its own: CPU is the scope's cgroup (user + sys of
every process in it, exited ones included), so for stoke it covers Kiln,
the shards and the Ember feed, which the harness starts there too.

Prints and writes (--out) one JSON line: batches, samples (batches x 32),
tokens, time to first batch, samples/s and tokens/s over the whole run and
over steady state (between 10% and 90% of the batches), CPU seconds and
cores, and the scope's peak memory.

DOP: Grain, mp_prefetch workers (and interleaved files); Zephon,
max_workers and fetch parallelism; stoke, the Ember feed's local[DOP]
(producer tasks), with --fetchers loader threads.

With --cpus, every thread of every process in the scope runs on those
CPUs (the core budget), except the consumer, the main thread, which runs
on --consumer_cpu once the first batch is in. Affinity is inherited, and a
thread re-pins threads that start on the consumer CPU every 0.1 s.
"""

import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time

import numpy as np

BATCH = 32
SEQ = 4097
HERE = os.path.dirname(os.path.abspath(__file__))


def grain_batches(data_dir, dop):
    import grain
    import grain.experimental as ge

    files = sorted(glob.glob(os.path.join(data_dir, "*.parquet")))
    ds = (
        grain.MapDataset.source(files)
        .shuffle(seed=0)
        .map(lambda path: ge.ParquetIterDataset(path))
    )
    ds = ge.InterleaveIterDataset(ds, cycle_length=dop)
    ds = ge.WindowShuffleIterDataset(ds, window_size=64, seed=0)
    ds = ds.mp_prefetch(grain.MultiprocessingOptions(num_workers=dop))
    for row in ds:
        yield np.asarray(row["tokens"]).reshape(BATCH, SEQ)


def zephon_batches(data_dir, dop):
    import zephon

    dataset = zephon.Dataset.from_path("rows", data_dir, fmt="parquet")
    source = zephon.StaticMixtureWorkSource(
        [dataset],
        {"rows": 1.0},
        chunk_size=min(1024, dataset.total()),
        shuffle_within_shard=True,
        shuffle_block_size="global",
        stop_after_passes=1,
    )
    pipeline = zephon.Pipeline(source).fetch(parallelism=dop).options(max_workers=dop)
    for record in pipeline:
        yield np.asarray(record.payload["tokens"]).reshape(BATCH, SEQ)


class Stoke:
    """A local Kiln (one DRAM server), two stoke shards and a stream, and
    the Ember feed job writing the files into it; batches from a Loader."""

    def __init__(self, args):
        self.args = args
        # Kiln's servers run the Kiln the build's clients were built with.
        self.kiln_home = os.path.join(args.ember_home, "third-party/kiln")
        self.work = tempfile.mkdtemp(prefix="dlbench-stoke-")
        self.procs = []
        self.closing = False
        env = dict(os.environ, KILN_CLUSTER_DIR=os.path.join(self.work, "kiln"))
        self.env = env
        self.metadata = (
            subprocess.check_output(
                [
                    os.path.join(self.kiln_home, "scripts/start_local_cluster.sh"),
                    "-p", str(args.kiln_port), "-d", "1", "-l", "0",
                    "-r", str(self.kiln_gib()), "-s", "1024", "-g", "1",
                ],
                env=env,
                text=True,
            )
            .strip()
            .splitlines()[-1]
        )
        shards = []
        for i in range(2):
            port = args.shard_port + i
            log = open(os.path.join(self.work, f"shard{i}.log"), "w")
            self.procs.append(
                subprocess.Popen(
                    [
                        os.path.join(args.build, "bin/stoke_shard"), f"--port={port}", "--bind_host=127.0.0.1",
                        f"--metadata={self.metadata}",
                    ],
                    stdout=log,
                    stderr=subprocess.STDOUT,
                )
            )
            shards.append(f"127.0.0.1:{port}")
        while "Shard at" not in open(os.path.join(self.work, "shard1.log")).read():
            time.sleep(0.1)
        import _stoke

        self.stoke = _stoke
        self.stream = "exp1"
        _stoke.create_stream(
            self.metadata, self.stream, shards, stop_fraction=1.0, task_budget=args.task_budget
        )

    def kiln_gib(self):
        # DRAM for what the stream may hold unconsumed (local[dop] tasks with
        # task_budget chunks each), plus the chunks read and removed during
        # one grace period (1 s), whose blocks Kiln frees only after it: 4 GiB
        # covers up to 4 GiB/s. With less, the feed's block allocations wait
        # for them.
        unconsumed = self.args.dop * self.args.task_budget * self.args.chunk_mb
        return -(-unconsumed // 1024) + 4

    def batches(self, data_dir, dop):
        self.feed = subprocess.Popen(
            [
                os.path.join(self.args.ember_home, ".venv/bin/python"), os.path.join(HERE, "feed.py"), data_dir, self.stream,
                f"--metadata={self.metadata}", f"--master=local[{dop}]",
                f"--chunk_mb={self.args.chunk_mb}",
            ],
            env=dict(os.environ, PYTHONPATH=os.path.join(self.args.build, "python")),
            stdout=open(self.args.feed_log, "w"),
            stderr=subprocess.STDOUT,
        )
        self.procs.append(self.feed)
        threading.Thread(target=self._watch_feed, daemon=True).start()
        self.loader = self.stoke.Loader(
            self.metadata, self.stream, "bench", fetchers=self.args.fetchers
        )
        for tensor in self.loader:
            yield np.from_dlpack(tensor)

    def _watch_feed(self):
        # The loader waits for a seal that a failed feed never writes.
        if self.feed.wait() != 0 and not self.closing:
            print(f"feed exited with {self.feed.returncode}, see {self.args.feed_log}", file=sys.stderr)
            self.close()
            os._exit(1)

    def close(self):
        # A feed still running (the consumer stopped early) waits for
        # consumption and is killed below.
        self.closing = True
        if getattr(self, "loader", None) is not None:
            self.loader_stats = self.loader.stats()
        try:
            self.stoke.remove_stream(self.metadata, self.stream)
        except Exception as e:  # noqa: BLE001
            print(f"remove_stream: {e}", file=sys.stderr)
        for p in self.procs:
            p.kill()
            p.wait()
        subprocess.run(
            [os.path.join(self.kiln_home, "scripts/stop_local_cluster.sh")],
            env=self.env,
            check=False,
        )
        shutil.rmtree(self.work, ignore_errors=True)


def pin(cgroup, cpus):
    """Keeps every thread in the cgroup but the main one on 'cpus'."""
    while True:
        for pid in open(os.path.join(cgroup, "cgroup.procs")).read().split():
            try:
                tids = os.listdir(f"/proc/{pid}/task")
            except FileNotFoundError:
                continue
            for tid in map(int, tids):
                if tid == os.getpid():
                    continue
                try:
                    if os.sched_getaffinity(tid) != cpus:
                        os.sched_setaffinity(tid, cpus)
                except OSError:  # exited
                    pass
        time.sleep(0.1)


def cgroup_dir():
    with open("/proc/self/cgroup") as f:
        return "/sys/fs/cgroup" + f.read().strip().split(":", 2)[2]


def cpu_seconds(cgroup):
    stat = dict(line.split() for line in open(os.path.join(cgroup, "cpu.stat")))
    return int(stat["user_usec"]) / 1e6, int(stat["system_usec"]) / 1e6


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("loader", choices=["grain", "zephon", "stoke"])
    parser.add_argument("data_dir")
    parser.add_argument("--dop", type=int, required=True)
    parser.add_argument("--max_batches", type=int, default=0, help="0: the whole dataset")
    parser.add_argument("--out", help="append the JSON line to this file")
    parser.add_argument("--cpus", help="the core budget, e.g. 0,1,8,9; default: no pinning")
    parser.add_argument("--consumer_cpu", type=int, default=7)
    stoke = parser.add_argument_group("stoke")
    stoke.add_argument("--ember_home", help="the Ember checkout the build is from")
    stoke.add_argument("--build", help="the stoke build of Ember (make_build.sh)")
    stoke.add_argument("--kiln_port", type=int, default=7670)
    stoke.add_argument("--shard_port", type=int, default=7680)
    stoke.add_argument("--fetchers", type=int, default=2, help="loader threads")
    stoke.add_argument("--task_budget", type=int, default=4)
    stoke.add_argument("--feed_log", default="feed.log", help="the Ember feed's output")
    stoke.add_argument("--chunk_mb", type=int, default=16)
    args = parser.parse_args()

    cgroup = cgroup_dir()
    cpus = {int(c) for c in args.cpus.split(",")} if args.cpus else None
    if cpus:
        os.sched_setaffinity(0, cpus)
        threading.Thread(target=pin, args=(cgroup, cpus), daemon=True).start()
    source = None
    if args.loader == "stoke":
        if not args.ember_home or not args.build:
            parser.error("stoke needs --ember_home and --build")
        sys.path.insert(0, os.path.join(args.build, "bin"))
        source = Stoke(args)
        batches = source.batches(args.data_dir, args.dop)
    else:
        batches = {"grain": grain_batches, "zephon": zephon_batches}[args.loader](
            args.data_dir, args.dop
        )

    limit = args.max_batches or sum(
        pq_rows(path) for path in glob.glob(os.path.join(args.data_dir, "*.parquet"))
    )
    stamps = np.zeros(limit)
    user0, sys0 = cpu_seconds(cgroup)
    start = time.perf_counter()
    n = 0
    try:
        for batch in batches:
            if batch.shape != (BATCH, SEQ):
                raise RuntimeError(f"batch of shape {batch.shape}")
            stamps[n] = time.perf_counter() - start
            if n == 0 and cpus:
                os.sched_setaffinity(0, {args.consumer_cpu})
            n += 1
            if n == limit:
                break
    finally:
        elapsed = time.perf_counter() - start
        user1, sys1 = cpu_seconds(cgroup)
        if source is not None:
            source.close()

    lo, hi = int(n * 0.1), max(int(n * 0.9) - 1, 0)
    steady = (hi - lo) / (stamps[hi] - stamps[lo]) if hi > lo else float("nan")
    cpu = (user1 - user0) + (sys1 - sys0)
    result = {
        "loader": args.loader,
        "dop": args.dop,
        "budget_cpus": len(cpus) if cpus else None,
        "fetchers": args.fetchers if args.loader == "stoke" else None,
        "batches": n,
        "samples": n * BATCH,
        "tokens": n * BATCH * SEQ,
        "seconds": round(elapsed, 3),
        "first_batch_s": round(stamps[0], 3) if n else None,
        "samples_per_s": round(n * BATCH / elapsed, 1),
        "steady_samples_per_s": round(steady * BATCH, 1),
        "steady_tokens_per_s": round(steady * BATCH * SEQ),
        "cpu_user_s": round(user1 - user0, 1),
        "cpu_sys_s": round(sys1 - sys0, 1),
        "cpu_cores": round(cpu / elapsed, 2),
        "memory_peak_gib": round(int(open(os.path.join(cgroup, "memory.peak")).read()) / 2**30, 2),
    }
    if source is not None:
        result["kiln_gib"] = source.kiln_gib()
        result["loader_stats"] = getattr(source, "loader_stats", None)
    line = json.dumps(result)
    print(line)
    if args.out:
        with open(args.out, "a") as f:
            f.write(line + "\n")


def pq_rows(path):
    import pyarrow.parquet as pq

    return pq.ParquetFile(path).metadata.num_rows


if __name__ == "__main__":
    main()
