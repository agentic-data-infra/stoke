"""Experiment 2b: one node's part of a run on the kind cluster (exp2b.py
starts one pod per node, each running this). The node runs one trainer
(rank = node index, as exp2.py's) and its share of the loader:

- Grain, Zephon: the rank's pipeline, with DOP / 4 workers (exp2.py).
- stoke: a Kiln DRAM server, a stoke shard and an Ember worker with
  DOP / 4 slots; node 0 also runs Kiln's metadata server, creates the stream on
  the four shards and runs the Ember feed's driver (feed_exp2.py).
  Kiln, the shards, the Ember workers and the loaders talk over TCP
  (the pods use the nodes' network); on one node, Kiln clients map the
  node's DRAM regions (shared /dev/shm), as Kiln does.

As in exp2.py, every thread of the pod runs on the node's share of the
budget (--cpus) except the trainer's main thread (--trainer_cpu), and CPU
and peak memory are the pod's cgroup's, from the trainer's start until
every node's trainer is done. Nodes meet in RUN_DIR (a directory every
node sees): node<i>.done when its trainer is done; node<i>.json, its
result, once all are; "failed" stops them all.
"""

import argparse
import glob
import json
import multiprocessing as mp
import os
import shutil
import subprocess
import sys
import threading
import time

import bench
import exp2

META_PORT, SERVER_PORT, SHARD_PORT, DRIVER_PORT, WORKER_PORT = 7570, 7580, 7590, 7077, 7078
STREAM = "bench"


def check_failed(run_dir):
    if os.path.exists(os.path.join(run_dir, "failed")):
        sys.exit("a node failed")


def wait_for(path_or_port, run_dir, timeout=120):
    """Waits for a file or for host:port to accept connections."""
    import socket

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        check_failed(run_dir)
        if isinstance(path_or_port, tuple):
            try:
                socket.create_connection(path_or_port, timeout=1).close()
                return
            except OSError:
                pass
        elif os.path.exists(path_or_port):
            return
        time.sleep(0.1)
    raise TimeoutError(f"waiting for {path_or_port}")


def clear_shm():
    for d in glob.glob("/dev/shm/kiln-*"):  # Kiln's DRAM regions, of this node's earlier runs
        shutil.rmtree(d)


class Node:
    def __init__(self, args):
        self.args = args
        self.procs = []
        self.host = args.hosts[args.node]
        self.metadata = f"{args.hosts[0]}:{META_PORT}"

    def start(self, name, cmd, env=None):
        log = open(os.path.join(self.args.run_dir, f"node{self.args.node}.{name}.log"), "w")
        self.procs.append(subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT, env=env))
        return self.procs[-1]

    def start_stoke(self):
        a, first = self.args, self.args.node == 0
        sys.path.insert(0, os.path.join(a.build, "bin"))
        import _stoke

        clear_shm()
        if first:
            self.start("kiln_meta", ["kiln_meta", f"--port={META_PORT}", "--grace_period_s=1"])
        wait_for((a.hosts[0], META_PORT), a.run_dir)
        self.start("kiln_server", [
            "kiln_server", f"--metadata={self.metadata}", f"--port={SERVER_PORT}", f"--host={self.host}",
            "--tier=dram", f"--regions={a.kiln_mb // 256}", "--region_size_mb=256",
        ])
        self.start("shard", [
            "stoke_shard", f"--port={SHARD_PORT}", f"--bind_host={self.host}", f"--metadata={self.metadata}",
        ])
        self.start("worker", [
            "ember_worker", f"--driver={a.hosts[0]}:{DRIVER_PORT}", f"--port={WORKER_PORT}", f"--host={self.host}",
            f"--node=node{a.node}", f"--slots={a.dop // a.ranks}", "--local_dir=/tmp/ember", "--connect_timeout_s=0",
        ])
        ready = os.path.join(a.run_dir, "stream")
        if first:
            for host in a.hosts:
                wait_for((host, SHARD_PORT), a.run_dir)
            _stoke.create_stream(
                self.metadata, STREAM, [f"{h}:{SHARD_PORT}" for h in a.hosts],
                stop_fraction=1.0, task_budget=a.task_budget,
            )
            env = dict(
                os.environ, PYTHONPATH=os.path.join(a.build, "python"),
                EMBER_CONF=f"ember.cluster.workers={len(a.hosts)}",
            )
            feed = self.start("feed", [
                "python3", os.path.join(os.path.dirname(exp2.__file__), "feed_exp2.py"), a.data_dir, STREAM,
                f"--metadata={self.metadata}", f"--master=ember://0.0.0.0:{DRIVER_PORT}", f"--chunk_mb={a.chunk_mb}",
            ], env=env)
            threading.Thread(target=self.watch_feed, args=(feed,), daemon=True).start()
            open(ready, "w").close()
        wait_for(ready, a.run_dir)
        return self.metadata, STREAM

    def watch_feed(self, feed):
        # The loaders wait for a seal that a failed feed never writes.
        if feed.wait() != 0 and not self.closing:
            open(os.path.join(self.args.run_dir, "failed"), "w").write(f"feed exited with {feed.returncode}\n")

    def close(self):
        self.closing = True
        for p in self.procs:
            p.kill()
            p.wait()
        if self.args.loader == "stoke":
            clear_shm()

    closing = False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("loader", choices=["grain", "zephon", "stoke"])
    parser.add_argument("data_dir")
    parser.add_argument("--node", type=int, required=True)
    parser.add_argument("--hosts", required=True, help="the nodes' IPs, comma separated, node 0 first")
    parser.add_argument("--run_dir", required=True)
    parser.add_argument("--dop", type=int, required=True, help="in all, as exp2.py's: DOP / nodes per node")
    parser.add_argument("--step_ms", type=float, required=True)
    parser.add_argument("--steps", type=int, default=0)
    parser.add_argument("--cpus", required=True, help="this node's share of the budget")
    parser.add_argument("--trainer_cpu", type=int, required=True)
    parser.add_argument("--build", default="/opt/ember", help="stoke: bin/ and python/ of the image")
    parser.add_argument("--kiln_mb", type=int, default=2048, help="stoke: this node's Kiln DRAM, in 256 MiB regions")
    parser.add_argument("--fetchers", type=int, default=2)
    parser.add_argument("--task_budget", type=int, default=4)
    parser.add_argument("--chunk_mb", type=int, default=16)
    args = parser.parse_args()
    args.hosts = args.hosts.split(",")
    # exp2.train's view: one rank of len(hosts), a trainer CPU per rank.
    args.ranks = len(args.hosts)
    args.trainer_cpus = {args.node: args.trainer_cpu}
    args.work = os.path.join(args.run_dir, "work")  # Zephon's, shared by the ranks
    os.makedirs(args.work, exist_ok=True)

    cgroup = bench.cgroup_dir()
    cpus = {int(c) for c in args.cpus.split(",")}
    skip = set()
    os.sched_setaffinity(0, cpus)
    threading.Thread(target=exp2.pin, args=(cgroup, cpus, skip), daemon=True).start()
    samples, done = [], threading.Event()
    threading.Thread(target=exp2.sample_cpu, args=(cgroup, samples, done), daemon=True).start()

    node = Node(args)
    stream = None
    try:
        if args.loader == "stoke":
            stream = node.start_stoke()
        ctx = mp.get_context("spawn")
        results = ctx.Queue()
        user0, sys0 = bench.cpu_seconds(cgroup)
        start = time.monotonic()
        trainer = ctx.Process(target=exp2.rank_main, args=(args, args.node, stream, results))
        trainer.start()
        skip.add(trainer.pid)
        while True:
            try:
                got = results.get(timeout=0.5)
                break
            except Exception:  # noqa: BLE001 (queue.Empty)
                check_failed(args.run_dir)
        if isinstance(got[1], str):
            raise RuntimeError(f"rank {args.node} failed: {got[1]}")
        open(os.path.join(args.run_dir, f"node{args.node}.done"), "w").close()
        for i in range(len(args.hosts)):
            wait_for(os.path.join(args.run_dir, f"node{i}.done"), args.run_dir, timeout=600)
        elapsed = time.monotonic() - start
        user1, sys1 = bench.cpu_seconds(cgroup)
        done.set()
        trainer.join(timeout=30)
        result = {
            "node": args.node,
            "rank": exp2.rank_summary(*got[1:], args.step_ms / 1000),
            "seconds": elapsed,
            "cpu_user_s": user1 - user0,
            "cpu_sys_s": sys1 - sys0,
            "samples": samples,
            "memory_peak_gib": int(open(os.path.join(cgroup, "memory.peak")).read()) / 2**30,
        }
        with open(os.path.join(args.run_dir, f"node{args.node}.json.tmp"), "w") as f:
            json.dump(result, f)
        os.rename(f.name, f.name[: -len(".tmp")])
    except BaseException as e:
        with open(os.path.join(args.run_dir, "failed"), "a") as f:
            f.write(f"node {args.node}: {type(e).__name__}: {e}\n")
        raise
    finally:
        # Every node's Kiln and shard serve the others' trainers until all are done.
        node.close()


if __name__ == "__main__":
    main()
