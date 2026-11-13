"""Experiment 2b: one run of Experiment 2 (exp2.py) on the kind cluster,
four nodes with one trainer each. Starts one pod per worker node
(exp2b_node.py: the node's trainer and its share of the loader), waits for
them, and prints (and appends to --out) one JSON line as exp2.py's, summed
over the nodes.

    python exp2b.py {grain,zephon,stoke} DATA_DIR --budget C --dop N --step_ms S [--steps M]

The CPUs are Experiment 2's: the budget of C CPUs is the first C/2 cores,
both hyperthreads of each (0, 8, 1, 9, ...), split evenly over the nodes
in that order (node i of kind's worker nodes, by name, gets the i-th C/4
of them), and the trainers run on CPUs 6, 7, 14 and 15, cores that no
budget CPU shares. Each node's container is held to its share of the
budget and its trainer's CPU and to --memory plus 1 GiB, and each pod to
--memory. The knob N is split evenly too: N/4 Grain or
Zephon workers per trainer (as exp2.py), N/4 slots per Ember worker.

DATA_DIR is a path under ~/data, which the nodes mount at /hostdata; the
nodes meet in ~/data/dlbench-kind (/dlbench), which also gets a copy of
the harness for the pods (cluster.yaml has both mounts).
"""

import argparse
import glob
import json
import os
import shutil
import subprocess
import time
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from exp2_common import BATCH, SEQ  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.expanduser("~/data")
SHARED = os.path.join(DATA, "dlbench-kind")  # /dlbench in the nodes
NAMESPACE = "ember"


def kubectl(*args, **kw):
    return subprocess.run(["kubectl", "-n", NAMESPACE, *args], check=True, text=True, capture_output=True, **kw).stdout


def disk_read_bytes(path):
    """Bytes read so far from the block device that holds path: a run that
    reads its data from the page cache reads next to nothing."""
    dev = os.stat(path).st_dev
    with open(f"/sys/dev/block/{os.major(dev)}:{os.minor(dev)}/stat") as f:
        return int(f.read().split()[2]) * 512


def worker_nodes():
    nodes = json.loads(kubectl("get", "nodes", "-o", "json"))["items"]
    nodes = [n for n in nodes if "node-role.kubernetes.io/control-plane" not in n["metadata"]["labels"]]
    nodes.sort(key=lambda n: n["metadata"]["name"])
    ip = lambda n: next(a["address"] for a in n["status"]["addresses"] if a["type"] == "InternalIP")  # noqa: E731
    return [(n["metadata"]["name"], ip(n)) for n in nodes]


TRAINER_CPUS = [6, 7, 14, 15]


def node_cpus(i, budget, nodes):
    """Node i's share of the budget, and its trainer's CPU."""
    cpus = [c for k in range(budget // 2) for c in (k, k + 8)]
    share = budget // nodes
    return cpus[i * share:(i + 1) * share], TRAINER_CPUS[i]


def pod(name, node, image, memory, command):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": NAMESPACE, "labels": {"app": "dlbench"}},
        "spec": {
            "nodeName": node,
            "restartPolicy": "Never",
            "hostNetwork": True,
            "dnsPolicy": "ClusterFirstWithHostNet",
            "enableServiceLinks": False,
            "terminationGracePeriodSeconds": 0,
            "containers": [{
                "name": "node",
                "image": image,
                "command": command,
                "env": [{"name": "PYTHONPATH", "value": "/dlbench/harness"}],
                "resources": {"limits": {"memory": memory}},
                "volumeMounts": [
                    {"name": "data", "mountPath": "/hostdata", "readOnly": True},
                    {"name": "shared", "mountPath": "/dlbench"},
                    {"name": "shm", "mountPath": "/dev/shm"},
                ],
            }],
            "volumes": [
                {"name": "data", "hostPath": {"path": "/hostdata"}},
                {"name": "shared", "hostPath": {"path": "/dlbench"}},
                {"name": "shm", "hostPath": {"path": "/dev/shm"}},
            ],
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("loader", choices=["grain", "zephon", "stoke"])
    parser.add_argument("data_dir", help="under ~/data")
    parser.add_argument("--budget", type=int, required=True, help="CPUs, a multiple of the nodes (4)")
    parser.add_argument("--dop", type=int, required=True, help="in all, a multiple of the nodes")
    parser.add_argument("--step_ms", type=float, required=True)
    parser.add_argument("--steps", type=int, default=0)
    parser.add_argument("--out")
    parser.add_argument("--image", required=True, help="kind/build_image.sh's")
    parser.add_argument("--memory", default="6Gi", help="per pod (node: 1 GiB more)")
    parser.add_argument("--fetchers", type=int, default=2)
    parser.add_argument("--task_budget", type=int, default=4)
    parser.add_argument("--chunk_mb", type=int, default=16)
    parser.add_argument("--timeout", type=float, default=300, help="seconds, on top of the run's steps")
    parser.add_argument("--log_dir", help="keep the nodes' logs here")
    parser.add_argument("--local_writes", action="store_true", help="stoke: the stream's producers write locally")
    args = parser.parse_args()

    nodes = worker_nodes()
    n = len(nodes)
    if args.budget % n or args.dop % n:
        parser.error(f"--budget and --dop must be multiples of the {n} nodes")
    dop = args.dop // n
    data = "/hostdata/" + os.path.relpath(os.path.realpath(args.data_dir), DATA)
    # Kiln per node: what its Ember tasks may hold unconsumed, plus 1 GiB of
    # blocks freed during the grace period (exp2.py's 4 GiB over 4 nodes), in
    # regions of 256 MiB.
    kiln_mb = -(-(dop * args.task_budget * args.chunk_mb + 1024) // 256) * 256

    harness = os.path.join(SHARED, "harness")
    os.makedirs(harness, exist_ok=True)
    for f in ["bench.py", "exp2.py", "exp2_common.py", "feed_exp2.py", "kind/exp2b_node.py"]:
        shutil.copy(os.path.join(os.path.dirname(HERE), f), harness)
    run_id = f"{args.loader}-{uuid.uuid4().hex[:8]}"
    run_dir = os.path.join(SHARED, "runs", run_id)
    os.makedirs(run_dir)

    mem_bytes = int(args.memory[:-2]) << {"Mi": 20, "Gi": 30}[args.memory[-2:]]
    kubectl("delete", "pod", "-l", "app=dlbench", "--wait", "--ignore-not-found")
    pods = []
    for i, (name, _) in enumerate(nodes):
        budget, trainer = node_cpus(i, args.budget, n)
        subprocess.run(
            ["docker", "update", f"--cpuset-cpus={','.join(map(str, budget + [trainer]))}",
             f"--memory={mem_bytes + (1 << 30)}", f"--memory-swap={mem_bytes + (1 << 30)}", name],
            check=True, capture_output=True,
        )
        command = [
            "/opt/venv/bin/python", "/dlbench/harness/exp2b_node.py", args.loader, data,
            f"--node={i}", f"--hosts={','.join(ip for _, ip in nodes)}", f"--run_dir=/dlbench/runs/{run_id}",
            f"--dop={args.dop}", f"--step_ms={args.step_ms}", f"--steps={args.steps}",
            f"--cpus={','.join(map(str, budget))}", f"--trainer_cpu={trainer}", f"--kiln_mb={kiln_mb}",
            f"--fetchers={args.fetchers}", f"--task_budget={args.task_budget}", f"--chunk_mb={args.chunk_mb}",
        ] + (["--local_writes"] if args.local_writes else [])
        pods.append(f"dlbench-{run_id}-{i}")
        kubectl("apply", "-f", "-", input=json.dumps(pod(pods[-1], name, args.image, args.memory, command)))

    deadline = time.monotonic() + args.timeout + args.steps * args.step_ms / 1000
    start = time.monotonic()
    read0 = disk_read_bytes(args.data_dir)
    try:
        while True:
            phases = [p["status"].get("phase") for p in json.loads(kubectl("get", "pod", "-l", "app=dlbench", "-o", "json"))["items"]]
            if all(p in ("Succeeded", "Failed") for p in phases) and len(phases) == n:
                break
            if time.monotonic() > deadline:
                open(os.path.join(run_dir, "failed"), "a").write("timed out\n")
                raise TimeoutError(f"pods: {phases}")
            time.sleep(0.5)
        elapsed = time.monotonic() - start
        disk_read = disk_read_bytes(args.data_dir) - read0
        for i, name in enumerate(pods):
            with open(os.path.join(run_dir, f"node{i}.pod.log"), "w") as f:
                f.write(kubectl("logs", name))
        failed = os.path.join(run_dir, "failed")
        if os.path.exists(failed) or "Failed" in phases:
            raise RuntimeError(open(failed).read() if os.path.exists(failed) else f"pods: {phases}")
        nodes_out = [json.load(open(os.path.join(run_dir, f"node{i}.json"))) for i in range(n)]
    finally:
        kubectl("delete", "pod", "-l", "app=dlbench", "--wait", "--ignore-not-found")
        if args.log_dir:
            os.makedirs(args.log_dir, exist_ok=True)
            for f in glob.glob(os.path.join(run_dir, "*.log")) + glob.glob(os.path.join(run_dir, "failed")):
                shutil.copy(f, os.path.join(args.log_dir, os.path.basename(f)))
        shutil.rmtree(run_dir, ignore_errors=True)

    step = args.step_ms / 1000
    per_rank = [o["rank"] for o in nodes_out]
    lo = max(r["window"][0] for r in per_rank)
    hi = min(r["window"][1] for r in per_rank)
    steady = 0.0
    for o in nodes_out:
        window = [(t, c) for t, c in o["samples"] if lo <= t <= hi]
        if len(window) < 2:
            steady = None
            break
        steady += (window[-1][1] - window[0][1]) / (window[-1][0] - window[0][0])
    waits = [r["wait_fraction"] for r in per_rank if r["wait_fraction"] is not None]
    for r in per_rank:
        del r["window"]
    result = {
        "loader": args.loader,
        "dop": args.dop,
        "budget_cpus": args.budget,
        "ranks": n,
        "nodes": n,
        "step_ms": args.step_ms,
        "rank_tokens_per_s_asked": round(BATCH * SEQ / step) if step else None,
        "max_wait_fraction": max(waits) if waits else None,
        "sustained": bool(waits) and max(waits) < 0.01,
        "steady_tokens_per_s": sum(r["steady_tokens_per_s"] or 0 for r in per_rank),
        "seconds": round(elapsed, 3),
        "cpu_user_s": round(sum(o["cpu_user_s"] for o in nodes_out), 1),
        "cpu_sys_s": round(sum(o["cpu_sys_s"] for o in nodes_out), 1),
        "cpu_cores": round(sum((o["cpu_user_s"] + o["cpu_sys_s"]) / o["seconds"] for o in nodes_out), 2),
        "steady_cpu_cores": round(steady, 2) if steady is not None else None,
        # The pods' peaks summed (an upper bound of the run's peak), and each pod's.
        "memory_peak_gib": round(sum(o["memory_peak_gib"] for o in nodes_out), 2),
        "node_memory_peak_gib": [round(o["memory_peak_gib"], 2) for o in nodes_out],
        "disk_read_gib": round(disk_read / 2**30, 2),
        "node_cpu_s": [round(o["cpu_user_s"] + o["cpu_sys_s"], 1) for o in nodes_out],
        "per_rank": per_rank,
    }
    result["net_gib"] = round(sum(o["net_tx_bytes"] for o in nodes_out) / 2**30, 3)
    if args.loader == "stoke":
        result["kiln_gib"] = kiln_mb * n / 1024
        result["fetchers"] = args.fetchers
        result["local_writes"] = args.local_writes
        # Chunks read, by the node Kiln stored them on, and by each node's loader.
        by_host = [o.get("loader", {}).get("chunks_by_host", {}) for o in nodes_out]
        hosts = sorted({h for b in by_host for h in b})
        result["chunks_by_host"] = {h: sum(b.get(h, 0) for b in by_host) for h in hosts}
        result["node_chunks_by_host"] = by_host
    line = json.dumps(result)
    print(line)
    if args.out:
        with open(args.out, "a") as f:
            f.write(line + "\n")


if __name__ == "__main__":
    main()
