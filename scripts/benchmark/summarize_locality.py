"""Locality metrics of run_exp2.sh KIND=1 stoke output directories, per budget
over the reps (median [min-max]): from the unthrottled runs, CPU-s and
network GiB per billion tokens, the kernel (sys) share of CPU, the fraction
of chunks each loader read from its own node's Kiln, and the chunks per
Kiln server (max / mean); plus the max sustained rate per trainer.

    python summarize_locality.py OUT_DIR...
"""

import glob
import json
import os
import re
import statistics
import sys
from collections import defaultdict

HOSTS = ["ember-worker", "ember-worker2", "ember-worker3", "ember-worker4"]


def line(path):
    with open(path) as f:
        lines = [l for l in f if l.startswith("{")]
    return json.loads(lines[-1]) if lines else None


def spread(values, fmt):
    values = [v for v in values if v is not None]
    if not values:
        return "-"
    return f"{fmt(statistics.median(values))} [{fmt(min(values))}-{fmt(max(values))}]"


def summarize(out):
    cells = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(out, "stoke-*-max.log"))):
        cell = os.path.basename(path)[: -len("-max.log")]
        budget = int(re.match(r"stoke-c(\d+)-", cell).group(1))
        r = line(path)
        probes = [line(p) for p in glob.glob(os.path.join(out, f"{cell}-p*.log"))]
        sustained = [p["rank_tokens_per_s_asked"] for p in probes if p and p["sustained"]]
        m = {"max": max(sustained) if sustained else None}
        if r:
            tokens = sum(x["tokens"] for x in r["per_rank"])
            cpu = r["cpu_user_s"] + r["cpu_sys_s"]
            m["cpu_per_btok"] = cpu / tokens * 1e9
            m["sys_share"] = r["cpu_sys_s"] / cpu
            if "net_gib" in r:
                m["net_per_btok"] = r["net_gib"] / tokens * 1e9
            if r.get("node_chunks_by_host"):
                by_node = r["node_chunks_by_host"]
                m["local"] = sum(n.get(HOSTS[i], 0) for i, n in enumerate(by_node)) / sum(
                    sum(n.values()) for n in by_node
                )
                per_server = [r["chunks_by_host"].get(h, 0) for h in HOSTS]
                m["balance"] = max(per_server) / statistics.mean(per_server)
        cells[budget].append(m)
    return cells


def main():
    rows = [
        "| run | CPUs | max sustained per trainer | CPU-s / B tokens | sys share | net GiB / B tokens "
        "| local reads | chunks per server, max / mean |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for out in sys.argv[1:]:
        for budget, reps in sorted(summarize(out).items()):
            get = lambda k: [m.get(k) for m in reps]  # noqa: E731
            rows.append(
                f"| {os.path.basename(out.rstrip('/'))} | {budget} | {spread(get('max'), lambda v: f'{v / 1e6:.1f}M')} "
                f"| {spread(get('cpu_per_btok'), lambda v: f'{v:.1f}')} | {spread(get('sys_share'), lambda v: f'{v:.2f}')} "
                f"| {spread(get('net_per_btok'), lambda v: f'{v:.2f}')} | {spread(get('local'), lambda v: f'{v:.0%}')} "
                f"| {spread(get('balance'), lambda v: f'{v:.2f}')} |"
            )
    print("\n".join(rows))


if __name__ == "__main__":
    main()
