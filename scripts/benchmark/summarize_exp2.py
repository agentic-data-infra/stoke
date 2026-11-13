"""Summarizes a run_exp2.sh output directory: per loader and budget, over
the reps, the max sustainable rate per trainer (median [min-max]), the
unthrottled rate, and the CPU and peak memory of the run at the max
sustainable rate.

    python summarize_exp2.py OUT_DIR
"""

import glob
import json
import os
import re
import statistics
import sys
from collections import defaultdict


def line(path):
    with open(path) as f:
        lines = [l for l in f if l.startswith("{")]
    return json.loads(lines[-1]) if lines else None


def spread(values, fmt):
    if not values:
        return "-"
    return f"{fmt(statistics.median(values))} [{fmt(min(values))}-{fmt(max(values))}]"


def mtok(v):
    return f"{v / 1e6:.1f}M"


def main():
    out = sys.argv[1]
    cells = defaultdict(list)  # (loader, budget) -> one dict per rep
    for path in sorted(glob.glob(os.path.join(out, "*-max.log"))):
        cell = os.path.basename(path)[: -len("-max.log")]
        loader, budget, _, _ = re.match(r"(\w+)-c(\d+)-n(\d+)-r(\d+)", cell).groups()
        unthrottled = line(path)
        probes = [line(p) for p in sorted(glob.glob(os.path.join(out, f"{cell}-p*.log")))]
        probes = [p for p in probes if p]
        sustained = [p for p in probes if p["sustained"]]
        best = max(sustained, key=lambda p: p["rank_tokens_per_s_asked"]) if sustained else None
        cells[(loader, int(budget))].append(
            {
                "unthrottled": min(r["steady_tokens_per_s"] for r in unthrottled["per_rank"])
                if unthrottled
                else None,
                "best": best,
                "lowest": min(probes, key=lambda p: p["rank_tokens_per_s_asked"]) if probes else None,
            }
        )

    rows = [
        "| loader | CPUs | reps | max sustained tokens/s per trainer | unthrottled (slowest trainer) "
        "| CPU cores at max | peak memory GiB at max |",
        "|---|---|---|---|---|---|---|",
    ]
    for (loader, budget), reps in sorted(cells.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        best = [r["best"] for r in reps if r["best"]]
        cell = spread([b["rank_tokens_per_s_asked"] for b in best], mtok)
        if len(best) < len(reps):
            lowest = [r["lowest"] for r in reps if not r["best"] and r["lowest"]]
            cell += f" ({len(reps) - len(best)} reps none: at {spread([l['rank_tokens_per_s_asked'] for l in lowest], mtok)} waits {spread([l['max_wait_fraction'] for l in lowest], lambda v: f'{v:.1%}')})"
        rows.append(
            f"| {loader} | {budget} | {len(reps)} | {cell} "
            f"| {spread([r['unthrottled'] for r in reps if r['unthrottled']], mtok)} "
            f"| {spread([b['steady_cpu_cores'] for b in best if b['steady_cpu_cores']], lambda v: f'{v:.2f}')} "
            f"| {spread([b['memory_peak_gib'] for b in best], lambda v: f'{v:.2f}')} |"
        )
    print("\n".join(rows))


if __name__ == "__main__":
    main()
