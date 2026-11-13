#!/bin/bash
# Experiment 1 at equal core budgets: Grain, then Zephon, then stoke, one
# run at a time, each reading the whole dataset with a warm page cache, in
# a scope of its own with a memory cap. A budget of C CPUs is C/2 whole
# cores (CPU k and its hyperthread k+8, from core 0 up, as on a machine of
# 8 cores and 16 CPUs); the consumer thread runs on CPU 7 (its sibling 15
# idle), core 6 is left to the rest of the machine. Each loader's knob
# (Grain workers, Zephon max_workers and fetch parallelism, stoke's Ember
# local[N]) takes each factor times C. Results (JSON lines, logs, versions)
# go to OUT_DIR.
#
# Usage: run_exp1.sh OUT_DIR [DATA_DIR] (default ~/data/dlbench-exp1)
# Environment: LOADERS ("grain zephon stoke"), BUDGETS ("2 4 8 12"),
# FACTORS ("1 2"), REPS (3), MEMORY_MAX (24G), MAX_BATCHES (0: the whole
# dataset), PYTHON (python3: the benchmark's venv); for stoke EMBER_HOME
# (the Ember checkout) and STOKE_BUILD (make_build.sh's BUILD_DIR).

set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
out=$1
data=${2:-$HOME/data/dlbench-exp1}
python=${PYTHON:-python3}
loaders=${LOADERS:-grain zephon stoke}
mkdir -p "$out"

{
  date -u
  echo "stoke $(git -C "$here" rev-parse HEAD) $(git -C "$here" status --porcelain | wc -l) changed"
  if [[ $loaders == *stoke* ]]; then
    cat "$STOKE_BUILD/BUILD_INFO"
  fi
  "$python" -V
  "$python" -c 'from importlib import metadata
for p in ("grain", "zephon", "pyarrow", "numpy"):
    try:
        print(f"{p}=={metadata.version(p)}")
    except metadata.PackageNotFoundError:
        print(f"{p} not installed")'
  uname -r
  lscpu -e=CPU,CORE
  free -g
  du -sh "$data"
  ls "$data" | wc -l
} >"$out/versions.txt" 2>&1

cpus() {  # the first $1 / 2 cores, both hyperthreads of each
  local list=()
  for ((k = 0; k < $1 / 2; k++)); do list+=("$k" "$((k + 8))"); done
  (IFS=,; echo "${list[*]}")
}

for loader in $loaders; do
  for budget in ${BUDGETS:-2 4 8 12}; do
    for factor in ${FACTORS:-1 2}; do
      for rep in $(seq "${REPS:-3}"); do
        name=$loader-c$budget-n$((factor * budget))-r$rep
        cat "$data"/*.parquet >/dev/null  # warm the page cache
        echo "== $name"
        stoke=()
        if [[ $loader == stoke ]]; then
          stoke=(--ember_home="$EMBER_HOME" --build="$STOKE_BUILD" --feed_log="$out/$name.feed.log")
        fi
        systemd-run --user --scope -p MemoryMax="${MEMORY_MAX:-24G}" -q \
          "$python" "$here/bench.py" "$loader" "$data" --dop=$((factor * budget)) \
          --cpus="$(cpus "$budget")" --consumer_cpu=7 "${stoke[@]}" \
          --max_batches="${MAX_BATCHES:-0}" --out="$out/results.jsonl" >"$out/$name.log" 2>&1 ||
          echo "failed, see $out/$name.log"
        grep '^{' "$out/$name.log" | tail -1
      done
    done
  done
done
