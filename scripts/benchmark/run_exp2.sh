#!/bin/bash
# Experiment 2 at equal core budgets: Grain, then Zephon, then stoke, one
# run at a time, each with 4 synthetic trainers (exp2.py) on the
# tokenized documents (prep_tokenized.py), warm page cache, in a scope of
# its own with a memory cap. A budget of C CPUs is C/2 whole cores (CPU k
# and k+8, from core 0 up); the trainers run on CPUs 6, 7, 14 and 15. Each
# loader's knob (Grain and Zephon workers in all, DOP / 4 per rank; stoke's
# Ember local[N]) takes each factor times C.
#
# For each loader, budget, factor and rep it sweeps the trainers' step
# time from slow to fast (STEP_MS) and stops at the first rate a rank
# cannot sustain (it waits 1% of the time or more): the last sustained one
# is that cell's max sustainable rate. Each run lasts about SECONDS_PER_RUN
# (fewer steps if the data ends first). Results (JSON lines, logs,
# versions) go to OUT_DIR.
#
# Usage: run_exp2.sh OUT_DIR [DATA_DIR] (default ~/data/dlbench-exp2)
# Environment: LOADERS ("grain zephon stoke"), BUDGETS ("4 8 12"),
# FACTORS ("1"), REPS (3), STEP_MS ("8 4 2 1.5 1 0.75 0.5"),
# SECONDS_PER_RUN (20), MEMORY_MAX (24G), PYTHON (python3: the benchmark's
# venv); for stoke EMBER_HOME and STOKE_BUILD (as in run_exp1.sh).

set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
out=$1
data=${2:-$HOME/data/dlbench-exp2}
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
  for budget in ${BUDGETS:-4 8 12}; do
    for factor in ${FACTORS:-1}; do
      for rep in $(seq "${REPS:-3}"); do
        for step in ${STEP_MS:-8 4 2 1.5 1 0.75 0.5}; do
          name=$loader-c$budget-n$((factor * budget))-r$rep-s$step
          steps=$(awk -v s="$step" -v d="${SECONDS_PER_RUN:-20}" 'BEGIN {print int(d * 1000 / s)}')
          cat "$data"/*.parquet >/dev/null  # warm the page cache
          echo "== $name"
          stoke=()
          if [[ $loader == stoke ]]; then
            stoke=(--ember_home="$EMBER_HOME" --build="$STOKE_BUILD" --feed_log="$out/$name.feed.log")
          fi
          systemd-run --user --scope -p MemoryMax="${MEMORY_MAX:-24G}" -q \
            "$python" "$here/exp2.py" "$loader" "$data" --dop=$((factor * budget)) \
            --cpus="$(cpus "$budget")" --step_ms="$step" --steps="$steps" "${stoke[@]}" \
            --out="$out/results.jsonl" >"$out/$name.log" 2>&1 ||
            echo "failed, see $out/$name.log"
          line=$(grep '^{' "$out/$name.log" | tail -1)
          echo "$line" | cut -c1-300
          [[ $line == *'"sustained": true'* ]] || break
        done
      done
    done
  done
done
