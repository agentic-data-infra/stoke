#!/bin/bash
# Experiment 2 at equal core budgets: Grain, Zephon and stoke, one
# run at a time, each with 4 synthetic trainers (exp2.py) on the
# tokenized documents (prep_tokenized.py), warm page cache, in a scope of
# its own with a memory cap. A budget of C CPUs is C/2 whole cores (CPU k
# and k+8, from core 0 up); the trainers run on CPUs 6, 7, 14 and 15. Each
# loader's knob (Grain and Zephon workers in all, DOP / 4 per rank; stoke's
# Ember local[N]) takes each factor times C.
#
# For each loader, budget, factor and rep it first runs the trainers
# unthrottled (step 0, MAX_STEPS steps each) and takes the slowest rank's
# steady rate R; then it bisects (PROBES runs, on a log scale between LOW
# and HIGH times R) for the fastest rate every rank sustains, waiting less
# than 1% of the time: that cell's max sustainable rate ("none" if even the
# lowest probe fails). Each of those runs lasts about SECONDS_PER_RUN (fewer steps if the
# data ends first). Results (JSON lines, logs, versions) go to OUT_DIR.
#
# Usage: run_exp2.sh OUT_DIR [DATA_DIR] (default ~/data/dlbench-exp2)
# Environment: LOADERS ("grain zephon stoke"), BUDGETS ("4 8 12"),
# FACTORS ("1"), REPS (3), MAX_STEPS (1000), LOW (0.25), HIGH (2), PROBES (5),
# SECONDS_PER_RUN (15), MEMORY_MAX (24G), PYTHON (python3: the benchmark's
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

run() {  # loader budget factor name step_ms steps; prints the JSON line
  local stoke=()
  if [[ $1 == stoke ]]; then
    stoke=(--ember_home="$EMBER_HOME" --build="$STOKE_BUILD" --feed_log="$out/$4.feed.log")
  fi
  cat "$data"/*.parquet >/dev/null  # warm the page cache
  systemd-run --user --scope -p MemoryMax="${MEMORY_MAX:-24G}" -q \
    "$python" "$here/exp2.py" "$1" "$data" --dop=$(($3 * $2)) --cpus="$(cpus "$2")" \
    --step_ms="$5" --steps="$6" "${stoke[@]}" --out="$out/results.jsonl" >"$out/$4.log" 2>&1 ||
    echo "failed, see $out/$4.log" >&2
  grep '^{' "$out/$4.log" | tail -1
}

# Reps outermost, so that a matrix cut short still compares every loader.
for rep in $(seq "${REPS:-3}"); do
  for budget in ${BUDGETS:-4 8 12}; do
    for factor in ${FACTORS:-1}; do
      for loader in $loaders; do
        cell=$loader-c$budget-n$((factor * budget))-r$rep
        echo "== $cell"
        line=$(run "$loader" "$budget" "$factor" "$cell-max" 0 "${MAX_STEPS:-1000}")
        # The slowest rank's steady tokens/s.
        rate=$(python3 -c 'import json, sys
print(min(r["steady_tokens_per_s"] for r in json.loads(sys.argv[1])["per_rank"]))' "$line") || continue
        echo "unthrottled: slowest rank $rate tokens/s"
        # Bisect the rate between LOW and HIGH times R on a log scale.
        lo=$(awk -v r="$rate" -v f="${LOW:-0.25}" 'BEGIN {print r * f}')
        hi=$(awk -v r="$rate" -v f="${HIGH:-2}" 'BEGIN {print r * f}')
        best=none
        for i in $(seq "${PROBES:-5}"); do
          ask=$(awk -v l="$lo" -v h="$hi" 'BEGIN {printf "%.0f", sqrt(l * h)}')
          step=$(awk -v r="$ask" 'BEGIN {printf "%.4f", 32 * 4097 * 1000 / r}')
          steps=$(awk -v s="$step" -v d="${SECONDS_PER_RUN:-15}" 'BEGIN {print int(d * 1000 / s)}')
          line=$(run "$loader" "$budget" "$factor" "$cell-p$i" "$step" "$steps")
          echo "$ask tokens/s: $(echo "$line" | grep -o '"max_wait_fraction": [^,]*, "sustained": [a-z]*')"
          if [[ $line == *'"sustained": true'* ]]; then lo=$ask; best=$ask; else hi=$ask; fi
        done
        echo "$cell max sustained per rank: $best tokens/s"
      done
    done
  done
done
