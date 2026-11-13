#!/bin/bash
# One stoke run of Experiment 1 at a budget of BUDGET CPUs (as in
# run_exp1.sh), profiled in steady state: a CPU profile of the budget CPUs,
# and where fresh memory comes from: every mmap/munmap/madvise call and
# every 50th page fault, with DWARF call stacks (Ember's libraries have no
# frame pointers). Needs sudo for perf.
#
# Usage: profile_faults.sh OUT_DIR [DATA_DIR] (default ~/data/dlbench-exp1)
# Environment: EMBER_HOME and STOKE_BUILD (as in run_exp1.sh), PYTHON
# (python3), BUDGET (2), DELAY (8 s before recording), SECONDS_RECORDED (15).

set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
out=$1
data=${2:-$HOME/data/dlbench-exp1}
python=${PYTHON:-python3}
mkdir -p "$out"
cat "$STOKE_BUILD/BUILD_INFO" >"$out/BUILD_INFO"

budget=${BUDGET:-2}
cpus=()  # the first budget / 2 cores, both hyperthreads of each
for ((k = 0; k < budget / 2; k++)); do cpus+=("$k" "$((k + 8))"); done
cpus=$(IFS=,; echo "${cpus[*]}")

cat "$data"/*.parquet >/dev/null  # warm the page cache
systemd-run --user --scope -p MemoryMax=24G -q \
  "$python" "$here/bench.py" stoke "$data" --dop="$budget" --cpus="$cpus" --consumer_cpu=7 \
  --ember_home="$EMBER_HOME" --build="$STOKE_BUILD" --out="$out/results.jsonl" \
  --feed_log="$out/feed.log" >"$out/bench.log" 2>&1 &
bench=$!

sleep "${DELAY:-8}"
duration=${SECONDS_RECORDED:-15}
sudo perf record -F 999 -g -C "$cpus,7" -o "$out/cpu.data" -- sleep "$duration" 2>"$out/perf-cpu.log" &
cpu=$!
sudo perf record -a --call-graph dwarf,16384 \
  -e syscalls:sys_enter_mmap -e syscalls:sys_enter_munmap -e syscalls:sys_enter_madvise \
  -e page-faults/period=50/ \
  -o "$out/mem.data" -- sleep "$duration" 2>"$out/perf-mem.log" &
mem=$!
wait "$cpu" "$mem" || true
wait "$bench"
sudo chown "$USER" "$out"/*.data
grep '^{' "$out/bench.log" | tail -1
