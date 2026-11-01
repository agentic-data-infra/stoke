#!/bin/bash
# Trains a small GPT on FineWeb-Edu while an Ember job produces its batches,
# through a stoke stream, all on this machine:
#
#   1. a Kiln cluster (one DRAM server) and two stoke shards;
#   2. `stoke create` of the stream, with a task budget;
#   3. the trainer (torchrun, RANKS ranks on CPU with gloo) and the Ember
#      feed job (feed.py), at the same time; each feed task waits once it
#      has TASK_BUDGET chunks not yet consumed, so the feed runs at the
#      trainer's pace;
#   4. the feed seals the stream when its job commits; the ranks stop
#      together when the stream ends (or after MAX_STEPS steps) and print
#      how long each waited for data. A feed still waiting for the trainer
#      then is stopped.
#
# Usage: examples/nanogpt/run.sh [PARQUET]
#   (default ~/data/fineweb-edu/raw/sample/10BT/000_00000.parquet)
#
# The parquet file is split once into INPUT_FILES (64) files in
# $INPUT_DIR (default <PARQUET>.split), so that the feed has many tasks
# writing in parallel (split_input.py).
#
# Needs stoke's Python module and .venv (README: Dataloader), and Ember's
# Python module built with stoke in $EMBER_HOME (default ~/source/ember):
#   buck2 build -c ember.stoke=true //python:_ember --out python/ember/_ember.so
# Environment: RANKS (2), MAX_STEPS (0: until the stream ends), KILN_PORT
# (7570), SHARD_PORT (7580, and the next one), STREAM (fineweb),
# EMBER_PYTHON (Ember's .venv), FEED_ARGS (more feed.py flags), TASK_BUDGET
# (4: chunks each feed task may have put and not yet consumed; 0 for no
# limit).

set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
example=$root/examples/nanogpt
parquet=${1:-$HOME/data/fineweb-edu/raw/sample/10BT/000_00000.parquet}
ranks=${RANKS:-2}
max_steps=${MAX_STEPS:-0}
kiln_port=${KILN_PORT:-7570}
shard_port=${SHARD_PORT:-7580}
stream=${STREAM:-fineweb}
ember_home=${EMBER_HOME:-$HOME/source/ember}
ember_python=${EMBER_PYTHON:-$ember_home/.venv/bin/python}
input_dir=${INPUT_DIR:-$parquet.split}
work=$(mktemp -d)
export KILN_CLUSTER_DIR=$work/kiln

bin() {
  (cd "$root" && buck2 build "$1" --show-full-output 2>/dev/null | awk '{print $2}')
}
shard_bin=$(bin //bin:stoke_shard)
stoke_bin=$(bin //bin:stoke)

pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  "$root/third-party/kiln/scripts/stop_local_cluster.sh"
  rm -rf "$work"
}
trap cleanup EXIT

if [[ ! -d "$input_dir" ]]; then
  "$ember_python" "$example/split_input.py" "$parquet" "$input_dir" \
    --files="${INPUT_FILES:-64}"
fi

# Removed chunks' blocks are reused after 2 s rather than 30.
metadata=$("$root/third-party/kiln/scripts/start_local_cluster.sh" \
  -p "$kiln_port" -d 1 -l 0 -r 4 -g 2)
export KILN_METADATA=$metadata
shards=()
for i in 0 1; do
  port=$((shard_port + i))
  "$shard_bin" --port="$port" --bind_host=127.0.0.1 >"$work/shard$i.log" 2>&1 &
  pids+=($!)
  shards+=("127.0.0.1:$port")
done
until grep -q "Shard at" "$work/shard1.log" 2>/dev/null; do sleep 0.1; done
"$stoke_bin" create "$stream" --shards="$(IFS=,; echo "${shards[*]}")" \
  --task_budget="${TASK_BUDGET:-4}" >/dev/null

PYTHONPATH=$root/python:$example "$root/.venv/bin/torchrun" \
  --nproc_per_node="$ranks" "$example/train.py" \
  --stream="$stream" --metadata="$metadata" --max_steps="$max_steps" &
trainer=$!
pids+=($trainer)

# shellcheck disable=SC2086
"$ember_python" "$example/feed.py" "$input_dir" "$stream" \
  --metadata="$metadata" ${FEED_ARGS:-} &
feed=$!
pids+=($feed)
wait "$trainer"
if kill -0 "$feed" 2>/dev/null; then
  echo "feed: stopped, the trainer is done"
  kill "$feed"
fi
wait "$feed" || true
"$stoke_bin" stats "$stream"
"$stoke_bin" rm "$stream"
