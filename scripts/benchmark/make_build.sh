#!/bin/bash
# Builds what the stoke runs need from an Ember checkout into BUILD_DIR, so
# that Ember's feed, the shards and the loader share one Kiln (Ember's
# pin): bin/stoke_shard, bin/stoke and bin/_stoke.so (stoke and Kiln as
# pinned by Ember), python/ember (Ember's Python package, built with
# stoke), and BUILD_INFO. It also builds Kiln's servers in the Ember
# checkout's Kiln, which bench.py starts. Builds go to Buck's isolation
# dir buck-out/stoke, next to Ember's default build.
#
# Usage: make_build.sh EMBER_HOME BUILD_DIR

set -euo pipefail

ember=$(cd "$1" && pwd)
out=$2
mkdir -p "$out/bin" "$out/python"

outputs=$(cd "$ember" && buck2 --isolation-dir stoke build -c ember.stoke=true \
  //python:_ember stoke//bin:stoke_shard stoke//bin:stoke stoke//python:_stoke \
  --show-full-output | awk '{print $1, $2}')
path() { awk -v target="$1" '$1 == target {print $2}' <<<"$outputs"; }

cp "$(path stoke//bin:stoke_shard)" "$(path stoke//bin:stoke)" "$(path stoke//python:_stoke)" "$out/bin/"
rm -rf "$out/python/ember"
cp -r "$ember/python/ember" "$out/python/"
cp "$(path root//python:_ember)" "$out/python/ember/_ember.so"
(cd "$ember/third-party/kiln" && buck2 build //bin:kiln_meta //bin:kiln_server >/dev/null)

{
  echo "built $(date -u) from $ember"
  for repo in . third-party/kiln third-party/stoke; do
    echo "$repo $(git -C "$ember/$repo" rev-parse HEAD) $(git -C "$ember/$repo" status --porcelain | wc -l) changed"
  done
  md5sum "$out"/bin/* "$out/python/ember/_ember.so"
} >"$out/BUILD_INFO"
cat "$out/BUILD_INFO"
