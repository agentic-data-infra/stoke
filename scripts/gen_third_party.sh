#!/bin/bash
# Generates the deps file of the Kiln submodule (the Buck cell kiln//), whose
# folly, fbthrift and gtest stoke builds against. Re-run after reinstalling
# folly or fbthrift.
#
# Usage: scripts/gen_third_party.sh [--prefix DIR]   (default /opt/deps-gcc12)

set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
gen=$root/third-party/kiln/scripts/gen_third_party.py
if [[ ! -x "$gen" ]]; then
  echo "third-party/kiln is empty: run git submodule update --init" >&2
  exit 1
fi
exec "$gen" "$@"
