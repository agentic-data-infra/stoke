#!/usr/bin/env bash
# Builds dlbench:<Ember commit> (Dockerfile) from an Ember checkout and the benchmark's venv, and loads it into the
# kind cluster "ember".
#
#   build_image.sh EMBER_HOME [VENV]      # VENV: the venv of requirements.txt, default ~/venvs/dlbench
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
ember=$(cd "$1" && pwd)
venv=$(cd "${2:-$HOME/venvs/dlbench}" && pwd)
tag=dlbench:$(git -C "$ember" rev-parse --short HEAD)
stage=$(mktemp -d); trap 'rm -rf "$stage"' EXIT
opt=$stage/rootfs/opt/ember
mkdir -p "$opt"/{bin,lib,python}
outputs=$(cd "$ember" && buck2 --isolation-dir stoke build -c ember.stoke=true //bin:ember_worker //python:_ember \
  stoke//bin:stoke_shard stoke//bin:stoke stoke//python:_stoke kiln//bin:kiln_meta kiln//bin:kiln_server \
  --show-full-output | awk '{print $1, $2}')
path() { awk -v t="$1" '$1 == t {print $2}' <<<"$outputs"; }
for t in root//bin:ember_worker kiln//bin:kiln_meta kiln//bin:kiln_server stoke//bin:stoke_shard stoke//bin:stoke \
  stoke//python:_stoke; do cp "$(path $t)" "$opt/bin/"; done
cp -r "$ember/python/ember" "$opt/python/"
cp "$(path root//python:_ember)" "$opt/python/ember/_ember.so"
# The libraries they link, but glibc's and Python's.
for f in "$opt"/bin/* "$opt/python/ember/_ember.so"; do ldd "$f" | awk '$3 ~ /^\// {print $3}'; done | sort -u |
  grep -vE '/(libc|libm|libdl|libpthread|librt|ld-linux[^/]*|libpython[^/]*)\.so' | xargs -I{} cp -L {} "$opt/lib/"
# The venv and its Python: the installation directory it names (a symlink for uv's) and the one behind it.
python_home=$(dirname "$(dirname "$(readlink "$venv/bin/python")")")
cp -a --parents "$venv" "$python_home" "$(readlink -f "$python_home")" "$stage/rootfs/"
{
  echo "image $tag built $(date -u) from $ember"
  for repo in . third-party/kiln third-party/stoke; do
    echo "$repo $(git -C "$ember/$repo" rev-parse HEAD) $(git -C "$ember/$repo" status --porcelain | wc -l) changed"
  done
  (cd "$opt" && md5sum bin/* python/ember/_ember.so)
} >"$opt/BUILD_INFO"
cat "$opt/BUILD_INFO"
docker build -q --build-arg VENV="$venv" -t "$tag" -f "$here/Dockerfile" "$stage"
kind load docker-image --name ember "$tag"
