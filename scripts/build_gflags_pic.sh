#!/bin/bash
# Builds gflags 2.2.2 as a static, position-independent library into
# /opt/deps-gcc12/gflags-pic, for stoke's Python module (python/BUCK): it
# links a gflags of its own, so that its folly and fbthrift register their
# flags apart from Ember's module in the same process. Once per machine;
# scripts/gen_third_party.sh and scripts/build_wheel.sh run it if missing.
set -euo pipefail

PREFIX=/opt/deps-gcc12/gflags-pic
VERSION=2.2.2
src=$(mktemp -d)
trap 'rm -rf "$src"' EXIT

curl -sSL "https://github.com/gflags/gflags/archive/refs/tags/v$VERSION.tar.gz" |
    tar xz -C "$src" --strip-components=1
cmake -S "$src" -B "$src/build" -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER=gcc-12 -DCMAKE_CXX_COMPILER=g++-12 \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
    -DBUILD_SHARED_LIBS=OFF -DBUILD_STATIC_LIBS=ON \
    -DBUILD_gflags_nothreads_LIB=OFF -DBUILD_TESTING=OFF \
    -DCMAKE_INSTALL_PREFIX="$PREFIX" >/dev/null
cmake --build "$src/build" -j "$(nproc)" >/dev/null
cmake --install "$src/build" >/dev/null
echo "gflags $VERSION in $PREFIX"
