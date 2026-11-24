#!/bin/bash
# Builds a manylinux wheel of stoke's Python package into dist/.
#
#   scripts/build_wheel.sh
#   pip install ember-*.whl dist/stoke-*.whl
#
# The module is built for the Python in .venv, so the wheel is tagged for
# that interpreter. auditwheel copies the shared libraries _stoke.so still
# links (gflags, glog, boost_context, ...) into the wheel. The wheel needs
# Ember's (from its scripts/build_wheel.sh) for feeds; torch is not pinned,
# so an installed one (e.g. a CPU build) is kept.
set -euo pipefail
cd "$(dirname "$0")/.."

PYTHON=.venv/bin/python
PLAT=manylinux_2_35_x86_64
TAG=$($PYTHON -c 'import sys; print(f"cp{sys.version_info.major}{sys.version_info.minor}")')

uv pip install -q -p $PYTHON wheel auditwheel patchelf

if [ ! -e /opt/gflags-pic-gcc12/lib/libgflags.a ]; then
    scripts/build_gflags_pic.sh
fi
buck2 build //python:_stoke --out python/stoke/_stoke.so

rm -rf dist build/wheel
uv build -q --wheel python/ -o build/wheel
# setuptools sees no extension to compile, so it tags the wheel py3-none-any;
# the prebuilt _stoke.so makes it a binary wheel for one interpreter.
$PYTHON -m wheel tags --remove --python-tag $TAG --abi-tag $TAG \
    --platform-tag linux_x86_64 build/wheel/stoke-*.whl
# auditwheel runs patchelf (installed into .venv above) from PATH.
PATH=$PWD/.venv/bin:$PATH $PYTHON -m auditwheel repair --plat $PLAT -w dist build/wheel/stoke-*.whl
rm -rf build/wheel
