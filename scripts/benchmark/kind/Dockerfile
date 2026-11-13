# Experiment 2b's image (build_image.sh): Ember's worker and Python package built with stoke (-c ember.stoke=true),
# Kiln's servers, and stoke's shard, CLI and loader module, from one Ember checkout (BUILD_INFO); the benchmark's
# Python venv (Grain, Zephon; _stoke.so is built for its Python) with the Python it was made from, at their paths on
# the build host (venvs are not relocatable), and at /opt/venv. The Ember feed runs on Ubuntu 22.04's Python 3.10, as
# Ember's .venv. Binaries and their non-glibc libraries are built on Ubuntu 22.04.
FROM ubuntu:22.04
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends python3 && rm -rf /var/lib/apt/lists/*
COPY rootfs/ /
ARG VENV
RUN ln -s "$VENV" /opt/venv
ENV PATH=/opt/ember/bin:$PATH LD_LIBRARY_PATH=/opt/ember/lib
