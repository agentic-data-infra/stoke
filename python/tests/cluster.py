"""A Kiln cluster and stoke shards in local processes, for the tests."""

import os
import socket
import subprocess
import tempfile
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def build(*targets):
    """The outputs of Buck targets, built if need be."""
    out = subprocess.run(
        ["buck2", "build", *targets, "--show-full-output"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [line.split()[1] for line in out.splitlines() if line.strip()]


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Cluster:
    """kiln_meta, one DRAM kiln_server and 'shards' stoke_shard processes."""

    def __init__(self, shards=2):
        self.num_shards = shards
        self.procs = []
        self.dir = tempfile.TemporaryDirectory()

    def _start(self, name, args, ready):
        log = os.path.join(self.dir.name, name + ".log")
        with open(log, "w") as out:
            proc = subprocess.Popen(args, stdout=out, stderr=subprocess.STDOUT)
        self.procs.append(proc)
        deadline = time.time() + 30
        while time.time() < deadline:
            with open(log) as f:
                if ready in f.read():
                    return
            if proc.poll() is not None:
                break
            time.sleep(0.05)
        raise RuntimeError(f"{name} did not start: " + open(log).read())

    def __enter__(self):
        meta, server, shard = build(
            "kiln//bin:kiln_meta", "kiln//bin:kiln_server", "//bin:stoke_shard"
        )
        port = free_port()
        self.metadata = f"localhost:{port}"
        self._start(
            "meta",
            [meta, f"--port={port}", "--grace_period_s=0"],
            "Metadata server at",
        )
        self._start(
            "server",
            [
                server,
                f"--metadata={self.metadata}",
                "--host=localhost",
                "--region_size_mb=64",
            ],
            " serves",
        )
        self.shards = []
        for i in range(self.num_shards):
            shard_port = free_port()
            self._start(
                f"shard{i}",
                [
                    shard,
                    f"--port={shard_port}",
                    "--bind_host=127.0.0.1",
                    f"--metadata={self.metadata}",
                ],
                "Shard at",
            )
            self.shards.append(f"127.0.0.1:{shard_port}")
        return self

    def __exit__(self, *exc):
        for proc in reversed(self.procs):
            proc.terminate()
        for proc in self.procs:
            proc.wait(timeout=30)
        self.dir.cleanup()
