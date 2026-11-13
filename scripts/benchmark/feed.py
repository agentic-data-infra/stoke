"""The Ember job that feeds stoke in Experiment 1: each row of the
pre-batched files (32 x 4097 tokens) becomes one int32 [32, 4097] batch
(stoke_pack(tokens, 32, 4096)), written into a stoke stream, which is
sealed when the job commits.

    python feed.py DATA_DIR STREAM --metadata HOST:PORT [--master local[N]]

Needs Ember's Python module built with stoke (-c ember.stoke=true) on
PYTHONPATH.
"""

import argparse
import time

from ember import Session
from ember.functions import call, col, lit


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir")
    parser.add_argument("stream")
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--chunk_mb", type=int, default=16, help="bytes of batches per chunk")
    args = parser.parse_args()

    ember = (
        Session.builder.appName("dlbench-feed")
        .master(args.master)
        .config("ember.kiln.metadata", args.metadata)
        .config("ember.stoke.chunk_bytes", str(args.chunk_mb << 20))
        .getOrCreate()
    )
    start = time.time()
    batches = (
        ember.read.parquet(args.data_dir)
        .select(call("stoke_pack", col("tokens"), lit(32), lit(4096)).alias("batch"))
        .write.stream(args.stream, seal=True)
    )
    print(f"feed: {batches} batches into {args.stream} in {time.time() - start:.1f} s", flush=True)
    ember.stop()


if __name__ == "__main__":
    main()
