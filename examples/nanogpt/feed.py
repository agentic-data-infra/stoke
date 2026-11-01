"""The Ember job that feeds the trainer: FineWeb-Edu documents to byte
tokens to int32 [b, T + 1] batches, written into a stoke stream.

    python examples/nanogpt/feed.py PARQUET STREAM [--metadata HOST:PORT]

PARQUET is a file or a directory of files.

Needs Ember's Python module built with stoke (-c ember.stoke=true), from
$EMBER_HOME (default ~/source/ember). Each scan task frames its batches
into chunks of --chunk_mb and puts each chunk into the stream as soon as
it is written, so the trainer starts while the job runs; with the
stream's task budget a task waits for the trainer. The stream is sealed
when the job commits.
"""

import argparse
import os
import sys
import time

sys.path.insert(
    0, os.path.join(os.environ.get("EMBER_HOME", os.path.expanduser("~/source/ember")), "python")
)

from ember import Session  # noqa: E402
from ember.functions import call, col, lit  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("parquet")
    parser.add_argument("stream")
    parser.add_argument("--metadata", default=os.environ.get("KILN_METADATA", "localhost:7470"))
    parser.add_argument("--master", default="local[8]")
    parser.add_argument("--min_score", type=int, default=4)
    parser.add_argument("--batch", type=int, default=16, help="B: sequences per batch at most")
    parser.add_argument("--sequence", type=int, default=128, help="T")
    parser.add_argument("--chunk_mb", type=int, default=4, help="bytes of batches per chunk")
    args = parser.parse_args()

    ember = (
        Session.builder.appName("nanogpt-feed")
        .master(args.master)
        .config("ember.kiln.metadata", args.metadata)
        .config("ember.stoke.chunk_bytes", str(args.chunk_mb << 20))
        .getOrCreate()
    )
    start = time.time()
    batches = (
        ember.read.parquet(args.parquet)
        .filter(col("int_score") >= args.min_score)
        .select(
            call(
                "stoke_pack",
                call("stoke_tokenize", col("text")),
                lit(args.batch),
                lit(args.sequence),
            ).alias("batch")
        )
        .filter(col("batch").isNotNull())
        .write.stream(args.stream, seal=True)
    )
    print(f"feed: {batches} batches into {args.stream} in {time.time() - start:.1f} s")
    ember.stop()


if __name__ == "__main__":
    main()
