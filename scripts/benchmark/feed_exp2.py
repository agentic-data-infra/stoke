"""The Ember job that feeds stoke in Experiment 2: reads the tokenized
documents (prep_tokenized.py), keeps those that pass the filter, and
writes their tokens into a stoke stream, which the stream writer packs
into int32 batches of [32, 4097] (write.stream(batch=32, sequence=4096)).
The stream is sealed when the job commits.

    python feed_exp2.py DATA_DIR STREAM --metadata HOST:PORT [--master local[N]]

Its scan shuffles each file's ranges (ember.files.shuffle=file, seed 0). More
Session configuration, which overrides that, comes from EMBER_CONF="k=v k=v ...". Needs Ember's Python module built with stoke (-c ember.stoke=true) on
PYTHONPATH.
"""

import argparse
import os
import time

from ember import Session

from exp2_common import BATCH, FILTER_SQL, SEQ


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("data_dir")
    parser.add_argument("stream")
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--chunk_mb", type=int, default=16, help="bytes of batches per chunk")
    args = parser.parse_args()

    builder = (
        Session.builder.appName("dlbench-feed-exp2")
        .master(args.master)
        .config("ember.kiln.metadata", args.metadata)
        .config("ember.stoke.chunk_bytes", str(args.chunk_mb << 20))
        .config("ember.files.shuffle", "file")
        .config("ember.files.shuffle_seed", "0")
    )
    for kv in os.environ.get("EMBER_CONF", "").split():
        builder = builder.config(*kv.split("=", 1))
    ember = builder.getOrCreate()
    start = time.time()
    docs = (
        ember.read.parquet(args.data_dir)
        .filter(FILTER_SQL)
        .select("tokens")
        .write.stream(args.stream, seal=True, batch=BATCH, sequence=SEQ - 1)
    )
    print(f"feed: {docs} documents into {args.stream} in {time.time() - start:.1f} s", flush=True)
    ember.stop()


if __name__ == "__main__":
    main()
