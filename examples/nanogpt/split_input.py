"""Splits a parquet file into FILES files of whole row groups.

    python examples/nanogpt/split_input.py PARQUET OUT_DIR [--files 64]

Ember scans a parquet file in one task; split files give the feed many
tasks that write into the stream in parallel.
"""

import argparse
import os

import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("parquet")
    parser.add_argument("out_dir")
    parser.add_argument("--files", type=int, default=64)
    args = parser.parse_args()

    source = pq.ParquetFile(args.parquet)
    groups = source.metadata.num_row_groups
    files = min(args.files, groups)
    os.makedirs(args.out_dir, exist_ok=True)
    for f in range(files):
        first, last = f * groups // files, (f + 1) * groups // files
        table = source.read_row_groups(list(range(first, last)))
        pq.write_table(table, os.path.join(args.out_dir, f"part-{f:05d}.parquet"))
    print(f"split {args.parquet} into {files} files in {args.out_dir}")


if __name__ == "__main__":
    main()
