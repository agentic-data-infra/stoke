"""Tokenized documents for the dataloader benchmark (Experiment 2).

    python prep_tokenized.py OUT_DIR INPUT.parquet... [--row_groups_per_file 16] [--workers N]

Reads FineWeb-Edu Parquet files and writes one row per document: its GPT-2
tokens (HF tokenizers, list<int32>, ending in the end-of-text token 50256
that separates documents once packed), int_score, language_score and
token_count (the number of tokens, the end-of-text token included). Every document is
kept; the loaders filter. Each input row group becomes one output row
group, in input order, and every --row_groups_per_file of them one file
(part-00000.parquet, ...), plain (uncompressed, no dictionary) like
Experiment 1's data.
"""

import argparse
import multiprocessing as mp
import os
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

END_OF_TEXT = 50256

_tokenizer = None


def _init():
    global _tokenizer
    from tokenizers import Tokenizer

    _tokenizer = Tokenizer.from_pretrained("gpt2")


def _tokenize(task):
    path, row_group = task
    table = pq.ParquetFile(path).read_row_group(
        row_group, columns=["text", "int_score", "language_score"]
    )
    encodings = _tokenizer.encode_batch(table["text"].to_pylist())
    ids = [np.append(np.asarray(e.ids, dtype=np.int32), END_OF_TEXT) for e in encodings]
    lengths = np.array([len(i) for i in ids], dtype=np.int32)
    offsets = np.zeros(len(lengths) + 1, dtype=np.int32)
    np.cumsum(lengths, out=offsets[1:])
    tokens = pa.ListArray.from_arrays(
        pa.array(offsets),
        pa.array(np.concatenate(ids), type=pa.int32()),
    )
    return pa.table(
        {
            "tokens": tokens,
            "int_score": table["int_score"],
            "language_score": table["language_score"],
            "token_count": pa.array(lengths.astype(np.int64)),
        }
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("out_dir")
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--row_groups_per_file", type=int, default=16)
    parser.add_argument("--workers", type=int, default=os.cpu_count())
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    start = time.time()
    tasks = [(path, g) for path in args.inputs for g in range(pq.ParquetFile(path).num_row_groups)]
    docs = tokens = 0
    writer = None
    with mp.get_context("spawn").Pool(args.workers, initializer=_init) as pool:
        for i, table in enumerate(pool.imap(_tokenize, tasks)):
            if i % args.row_groups_per_file == 0:
                if writer is not None:
                    writer.close()
                out = os.path.join(args.out_dir, f"part-{i // args.row_groups_per_file:05d}.parquet")
                writer = pq.ParquetWriter(out, table.schema, compression="none", use_dictionary=False)
            writer.write_table(table)
            docs += table.num_rows
            tokens += int(table["token_count"].to_numpy().sum())
    writer.close()
    print(f"{docs} documents, {tokens} tokens in {len(tasks)} row groups, {time.time() - start:.0f} s")


if __name__ == "__main__":
    main()
