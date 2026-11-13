"""Pre-batched, pre-tokenized sequences for the dataloader benchmark
(Experiment 1).

    python prep_data.py OUT_DIR INPUT.parquet... [--target_gb 20]

Reads FineWeb-Edu Parquet files (column "text"), tokenizes with GPT-2 (HF
tokenizers), joins the documents in input order with <|endoftext|> after
each, and cuts the token stream into sequences of --seq_len tokens (the
tail of each input row group is dropped). Each output row is one batch:
--batch consecutive sequences, flattened into one list<int32> column
"tokens" (reshape to [batch, seq_len]; for stoke, stoke_pack(tokens, 32,
4096)). Files hold --rows_per_file rows, plain (uncompressed, no
dictionary), in row groups of --row_group_rows. It stops after --target_gb
of tokens.

Workers tokenize one input row group each; the main process writes, so
files are full and named in order (part-00000.parquet, ...).
"""

import argparse
import multiprocessing as mp
import os
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

EOS = 50256
_tokenizer = None


def _init():
    global _tokenizer
    from tokenizers import Tokenizer

    _tokenizer = Tokenizer.from_pretrained("gpt2")


def _tokenize(task):
    path, row_group, seq_len = task
    texts = pq.ParquetFile(path).read_row_group(row_group, columns=["text"])["text"].to_pylist()
    ids = [np.asarray(e.ids + [EOS], dtype=np.int32) for e in _tokenizer.encode_batch(texts)]
    tokens = np.concatenate(ids)
    n = len(tokens) // seq_len
    return tokens[: n * seq_len].reshape(n, seq_len)


def _write(out_dir, index, sequences, batch, row_group_rows):
    rows = sequences.reshape(len(sequences) // batch, -1)
    n, row_len = rows.shape
    tokens = pa.ListArray.from_arrays(
        pa.array(np.arange(0, (n + 1) * row_len, row_len, dtype=np.int32)),
        pa.array(rows.reshape(-1)),
    )
    pq.write_table(
        pa.table({"tokens": tokens}),
        os.path.join(out_dir, f"part-{index:05d}.parquet"),
        compression="none",
        use_dictionary=False,
        row_group_size=row_group_rows,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("out_dir")
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--seq_len", type=int, default=4097, help="tokens per sequence")
    parser.add_argument("--batch", type=int, default=32, help="sequences per row")
    parser.add_argument("--rows_per_file", type=int, default=128)
    parser.add_argument("--row_group_rows", type=int, default=8)
    parser.add_argument("--target_gb", type=float, default=20)
    parser.add_argument("--workers", type=int, default=os.cpu_count())
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    tasks = [
        (path, g, args.seq_len)
        for path in args.inputs
        for g in range(pq.ParquetFile(path).num_row_groups)
    ]
    per_file = args.rows_per_file * args.batch
    target = int(args.target_gb * 1e9) // (args.seq_len * 4)
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    start = time.time()
    pending, written, files = [], 0, 0
    with mp.get_context("spawn").Pool(args.workers, initializer=_init) as pool:
        for sequences in pool.imap(_tokenize, tasks):
            pending.append(sequences)
            buffered = sum(len(s) for s in pending)
            while buffered >= per_file and written < target:
                block = np.concatenate(pending)
                _write(args.out_dir, files, block[:per_file], args.batch, args.row_group_rows)
                pending = [block[per_file:]]
                buffered -= per_file
                written += per_file
                files += 1
            if written >= target:
                pool.terminate()
                break
    gb = written * args.seq_len * 4 / 1e9
    print(f"{files} files, {written // args.batch} rows of {args.batch} sequences, {gb:.2f} GB in {time.time() - start:.1f} s")


if __name__ == "__main__":
    main()
