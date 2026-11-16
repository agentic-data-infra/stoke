"""Trains a small Transformer on FineWeb-Edu, fed by an Ember job in the same
process, through a stoke stream on this machine: no servers to start.

    python examples/local/train.py PARQUET [--max_steps N]

PARQUET is a file or a directory of files with FineWeb-Edu's columns
(text, int_score, language). Needs the ember and stoke packages and torch.

The Ember job keeps English documents of score 4 and up and more than 200
characters; the stoke connector tokenizes their text into bytes and packs
the tokens into int32 [16, 129] batches. The job runs on a thread while the
trainer reads the batches; with the stream's task budget each feed task
waits for the trainer. The loop ends when the job is done, or after
--max_steps steps; leaving the `with` then ends the job.
"""

import argparse
import time

import torch
import torch.nn as nn
import torch.nn.functional as F
from ember import Session
from ember.functions import col, length

import stoke

VOCAB = 257  # byte tokens and the end of text (stoke/format/Packer.h)
SEQUENCE = 128


class Transformer(nn.Module):
    """A causal language model: token and position embeddings, a stack of
    pre-norm Transformer layers and a head tied to the token embedding."""

    def __init__(self, layers=2, heads=4, width=128):
        super().__init__()
        self.tokens = nn.Embedding(VOCAB, width)
        self.positions = nn.Embedding(SEQUENCE, width)
        layer = nn.TransformerEncoderLayer(
            width, heads, 4 * width, dropout=0.0, batch_first=True, norm_first=True
        )
        self.layers = nn.TransformerEncoder(layer, layers, enable_nested_tensor=False)
        self.norm = nn.LayerNorm(width)
        self.mask = nn.Transformer.generate_square_subsequent_mask(SEQUENCE)

    def forward(self, x, y):
        positions = torch.arange(x.shape[1])
        h = self.tokens(x) + self.positions(positions)
        h = self.layers(h, mask=self.mask, is_causal=True)
        logits = self.norm(h) @ self.tokens.weight.T
        return F.cross_entropy(logits.reshape(-1, VOCAB), y.reshape(-1))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("parquet")
    parser.add_argument("--max_steps", type=int, default=0, help="0: until the job is done")
    parser.add_argument("--log_every", type=int, default=50)
    args = parser.parse_args()

    ember = Session.builder.appName("local-train").master("local[4]").getOrCreate()
    docs = (
        ember.read.parquet(args.parquet)
        .filter((col("int_score") >= 4) & (col("language") == "en"))
        .filter(length(col("text")) > 200)
        .select("text")
    )

    torch.manual_seed(1234)
    model = Transformer()
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3)

    start = time.time()
    step = 0
    with stoke.LocalStream("fineweb", task_budget=4, memory="1g") as stream:
        stream.feed(
            docs.write.format("stoke").option("batch", 16).option("sequence", SEQUENCE)
        )
        dataset = stream.dataset()
        for batch in dataset:
            step += 1
            x, y = batch[:, :-1].long(), batch[:, 1:].long()
            loss = model(x, y)
            optimizer.zero_grad(set_to_none=True)
            loss.backward()
            optimizer.step()
            if step % args.log_every == 0:
                print(f"step {step:5d}  loss {loss.item():.3f}", flush=True)
            if step == args.max_steps:
                break
        stats = dataset.stats()
    print(
        f"{step} steps in {time.time() - start:.1f} s; waited for data "
        f"{stats['wait_seconds']:.1f} s; {stats['chunks']} chunks, "
        f"{stats['bytes'] / 2**20:.1f} MiB"
    )
    ember.stop()


if __name__ == "__main__":
    main()
