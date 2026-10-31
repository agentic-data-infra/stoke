"""A nanoGPT-style trainer fed by a stoke stream, one process per rank.

    torchrun --nproc_per_node=2 examples/nanogpt/train.py --stream fineweb

Each rank reads its own batches from the stream (StokeDataset, int32
[b, T + 1] batches of byte tokens from Ember's stoke_pack), while the
producer still writes it. All ranks stop at the same step once one of them
runs out (all_ranks_have_data). At the end every rank reports how much of
its time it waited for data.
"""

import argparse
import os
import time

import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel

import stoke
from model import GPT

VOCAB = 257  # byte tokens and the end of text (stoke_tokenize)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stream", required=True)
    parser.add_argument("--metadata", default=None, help="Kiln metadata server")
    parser.add_argument("--run", default="nanogpt")
    parser.add_argument("--sequence", type=int, default=128, help="T")
    parser.add_argument("--lr", type=float, default=1e-3)
    parser.add_argument("--max_steps", type=int, default=0, help="0: until the stream ends")
    parser.add_argument("--log_every", type=int, default=50)
    args = parser.parse_args()

    dist.init_process_group("gloo")
    rank, world = dist.get_rank(), dist.get_world_size()
    torch.manual_seed(1234)
    torch.set_num_threads(max(1, (os.cpu_count() or 2) // (2 * world)))

    model = DistributedDataParallel(GPT(VOCAB, args.sequence))
    optimizer = torch.optim.AdamW(model.parameters(), lr=args.lr)
    dataset = stoke.StokeDataset(
        args.stream,
        job=f"{args.run}-rank{rank}",
        # A restarted rank releases the chunks its previous run leased.
        incarnation=int(os.environ.get("TORCHELASTIC_RESTART_COUNT", "0")),
        metadata=args.metadata,
    )
    batches = iter(dataset)

    start = time.time()
    step, rows = 0, 0
    while args.max_steps == 0 or step < args.max_steps:
        batch = next(batches, None)
        if not stoke.all_ranks_have_data(batch is not None):
            break
        x, y = batch[:, :-1].long(), batch[:, 1:].long()
        loss = model(x, y)
        optimizer.zero_grad(set_to_none=True)
        loss.backward()
        optimizer.step()
        step += 1
        rows += batch.shape[0]
        if rank == 0 and step % args.log_every == 0:
            print(f"step {step:5d}  loss {loss.item():.3f}", flush=True)

    elapsed = time.time() - start
    stats = dataset.stats()
    report = torch.tensor(
        [stats.get("wait_seconds", 0.0), elapsed, rows, stats.get("chunks", 0)],
        dtype=torch.float64,
    )
    reports = [torch.zeros_like(report) for _ in range(world)]
    dist.all_gather(reports, report)
    if rank == 0:
        print(f"stopped after {step} steps, {elapsed:.1f} s")
        print("rank  waited s  wait fraction  sequences  chunks")
        for r, (waited, total, n, chunks) in enumerate(t.tolist() for t in reports):
            print(f"{r:4d}  {waited:8.2f}  {waited / total:13.1%}  {int(n):9d}  {int(chunks):6d}")
    dist.destroy_process_group()


if __name__ == "__main__":
    main()
