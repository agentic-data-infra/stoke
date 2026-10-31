"""stoke streams as torch datasets."""

import os

import torch
import torch.distributed as dist

from stoke._stoke import Loader


def default_metadata():
    """The Kiln cluster: $KILN_METADATA, else localhost:7470."""
    return os.environ.get("KILN_METADATA", "localhost:7470")


class StokeDataset(torch.utils.data.IterableDataset):
    """The batches of a stoke stream, as tensors, until the stream ends.

    Batches come batched (each is one tensor as the producer packed it), and
    the loader fetches with threads of its own, so use it with
    DataLoader(dataset, batch_size=None, num_workers=0), or iterate it
    directly.

    'job' names the consumer and must differ between ranks, e.g.
    f"{run}-rank{rank}". Restart with a higher 'incarnation': chunks leased
    to the previous one go back to the stream. Other keyword arguments go to
    Loader (fetchers, buffered_batches, chunks_per_take, wait_ms, ...).
    """

    def __init__(self, stream, *, job, incarnation=0, metadata=None, **options):
        super().__init__()
        self.stream = stream
        self.job = job
        self.incarnation = incarnation
        self.metadata = metadata or default_metadata()
        self.options = options
        self.loader = None

    def __iter__(self):
        self.loader = Loader(
            self.metadata,
            self.stream,
            self.job,
            self.incarnation,
            **self.options,
        )
        for tensor in self.loader:
            yield torch.from_dlpack(tensor)

    def stats(self):
        """The loader's counters; wait_seconds is the time spent starving."""
        return self.loader.stats() if self.loader else {}


def all_ranks_have_data(has_data):
    """Whether every rank still has a batch for this step.

    Shards end a stream at about the same time for all ranks, but not at the
    same step: call this every step and stop all ranks together once it is
    False.
    """
    flag = torch.tensor([1 if has_data else 0], dtype=torch.int32)
    dist.all_reduce(flag, op=dist.ReduceOp.MIN)
    return bool(flag.item())
