"""stoke's dataloader: training batches of a stoke stream for PyTorch.

Loader is the C++ dataloader: its fetcher threads take chunks from the
stream's shards and read them from Kiln; iterating it yields Tensors, which
torch.from_dlpack (or numpy.from_dlpack) takes without copying.
StokeDataset wraps it as a torch IterableDataset. Producer puts chunks of
NumPy arrays, for feeds that are not Ember jobs and for tests.
"""

from stoke._stoke import (
    Loader,
    Producer,
    Tensor,
    create_stream,
    remove_stream,
    seal_stream,
)

__all__ = [
    "Loader",
    "Producer",
    "Tensor",
    "create_stream",
    "remove_stream",
    "seal_stream",
    "StokeDataset",
    "all_ranks_have_data",
]


def __getattr__(name):
    # The torch parts only when asked for, so Loader works without torch.
    if name in ("StokeDataset", "all_ranks_have_data"):
        from stoke import dataset

        return getattr(dataset, name)
    raise AttributeError(name)
