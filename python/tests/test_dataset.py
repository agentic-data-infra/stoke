"""The Python dataloader against shard and Kiln processes."""

import threading

import numpy as np
import pytest
import torch

import stoke
from cluster import Cluster


@pytest.fixture(scope="module")
def cluster():
    with Cluster(shards=2) as c:
        yield c


def test_dataset_yields_every_batch_as_a_tensor(cluster):
    stoke.create_stream(cluster.metadata, "train", cluster.shards, 1.0)
    producer = stoke.Producer(cluster.metadata, "train")
    chunks, per_chunk = 12, 3

    def produce():
        for c in range(chunks):
            arrays = [
                np.full((2, 8), c * per_chunk + b, dtype=np.int32)
                for b in range(per_chunk)
            ]
            producer.put(arrays)
        stoke.seal_stream(cluster.metadata, "train")

    thread = threading.Thread(target=produce)
    thread.start()
    dataset = stoke.StokeDataset(
        "train", job="rank0", metadata=cluster.metadata, wait_ms=50
    )
    loader = torch.utils.data.DataLoader(dataset, batch_size=None)
    seen = []
    for batch in loader:
        assert batch.dtype == torch.int32
        assert batch.shape == (2, 8)
        assert torch.all(batch == batch[0, 0])
        seen.append(int(batch[0, 0]))
    thread.join()
    assert sorted(seen) == list(range(chunks * per_chunk))
    stats = dataset.stats()
    assert stats["chunks"] == chunks
    assert stats["lost_chunks"] == 0
    assert stats["wait_seconds"] >= 0
    assert stoke.remove_stream(cluster.metadata, "train")


def test_tensors_outlive_the_loader(cluster):
    stoke.create_stream(cluster.metadata, "keep", cluster.shards)
    stoke.Producer(cluster.metadata, "keep").put(
        [
            np.arange(6, dtype=np.float32).reshape(2, 3),
            np.array([True, False]),
            np.array([7, 8, 9], dtype=np.uint16),
        ]
    )
    loader = stoke.Loader(cluster.metadata, "keep", "rank0", fetchers=1)
    first, second, third = loader.next(), loader.next(), loader.next()
    assert first.shape == (2, 3) and first.dtype == "float32"
    del loader
    tensors = {tuple(t.shape): torch.from_dlpack(t) for t in (first, second)}
    del first, second
    assert tensors[(2, 3)].tolist() == [[0, 1, 2], [3, 4, 5]]
    assert tensors[(2,)].tolist() == [True, False]
    # NumPy takes them too.
    assert np.from_dlpack(third).tolist() == [7, 8, 9]
    assert stoke.remove_stream(cluster.metadata, "keep")


def test_errors_surface_as_python_exceptions(cluster):
    with pytest.raises(RuntimeError, match="no stream"):
        stoke.Loader(cluster.metadata, "nope", "rank0")
    with pytest.raises(ValueError, match="dtype"):
        stoke.create_stream(cluster.metadata, "c", cluster.shards)
        stoke.Producer(cluster.metadata, "c").put([np.zeros(2, np.complex64)])
