"""LocalStream: Kiln, a shard and the feed in this process."""

import time

import numpy as np
import pytest
import torch

import stoke
from stoke.local import parse_memory


class ArrayWriter:
    """Stands in for Ember's DataframeWriter: save() puts 'chunks' chunks of
    one int32 batch each, then fails or seals as the options say."""

    def __init__(self, chunks, fail=False):
        self.chunks = chunks
        self.fail = fail
        self.options = {}

    def option(self, key, value):
        self.options[key] = value
        return self

    def save(self, name):
        metadata = self.options["kiln_metadata"]
        producer = stoke.Producer(metadata, name)
        for c in range(self.chunks):
            producer.put([np.full(4, c, dtype=np.int32)])
        if self.fail:
            raise ValueError("the feed failed")
        if self.options["seal"]:
            stoke.seal_stream(metadata, name)
        return self.chunks


def test_parse_memory():
    assert parse_memory("1g") == 1 << 30
    assert parse_memory("512M") == 512 << 20
    assert parse_memory(4096) == 4096
    assert parse_memory("4096") == 4096


def test_dataset_reads_what_the_feed_wrote():
    with stoke.LocalStream("local", task_budget=2, memory="64m") as stream:
        stream.feed(ArrayWriter(20))
        dataset = stream.dataset(wait_ms=50)
        seen = [int(batch[0]) for batch in dataset]
        assert sorted(seen) == list(range(20))
        stats = dataset.stats()
        assert stats["chunks"] == 20
        assert stats["lost_chunks"] == 0


def test_feed_error_ends_the_loop_and_is_raised():
    with stoke.LocalStream("failing", memory="64m") as stream:
        stream.feed(ArrayWriter(3, fail=True))
        seen = []
        with pytest.raises(ValueError, match="the feed failed"):
            for batch in stream.dataset(wait_ms=50):
                seen.append(int(batch[0]))
        assert sorted(seen) == [0, 1, 2]


def test_stopping_early_ends_a_waiting_feed():
    start = time.time()
    with stoke.LocalStream("early", task_budget=1, memory="64m") as stream:
        writer = ArrayWriter(1000)
        stream.feed(writer)
        for step, batch in enumerate(stream.dataset(wait_ms=50)):
            if step == 2:
                break
        feed = stream._feed
    # The feed waited for its budget; removing the stream failed it.
    assert not feed.is_alive()
    assert time.time() - start < 30


@pytest.fixture(scope="module")
def ember_texts():
    """An Ember Dataframe of text, if Ember's module is installed."""
    ember = pytest.importorskip("ember")
    pa = pytest.importorskip("pyarrow")
    session = (
        ember.Session.builder.appName("local-stream").master("local[2]").getOrCreate()
    )
    texts = [f"document {i} " * 20 for i in range(300)]
    yield session.createDataFrame(pa.table({"text": texts}))
    session.stop()


def packed(df):
    # About two [4, 33] int32 batches per chunk.
    return (
        df.write.format("stoke")
        .option("batch", 4)
        .option("sequence", 32)
        .option("chunk_bytes", 1024)
    )


def test_ember_feed(ember_texts):
    with stoke.LocalStream("ember", memory="64m") as stream:
        stream.feed(packed(ember_texts))
        batches = list(stream.dataset(wait_ms=50))
    assert len(batches) > 50
    for batch in batches:
        assert batch.dtype == torch.int32
        assert batch.shape[1] == 33 and batch.shape[0] <= 4


def test_ember_feed_stopped_early(ember_texts):
    start = time.time()
    with stoke.LocalStream("ember-early", task_budget=1, memory="64m") as stream:
        stream.feed(packed(ember_texts))
        for step, batch in enumerate(stream.dataset(wait_ms=50)):
            if step == 2:
                break
        feed = stream._feed
    assert not feed.is_alive()
    assert time.time() - start < 30
