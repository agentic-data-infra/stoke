"""stoke on one machine: a stream whose Kiln cluster and shard run in this
process, fed by an Ember job on a thread of it."""

import threading

from stoke._stoke import LocalCluster, create_stream, remove_stream, seal_stream
from stoke.dataset import StokeDataset

_UNITS = {"k": 1 << 10, "m": 1 << 20, "g": 1 << 30}


def parse_memory(memory):
    """Bytes of e.g. 1 << 30, "1g", "512m" or "64k"."""
    if isinstance(memory, int):
        return memory
    text = memory.strip().lower()
    if text and text[-1] in _UNITS:
        return int(float(text[:-1]) * _UNITS[text[-1]])
    return int(text)


class LocalStream:
    """A stoke stream on this machine, with nothing to start beforehand.

        with stoke.LocalStream("docs") as stream:
            stream.feed(df.write.format("stoke").option("batch", 16).option("sequence", 128))
            for batch in stream.dataset():
                ...

    Entering it starts Kiln (one DRAM server holding 'memory', e.g. "1g")
    and one shard in this process and creates the stream; with a
    'task_budget' of N, each feed task waits once it has N chunks not yet
    consumed, so the feed runs at the trainer's pace. feed() runs an Ember
    write on a thread, dataset() reads the stream. Leaving it ends a feed the
    trainer did not wait for and stops the shard and Kiln.
    """

    def __init__(self, name, task_budget=4, memory="1g"):
        self.name = name
        self.task_budget = task_budget
        self.memory = parse_memory(memory)
        self._cluster = None
        self._feed = None
        self._error = None
        # Whether __exit__ removed the stream under a running feed; guards
        # the feed sealing the stream after a failure.
        self._removed = False
        self._lock = threading.Lock()
        self._datasets = []

    def __enter__(self):
        self._cluster = LocalCluster(self.memory)
        try:
            create_stream(
                self.metadata,
                self.name,
                [self._cluster.shard],
                stop_fraction=1.0,
                task_budget=self.task_budget,
            )
        except BaseException:
            self._cluster.close()
            raise
        return self

    @property
    def metadata(self):
        """The Kiln cluster, as Ember's kiln_metadata option takes it."""
        return self._cluster.metadata

    def feed(self, writer):
        """Runs writer.save(name) on a thread: an Ember DataframeWriter of
        format("stoke"), which this points at the local cluster and has seal
        the stream when its job commits. If the job fails, the stream is
        sealed too, so that the dataset's loop ends; it then raises the
        job's error.
        """
        if self._feed is not None:
            raise RuntimeError("a LocalStream has one feed")
        writer = writer.option("kiln_metadata", self.metadata).option("seal", True)
        self._feed = threading.Thread(
            target=self._run_feed, args=(writer,), name=f"stoke-feed-{self.name}"
        )
        self._feed.start()

    def _run_feed(self, writer):
        try:
            writer.save(self.name)
        except Exception as e:
            with self._lock:
                self._error = e
                if not self._removed:
                    seal_stream(self.metadata, self.name)

    def dataset(self, **options):
        """A StokeDataset of the stream; its loop ends when the stream does
        and then raises the feed's error, if any. Options go to StokeDataset
        (job defaults to "local")."""
        options.setdefault("job", "local")
        dataset = _LocalDataset(self, **options)
        self._datasets.append(dataset)
        return dataset

    def _raise_feed_error(self):
        # The stream ended, so the feed sealed it and is done or about to be.
        if self._feed is not None:
            self._feed.join()
        error, self._error = self._error, None
        if error is not None:
            raise error

    def __exit__(self, exc_type, exc, tb):
        try:
            if self._feed is not None:
                with self._lock:
                    if self._feed.is_alive():
                        # The trainer stopped first: the feed's tasks fail
                        # once the stream is gone, and that error is expected.
                        self._removed = True
                        remove_stream(self.metadata, self.name)
                self._feed.join()
            # Loaders stop their fetchers before the shard goes away.
            for dataset in self._datasets:
                dataset.loader = None
        finally:
            self._cluster.close()
        if exc_type is None and not self._removed:
            self._raise_feed_error()


class _LocalDataset(StokeDataset):
    def __init__(self, stream, **options):
        super().__init__(stream.name, metadata=stream.metadata, **options)
        self._stream = stream

    def __iter__(self):
        yield from super().__iter__()
        self._stream._raise_feed_error()
