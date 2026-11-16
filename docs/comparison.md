# How stoke compares

stoke next to other data loaders for training, as of November 2026. stoke
entries describe what is implemented today; planned work is marked as such.
Claims about other systems cite their documentation (`[n]`, see Sources).

Legend: ✅ supported, 🟡 partial, limited or through an add-on, ⬜ planned,
❌ not available.

| | **stoke** (+ [Ember](https://github.com/agentic-data-infra/ember), [Kiln](https://github.com/agentic-data-infra/kiln)) | **Grain** | **Zephon** | **PyTorch DataLoader** / torchdata | **Mosaic Streaming** | **Ray Data** (Ray Train ingest) | **tf.data service** | **NVIDIA DALI** |
|---|---|---|---|---|---|---|---|---|
| **Where preprocessing runs** | ✅ On any CPU nodes, as an Ember job; the trainer's loader only fetches ready batches (a) | Trainer host, child processes [1] | Trainer host, threads or processes [6][7] | Trainer host, worker processes [9] | Trainer host, DataLoader workers [12] | ✅ Separate CPU nodes [15] | ✅ Separate workers [18] | Trainer's GPU and CPU [21] |
| **Preprocessing scales past one host** | ✅ Any number of Ember workers; one job feeds all ranks | ❌ [1] | 🟡 Experimental Ray runner [6] | ❌ [9] | ❌ | ✅ [15] | ✅ [18] | ❌ |
| **Implementation language** | C++20 (Velox, folly coroutines, fbthrift), Python bindings | Python [2] | Python [6] | Python, C++ core | Python | C++ core, Python Ray Data [16] | C++ runtime, Python API | C++/CUDA, Python API [21] |
| **Transform API** | Ember DataFrame (filters, joins, SQL expressions) plus stoke's packer | Python dataset ops [3] | Python pipeline operators [6] | Python `Dataset` | Python `Dataset` | Ray Data Dataset API [15] | `tf.data` ops [20] | DALI operators [21] |
| **Framework** | PyTorch `IterableDataset`, zero-copy DLPack tensors | JAX, PyTorch [2] | PyTorch [7] | PyTorch | PyTorch [12] | PyTorch and others via Ray Train [15] | TensorFlow [18] | PyTorch, TF, JAX, PaddlePaddle [21] |
| **Shuffle** | ✅ Seeded file or global split shuffle, random chunk order (b) | ✅ Global [2] | ✅ Global [6] | ✅ Sampler [9] | ✅ Five algorithms [13] | 🟡 Global or local [15] | 🟡 `tf.data` shuffle buffer [20] | 🟡 Reader shuffle [21] |
| **Mixing sources** | ✅ In the DataFrame query (c) | ✅ Weighted `mix` [4] | ✅ Static and dynamic mixtures [6][7] | ✅ `MultiNodeWeightedSampler` [10] | ✅ Per-stream proportions [14] | 🟡 Custom code | ✅ `tf.data` `sample_from_datasets` [20] | Not documented |
| **Checkpoint / resume** | 🟡 At-most-once: a restarted rank returns its unfinished chunks to the queue; ⬜ exact resume | ✅ Exact [5] | ✅ Exact, across GPU counts [8] | 🟡 With torchdata `StatefulDataLoader` [11] | ✅ Exact, across GPU counts [13] | Not documented [15] | 🟡 Restarted workers redo their shard; at-most-once with dynamic sharding [19] | ✅ [22] |
| **Producer fault tolerance** | ✅ Ember task retry; retried tasks produce the same chunk and stoke drops the duplicate | Not documented | Not documented | Not documented | Not documented | ✅ Task retry, lineage [17] | ✅ Workers can restart and rejoin [19] | n/a |
| **Autoscaling** | ⬜ From stream backlog and starving consumers | ❌ | ❌ | ❌ | ❌ | ✅ Ray autoscaler [23] | 🟡 Dynamic sharding tolerates resizing; no autoscaler [19] | ❌ |
| **Backpressure** | ✅ Bound on unconsumed chunks per producer task | Prefetch buffer [1] | Bounded queues [7] | `prefetch_factor` [9] | Local cache limit [12] | ✅ Streaming executor [16] | Not documented | Not documented |
| **Data transport** | Kiln: shared memory on the same host, TCP or RDMA across hosts; the queue only carries 16-byte chunk ids | Processes on one host [1] | Pipes, shared memory or threads [7] | Processes on one host [9] | Download from object store into a local cache [12] | Ray object store [16] | gRPC [19] | GPU memory [21] |
| **Locality** | ✅ Consumers read chunks from their own host first; optional local writes | n/a | n/a | n/a | n/a | 🟡 `locality_hints` [15] | Not documented | n/a |
| **RDMA** | ✅ Through Kiln (d) | ❌ | ❌ | ❌ | ❌ | ❌ [16] | ❌ | ❌ |
| **GPU** | ⬜ Direct reads from Kiln into GPU memory | ❌ CPU [2] | ❌ | 🟡 Pinned memory [9] | ❌ | 🟡 GPU stages in `map_batches` [16] | ❌ | ✅ GPU decode, GPUDirect Storage [21] |
| **License** | Apache 2.0 | Apache 2.0 [2] | Apache 2.0 [6] | BSD-3 [24] | Apache 2.0 [25] | Apache 2.0 [26] | Apache 2.0 [19] | Apache 2.0 [21] |

## Notes on stoke

a. Ember workers write batches into Kiln chunks while the job runs; stoke
   queues their ids, and each rank's loader takes chunks and hands out
   tensors that point into them. Trainers never talk to Ember, and Ember
   knows nothing about the training job. See the [README](../README.md).
b. Ember's `ember.files.shuffle = file | global`, with a seed, permutes the
   input splits before they are handed to tasks; consumers take chunks in
   random order.
c. For example `filter(rand() < p)` on each source, then `unionAll`.
d. Build with `-c kiln.rdma=true`; see Kiln's README, "Kiln over RDMA".

## Related work

Meta's Data PreProcessing service (DPP) also separates preprocessing from
trainers, with a master, stateless workers and a client per trainer node
[27]. It is not open source.

## Sources

1. Grain DataLoader guide: https://google-grain.readthedocs.io/en/latest/tutorials/data_loader_tutorial.html
2. Grain: https://github.com/google/grain ; https://google-grain.readthedocs.io/en/latest/
3. Grain Dataset API: https://google-grain.readthedocs.io/en/latest/grain.dataset.html
4. Grain `mix`: https://google-grain.readthedocs.io/en/latest/grain.dataset.html
5. Grain checkpointing: https://google-grain.readthedocs.io/en/latest/grain.checkpoint.html
6. Zephon on PyPI: https://pypi.org/project/zephon/
7. Zephon: https://github.com/datologyai/zephon
8. Zephon paper: https://arxiv.org/abs/2610.03087
9. PyTorch DataLoader arguments: https://meta-pytorch.org/data/main/torchdata.stateful_dataloader.html
10. torchdata.nodes: https://meta-pytorch.org/data/main/torchdata.nodes.html
11. torchdata StatefulDataLoader: https://meta-pytorch.org/data/main/torchdata.stateful_dataloader.html
12. Mosaic Streaming: https://docs.mosaicml.com/projects/streaming/en/latest/
13. Mosaic elastic determinism and shuffling: https://docs.mosaicml.com/projects/streaming/en/latest/distributed_training/elastic_determinism.html ; https://docs.mosaicml.com/projects/streaming/en/latest/dataset_configuration/shuffling.html
14. Mosaic mixing: https://docs.mosaicml.com/projects/streaming/en/latest/dataset_configuration/mixing_data_sources.html
15. Ray Train data loading: https://docs.ray.io/en/latest/train/user-guides/data-loading-preprocessing.html
16. Ray Data internals: https://docs.ray.io/en/latest/data/data-internals.html
17. Ray object fault tolerance: https://docs.ray.io/en/latest/ray-core/fault_tolerance/objects.html
18. tf.data service paper: https://arxiv.org/abs/2210.14826
19. tf.data service API (sharding policies, worker restarts, protocol): https://github.com/tensorflow/tensorflow/blob/master/tensorflow/python/data/experimental/ops/data_service_ops.py
20. `tf.data.Dataset`: https://www.tensorflow.org/api_docs/python/tf/data/Dataset
21. NVIDIA DALI: https://github.com/NVIDIA/DALI ; https://docs.nvidia.com/deeplearning/dali/user-guide/docs/
22. DALI checkpointing: https://docs.nvidia.com/deeplearning/dali/user-guide/docs/dali_dynamic/checkpointing.html
23. Ray autoscaling: https://docs.ray.io/en/latest/cluster/vms/user-guides/configuring-autoscaling.html
24. PyTorch license: https://github.com/pytorch/pytorch/blob/main/LICENSE
25. Mosaic Streaming license: https://github.com/mosaicml/streaming/blob/main/LICENSE
26. Ray license: https://github.com/ray-project/ray/blob/master/LICENSE
27. Meta DPP (ISCA 2022): https://arxiv.org/abs/2108.09373
