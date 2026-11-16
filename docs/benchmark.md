# stoke's benchmark

`examples/bench` measures stoke alone, with synthetic chunks;
[scripts/benchmark](../scripts/benchmark/README.md) compares stoke with
other dataloaders on real training data.

`examples/bench` runs synthetic producers and consumers (one dataloader
each) against shards and Kiln in one process, over TCP, and prints for 1,
2, 4 and 8 consumers: chunks/s and GB/s, mean take time, the share of
empty takes, and how much of the time consumers starved. `--step_us`
simulates a training step per batch, `--produce_rate` limits producers.

```
buck2 run //examples/bench:stoke_bench -- --step_us=2000
```

On velox-dev (16 vCPUs, 4 shards, 4 producers, 500 chunks of 4 MiB with 4
batches each, a 2 ms step per batch):

| consumers | chunks/s | GB/s | take ms | empty takes | starved (mean) |
|---:|---:|---:|---:|---:|---:|
| 1 | 118 | 0.50 | 0.23 | 3% | 0.6% |
| 2 | 236 | 0.99 | 0.23 | 10% | 0.4% |
| 4 | 468 | 1.96 | 1.5 | 38% | 1.0% |
| 8 | 648 | 2.72 | 7.3 | 67% | 31% |

Consumers scale linearly until the producers (about 700 chunks/s here)
can no longer keep up; then they wait parked at shards, not polling. Kiln
stores chunks in whole 1 MiB blocks and reuses the blocks of removed
chunks only after its grace period (30 s by default), so DRAM needs room
for that much throughput beyond the backlog; the benchmark uses 1 s.

`--task_budget` bounds each producer (a Producer per producer thread, as
per Ember task) to that many chunks put and not yet consumed, and the
benchmark prints the most chunks the stream held unconsumed and how long
producers waited. With producers far faster than 2 consumers (300 chunks
of 1 MiB, 4 batches each, a 5 ms step, `--max_buffered_batches=16`):

| task budget | chunks/s | most unconsumed | producers waited | starved (mean) |
|---:|---:|---:|---:|---:|
| none | 98 | 286 | 0% | 0.7% |
| 2 | 95 | 8 | 91% | 2.8% |
| 4 | 97 | 16 | 90% | 0.7% |

The stream never holds more than producers × budget chunks, at the
consumers' pace; a budget of 2 is too small to keep 2 consumers fed, 4 is
enough.
