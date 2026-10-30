# kill-experiments

One-evening tests that decide whether an idea is worth building, before building it. Each one names the expensive
operation behind a vendor's benchmark, tries the cheapest version of a fix, and states its verdict rule up front.

Both need a disk where fsync is real. On WSL2, fsync takes about 0 ms and nothing here can show a difference, so run
them in a GitHub Codespace or on Linux.

## Results

<!-- results:start -->
### DBOS: batching step checkpoints

DBOS writes one Postgres transaction per workflow step. Postgres 16 with fsync and synchronous_commit on.

**GitHub Codespaces, 2 CPUs, fsync 0.896 ms:**

| Workflows | One commit per step, DBOS today | Postgres commit_delay | Combiner | Combiner vs best baseline | p99, best baseline to combiner |
|---|---|---|---|---|---|
| 1 | 1,042 steps/s | 1,140 steps/s | 1,005 steps/s | 0.9x | 3.63 ms to 4.18 ms |
| 8 | 2,852 steps/s | 2,595 steps/s | 6,566 steps/s | 2.3x | 7.52 ms to 4.83 ms |
| 64 | 4,894 steps/s | 5,789 steps/s | 24,030 steps/s | 4.2x | 26.28 ms to 6.42 ms |
| 256 | 4,658 steps/s | 5,163 steps/s | 36,369 steps/s | 7.0x | 141.52 ms to 13.90 ms |

**1-CPU Linux sandbox, faster disk:** 4.8x the better baseline at 64 workflows, with p99 from 7.73 ms to 1.54 ms.

One commit per step flattens out near 4,700 steps/s; the combiner keeps scaling, and at 256 workflows it cuts p99
step latency about 10x. Every acknowledged step was in the table exactly once in every run. With one workflow the
combiner's queue costs a little, so a real patch should bypass it when nothing else is waiting. This applies to
`@DBOS.step` checkpoints, which DBOS writes as their own insert; `@DBOS.transaction` checkpoints share the user's
transaction and cannot be merged across workflows. **Verdict: survives.**

**Follow-up inside real DBOS:** batching reached about 1.5x at 64 workflows, because each real step runs two transactions, about 13 round trips with network latency, and about 2,500 Python calls, and batching shares only one of the transactions. The bare-insert test above measured the commit alone. Details at [prithsk/dbos-step-cost](https://github.com/prithsk/dbos-step-cost).

### Electric SQL: shape matching for WHERE clauses it can't index

A fresh Postgres 16 and Electric 1.8.1 per configuration in a GitHub Codespace. Each row times one insert from its
commit until it reaches a subscribed probe shape, waiting for that exact row; p50 of 10 to 15 inserts.

| Shapes | Indexed, `tenant_id = k` | Unindexed, `price BETWEEN ...` | Unindexed vs indexed |
|---|---|---|---|
| 0 | 2.9 ms | | |
| 2,000 | 2.9 ms | 9.7 ms | 3.4x |
| 10,000 | 3.1 ms | 69.5 ms | 22.2x |

Electric's index for `field = constant` holds flat at 10,000 shapes. Range shapes add cost to every write, and the
cost grew faster than the shape count in these two runs: 5x more shapes made the added delay about 10x larger. At
10,000 range shapes the p90 reached 93.7 ms. This tested range clauses only. **Verdict: survives.** An index for range
clauses, like the one Electric already has for equality, should flatten it.
<!-- results:end -->

## DBOS combiner, `dbos/combiner.py`

- **Claim:** "Each step checkpoint is a single Postgres write (1 to 2 ms)"; 43K workflows/s on a 96-vCPU database,
  measured with workflows that have no steps.
- **Left out:** workflows with steps, at concurrency, on a normal database.
- **Test:** one INSERT and one COMMIT per step, the same with Postgres's own group commit on (`commit_delay`), and a
  combiner that writes every waiting step in one transaction and releases each step only after the commit.
- **Survives if:** the combiner beats the better baseline by 2x or more at 64 workflows.
- **Correctness:** every acknowledged step present exactly once after every run.

## Electric SQL fan-out, `electric/fanout.py`

- **Claim:** Electric indexes `field = constant` shapes, so 10,000 of them cost about 0.2 ms per write; its docs say
  other WHERE clauses are checked one shape at a time.
- **Test:** a fresh Postgres and Electric per configuration; create 0, 1,000 or 5,000 shapes that are either indexed
  (`tenant_id = k`) or not (`price BETWEEN ...`); then time one insert from commit until it reaches a subscribed probe
  shape, waiting for that exact row.
- **Survives if:** unindexed latency at 5,000 shapes is at least 2x indexed latency at 5,000 shapes.

## Run them in a GitHub Codespace

```
bash run_dbos.sh        # about 2 minutes
bash run_electric.sh    # about 15 to 30 minutes, mostly creating shapes
```

Each writes its full output to `results/`.

## Tests

`python test_kill.py` runs without Docker: the Electric harness against a fake server that slows each write by a
known amount per unindexed shape, plus the DBOS helpers.
