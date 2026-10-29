# kill-experiments

One-evening tests that decide whether an idea is worth building, before building it. Each one names the expensive
operation behind a vendor's benchmark, tries the cheapest version of a fix, and states its verdict rule up front.

Both need a disk where fsync is real. On WSL2, fsync takes about 0 ms and nothing here can show a difference, so run
them in a GitHub Codespace or on Linux.

## Results

<!-- results:start -->
### DBOS: batching step checkpoints, first run

DBOS writes one Postgres transaction per workflow step. On a 1-CPU Linux sandbox with real fsync, Postgres 16 with
fsync and synchronous_commit on:

| Workflows | One commit per step, DBOS today | Postgres commit_delay | Combiner | Combiner vs best baseline |
|---|---|---|---|---|
| 1 | 4,155 steps/s | 4,694 steps/s | 3,891 steps/s | 0.8x |
| 8 | 9,103 steps/s | 4,648 steps/s | 19,748 steps/s | 2.2x |
| 64 | 11,182 steps/s | 15,526 steps/s | 74,226 steps/s | 4.8x |

At 64 workflows the combiner also cut p99 step latency from 7.73 ms to 1.54 ms. Every acknowledged step was in the
table exactly once in every run. With one workflow the combiner's queue costs a little, so a real patch should bypass
it when nothing else is waiting. **Verdict: survives.**

### Electric SQL: shape matching for WHERE clauses it can't index

Not run yet. The harness is tested against a fake Electric server that adds a known per-shape cost, and it detects it.
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
