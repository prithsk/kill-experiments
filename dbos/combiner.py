"""DBOS kill experiment: can batching step checkpoints beat Postgres's own group commit?

DBOS writes one Postgres transaction per workflow step. This compares, at 1, 8, 64 and 256 concurrent workflows:
  naive        one INSERT and one COMMIT per step, what DBOS does today
  commit_delay the same, with Postgres's built-in group commit turned on (the honest baseline)
  combiner     concurrent steps share one transaction; each step returns only after its commit
The idea survives if the combiner beats the better of the two baselines by 2x or more at 64 workflows.
Needs a Postgres where fsync is real. On WSL2 fsync is free and this test cannot show anything.
"""
import argparse, asyncio, os, statistics, time
import asyncpg

DSN = os.environ.get("PG_DSN", "postgresql://postgres@127.0.0.1:5433/postgres")
TABLE = """CREATE TABLE IF NOT EXISTS operation_outputs (
  workflow_uuid text NOT NULL, function_id int NOT NULL, output text,
  PRIMARY KEY (workflow_uuid, function_id))"""
PAYLOAD = "x" * 200   # a small serialized step result

def fsync_probe(path="."):
    fd = os.open(os.path.join(path, ".fsync_probe"), os.O_CREAT | os.O_WRONLY | os.O_TRUNC); lat = []
    for _ in range(200):
        os.write(fd, b"x" * 256); t = time.perf_counter(); os.fsync(fd); lat.append((time.perf_counter() - t) * 1000)
    os.close(fd); os.remove(os.path.join(path, ".fsync_probe")); return statistics.median(lat)

async def reset(conn, commit_delay):
    await conn.execute(TABLE); await conn.execute("TRUNCATE operation_outputs")
    await conn.execute(f"ALTER SYSTEM SET commit_delay = {commit_delay}")
    await conn.execute("ALTER SYSTEM SET commit_siblings = 5")
    await conn.execute("SELECT pg_reload_conf()"); await asyncio.sleep(0.3)

async def run_naive(n, seconds):
    pool = await asyncpg.create_pool(DSN, min_size=n, max_size=n)
    lat, stop = [], time.perf_counter() + seconds
    async def workflow(w):
        step = 0
        async with pool.acquire() as c:
            while time.perf_counter() < stop:
                t = time.perf_counter()
                await c.execute("INSERT INTO operation_outputs VALUES ($1, $2, $3)", f"wf{w}", step, PAYLOAD)
                lat.append(time.perf_counter() - t); step += 1
    t0 = time.perf_counter(); await asyncio.gather(*(workflow(w) for w in range(n))); el = time.perf_counter() - t0
    await pool.close(); return len(lat) / el, lat

async def run_combiner(n, seconds, max_batch=1024):
    conn = await asyncpg.connect(DSN)
    q, lat, stop, batches = asyncio.Queue(), [], time.perf_counter() + seconds, []
    async def combiner():
        while True:
            first = await q.get()
            if first is None:
                return
            items = [first]
            while len(items) < max_batch and not q.empty():
                nxt = q.get_nowait()
                if nxt is None:
                    q.put_nowait(None); break
                items.append(nxt)
            # One statement, one transaction, one commit for every step that was waiting.
            await conn.execute("INSERT INTO operation_outputs SELECT * FROM unnest($1::text[], $2::int[], $3::text[])",
                               [i[0] for i in items], [i[1] for i in items], [i[2] for i in items])
            batches.append(len(items))
            for i in items:
                i[3].set_result(None)            # released only after the commit returned
    async def workflow(w):
        step, loop = 0, asyncio.get_running_loop()
        while time.perf_counter() < stop:
            t = time.perf_counter(); f = loop.create_future()
            await q.put((f"wf{w}", step, PAYLOAD, f)); await f
            lat.append(time.perf_counter() - t); step += 1
    task = asyncio.create_task(combiner())
    t0 = time.perf_counter(); await asyncio.gather(*(workflow(w) for w in range(n))); el = time.perf_counter() - t0
    await q.put(None); await task; await conn.close()
    return len(lat) / el, lat, (statistics.mean(batches) if batches else 0)

async def verify(expected):
    """Every step a workflow saw acknowledged must be in the table exactly once."""
    conn = await asyncpg.connect(DSN)
    rows = await conn.fetchval("SELECT count(*) FROM operation_outputs")
    dups = await conn.fetchval("SELECT count(*) FROM (SELECT workflow_uuid, function_id FROM operation_outputs GROUP BY 1, 2 HAVING count(*) > 1) d")
    await conn.close(); return rows == expected and dups == 0

def pct(v, p): return sorted(v)[min(len(v) - 1, int(p * len(v)))] * 1000

async def main(clients, seconds):
    fs = fsync_probe()
    print(f"fsync median on this disk: {fs:.3f} ms" + ("   WARNING: fsync looks free here (WSL2?), results will not mean anything" if fs < 0.05 else ""))
    admin = await asyncpg.connect(DSN)
    rows, verdict_at = [], {}
    for n in clients:
        res = {}
        for mode in ("naive", "commit_delay", "combiner"):
            await reset(admin, 1000 if mode == "commit_delay" else 0)
            if mode == "combiner":
                tput, lat, avg_batch = await run_combiner(n, seconds)
            else:
                tput, lat = await run_naive(n, seconds); avg_batch = 1
            ok = await verify(len(lat))
            res[mode] = (tput, pct(lat, .5), pct(lat, .99), avg_batch, ok)
            print(f"  {n:>3} workflows  {mode:12s} {tput:>9,.0f} steps/s  p50 {res[mode][1]:6.2f} ms  p99 {res[mode][2]:7.2f} ms"
                  f"{'  batch ' + format(avg_batch, '.1f') if mode == 'combiner' else ''}  {'ok' if ok else 'CHECK FAILED'}", flush=True)
        best_base = max(res["naive"][0], res["commit_delay"][0])
        verdict_at[n] = res["combiner"][0] / best_base
        rows.append((n, res))
    await reset(admin, 0); await admin.close()
    print("\n| Workflows | Naive | Postgres commit_delay | Combiner | Combiner p99 | Combiner vs best baseline |")
    print("|---|---|---|---|---|---|")
    for n, r in rows:
        print(f"| {n} | {r['naive'][0]:,.0f} steps/s | {r['commit_delay'][0]:,.0f} steps/s | {r['combiner'][0]:,.0f} steps/s | {r['combiner'][2]:.2f} ms | {verdict_at[n]:.1f}x |")
    key = 64 if 64 in verdict_at else max(verdict_at)
    v = verdict_at[key]
    print(f"\nVerdict at {key} workflows: combiner is {v:.1f}x the better baseline. "
          + ("SURVIVES: worth building into DBOS." if v >= 2 else "KILLED: Postgres's own group commit already gets most of this."))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", default="1,8,64,256"); ap.add_argument("--seconds", type=float, default=8)
    a = ap.parse_args(); asyncio.run(main([int(x) for x in a.clients.split(",")], a.seconds))
