"""Electric SQL kill experiment: does write matching slow down with many shapes whose WHERE clause Electric can't index?

Electric's docs say `field = constant` shapes are indexed, so 10,000 of them cost almost nothing per write, while
other WHERE clauses are checked one shape at a time. This creates S shapes of one kind, then times how long a single
insert takes to reach a subscribed probe shape, from a fresh Electric each time.
  indexed     WHERE tenant_id = k                        (Electric's optimized form)
  unindexed   WHERE price BETWEEN k*1000 AND k*1000+999  (a range, checked per shape)
The idea survives if unindexed latency at the largest S is at least 2x the indexed latency at the same S.
"""
import argparse, asyncio, os, statistics, subprocess, time, urllib.parse
import asyncpg, httpx

BASE = os.environ.get("ELECTRIC_URL", "http://127.0.0.1:3000")
DSN = os.environ.get("PG_DSN", "postgresql://postgres:pg@127.0.0.1:54321/electric")

def where_for(kind, k):
    return f"tenant_id = {k}" if kind == "indexed" else f"price BETWEEN {k * 1000} AND {k * 1000 + 999}"

def shape_url(where=None, handle=None, offset="-1", live=False):
    q = {"table": "items", "offset": offset}
    if where: q["where"] = where
    if handle: q["handle"] = handle
    if live: q["live"] = "true"
    return f"{BASE}/v1/shape?" + urllib.parse.urlencode(q)

def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True, cwd=os.path.dirname(os.path.abspath(__file__)),
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

async def wait_ready(client, timeout=120):
    t = time.time()
    while time.time() - t < timeout:
        try:
            r = await client.get(f"{BASE}/v1/health")
            if r.status_code == 200 and "active" in r.text: return
        except Exception: pass
        await asyncio.sleep(1)
    raise SystemExit("Electric did not become healthy; run `docker compose logs electric` and paste it to Claude")

async def setup_db():
    for _ in range(60):
        try:
            conn = await asyncpg.connect(DSN); break
        except Exception: await asyncio.sleep(1)
    else: raise SystemExit("Postgres did not start")
    await conn.execute("CREATE TABLE IF NOT EXISTS items (id serial PRIMARY KEY, tenant_id int, price int, payload text)")
    await conn.execute("INSERT INTO items (tenant_id, price, payload) SELECT g, g * 1000, 'seed' FROM generate_series(1, 200) g")
    return conn

async def sync_to_live(client, where):
    """Create (or join) a shape and follow it until up to date; returns (handle, offset)."""
    r = await client.get(shape_url(where)); handle, offset = r.headers.get("electric-handle"), r.headers.get("electric-offset")
    for _ in range(20):
        if r.headers.get("electric-up-to-date") is not None: break
        r = await client.get(shape_url(where, handle, offset)); offset = r.headers.get("electric-offset", offset)
    return handle, offset

async def create_shapes(client, kind, n, concurrency=16):
    sem = asyncio.Semaphore(concurrency)
    async def one(k):
        async with sem:
            r = await client.get(shape_url(where_for(kind, k)))
            if r.status_code >= 400: raise SystemExit(f"shape creation failed ({r.status_code}): {r.text[:200]}")
    await asyncio.gather(*(one(k) for k in range(1, n + 1)))

async def probe(client, conn, rounds):
    """Time from an insert's commit to its arrival on a subscribed shape that matches only probe rows."""
    where = "tenant_id = -1"; handle, offset = await sync_to_live(client, where); lat = []
    for i in range(rounds):
        live = asyncio.create_task(client.get(shape_url(where, handle, offset, live=True), timeout=30))
        await asyncio.sleep(0.3)
        token = f"probe-{i}-{time.time_ns()}"     # unique, so an earlier row can never stop the clock
        await conn.execute("INSERT INTO items (tenant_id, price, payload) VALUES (-1, -1, $1)", token)
        t = time.perf_counter(); r = await live
        while token not in r.text:                 # stale data or a control message: keep polling
            offset = r.headers.get("electric-offset", offset)
            r = await client.get(shape_url(where, handle, offset, live=True), timeout=30)
        lat.append((time.perf_counter() - t) * 1000); offset = r.headers.get("electric-offset", offset)
    return lat

async def one_config(kind, n, rounds, fresh=True):
    if fresh:
        compose("down", "-v"); compose("up", "-d")
    async with httpx.AsyncClient(timeout=60) as client:
        await wait_ready(client); conn = await setup_db()
        t = time.perf_counter(); await create_shapes(client, kind, n); made = time.perf_counter() - t
        lat = await probe(client, conn, rounds); await conn.close()
    return statistics.median(lat), sorted(lat)[int(0.9 * (len(lat) - 1))], made

async def main(shapes, kinds, rounds):
    res = {}
    for kind in kinds:
        for n in shapes:
            p50, p90, made = await one_config(kind, n, rounds)
            res[(kind, n)] = (p50, p90)
            print(f"  {kind:9s} {n:>6} shapes: insert to probe p50 {p50:7.1f} ms  p90 {p90:7.1f} ms  (created in {made:.0f} s)", flush=True)
    compose("down", "-v")
    print("\n| Shapes | Indexed WHERE p50 | Unindexed WHERE p50 | Unindexed vs indexed |", "|---|---|---|---|", sep="\n")
    for n in shapes:
        i, u = res.get(("indexed", n)), res.get(("unindexed", n))
        if i and u: print(f"| {n} | {i[0]:.1f} ms | {u[0]:.1f} ms | {u[0]/i[0]:.1f}x |")
    top = max(shapes)
    if ("indexed", top) in res and ("unindexed", top) in res:
        v = res[("unindexed", top)][0] / res[("indexed", top)][0]
        print(f"\nVerdict at {top} shapes: unindexed is {v:.1f}x slower than indexed. "
              + ("SURVIVES: indexing more WHERE forms could win big." if v >= 2 else "KILLED: matching cost does not grow enough to matter."))

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--shapes", default="0,1000,5000"); ap.add_argument("--kinds", default="indexed,unindexed")
    ap.add_argument("--rounds", type=int, default=15)
    a = ap.parse_args(); asyncio.run(main([int(x) for x in a.shapes.split(",")], a.kinds.split(","), a.rounds))
