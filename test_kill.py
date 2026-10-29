"""Tests for both kill experiments that run without Docker: the Electric harness against a fake Electric server,
and the DBOS combiner's helpers. The fake server slows each write by 0.2 ms per unindexed shape, so the test also
proves the harness can see a slowdown when one exists."""
import asyncio, json, sys, threading, time, urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
sys.path[:0] = ["electric", "dbos"]
import fanout, combiner

STATE = {"shapes": {}, "inserts": 0, "lock": threading.Lock()}
class FakeElectric(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def send(self, body, headers):
        self.send_response(200)
        for k, v in headers.items(): self.send_header(k, v)
        data = json.dumps(body).encode(); self.send_header("content-length", str(len(data))); self.end_headers(); self.wfile.write(data)
    def do_GET(self):
        u = urllib.parse.urlparse(self.path); q = dict(urllib.parse.parse_qsl(u.query))
        if u.path == "/v1/health":
            return self.send({"status": "active"}, {})
        where = q.get("where", "")
        with STATE["lock"]:
            handle = STATE["shapes"].setdefault(where, f"h{len(STATE['shapes'])}")
        if q.get("live") != "true":
            return self.send([], {"electric-handle": handle, "electric-offset": "0_0", "electric-up-to-date": ""})
        seen = int(q["offset"].split("_")[0]); t = time.time()
        while STATE["inserts"] <= seen and time.time() - t < 5:
            time.sleep(0.002)
        if STATE["inserts"] <= seen:
            return self.send([{"headers": {"control": "up-to-date"}}], {"electric-offset": q["offset"], "electric-up-to-date": ""})
        unindexed = sum(1 for w in STATE["shapes"] if "BETWEEN" in w)
        time.sleep(unindexed * 0.0002)              # per-write matching cost of unindexed shapes
        self.send([{"headers": {"operation": "insert"}, "value": {"payload": STATE.get("last", "")}}], {"electric-offset": f"{STATE['inserts']}_0", "electric-up-to-date": ""})

class FakeConn:
    async def execute(self, sql, token):
        STATE["last"] = token; STATE["inserts"] += 1

srv = ThreadingHTTPServer(("127.0.0.1", 3999), FakeElectric); threading.Thread(target=srv.serve_forever, daemon=True).start()
fanout.BASE = "http://127.0.0.1:3999"

# Query building
assert fanout.where_for("indexed", 7) == "tenant_id = 7"
assert fanout.where_for("unindexed", 2) == "price BETWEEN 2000 AND 2999"
u = fanout.shape_url("price BETWEEN 1 AND 2", "h1", "3_0", live=True)
assert "where=price+BETWEEN+1+AND+2" in u and "handle=h1" in u and "offset=3_0" in u and "live=true" in u

async def scenario():
    import httpx
    async with httpx.AsyncClient(timeout=30) as c:
        await fanout.wait_ready(c, timeout=5)
        await fanout.create_shapes(c, "indexed", 50)
        base = statistics.median(await fanout.probe(c, FakeConn(), 4))
        await fanout.create_shapes(c, "unindexed", 50)
        slow = statistics.median(await fanout.probe(c, FakeConn(), 4))
        return base, slow
import statistics
base, slow = asyncio.run(scenario())
assert len([w for w in STATE["shapes"] if w.startswith("tenant_id = ") and w != "tenant_id = -1"]) == 50
assert len([w for w in STATE["shapes"] if "BETWEEN" in w]) == 50
assert slow > base + 5, (base, slow)          # 50 unindexed shapes x 0.2 ms = 10 ms more per write
print(f"electric harness: probe latency {base:.1f} ms with indexed shapes, {slow:.1f} ms after 50 unindexed shapes")

# DBOS helpers
assert combiner.pct([0.001, 0.002, 0.003, 0.004], 0.5) == 3.0
assert combiner.fsync_probe(".") >= 0
print("all tests passed")
