#!/usr/bin/env bash
# DBOS kill experiment on a real disk: Postgres 16 in Docker, then the combiner test. Writes results/dbos.log.
set -e; cd "$(dirname "$0")"; mkdir -p results
docker rm -f pgk >/dev/null 2>&1 || true
docker run -d --name pgk -p 5433:5432 -e POSTGRES_PASSWORD=pg postgres:16 -c max_connections=300 -c shared_buffers=256MB >/dev/null
pip install -q asyncpg httpx
sleep 10
PG_DSN=postgresql://postgres:pg@127.0.0.1:5433/postgres python3 dbos/combiner.py --clients 1,8,64,256 --seconds 8 2>&1 | tee results/dbos.log
docker rm -f pgk >/dev/null
