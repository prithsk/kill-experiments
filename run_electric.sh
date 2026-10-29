#!/usr/bin/env bash
# Electric SQL kill experiment: fresh Postgres and Electric per configuration, via Docker Compose. Writes results/electric.log.
set -e; cd "$(dirname "$0")"; mkdir -p results
pip install -q asyncpg httpx
docker compose -f electric/docker-compose.yml pull -q
python3 electric/fanout.py --shapes 0,1000,5000 --rounds 15 2>&1 | tee results/electric.log
