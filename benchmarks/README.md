# Benchmarks

Every number here is produced by a script in this directory and committed with
its raw result file. Nothing is estimated.

## Insert throughput

**Method.** `bench_inserts.py` replays a fixed recording of real Binance frames
(`data/sample-2026-10-02.jsonl.gz`: 120 s of btcusdt + ethusdt, 14,020 trades
and 2,378 depth updates) into a dedicated `tickwatch_bench` database as fast as
the writer allows. Frames are parsed before the clock starts, so the result is
database write throughput only. Tables are truncated before each run; 3 runs,
median reported. Each insert also maintains the primary-key index and
TimescaleDB's default `time DESC` index.

```
python benchmarks/bench_inserts.py --input benchmarks/data/sample-2026-10-02.jsonl.gz \
    --runs 3 --out benchmarks/results/<name>.json
```

| Stage | Writer | Median rows/s | p50 | p95 | p99 | Result file |
|---|---|---:|---:|---:|---:|---|
| 1 | One `INSERT` + `COMMIT` per row | **1,180** | 0.83 ms | 0.96 ms | 1.12 ms | [`stage1_naive.json`](results/stage1_naive.json) |

Measured 2026-10-02 on an Apple M4 Pro (24 GB), Postgres 16.15 + TimescaleDB
2.30.2 in Docker Desktop (14 CPUs, 7.7 GB), `synchronous_commit=on`, single
connection over loopback. Full environment is in the result file.

### Caveats -- read before quoting these numbers

- **This is replay capacity, not live load.** Live traffic for two symbols
  was ~140-310 msg/s when recorded, so the naive writer keeps up with today's
  live stream with roughly 4-8x headroom. The limit matters for bursts
  (volatile markets spike trade rates by an order of magnitude), backfill,
  replay, and adding symbols -- not for a quiet afternoon on two pairs.
- **Docker Desktop on macOS.** Postgres runs in a Linux VM whose virtual disk
  sits on the host SSD. Per-commit flush latency there may not match a Linux
  server with local disk; re-measure on the deployment host (Stage 7) before
  comparing across machines.
- **Client and database share a machine.** A real network hop adds round-trip
  time to every row, which hurts a one-row-per-round-trip writer most.
- **Stage 1 is per-row commit, the worst case by design.** Each row pays one
  client-server round trip plus one WAL flush. The p50 of 0.83 ms per row is
  almost entirely that fixed overhead, which is why throughput is ~1/0.83 ms.
