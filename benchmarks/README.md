# Benchmarks

Every number here is produced by a script in this directory and committed with
its raw result file. Nothing is estimated.

## Insert throughput

**Method.** `bench_inserts.py` replays a fixed recording of real Binance frames
into a dedicated `tickwatch_bench` database as fast as the writer allows.
Frames are parsed before the clock starts, so the result is database write
throughput only. Tables are truncated before each run; 3 runs, median
reported. Every write also maintains the primary-key index and TimescaleDB's
default `time DESC` index. The live consumer was stopped while benchmarks ran.

```
python benchmarks/bench_inserts.py --input benchmarks/data/sample-2026-10-02-15m.jsonl.gz \
    --writer <writer> --batch-size 2000 --runs 3 --out benchmarks/results/<name>.json
```

### Before and after

Input: `data/sample-2026-10-02-15m.jsonl.gz` -- 15 minutes of btcusdt + ethusdt,
126,520 rows (108,626 trades, 17,894 depth updates).

| Writer | Batch | Median rows/s | vs naive | Latency p50 / p99 | Result file |
|---|---:|---:|---:|---|---|
| One `INSERT` + `COMMIT` per row (Stage 1) | 1 | **1,193** | 1x | 0.83 / 1.16 ms per row | [`stage2_naive`](results/stage2_naive.json) |
| Pipelined `INSERT ... ON CONFLICT`, one txn per batch | 500 | 20,551 | 17x | 23.9 / 31.6 ms per flush | [`stage2_batch-insert`](results/stage2_batch-insert.json) |
| `COPY` to temp staging + `INSERT ... ON CONFLICT` | 500 | 47,098 | 40x | 10.6 / 20.7 ms per flush | [`stage2_batch-copy`](results/stage2_batch-copy.json) |
| Same, at the chosen batch size | 2000 | 69,133 | 58x | 28.4 / 35.1 ms per flush | [`batch-copy_2000`](results/stage2_sweep/batch-copy_2000.json) |
| **Same + `(symbol, time DESC)` index (current live config)** | **2000** | **61,340** | **51x** | 31.5 / 40.7 ms per flush | [`..._with_symbol_time_idx`](results/stage2_batch-copy_2000_with_symbol_time_idx.json) |
| `COPY` direct, no dedup (reference only) | 500 | 83,226 | 70x | 5.7 / 14.6 ms per flush | [`stage2_batch-copy-direct`](results/stage2_batch-copy-direct.json) |

Run-to-run spread (max - min, as % of median) was 0.6-5.4% for the rows
above; in the sweep it reached 7.5% at batch 100 and 10.8% at batch 5000.
Every run stored all 126,520 rows. The Stage 1 baseline on the original 2-minute recording was
1,180 rows/s ([`stage1_naive.json`](results/stage1_naive.json)), within 1% of
the figure above.

**What changed and why it's faster.**

1. **Batching (1x -> 17x).** The naive writer pays a client-server round trip
   and a WAL flush to disk *per row*; at ~0.83 ms each, that fixed cost caps it
   near 1/0.83 ms = 1,200 rows/s. One transaction per batch pays the commit
   flush once per 500 rows, and psycopg pipelines the `executemany` so the
   round trips overlap.
2. **COPY (17x -> 40x at the same batch size).** Batched `INSERT` still sends,
   parses and plans one statement per row on the server. `COPY` streams rows
   in a single command with no per-row statement overhead.
3. **Batch size (40x -> 58x).** See the sweep below.
4. **Extra index (58x -> 51x).** The query index added below costs 11% of
   write throughput; see [Query: latest trade per symbol](#query-latest-trade-per-symbol).

**What idempotency costs.** `COPY` has no `ON CONFLICT`, so one row we
already stored (e.g. resent after a reconnect) would abort the whole batch.
The live writer therefore COPYs into a session-local temp table and merges
with `INSERT ... SELECT ... ON CONFLICT DO NOTHING`. Direct COPY is 77%
faster (83k vs 47k at batch 500), so safe re-delivery costs ~43% of COPY
throughput. We pay it: correctness on reconnect matters more than headroom
we don't need at live rates.

### Batch-size sweep

`batch-copy`, same input, median of 3 runs each
([`results/stage2_sweep/`](results/stage2_sweep/)):

| Batch size | 50 | 100 | 500 | 1000 | 2000 | 5000 |
|---|---:|---:|---:|---:|---:|---:|
| rows/s | 14,874 | 21,671 | 47,098 | 57,036 | **69,133** | 74,372 |
| p99 flush | 5.5 ms | 6.8 ms | 20.7 ms | 27.7 ms | **35.1 ms** | 175.7 ms |

Throughput rises steeply to ~1000 and flattens after 2000; going to 5000 buys
8% more throughput for a 5x worse p99 flush. Default set to **2000**. At live
rates the 200 ms time bound flushes long before a batch fills, so batch size
only matters when the writer has a backlog to clear.

## Query: latest trade per symbol

```sql
SELECT DISTINCT ON (symbol) symbol, time, price FROM trades ORDER BY symbol, time DESC;
```

A "last price" panel or the anomaly detector asks this constantly.
`explain_query.py` warms the cache, then takes the median server-side
`Execution Time` from 20 runs of `EXPLAIN (ANALYZE, BUFFERS)` against the live
database. Full plans are saved next to each result.

| | Index | Rows in `trades` | Median | Buffers | Plan | Result |
|---|---|---:|---:|---:|---|---|
| Before | PK `(symbol, trade_id, time)`, `(time DESC)` | 102,194 | **26.4 ms** | 2,502 + disk spill | [`before.txt`](results/stage2_query_before.txt) | [`.json`](results/stage2_query_before.json) |
| After | + `(symbol, time DESC)` | 103,459 | **0.018 ms** | 8 | [`after.txt`](results/stage2_query_after.txt) | [`.json`](results/stage2_query_after.json) |

**Reading the "before" plan.** To return 2 rows, Postgres walks the primary
key, which leads with `symbol` but then orders by `trade_id`, not `time`. So it
reads all ~102k rows and sorts each symbol's rows by time. The sort exceeds
`work_mem` and spills to disk (`Sort Method: external merge`). Cost grows
linearly with the table, and this table only grows.

**The fix.** An index on `(symbol, time DESC)` holds each symbol's rows
newest-first, so TimescaleDB's `SkipScan` probes the index once per distinct
symbol: 8 buffer hits, regardless of table size. Below ~0.1 ms the timing is
close to timer resolution, so the buffer count (2,502 -> 8) is the more
robust comparison than the ~1,450x time ratio.

**What it costs.** One more index to maintain on every insert: batched COPY at
batch 2000 fell from 69,133 to 61,340 rows/s (-11%), median flush 28.4 -> 31.5
ms. Live load uses under 2% of that capacity, so it's an easy trade.

**Alternative tested and rejected.** Binance trade IDs increase with time per
symbol, so `ORDER BY symbol, trade_id DESC` looked like it might reuse the
primary key with no new index. It doesn't: the planner still reads all rows
(103,459) and spills a sort to disk -- 34.7 ms in a single `EXPLAIN ANALYZE`,
no SkipScan. It would also tie correctness to an exchange implementation
detail.

## Environment

Measured 2026-10-02 on an Apple M4 Pro (24 GB), Postgres 16.15 + TimescaleDB
2.30.2 in Docker Desktop (14 CPUs, 7.7 GB), `synchronous_commit=on`, single
connection over loopback. Full environment is in each result file.

### Caveats -- read before quoting these numbers

- **This is replay capacity, not live load.** Live traffic for two symbols
  ran at roughly 25-860 rows/s in 10 s windows on 2026-10-02. The naive writer
  kept up with that, but the busiest window already reached 73% of its
  capacity; a volatile market would exceed it. The batched writer's headroom
  matters for bursts, backfill, replay and adding symbols.
- **Batch writers report flush latency, not per-row latency.** In live use a
  row also waits up to `max_delay_s` (200 ms) in the queue before its batch
  flushes. That is a deliberate latency-for-throughput trade.
- **Docker Desktop on macOS.** Postgres runs in a Linux VM whose virtual disk
  sits on the host SSD. Commit flush latency there may not match a Linux
  server with local disk; re-measure on the deployment host (Stage 7) before
  comparing across machines.
- **Client and database share a machine.** A real network hop adds round-trip
  time, which hurts the per-row writer most and widens the gap.
