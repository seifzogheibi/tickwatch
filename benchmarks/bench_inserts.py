"""Benchmark sustained insert throughput by replaying a recorded stream.

Writes into a dedicated `<db>_bench` database (created if missing) so that
truncating between runs can never touch live data. All frames are parsed
before timing starts: the number measured is database write throughput,
not parsing.

Usage:
    python benchmarks/bench_inserts.py \
        --input benchmarks/data/sample-2026-10-02-15m.jsonl.gz \
        --writer <writer> --batch-size 500 --runs 3 --out benchmarks/results/<name>.json
"""

import argparse
import asyncio
import gzip
import hashlib
import json
import platform
import statistics
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo

from tickwatch.config import load_settings
from tickwatch.db import apply_schema
from tickwatch.parse import DepthUpdate, Trade, parse_message
from tickwatch.writer import BatchWriter, FlushFn, NaiveWriter, flush_insert

FLUSHES: dict[str, FlushFn] = {"batch-insert": flush_insert}


def load_frames(path: Path) -> list[str]:
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt") as f:
        return [line for line in f if line.strip()]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


async def ensure_bench_db(main_dsn: str, bench_db: str) -> None:
    async with await psycopg.AsyncConnection.connect(main_dsn, autocommit=True) as conn:
        cur = await conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (bench_db,))
        if await cur.fetchone() is None:
            await conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(bench_db)))


WRITERS = {
    "naive": "NaiveWriter (one INSERT + COMMIT per row)",
    "batch-insert": "BatchWriter + flush_insert (pipelined INSERT, one txn per batch)",
}


def _pcts(samples_ns: list[int]) -> dict:
    # inclusive: interpolate within the observed range (exclusive can report p99 > max).
    q = statistics.quantiles(samples_ns, n=100, method="inclusive")
    return {
        "p50": round(q[49] / 1e6, 3),
        "p95": round(q[94] / 1e6, 3),
        "p99": round(q[98] / 1e6, 3),
        "max": round(max(samples_ns) / 1e6, 3),
    }


async def _replay_naive(conn: psycopg.AsyncConnection, items: list) -> dict:
    writer = NaiveWriter(conn)
    latencies_ns = []
    for item in items:
        t0 = time.perf_counter_ns()
        await writer.write(item)
        latencies_ns.append(time.perf_counter_ns() - t0)
    return {"latency_ms": _pcts(latencies_ns)}


async def _replay_batch(
    conn: psycopg.AsyncConnection, items: list, flush: FlushFn, batch_size: int
) -> dict:
    flushes: list[tuple[int, float]] = []
    writer = BatchWriter(
        conn, flush=flush, batch_size=batch_size, on_flush=lambda n, s: flushes.append((n, s))
    )
    async with asyncio.TaskGroup() as tg:
        tg.create_task(writer.run())
        for item in items:
            await writer.put(item)
        await writer.close()
    return {
        "batch_size": batch_size,
        "flushes": len(flushes),
        "mean_rows_per_flush": round(sum(n for n, _ in flushes) / len(flushes), 1),
        "flush_ms": _pcts([int(s * 1e9) for _, s in flushes]),
    }


async def run_once(
    dsn: str, items: list[Trade | DepthUpdate], writer: str, batch_size: int
) -> dict:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        await conn.execute("TRUNCATE trades, depth_updates")
        await conn.commit()
        start = time.perf_counter_ns()
        if writer == "naive":
            detail = await _replay_naive(conn, items)
        else:
            detail = await _replay_batch(conn, items, FLUSHES[writer], batch_size)
        wall_s = (time.perf_counter_ns() - start) / 1e9
        cur = await conn.execute(
            "SELECT (SELECT count(*) FROM trades) + (SELECT count(*) FROM depth_updates)"
        )
        (stored,) = await cur.fetchone()

    return {
        "rows": len(items),
        "rows_stored": stored,
        "wall_s": round(wall_s, 3),
        "rows_per_s": round(len(items) / wall_s, 1),
        **detail,
    }


def _cmd(*args: str) -> str:
    try:
        return subprocess.run(args, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


async def environment(dsn: str) -> dict:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:

        async def one(q: str) -> str:
            cur = await conn.execute(q)
            return (await cur.fetchone())[0]

        pg = await one("SELECT version()")
        tsdb = await one("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
        sync_commit = await one("SHOW synchronous_commit")
        fsync = await one("SHOW fsync")
    return {
        "host_cpu": _cmd("sysctl", "-n", "machdep.cpu.brand_string"),
        "host_mem_gb": round(int(_cmd("sysctl", "-n", "hw.memsize") or 0) / 2**30, 1),
        "host_os": platform.platform(),
        "docker_cpus": _cmd("docker", "info", "--format", "{{.NCPU}}"),
        "docker_mem_gb": round(
            int(_cmd("docker", "info", "--format", "{{.MemTotal}}") or 0) / 2**30, 1
        ),
        "postgres": pg,
        "timescaledb": tsdb,
        "synchronous_commit": sync_commit,
        "fsync": fsync,
        "python": sys.version.split()[0],
        "psycopg": psycopg.__version__,
        "git_commit": _cmd("git", "rev-parse", "--short", "HEAD"),
    }


async def main_async(args: argparse.Namespace) -> dict:
    settings = load_settings()
    bench_db = f"{settings.pg_db}_bench"
    dsn = make_conninfo(settings.pg_dsn, dbname=bench_db)
    await ensure_bench_db(settings.pg_dsn, bench_db)
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        await apply_schema(conn)

    frames = load_frames(args.input)
    received_at = datetime.now(UTC)
    items = [i for f in frames if (i := parse_message(f, received_at)) is not None]
    print(f"{len(frames)} frames -> {len(items)} rows; {args.runs} runs", flush=True)

    runs = []
    for n in range(args.runs):
        r = await run_once(dsn, items, args.writer, args.batch_size)
        print(f"run {n + 1}: {r}", flush=True)
        runs.append(r)

    return {
        "benchmark": "sustained inserts, replayed recording",
        "writer": WRITERS[args.writer],
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "command": " ".join(sys.argv),
        "input": {"path": str(args.input), "sha256": sha256(args.input), "frames": len(frames)},
        "environment": await environment(dsn),
        "runs": runs,
        "median_rows_per_s": statistics.median(r["rows_per_s"] for r in runs),
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--writer", choices=WRITERS, default="naive")
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--out", type=Path, required=True)
    args = p.parse_args()
    result = asyncio.run(main_async(args))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(f"median {result['median_rows_per_s']} rows/s -> {args.out}")


if __name__ == "__main__":
    main()
