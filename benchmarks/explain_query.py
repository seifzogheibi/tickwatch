"""Time a query with EXPLAIN ANALYZE and save the plan.

Runs against the live database (read-only). The query is executed once to
warm the cache, then `--runs` more times; the reported figure is the median
server-side execution time from EXPLAIN ANALYZE, so client round trips are
excluded. The consumer may be writing concurrently, which adds some noise.

Usage:
    python benchmarks/explain_query.py --label before \
        --out benchmarks/results/stage2_query_before
"""

import argparse
import json
import statistics
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import psycopg

from tickwatch.config import load_settings

# Latest trade per symbol: what a "last price" panel or the anomaly detector
# asks for constantly.
QUERY = """
SELECT DISTINCT ON (symbol) symbol, time, price
FROM trades
ORDER BY symbol, time DESC
"""


def _git_commit() -> str:
    out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True)
    return out.stdout.strip() or "unknown"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--label", required=True)
    p.add_argument("--runs", type=int, default=10)
    p.add_argument("--out", type=Path, required=True, help="path prefix; writes .json and .txt")
    args = p.parse_args()

    with psycopg.connect(load_settings().pg_dsn) as conn:
        rows, size, chunks = conn.execute(
            """
            SELECT (SELECT count(*) FROM trades),
                   pg_size_pretty(hypertable_size('trades')),
                   (SELECT count(*) FROM timescaledb_information.chunks
                     WHERE hypertable_name = 'trades')
            """
        ).fetchone()
        indexes = [
            r[0]
            for r in conn.execute(
                "SELECT indexdef FROM pg_indexes WHERE tablename = 'trades' ORDER BY indexname"
            )
        ]
        conn.execute(QUERY).fetchall()  # warm-up
        times_ms = []
        for _ in range(args.runs):
            (plan,) = conn.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {QUERY}").fetchone()
            times_ms.append(plan[0]["Execution Time"])
        text_plan = "\n".join(
            r[0] for r in conn.execute(f"EXPLAIN (ANALYZE, BUFFERS) {QUERY}").fetchall()
        )

    result = {
        "label": args.label,
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "git_commit": _git_commit(),
        "query": " ".join(QUERY.split()),
        "trades_rows": rows,
        "trades_size": size,
        "trades_chunks": chunks,
        "indexes": indexes,
        "runs": args.runs,
        "execution_ms": {
            "median": round(statistics.median(times_ms), 3),
            "min": round(min(times_ms), 3),
            "max": round(max(times_ms), 3),
        },
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.with_suffix(".json").write_text(json.dumps(result, indent=2) + "\n")
    args.out.with_suffix(".txt").write_text(text_plan + "\n")
    print(json.dumps(result, indent=2))
    print(text_plan)


if __name__ == "__main__":
    main()
