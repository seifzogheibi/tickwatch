"""Compare the z-score and Isolation Forest detectors over the same buckets.

Re-runs both detectors over features_1s (they're deterministic, so this
reproduces exactly what the pipeline did -- checked against anomaly_flags)
to get every bucket's score, not just the flagged ones. That separates two
questions that flag counts alone conflate:

  1. At their production thresholds, how often does each fire, and on which
     seconds? (Largely a question of threshold calibration.)
  2. If both flagged the same number of seconds, would they pick the same
     ones? (A question about the methods.)

Reads the configured database -- normally a scratch DB filled by
`tickwatch-replay`, so the result is reproducible from the archive.

Usage: PGDATABASE=tickwatch_replay python analysis/compare_detectors.py > report.md
"""

import asyncio
import math
import subprocess
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import numpy as np
import psycopg
from scipy.stats import spearmanr

from tickwatch.config import load_settings
from tickwatch.detectors import FEATURE_NAMES, IsolationForestDetector, ZScoreDetector, vector
from tickwatch.features import Features

NEAR = timedelta(seconds=2)
MATCHED_RATES = (0.001, 0.01)


@dataclass
class Scored:
    time: datetime
    x: np.ndarray
    z: float  # max |z|
    z_flag: bool
    f: float  # forest anomaly score
    f_thr: float
    f_flag: bool


async def rescore(feats: list[Features]) -> list[Scored]:
    """Run both detectors exactly as the pipeline does; keep buckets both scored."""
    z, forest = ZScoreDetector(), IsolationForestDetector()
    out = []
    for f in feats:
        zf, ff = await z.score(f), await forest.score(f)
        if z.last_score is not None and forest.last_score is not None:
            out.append(
                Scored(
                    f.time,
                    vector(f),
                    z.last_score,
                    zf is not None,
                    forest.last_score,
                    forest.last_threshold,
                    ff is not None,
                )
            )
    return out


def load(conn: psycopg.Connection, symbol: str):
    rows = conn.execute(
        "SELECT time, symbol, trade_count, volume, notional, spread_bps, mid, abs_return_bps"
        " FROM features_1s WHERE symbol = %s ORDER BY time",
        (symbol,),
    ).fetchall()
    stored: dict[str, dict[datetime, dict]] = {"zscore": {}, "iforest": {}}
    for t, det, feats in conn.execute(
        "SELECT time, detector, features FROM anomaly_flags WHERE symbol = %s", (symbol,)
    ):
        stored[det][t] = feats
    return [Features(*r) for r in rows], stored


def percentiles(matrix: np.ndarray, rows: list[Scored]) -> str:
    if not rows:
        return "n/a"
    parts = []
    for i, name in enumerate(FEATURE_NAMES):
        ranks = [float((matrix[:, i] < r.x[i]).mean() * 100) for r in rows]
        parts.append(f"{name} p{np.median(ranks):.0f}")
    return ", ".join(parts)


def fmt(r: Scored) -> str:
    vals = ", ".join(f"{n}={v:.3g}" for n, v in zip(FEATURE_NAMES, r.x, strict=True))
    return f"{r.time:%H:%M:%S} z={r.z:.1f} forest={r.f:.3f} (thr {r.f_thr:.3f}): {vals}"


def report_symbol(symbol: str, scored: list[Scored], stored: dict, out: list) -> None:
    out.append(f"### {symbol}\n")
    if not scored:
        out.append("No seconds where both detectors could score yet.\n")
        return
    n = len(scored)
    start, end = scored[0].time, scored[-1].time
    matrix = np.array([r.x for r in scored])
    hours = n / 3600

    # Sanity check: the re-run must reproduce the pipeline's stored flags.
    z_set = {r.time for r in scored if r.z_flag}
    f_set = {r.time for r in scored if r.f_flag}
    stored_z = {t for t in stored["zscore"] if start <= t <= end}
    stored_f = {t for t in stored["iforest"] if start <= t <= end}
    reproduced = z_set == stored_z and f_set == stored_f
    out.append(
        f"{n:,} seconds where both detectors scored, {start:%Y-%m-%d %H:%M:%S} to "
        f"{end:%H:%M:%S} UTC ({hours:.2f} h). Re-running the detectors reproduces "
        f"the stored flags exactly: **{'yes' if reproduced else 'NO'}**.\n"
    )

    out.append(
        "**1. At production thresholds** (z-score: any |z| > 4; forest: above the "
        "99.9th percentile of its own training scores)\n"
    )
    both, z_only, f_only = z_set & f_set, z_set - f_set, f_set - z_set
    near = {t for t in z_only if any(abs(t - u) <= NEAR for u in f_set)}
    out.append("| | z-score | Isolation Forest |\n|---|---:|---:|")
    out.append(f"| flags | {len(z_set)} | {len(f_set)} |")
    out.append(f"| per hour | {len(z_set) / hours:.1f} | {len(f_set) / hours:.1f} |")
    out.append(f"| % of seconds | {len(z_set) / n:.2%} | {len(f_set) / n:.2%} |")
    out.append(f"| also flagged by the other (same second) | {len(both)} | {len(both)} |")
    out.append(f"| flagged only by this one | {len(z_only)} | {len(f_only)} |")
    union = z_set | f_set
    jaccard = f"{len(both) / len(union):.2f}" if union else "n/a"
    out.append(
        f"\nJaccard overlap: {jaccard}. Of the {len(z_only)} z-only flags, {len(near)} "
        "have a forest flag within +-2 s.\n"
    )
    drivers = Counter(
        max((k for k in feats if k.startswith("z_")), key=lambda k: abs(feats[k]))[2:]
        for t, feats in stored["zscore"].items()
        if start <= t <= end
    )
    if drivers:
        out.append(
            "Feature with the largest |z| in each z-score flag: "
            + ", ".join(f"{name} {c}" for name, c in drivers.most_common())
            + ".\n"
        )

    out.append("**2. At matched alarm rates** (each detector's top-k seconds by score)\n")
    out.append("| rate | k | top-k sets overlap | expected by chance |\n|---|---:|---:|---:|")
    zs, fs = np.array([r.z for r in scored]), np.array([r.f for r in scored])
    top1 = None
    for rate in MATCHED_RATES:
        k = max(1, math.ceil(rate * n))
        z_top = set(np.argsort(-zs, kind="stable")[:k].tolist())
        f_top = set(np.argsort(-fs, kind="stable")[:k].tolist())
        if rate == 0.01:
            top1 = (z_top, f_top)
        out.append(f"| {rate:.1%} | {k} | {len(z_top & f_top) / k:.0%} | {k / n:.1%} |")
    rho = spearmanr(zs, fs).statistic
    out.append(
        f"\nSpearman rank correlation of the two scores over all {n:,} seconds: **{rho:.2f}**.\n"
    )

    z_top, f_top = top1
    out.append(
        "Median feature percentile (within this symbol's window) among each detector's top 1%:\n"
    )
    out.append(f"- picked by both: {percentiles(matrix, [scored[i] for i in z_top & f_top])}")
    out.append(f"- z-score only: {percentiles(matrix, [scored[i] for i in z_top - f_top])}")
    out.append(f"- forest only: {percentiles(matrix, [scored[i] for i in f_top - z_top])}\n")

    def examples(title: str, idx: set, by: str) -> None:
        rows = sorted((scored[i] for i in idx), key=lambda r: -getattr(r, by))[:5]
        out.append(f"{title}:\n")
        out.extend(f"- {fmt(r)}" for r in rows) if rows else out.append("- none")
        out.append("")

    examples("Strongest top-1% picks by z-score only", z_top - f_top, "z")
    examples("Strongest top-1% picks by the forest only", f_top - z_top, "f")
    examples("Strongest agreements", z_top & f_top, "f")


def main() -> None:
    settings = load_settings()
    commit = (
        subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], capture_output=True, text=True
        ).stdout.strip()
        or "unknown"
    )
    out = [
        "## Detector comparison (generated)\n",
        f"Generated {datetime.now(UTC):%Y-%m-%d %H:%M} UTC by "
        f"`analysis/compare_detectors.py` at commit `{commit}` from database "
        f"`{settings.pg_db}`.\n",
    ]
    with psycopg.connect(settings.pg_dsn) as conn:
        symbols = [r[0] for r in conn.execute("SELECT DISTINCT symbol FROM features_1s ORDER BY 1")]
        for s in symbols:
            feats, stored = load(conn, s)
            report_symbol(s, asyncio.run(rescore(feats)), stored, out)
    print("\n".join(out))


if __name__ == "__main__":
    main()
