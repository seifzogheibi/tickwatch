"""Compare the z-score and Isolation Forest detectors over the same buckets.

Reads features_1s and anomaly_flags from the configured database (typically
a scratch DB filled by `tickwatch-replay`, so the result is reproducible from
the archive) and prints a Markdown report:

  - the window where *both* detectors were able to flag (the forest only
    starts after its first model swaps in)
  - per symbol: flag counts, rates, overlap (same bucket and within +-2 s)
  - which feature drove each z-score flag
  - where each detector's disagreements sit in the feature distribution
  - the strongest examples of each kind of disagreement

Usage: PGDATABASE=tickwatch_replay python analysis/compare_detectors.py > report.md
"""

import argparse
import math
from collections import Counter
from datetime import timedelta

import numpy as np
import psycopg

from tickwatch.config import load_settings
from tickwatch.detectors import FEATURE_NAMES, IsolationForestDetector, vector
from tickwatch.features import Features

NEAR = timedelta(seconds=2)
_FOREST = IsolationForestDetector()
# The forest scores its first bucket once the first fit (started at bucket
# train_window) swaps in, swap_after buckets later.
FOREST_FIRST_SCORED = _FOREST.train_window + _FOREST.swap_after


def load(conn: psycopg.Connection, symbol: str):
    rows = conn.execute(
        "SELECT time, symbol, trade_count, volume, notional, spread_bps, mid, abs_return_bps"
        " FROM features_1s WHERE symbol = %s ORDER BY time",
        (symbol,),
    ).fetchall()
    feats = [Features(*r) for r in rows]
    vecs = [(f.time, v) for f in feats if (v := vector(f)) is not None]
    flags = conn.execute(
        "SELECT time, detector, score, threshold, features FROM anomaly_flags"
        " WHERE symbol = %s ORDER BY time",
        (symbol,),
    ).fetchall()
    return vecs, flags


def pct_rank(column: np.ndarray, value: float) -> float:
    return float((column < value).mean() * 100)


def fmt_vec(v: dict) -> str:
    return ", ".join(f"{n}={v[n]:.3g}" for n in FEATURE_NAMES)


def report_symbol(symbol: str, vecs, flags, out: list[str]) -> dict:
    if len(vecs) <= FOREST_FIRST_SCORED:
        out.append(
            f"### {symbol}\n\nNot enough data: {len(vecs)} scoreable buckets, the forest "
            f"needs {FOREST_FIRST_SCORED} before its first score.\n"
        )
        return {}
    start, end = vecs[FOREST_FIRST_SCORED - 1][0], vecs[-1][0]
    window = [(t, v) for t, v in vecs if t >= start]
    matrix = np.array([v for _, v in window])
    by_det: dict[str, dict] = {"zscore": {}, "iforest": {}}
    for t, det, score, thr, feats in flags:
        if t >= start:
            by_det[det][t] = (score, thr, feats)
    z, f = set(by_det["zscore"]), set(by_det["iforest"])
    both, z_only, f_only = z & f, z - f, f - z
    near = {t for t in z_only if any(abs(t - u) <= NEAR for u in f)}
    hours = len(window) / 3600

    out.append(f"### {symbol}\n")
    out.append(
        f"Window where both detectors could flag: {start:%Y-%m-%d %H:%M:%S} to "
        f"{end:%H:%M:%S} UTC, {len(window):,} scored buckets ({hours:.2f} h).\n"
    )
    out.append("| | z-score | Isolation Forest |\n|---|---:|---:|")
    out.append(f"| flags | {len(z)} | {len(f)} |")
    out.append(f"| per hour | {len(z) / hours:.1f} | {len(f) / hours:.1f} |")
    out.append(f"| % of buckets | {len(z) / len(window):.3%} | {len(f) / len(window):.3%} |")
    out.append(f"| flagged by both (same second) | {len(both)} | {len(both)} |")
    out.append(f"| flagged only by this one | {len(z_only)} | {len(f_only)} |")
    union = len(z | f)
    jacc = len(both) / union if union else math.nan
    out.append(
        f"\nJaccard overlap (same second): **{jacc:.2f}**. "
        f"{len(near)} of the {len(z_only)} z-only flags have a forest flag within "
        f"+-2 s, so they are timing near-misses rather than real disagreement.\n"
    )

    drivers = Counter(
        max((k for k in feats if k.startswith("z_")), key=lambda k: abs(feats[k]))[2:]
        for t, (_, _, feats) in by_det["zscore"].items()
    )
    out.append(
        "Feature with the largest |z| in each z-score flag: "
        + ", ".join(f"{n} {c}" for n, c in drivers.most_common())
        + ".\n"
    )

    def percentile_profile(times: set) -> str:
        if not times:
            return "n/a"
        vals = [by_det["zscore"].get(t) or by_det["iforest"].get(t) for t in times]
        med = []
        for i, name in enumerate(FEATURE_NAMES):
            ranks = [pct_rank(matrix[:, i], v[2][name]) for v in vals]
            med.append(f"{name} p{np.median(ranks):.0f}")
        return ", ".join(med)

    out.append(
        "Median percentile of each feature (within this symbol's window) at flagged buckets:\n"
    )
    out.append(f"- both: {percentile_profile(both)}")
    out.append(f"- z-score only: {percentile_profile(z_only)}")
    out.append(f"- forest only: {percentile_profile(f_only)}\n")

    def examples(title: str, times: set, det: str, n: int = 5) -> None:
        top = sorted(times, key=lambda t: -by_det[det][t][0])[:n]
        out.append(f"{title} (top {len(top)} by {det} score):\n")
        for t in top:
            score, thr, feats = by_det[det][t]
            out.append(f"- {t:%H:%M:%S} score {score:.2f} (threshold {thr:.2f}): {fmt_vec(feats)}")
        out.append("")

    examples("Strongest z-score-only flags", z_only - near, "zscore")
    examples("Strongest forest-only flags", f_only, "iforest")
    examples("Strongest agreements", both, "iforest")
    return {"z": len(z), "f": len(f), "both": len(both), "buckets": len(window)}


def main() -> None:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    out = ["## Detector comparison\n"]
    with psycopg.connect(load_settings().pg_dsn) as conn:
        symbols = [r[0] for r in conn.execute("SELECT DISTINCT symbol FROM features_1s ORDER BY 1")]
        for s in symbols:
            report_symbol(s, *load(conn, s), out)
    print("\n".join(out))


if __name__ == "__main__":
    main()
