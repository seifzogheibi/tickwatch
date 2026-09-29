import asyncio
import math
import random
from datetime import timedelta

import pytest

from tickwatch.detectors import IsolationForestDetector, ZScoreDetector, vector
from tickwatch.features import Features

from .helpers import T0

TICK_BPS = 0.0012  # one 0.01 tick on BTCUSDT near 84,000


def bucket(i: int, count: int = 5, notional: float = 1000.0, spread: float = TICK_BPS,
           ret: float = 0.1, symbol: str = "BTCUSDT") -> Features:  # fmt: skip
    return Features(
        time=T0 + timedelta(seconds=i),
        symbol=symbol,
        trade_count=count,
        volume=notional / 84000,
        notional=notional,
        spread_bps=spread,
        mid=84000.0,
        abs_return_bps=ret,
    )


def noisy(i: int, rng: random.Random, **kw) -> Features:
    defaults = dict(
        count=rng.randint(2, 8), notional=rng.uniform(500, 1500), ret=rng.uniform(0, 0.3)
    )
    return bucket(i, **{**defaults, **kw})


def run(detector, buckets) -> list:
    async def go() -> list:
        return [flag for b in buckets if (flag := await detector.score(b))]

    return asyncio.run(go())


def test_vector_is_none_without_a_quote() -> None:
    no_quote = Features(T0, "BTCUSDT", 1, 1.0, 1.0, None, None, None)
    assert vector(no_quote) is None


# --- z-score ---------------------------------------------------------------


def test_zscore_is_silent_during_warmup() -> None:
    d = ZScoreDetector(min_samples=60)
    rng = random.Random(0)
    flags = run(d, [noisy(i, rng) for i in range(59)] + [bucket(59, notional=1e9)])
    assert flags == []  # the spike arrives before 60 samples of baseline


def test_zscore_flags_a_notional_spike_and_names_the_feature() -> None:
    rng = random.Random(0)
    buckets = [noisy(i, rng) for i in range(300)] + [bucket(300, count=200, notional=5e6)]
    [flag] = run(ZScoreDetector(), buckets)
    assert flag.time == buckets[-1].time and flag.detector == "zscore"
    assert flag.features["z_log_notional"] > 4
    assert flag.score == pytest.approx(
        max(abs(v) for k, v in flag.features.items() if k.startswith("z_"))
    )


def test_zscore_pinned_spread_does_not_divide_by_zero() -> None:
    # Spread exactly one tick for 300 s: std is 0. Without the floor, the
    # widening would score z = inf (or nan); with it, it's finite and flagged.
    rng = random.Random(1)
    buckets = [noisy(i, rng) for i in range(300)]
    wide = bucket(300, spread=5 * TICK_BPS)
    [flag] = run(ZScoreDetector(), [*buckets, wide])
    assert math.isfinite(flag.score)
    assert flag.features["z_spread_bps"] == pytest.approx((5 - 1) / 0.5)


def test_zscore_one_extra_tick_is_not_an_anomaly() -> None:
    rng = random.Random(2)
    buckets = [noisy(i, rng) for i in range(300)] + [bucket(300, spread=2 * TICK_BPS)]
    assert run(ZScoreDetector(), buckets) == []  # z = (2-1)/0.5 = 2


def test_zscore_constant_feature_float_noise_is_not_an_anomaly() -> None:
    # Regression: 300 copies of 0.0374 have std 6.9e-18, not 0, so a 1e-6
    # change scored z ~ 1e11. Every feature here is constant, then nudged.
    eth_tick = 0.0374
    buckets = [bucket(i, count=5, notional=1000, spread=eth_tick, ret=0.1) for i in range(300)]
    nudged = bucket(300, count=5, notional=1000.001, spread=eth_tick + 1e-6, ret=0.1)
    assert run(ZScoreDetector(), [*buckets, nudged]) == []


def test_zscore_symbols_have_separate_baselines() -> None:
    rng = random.Random(3)
    btc = [noisy(i, rng) for i in range(300)]
    # ETH normally trades 1000x the notional; that's its normal, not an anomaly.
    eth = [noisy(i, rng, notional=rng.uniform(5e5, 1.5e6), symbol="ETHUSDT") for i in range(300)]
    interleaved = [b for pair in zip(btc, eth, strict=True) for b in pair]
    assert run(ZScoreDetector(), interleaved) == []


# --- Isolation Forest ------------------------------------------------------


def forest(**kw) -> IsolationForestDetector:
    return IsolationForestDetector(
        **{"train_window": 300, "refit_every": 100, "swap_after": 10, "n_estimators": 50, **kw}
    )


def test_forest_is_silent_until_first_model_swaps_in() -> None:
    rng = random.Random(0)
    d = forest()
    # The first fit starts on the 300th bucket and swaps in on the 310th
    # (index 309), so an outlier at index 308 is never scored.
    outlier = {"count": 500, "notional": 1e8, "ret": 50}
    buckets = [noisy(i, rng) for i in range(308)]
    buckets += [bucket(308, **outlier), bucket(309, **outlier)]
    assert [f.time for f in run(d, buckets)] == [buckets[309].time]  # only the scored one


def test_forest_flags_an_obvious_outlier_after_warmup() -> None:
    rng = random.Random(0)
    buckets = [noisy(i, rng) for i in range(400)]
    buckets.append(bucket(400, count=500, notional=1e8, spread=10 * TICK_BPS, ret=50))
    flags = run(forest(), buckets)
    assert flags and flags[-1].time == buckets[-1].time
    assert flags[-1].score > flags[-1].threshold


def test_forest_flag_rate_on_normal_data_is_near_the_quantile() -> None:
    rng = random.Random(0)
    d = forest(quantile=0.99)
    flags = run(d, [noisy(i, rng) for i in range(3000)])
    scored = 3000 - 310
    # ~1% expected on in-distribution data; allow wide slack for sampling.
    assert 0.002 < len(flags) / scored < 0.03


def test_forest_refits_on_schedule() -> None:
    rng = random.Random(0)
    d = forest()
    run(d, [noisy(i, rng) for i in range(605)])
    # Fits start on buckets 300, 400, 500, 600 and each swaps in 10 later, so
    # after 605 buckets three (310, 410, 510) have landed and one is pending.
    st = d._state["BTCUSDT"]
    assert st.fits == 3 and st.pending is not None


def test_forest_is_deterministic() -> None:
    def flags_for_seed_data() -> list:
        rng = random.Random(7)
        buckets = [noisy(i, rng) for i in range(1500)]
        for i in (700, 900, 1200):
            buckets[i] = bucket(i, count=300, notional=5e7, ret=20)
        return [(f.time, round(f.score, 12)) for f in run(forest(), buckets)]

    assert flags_for_seed_data() == flags_for_seed_data()


def test_last_score_is_reported_for_unflagged_buckets_too() -> None:
    rng = random.Random(0)
    z, forest_ = ZScoreDetector(), forest()
    scores: list[tuple] = []

    async def go() -> None:
        for i in range(400):
            b = noisy(i, rng)
            zf, ff = await z.score(b), await forest_.score(b)
            scores.append((z.last_score, zf, forest_.last_score, ff))

    asyncio.run(go())
    assert all(zs is None for zs, *_ in scores[:60])  # warm-up: not scored
    assert all(zs is not None for zs, *_ in scores[60:])
    assert all(fs is not None for *_, fs, _ in scores[309:])  # forest live from 310th
    # A flag is raised exactly when the reported score beats the threshold.
    assert all((zf is not None) == (zs > 4) for zs, zf, *_ in scores[60:])
