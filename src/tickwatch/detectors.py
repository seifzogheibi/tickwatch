"""Online anomaly detectors over per-second features.

Both detectors see the same transformed feature vector, so when they
disagree it's about the method, not the inputs:

    log1p(trade_count), log1p(notional), spread_bps, log1p(abs_return_bps)

Counts, notional and returns are heavy-tailed, hence the logs. Spread is
left in bps: it sits at exactly one tick almost all the time, so its scale
is already tiny and a log would hide how many ticks it widened by.
"""

import asyncio
import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime

import numpy as np
from sklearn.ensemble import IsolationForest

from tickwatch.features import Features

FEATURE_NAMES = ("log_trade_count", "log_notional", "spread_bps", "log_abs_return_bps")
SPREAD = FEATURE_NAMES.index("spread_bps")


def vector(f: Features) -> np.ndarray | None:
    """Detector input for one bucket, or None if it lacks a quote (no spread/return yet)."""
    if f.spread_bps is None or f.abs_return_bps is None:
        return None
    return np.array(
        [
            math.log1p(f.trade_count),
            math.log1p(f.notional),
            f.spread_bps,
            math.log1p(f.abs_return_bps),
        ]
    )


@dataclass(frozen=True, slots=True)
class Flag:
    time: datetime  # bucket start
    symbol: str
    detector: str
    score: float
    threshold: float
    features: dict  # feature values (and per-feature z for zscore) at flag time


class ZScoreDetector:
    """Rolling z-score per symbol per feature; flags when any |z| > threshold.

    The baseline is the previous `window` buckets (5 min), not including the
    one being scored. Spread needs a floor on its standard deviation: it is
    pinned at one tick for long stretches, so its std is often exactly 0 and
    the first widening would score z = infinity. The floor is half the
    window's median spread (~half a tick), so a spread at 3x typical scores
    z ~ 4. Other features skip a z when their std is (numerically) 0.
    """

    name = "zscore"

    def __init__(self, window: int = 300, min_samples: int = 60, threshold: float = 4.0) -> None:
        self.window = window
        self.min_samples = min_samples
        self.threshold = threshold
        self._history: dict[str, deque[np.ndarray]] = {}

    async def score(self, f: Features) -> Flag | None:
        x = vector(f)
        if x is None:
            return None
        history = self._history.setdefault(f.symbol, deque(maxlen=self.window))
        flag = None
        if len(history) >= self.min_samples:
            past = np.array(history)
            mean, std = past.mean(axis=0), past.std(axis=0)
            # A constant window can come out with std ~1e-18 instead of 0
            # (300 copies of ETH's one-tick spread, 0.0374 bps, give 6.9e-18),
            # which would turn a tiny change into z ~ 1e11. Treat that as 0.
            std[std <= 1e-9 * np.maximum(1.0, np.abs(mean))] = 0.0
            std[SPREAD] = max(std[SPREAD], 0.5 * float(np.median(past[:, SPREAD])))
            z = np.divide(x - mean, std, out=np.zeros_like(x), where=std > 0)
            score = float(np.max(np.abs(z)))
            if score > self.threshold:
                flag = Flag(
                    time=f.time,
                    symbol=f.symbol,
                    detector=self.name,
                    score=score,
                    threshold=self.threshold,
                    features={
                        **dict(zip(FEATURE_NAMES, x.tolist(), strict=True)),
                        **{f"z_{n}": v for n, v in zip(FEATURE_NAMES, z.tolist(), strict=True)},
                    },
                )
        history.append(x)
        return flag


def _fit(data: np.ndarray, n_estimators: int, seed: int, quantile: float):
    model = IsolationForest(n_estimators=n_estimators, random_state=seed).fit(data)
    # Anomaly score = -score_samples (higher = more anomalous). The threshold
    # is a quantile of the training scores, rather than sklearn's
    # `contamination`, so "how often should this fire" is an explicit choice.
    threshold = float(np.quantile(-model.score_samples(data), quantile))
    return model, threshold


@dataclass
class _ForestState:
    history: deque
    seen: int = 0  # buckets scored for this symbol
    model: IsolationForest | None = None
    threshold: float = math.inf
    last_fit_at: int = 0
    pending: asyncio.Future | None = None
    swap_at: int = 0
    fits: int = 0


class IsolationForestDetector:
    """Per-symbol Isolation Forest, refit periodically on a trailing window.

    Fitting runs in a worker thread so the event loop keeps ingesting. To keep
    replay deterministic, a new model replaces the old one after exactly
    `swap_after` more buckets -- awaiting the fit if it isn't done yet --
    rather than whenever the thread happens to finish. Refits are counted in
    buckets (event time), never wall-clock time, for the same reason.
    """

    name = "iforest"

    def __init__(
        self,
        train_window: int = 3600,
        refit_every: int = 600,
        swap_after: int = 30,
        quantile: float = 0.999,
        n_estimators: int = 100,
        seed: int = 0,
    ) -> None:
        self.train_window = train_window
        self.refit_every = refit_every
        self.swap_after = swap_after
        self.quantile = quantile
        self.n_estimators = n_estimators
        self.seed = seed
        self._state: dict[str, _ForestState] = {}

    async def score(self, f: Features) -> Flag | None:
        x = vector(f)
        if x is None:
            return None
        st = self._state.setdefault(f.symbol, _ForestState(deque(maxlen=self.train_window)))
        st.seen += 1

        if st.pending is not None and st.seen >= st.swap_at:
            st.model, st.threshold = await st.pending
            st.pending = None
            st.fits += 1

        flag = None
        if st.model is not None:
            score = float(-st.model.score_samples(x.reshape(1, -1))[0])
            if score > st.threshold:
                flag = Flag(
                    time=f.time,
                    symbol=f.symbol,
                    detector=self.name,
                    score=score,
                    threshold=st.threshold,
                    features=dict(zip(FEATURE_NAMES, x.tolist(), strict=True)),
                )
        st.history.append(x)

        due = st.model is None or st.seen - st.last_fit_at >= self.refit_every
        if st.pending is None and len(st.history) == self.train_window and due:
            data = np.array(st.history)  # snapshot: the deque keeps changing
            st.pending = asyncio.ensure_future(
                asyncio.to_thread(_fit, data, self.n_estimators, self.seed, self.quantile)
            )
            st.swap_at = st.seen + self.swap_after
            st.last_fit_at = st.seen
        return flag
