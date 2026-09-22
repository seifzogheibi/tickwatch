"""Per-symbol, per-second market features: the input to anomaly detection.

Buckets are 1 s of *receive* time. bookTicker quotes carry no exchange
timestamp, so receive time is the only clock all streams share; it is also
monotonic in processing order (no late data) and recorded in the archive, so
replay rebuilds identical buckets. Cost: buckets are skewed ~100-250 ms
against exchange time.
"""

import math
from dataclasses import dataclass
from datetime import datetime, timedelta

from tickwatch.parse import BookTicker, Trade

BUCKET = timedelta(seconds=1)
# Seconds with no events inside a short silence are emitted as zero-trade
# buckets. A longer silence means we weren't receiving (outage, reconnect),
# not that nobody traded, so those seconds are skipped rather than invented.
MAX_FILL_S = 5


@dataclass(frozen=True, slots=True)
class Features:
    time: datetime  # bucket start
    symbol: str
    trade_count: int
    volume: float  # base asset
    notional: float  # quote asset (sum of price * qty)
    spread_bps: float | None  # time-weighted mean over the bucket; None before any quote
    mid: float | None  # mid at bucket end
    abs_return_bps: float | None  # |ln(mid / previous bucket's mid)| * 1e4


@dataclass
class _SymbolState:
    trade_count: int = 0
    volume: float = 0.0
    notional: float = 0.0
    spread_bps: float | None = None  # current quoted spread
    mid: float | None = None  # current mid
    quote_since: datetime | None = None  # when the current quote took effect (clipped to bucket)
    spread_area: float = 0.0  # sum of spread * seconds within the bucket
    covered_s: float = 0.0  # seconds of the bucket with a known quote
    prev_mid: float | None = None  # mid at the end of the previous emitted bucket


def _floor(t: datetime) -> datetime:
    return t.replace(microsecond=0)


class FeatureBuilder:
    def __init__(self) -> None:
        self._symbols: dict[str, _SymbolState] = {}
        self._bucket: datetime | None = None  # start of the open bucket

    def on_event(self, item: Trade | BookTicker) -> list[Features]:
        """Account for one trade or quote; return any buckets it closed."""
        t = item.received_at
        closed = self._advance(_floor(t))
        st = self._symbols.setdefault(item.symbol, _SymbolState())
        if isinstance(item, Trade):
            st.trade_count += 1
            st.volume += float(item.qty)
            st.notional += float(item.qty * item.price)
        else:
            self._accrue(st, t)
            bid, ask = float(item.bid), float(item.ask)
            st.mid = (bid + ask) / 2
            st.spread_bps = (ask - bid) / st.mid * 1e4
            st.quote_since = t
        return closed

    def _accrue(self, st: _SymbolState, until: datetime) -> None:
        """Add the current quote's spread * time held, up to `until`."""
        if st.spread_bps is None or st.quote_since is None:
            return
        held = (until - max(st.quote_since, self._bucket)).total_seconds()
        if held > 0:
            st.spread_area += st.spread_bps * held
            st.covered_s += held

    def _advance(self, bucket: datetime) -> list[Features]:
        if self._bucket is None:
            self._bucket = bucket
            return []
        out: list[Features] = []
        while self._bucket < bucket:
            out.extend(self._close_bucket())
            self._bucket += BUCKET
            if bucket - self._bucket > timedelta(seconds=MAX_FILL_S):
                # Long silence: jump ahead without emitting invented buckets.
                # The pre-silence quote is stale, so spread and mid stay
                # unknown until a fresh quote arrives.
                self._bucket = bucket
                for st in self._symbols.values():
                    st.spread_bps = st.mid = st.quote_since = st.prev_mid = None
        return out

    def _close_bucket(self) -> list[Features]:
        end = self._bucket + BUCKET
        out = []
        for symbol, st in self._symbols.items():
            self._accrue(st, end)
            ret = None
            if st.mid is not None and st.prev_mid is not None:
                ret = abs(math.log(st.mid / st.prev_mid)) * 1e4
            out.append(
                Features(
                    time=self._bucket,
                    symbol=symbol,
                    trade_count=st.trade_count,
                    volume=st.volume,
                    notional=st.notional,
                    spread_bps=st.spread_area / st.covered_s if st.covered_s else None,
                    mid=st.mid,
                    abs_return_bps=ret,
                )
            )
            st.trade_count, st.volume, st.notional = 0, 0.0, 0.0
            st.spread_area, st.covered_s = 0.0, 0.0
            st.prev_mid = st.mid
        return out
