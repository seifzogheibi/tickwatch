import math
from datetime import timedelta
from decimal import Decimal

import pytest

from tickwatch.features import FeatureBuilder
from tickwatch.parse import BookTicker, Trade

from .helpers import T0


def at(seconds: float) -> object:
    return T0 + timedelta(seconds=seconds)


def quote(t: float, bid: str, ask: str, symbol: str = "BTCUSDT") -> BookTicker:
    return BookTicker(
        symbol=symbol,
        update_id=1,
        bid=Decimal(bid),
        bid_qty=Decimal(1),
        ask=Decimal(ask),
        ask_qty=Decimal(1),
        received_at=at(t),
    )


def trade(t: float, price: str, qty: str, symbol: str = "BTCUSDT") -> Trade:
    return Trade(
        time=at(t),
        symbol=symbol,
        trade_id=1,
        price=Decimal(price),
        qty=Decimal(qty),
        buyer_is_maker=False,
        event_time=at(t),
        received_at=at(t),
    )


def feed(events) -> list:
    b = FeatureBuilder()
    out = []
    for e in events:
        out.extend(b.on_event(e))
    return out


def test_trades_are_summed_per_second() -> None:
    [f] = feed([trade(0.1, "100", "2"), trade(0.9, "101", "1"), trade(1.0, "100", "1")])
    assert (f.time, f.trade_count, f.volume, f.notional) == (T0, 2, 3.0, 301.0)


def test_spread_is_time_weighted_within_the_bucket() -> None:
    # 10 bps spread held 0.5 s, then 30 bps held 0.5 s -> mean 20 bps.
    # (An update-weighted mean would also give 20 here; the next test differs.)
    [f] = feed(
        [
            quote(0.0, "99.95", "100.05"),  # 10 bps
            quote(0.5, "99.85", "100.15"),  # 30 bps
            trade(1.2, "100", "1"),  # closes bucket 0
        ]
    )
    assert f.spread_bps == pytest.approx(20.0)


def test_brief_quotes_count_for_their_duration_not_their_number() -> None:
    # 10 bps held 0.9 s, then three 50 bps quotes in the last 0.1 s.
    [f] = feed(
        [
            quote(0.0, "99.95", "100.05"),
            quote(0.90, "99.75", "100.25"),
            quote(0.93, "99.75", "100.25"),
            quote(0.96, "99.75", "100.25"),
            trade(1.5, "100", "1"),
        ]
    )
    assert f.spread_bps == pytest.approx(0.9 * 10 + 0.1 * 50)  # 14, not 40


def test_a_quote_carries_over_into_following_seconds() -> None:
    out = feed([quote(0.5, "99.95", "100.05"), trade(1.5, "100", "1"), trade(2.1, "100", "1")])
    first, second = out
    assert first.spread_bps == pytest.approx(10.0)  # only the quoted 0.5 s counts
    assert second.spread_bps == pytest.approx(10.0)  # held the whole second


def test_return_is_abs_log_mid_change_between_buckets() -> None:
    out = feed([quote(0.1, "99", "101"), quote(1.1, "101", "103"), trade(2.1, "1", "1")])
    assert out[0].abs_return_bps is None  # no previous mid
    assert out[1].abs_return_bps == pytest.approx(abs(math.log(102 / 100)) * 1e4)


def test_quiet_seconds_inside_a_short_silence_are_zero_volume_buckets() -> None:
    out = feed([trade(0.1, "100", "1"), trade(3.1, "100", "1"), trade(4.1, "100", "1")])
    assert [(f.time - T0).seconds for f in out] == [0, 1, 2, 3]
    assert [f.trade_count for f in out] == [1, 0, 0, 1]


def test_long_silence_is_skipped_not_filled_and_stale_quote_dropped() -> None:
    out = feed(
        [
            quote(0.1, "99.95", "100.05"),
            trade(0.2, "100", "1"),
            trade(60.2, "100", "1"),  # 60 s with nothing: we were disconnected
            trade(61.2, "100", "1"),
        ]
    )
    assert [(f.time - T0).seconds for f in out] == [0, 60]
    after = out[1]
    assert after.spread_bps is None and after.mid is None and after.abs_return_bps is None


def test_every_seen_symbol_gets_a_bucket_each_second() -> None:
    out = feed(
        [
            trade(0.1, "100", "1", "BTCUSDT"),
            trade(0.2, "10", "1", "ETHUSDT"),
            trade(1.3, "100", "1", "BTCUSDT"),
            trade(2.0, "100", "1", "BTCUSDT"),
        ]
    )
    assert [(f.time - T0).seconds for f in out] == [0, 0, 1, 1]
    assert [(f.symbol, f.trade_count) for f in out] == [
        ("BTCUSDT", 1),
        ("ETHUSDT", 1),
        ("BTCUSDT", 1),
        ("ETHUSDT", 0),
    ]
