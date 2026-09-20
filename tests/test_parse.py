import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from tickwatch.archive import iter_archive
from tickwatch.parse import PARSE_ERRORS, DepthUpdate, Trade, parse_message

from .helpers import ARCHIVE, DEPTH_FRAME, TRADE_FRAME

RECEIVED = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)


def test_trade_fields_and_types() -> None:
    t = parse_message(TRADE_FRAME, RECEIVED)
    assert t == Trade(
        time=datetime(2026, 10, 2, 17, 3, 50, 913000, tzinfo=UTC),
        symbol="BTCUSDT",
        trade_id=6732650448,
        price=Decimal("84922.04000000"),
        qty=Decimal("0.00023000"),
        buyer_is_maker=True,
        event_time=datetime(2026, 10, 2, 17, 3, 50, 914000, tzinfo=UTC),
        received_at=RECEIVED,
    )


def test_trade_price_is_exact_decimal_not_float() -> None:
    frame = TRADE_FRAME.replace("84922.04000000", "0.10000001")
    assert parse_message(frame, RECEIVED).price == Decimal("0.10000001")


def test_depth_update_fields() -> None:
    d = parse_message(DEPTH_FRAME, RECEIVED)
    assert isinstance(d, DepthUpdate)
    assert (d.symbol, d.first_update_id, d.final_update_id) == ("ETHUSDT", 81705043969, 81705044008)
    assert d.time == datetime(2026, 10, 2, 17, 3, 51, 15000, tzinfo=UTC)
    # Levels stay as exchange strings; qty "0" (level removed) is preserved.
    assert d.bids == [["2678.74000000", "1.20000000"], ["2678.50000000", "0.00000000"]]
    assert d.asks == [["2678.75000000", "3.10000000"]]


def test_accepts_bytes() -> None:
    assert parse_message(TRADE_FRAME.encode(), RECEIVED) == parse_message(TRADE_FRAME, RECEIVED)


def test_unknown_event_type_is_ignored() -> None:
    frame = json.dumps({"stream": "btcusdt@kline_1m", "data": {"e": "kline", "s": "BTCUSDT"}})
    assert parse_message(frame, RECEIVED) is None


@pytest.mark.parametrize(
    "frame",
    [
        "not json",
        "",
        '{"result":null,"id":1}',  # subscription ack: no "data"
        '{"stream":"x","data":{"e":"trade","s":"BTCUSDT"}}',  # missing fields
        '{"stream":"x","data":{"e":"trade","E":1,"s":"B","t":1,"p":"abc","q":"1","T":1,"m":true}}',
    ],
)
def test_malformed_frames_raise_errors_the_pipeline_catches(frame: str) -> None:
    # Pipeline.on_frame catches exactly PARSE_ERRORS to count and skip bad
    # frames; anything else would crash the consumer.
    with pytest.raises(PARSE_ERRORS):
        parse_message(frame, RECEIVED)


def test_every_frame_in_real_archive_parses() -> None:
    kinds = {"trade": 0, "depth": 0}
    for received_at, payload in iter_archive([ARCHIVE]):
        if payload.startswith("#"):
            continue
        item = parse_message(payload, received_at)
        kinds["trade" if isinstance(item, Trade) else "depth"] += 1
        assert item.received_at == received_at
    assert kinds == {"trade": 841, "depth": 324}
