import asyncio

from tickwatch.archive import CONNECTED_MARKER, iter_archive
from tickwatch.gaps import Gap
from tickwatch.parse import DepthUpdate, Trade
from tickwatch.pipeline import Pipeline, Stats

from .helpers import ARCHIVE, BOOK_TICKER_FRAME, DEPTH_FRAME, T0, TRADE_FRAME


class FakeWriter:
    def __init__(self) -> None:
        self.items: list = []

    async def put(self, item) -> None:
        self.items.append(item)


def run_frames(frames: list[str]) -> tuple[FakeWriter, Stats]:
    writer, stats = FakeWriter(), Stats()
    pipeline = Pipeline(writer, stats)

    async def go() -> None:
        for f in frames:
            if f == CONNECTED_MARKER:
                pipeline.on_connected(T0)
            else:
                await pipeline.on_frame(T0, f)

    asyncio.run(go())
    return writer, stats


def test_malformed_frames_are_counted_and_skipped_not_raised() -> None:
    bad_price = TRADE_FRAME.replace('"p":"84922.04000000"', '"p":"abc"')
    writer, stats = run_frames(["not json", bad_price, TRADE_FRAME, DEPTH_FRAME])
    assert stats.parse_errors == 2
    assert stats.frames == 4
    assert [type(i) for i in writer.items] == [Trade, DepthUpdate]


def test_quotes_are_not_written_row_by_row() -> None:
    writer, stats = run_frames([BOOK_TICKER_FRAME, TRADE_FRAME])
    assert [type(i) for i in writer.items] == [Trade]
    assert stats.parse_errors == 0


def test_unknown_event_types_are_dropped_silently() -> None:
    writer, stats = run_frames(['{"stream":"x","data":{"e":"kline"}}'])
    assert writer.items == [] and stats.parse_errors == 0


def test_real_archive_through_pipeline() -> None:
    # The same calls replay makes, with a fake writer instead of the database.
    writer, stats = FakeWriter(), Stats()
    pipeline = Pipeline(writer, stats)

    async def go() -> None:
        for received_at, payload in iter_archive([ARCHIVE]):
            if payload == CONNECTED_MARKER:
                pipeline.on_connected(received_at)
            else:
                await pipeline.on_frame(received_at, payload)

    asyncio.run(go())
    items = writer.items
    counts = {t: sum(isinstance(i, t) for i in items) for t in (Trade, DepthUpdate, Gap)}
    assert counts == {Trade: 841, DepthUpdate: 324, Gap: 2}
    gaps = [i for i in items if isinstance(i, Gap)]
    assert {(g.symbol, g.cause, g.missing_update_ids) for g in gaps} == {
        ("BTCUSDT", "reconnect", 1821),
        ("ETHUSDT", "reconnect", 771),
    }
    # Each gap is enqueued immediately before the update that revealed it.
    for g in gaps:
        nxt = items[items.index(g) + 1]
        assert isinstance(nxt, DepthUpdate) and nxt.first_update_id == g.first_update_id
