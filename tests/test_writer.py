"""BatchWriter batching logic, with a fake flush function instead of a database."""

import asyncio

import pytest

from tickwatch.parse import DepthUpdate, Trade
from tickwatch.writer import BatchWriter

from .helpers import T0, depth


def trade(i: int) -> Trade:
    return Trade(
        time=T0,
        symbol="BTCUSDT",
        trade_id=i,
        price=1,
        qty=1,
        buyer_is_maker=False,
        event_time=T0,
        received_at=T0,
    )


class Recorder:
    """Stands in for flush_copy; records each batch it receives."""

    def __init__(self) -> None:
        self.batches: list[tuple[list, list]] = []

    async def __call__(self, conn, trades, depths) -> None:
        self.batches.append((list(trades), list(depths)))

    @property
    def sizes(self) -> list[int]:
        return [len(t) + len(d) for t, d in self.batches]


def make(**kw) -> tuple[BatchWriter, Recorder]:
    rec = Recorder()
    return BatchWriter(conn=None, flush=rec, **kw), rec


def test_flushes_when_batch_size_is_reached() -> None:
    async def go() -> list[int]:
        w, rec = make(batch_size=2, max_delay_s=60)
        for i in range(5):
            await w.put(trade(i))
        await w.close()
        await w.run()
        return rec.sizes

    # Full batches go out at size; the remainder is flushed on close.
    assert asyncio.run(go()) == [2, 2, 1]


def test_flushes_partial_batch_after_max_delay() -> None:
    async def go() -> list[int]:
        w, rec = make(batch_size=1000, max_delay_s=0.05)
        task = asyncio.create_task(w.run())
        for i in range(3):
            await w.put(trade(i))
        await asyncio.sleep(0.2)
        sizes_before_close = rec.sizes
        await w.close()
        await task
        return sizes_before_close

    # A quiet stream still lands without waiting for 1000 rows or a close.
    assert asyncio.run(go()) == [3]


def test_flushes_immediately_when_deadline_already_passed() -> None:
    # Under load the deadline can expire while the queue is still draining;
    # the writer must then flush what it has rather than wait for more.
    async def go() -> list[int]:
        w, rec = make(batch_size=1000, max_delay_s=0)
        task = asyncio.create_task(w.run())
        await w.put(trade(1))
        await asyncio.sleep(0.05)
        sizes_before_close = rec.sizes
        await w.close()
        await task
        return sizes_before_close

    assert asyncio.run(go()) == [1]


def test_full_queue_blocks_put() -> None:
    async def go() -> None:
        w, _ = make(queue_size=2)
        await w.put(trade(1))
        await w.put(trade(2))
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(w.put(trade(3)), timeout=0.05)

    asyncio.run(go())


def test_backpressure_releases_as_the_writer_drains() -> None:
    async def go() -> int:
        w, rec = make(queue_size=2, batch_size=2, max_delay_s=0.01)
        task = asyncio.create_task(w.run())
        for i in range(50):  # far more than the queue holds
            await asyncio.wait_for(w.put(trade(i)), timeout=1)
        await w.close()
        await task
        return sum(rec.sizes)

    assert asyncio.run(go()) == 50


def test_close_flushes_everything_queued_in_order() -> None:
    async def go() -> list[int]:
        w, rec = make(batch_size=3, max_delay_s=60)
        for i in range(7):
            await w.put(trade(i))
        await w.close()
        await w.run()
        return [t.trade_id for batch, _ in rec.batches for t in batch]

    assert asyncio.run(go()) == list(range(7))


def test_trades_and_depth_updates_are_split_for_the_flush() -> None:
    async def go() -> Recorder:
        w, rec = make(batch_size=10, max_delay_s=60)
        await w.put(trade(1))
        await w.put(depth("BTCUSDT", 1, 2))
        await w.put(trade(2))
        await w.close()
        await w.run()
        return rec

    rec = asyncio.run(go())
    [(trades, depths)] = rec.batches
    assert [t.trade_id for t in trades] == [1, 2]
    assert all(isinstance(d, DepthUpdate) for d in depths) and len(depths) == 1


def test_on_flush_reports_rows_and_duration() -> None:
    seen: list[tuple[int, float]] = []

    async def go() -> None:
        w = BatchWriter(
            conn=None, flush=Recorder(), batch_size=2, on_flush=lambda n, s: seen.append((n, s))
        )
        for i in range(3):
            await w.put(trade(i))
        await w.close()
        await w.run()

    asyncio.run(go())
    assert [n for n, _ in seen] == [2, 1]
    assert all(s >= 0 for _, s in seen)


def test_flush_failure_propagates_out_of_run() -> None:
    async def boom(conn, trades, depths) -> None:
        raise RuntimeError("db down")

    async def go() -> None:
        w = BatchWriter(conn=None, flush=boom)
        await w.put(trade(1))
        await w.close()
        await w.run()

    # The consumer relies on this to stop rather than silently drop rows.
    with pytest.raises(RuntimeError, match="db down"):
        asyncio.run(go())
