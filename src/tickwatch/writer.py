"""Persist parsed records to Postgres."""

import asyncio
import time
import weakref
from collections.abc import Awaitable, Callable

import psycopg
from psycopg.types.json import Jsonb

from tickwatch.parse import DepthUpdate, Trade

TRADE_COLUMNS = (
    "time",
    "symbol",
    "trade_id",
    "price",
    "qty",
    "buyer_is_maker",
    "event_time",
    "received_at",
)
DEPTH_COLUMNS = (
    "time",
    "symbol",
    "first_update_id",
    "final_update_id",
    "bids",
    "asks",
    "received_at",
)

# ON CONFLICT DO NOTHING: a trade or depth update we've already stored
# (e.g. resent after a reconnect) is skipped rather than aborting the write.
INSERT_TRADE = f"""
INSERT INTO trades ({", ".join(TRADE_COLUMNS)})
VALUES ({", ".join(["%s"] * len(TRADE_COLUMNS))})
ON CONFLICT DO NOTHING
"""

INSERT_DEPTH = f"""
INSERT INTO depth_updates ({", ".join(DEPTH_COLUMNS)})
VALUES ({", ".join(["%s"] * len(DEPTH_COLUMNS))})
ON CONFLICT DO NOTHING
"""


def trade_row(t: Trade) -> tuple:
    return (
        t.time,
        t.symbol,
        t.trade_id,
        t.price,
        t.qty,
        t.buyer_is_maker,
        t.event_time,
        t.received_at,
    )


def depth_row(d: DepthUpdate) -> tuple:
    return (
        d.time,
        d.symbol,
        d.first_update_id,
        d.final_update_id,
        Jsonb(d.bids),
        Jsonb(d.asks),
        d.received_at,
    )


class NaiveWriter:
    """One INSERT and one COMMIT per row.

    Deliberately slow: every row pays a full network round trip plus a WAL
    flush on commit. This is the Stage 1 baseline that batching/COPY is
    measured against -- don't optimise it.
    """

    def __init__(self, conn: psycopg.AsyncConnection) -> None:
        self.conn = conn

    async def write(self, item: Trade | DepthUpdate) -> None:
        async with self.conn.cursor() as cur:
            if isinstance(item, Trade):
                await cur.execute(INSERT_TRADE, trade_row(item))
            else:
                await cur.execute(INSERT_DEPTH, depth_row(item))
        await self.conn.commit()


type Item = Trade | DepthUpdate
type FlushFn = Callable[[psycopg.AsyncConnection, list[Trade], list[DepthUpdate]], Awaitable[None]]


async def flush_insert(
    conn: psycopg.AsyncConnection, trades: list[Trade], depths: list[DepthUpdate]
) -> None:
    """Batched INSERT ... ON CONFLICT in one transaction.

    psycopg's executemany pipelines the statements, so the batch costs one
    commit and few round trips, but the server still parses and plans each row.
    """
    async with conn.transaction(), conn.cursor() as cur:
        if trades:
            await cur.executemany(INSERT_TRADE, [trade_row(t) for t in trades])
        if depths:
            await cur.executemany(INSERT_DEPTH, [depth_row(d) for d in depths])


_STOP = object()


class BatchWriter:
    """Accumulate rows from a bounded queue and flush them in batches.

    A batch is flushed when it reaches `batch_size` rows or when `max_delay_s`
    has passed since its first row, whichever comes first -- so throughput
    comes from large batches under load while a quiet stream still lands in
    the database promptly. The queue is bounded: if the database falls
    behind, `put` blocks, pushing back on the producer instead of growing
    memory without limit.
    """

    def __init__(
        self,
        conn: psycopg.AsyncConnection,
        flush: FlushFn = flush_insert,
        batch_size: int = 500,
        max_delay_s: float = 0.2,
        queue_size: int = 10_000,
        on_flush: Callable[[int, float], None] | None = None,
    ) -> None:
        self.conn = conn
        self._flush_fn = flush
        self.batch_size = batch_size
        self.max_delay_s = max_delay_s
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_size)
        self._on_flush = on_flush

    async def put(self, item: Item) -> None:
        await self.queue.put(item)

    async def close(self) -> None:
        """Ask `run` to flush what's queued and return."""
        await self.queue.put(_STOP)

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        stopping = False
        while not stopping:
            first = await self.queue.get()
            if first is _STOP:
                return
            batch = [first]
            deadline = loop.time() + self.max_delay_s
            while len(batch) < self.batch_size:
                # Drain without waiting first: under load the queue is full
                # and a per-item wait_for would dominate the cost.
                try:
                    item = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    timeout = deadline - loop.time()
                    if timeout <= 0:
                        break
                    try:
                        item = await asyncio.wait_for(self.queue.get(), timeout)
                    except TimeoutError:
                        break
                if item is _STOP:
                    stopping = True
                    break
                batch.append(item)
            await self._flush(batch)

    async def _flush(self, batch: list[Item]) -> None:
        trades = [i for i in batch if isinstance(i, Trade)]
        depths = [i for i in batch if isinstance(i, DepthUpdate)]
        t0 = time.perf_counter()
        await self._flush_fn(self.conn, trades, depths)
        if self._on_flush is not None:
            self._on_flush(len(batch), time.perf_counter() - t0)


def _copy_sql(table: str, columns: tuple[str, ...]) -> str:
    return f"COPY {table} ({', '.join(columns)}) FROM STDIN"


async def _copy_rows(cur: psycopg.AsyncCursor, sql: str, rows: list[tuple]) -> None:
    async with cur.copy(sql) as copy:
        for row in rows:
            await copy.write_row(row)


async def flush_copy_direct(
    conn: psycopg.AsyncConnection, trades: list[Trade], depths: list[DepthUpdate]
) -> None:
    """COPY straight into the hypertables.

    Fastest path, but COPY has no ON CONFLICT: one row we've already stored
    aborts the whole batch. Kept as a reference point for what dedup costs,
    not used by the consumer.
    """
    async with conn.transaction(), conn.cursor() as cur:
        if trades:
            await _copy_rows(
                cur, _copy_sql("trades", TRADE_COLUMNS), [trade_row(t) for t in trades]
            )
        if depths:
            await _copy_rows(
                cur, _copy_sql("depth_updates", DEPTH_COLUMNS), [depth_row(d) for d in depths]
            )


_STAGING_DDL = """
CREATE TEMP TABLE IF NOT EXISTS trades_staging
    (LIKE trades INCLUDING DEFAULTS) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE IF NOT EXISTS depth_updates_staging
    (LIKE depth_updates INCLUDING DEFAULTS) ON COMMIT DELETE ROWS;
"""

_MERGE_TRADES = f"""
INSERT INTO trades ({", ".join(TRADE_COLUMNS)})
SELECT {", ".join(TRADE_COLUMNS)} FROM trades_staging
ON CONFLICT DO NOTHING
"""

_MERGE_DEPTH = f"""
INSERT INTO depth_updates ({", ".join(DEPTH_COLUMNS)})
SELECT {", ".join(DEPTH_COLUMNS)} FROM depth_updates_staging
ON CONFLICT DO NOTHING
"""

# Connections whose session already has the temp staging tables.
_staged: weakref.WeakSet[psycopg.AsyncConnection] = weakref.WeakSet()


async def flush_copy(
    conn: psycopg.AsyncConnection, trades: list[Trade], depths: list[DepthUpdate]
) -> None:
    """COPY into session-local staging tables, then INSERT ... SELECT ON CONFLICT.

    Keeps COPY's cheap bulk transfer while staying idempotent like the other
    writers. The staging tables are TEMP (no WAL) and ON COMMIT DELETE ROWS,
    so they are empty at the start of every batch.
    """
    if conn not in _staged:
        await conn.execute(_STAGING_DDL)
        await conn.commit()
        _staged.add(conn)
    async with conn.transaction(), conn.cursor() as cur:
        if trades:
            await _copy_rows(
                cur, _copy_sql("trades_staging", TRADE_COLUMNS), [trade_row(t) for t in trades]
            )
            await cur.execute(_MERGE_TRADES)
        if depths:
            await _copy_rows(
                cur,
                _copy_sql("depth_updates_staging", DEPTH_COLUMNS),
                [depth_row(d) for d in depths],
            )
            await cur.execute(_MERGE_DEPTH)
