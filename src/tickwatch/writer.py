"""Persist parsed records to Postgres."""

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
