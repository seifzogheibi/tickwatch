"""Persist parsed records to Postgres."""

import psycopg
from psycopg.types.json import Jsonb

from tickwatch.parse import DepthUpdate, Trade

# ON CONFLICT DO NOTHING: a trade or depth update we've already stored
# (e.g. resent after a reconnect) is skipped rather than aborting the write.
INSERT_TRADE = """
INSERT INTO trades (time, symbol, trade_id, price, qty, buyer_is_maker, event_time, received_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""

INSERT_DEPTH = """
INSERT INTO depth_updates
    (time, symbol, first_update_id, final_update_id, bids, asks, received_at)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT DO NOTHING
"""


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
                await cur.execute(
                    INSERT_TRADE,
                    (
                        item.time,
                        item.symbol,
                        item.trade_id,
                        item.price,
                        item.qty,
                        item.buyer_is_maker,
                        item.event_time,
                        item.received_at,
                    ),
                )
            else:
                await cur.execute(
                    INSERT_DEPTH,
                    (
                        item.time,
                        item.symbol,
                        item.first_update_id,
                        item.final_update_id,
                        Jsonb(item.bids),
                        Jsonb(item.asks),
                        item.received_at,
                    ),
                )
        await self.conn.commit()
