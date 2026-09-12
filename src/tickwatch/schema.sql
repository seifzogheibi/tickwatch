-- tickwatch schema. Idempotent: safe to run against an existing database.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- One row per executed trade.
-- `time` is the exchange's trade time (T); `event_time` is when Binance emitted
-- the event (E); `received_at` is when our consumer read it off the socket.
-- Prices and quantities are numeric (exact decimal) -- Binance sends them as
-- strings precisely so clients don't lose precision to floats.
CREATE TABLE IF NOT EXISTS trades (
    time            timestamptz NOT NULL,
    symbol          text        NOT NULL,
    trade_id        bigint      NOT NULL,
    price           numeric     NOT NULL,
    qty             numeric     NOT NULL,
    buyer_is_maker  boolean     NOT NULL,
    event_time      timestamptz NOT NULL,
    received_at     timestamptz NOT NULL,
    -- Hypertable unique constraints must include the partitioning column.
    PRIMARY KEY (symbol, trade_id, time)
);

SELECT create_hypertable('trades', by_range('time', INTERVAL '1 day'), if_not_exists => TRUE);

-- Per-symbol, newest-first. Serves "latest trade per symbol" (DISTINCT ON
-- symbol) via TimescaleDB SkipScan: one index probe per symbol instead of
-- reading and sorting the whole table. The PK leads with symbol too, but it
-- orders by trade_id, so it can't return rows newest-first by time.
CREATE INDEX IF NOT EXISTS trades_symbol_time_idx ON trades (symbol, time DESC);

-- One row per diff-depth message. Bid/ask changes stay together as JSONB
-- arrays of [price, qty] strings: the message, not the price level, is the
-- unit we reason about (U/u sequencing, gap detection).
CREATE TABLE IF NOT EXISTS depth_updates (
    time             timestamptz NOT NULL,
    symbol           text        NOT NULL,
    first_update_id  bigint      NOT NULL,
    final_update_id  bigint      NOT NULL,
    bids             jsonb       NOT NULL,
    asks             jsonb       NOT NULL,
    received_at      timestamptz NOT NULL,
    PRIMARY KEY (symbol, final_update_id, time)
);

SELECT create_hypertable('depth_updates', by_range('time', INTERVAL '1 day'), if_not_exists => TRUE);
