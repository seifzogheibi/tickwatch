# Known limitations

Things we know are wrong or missing, found by testing, with what it would take
to fix them. Kept current; items move out when fixed.

## Every reconnect loses depth updates, including Binance's daily close

A new connection's first depth event starts at the *current* update ID; the
stream does not replay what was missed. Measured 2026-10-02 through a proxy
that closes cleanly after 5 s: reconnecting in ~0.3-0.6 s still skipped
690-2,052 update IDs per symbol. Binance closes every connection after 24 h,
so this happens at least once a day even with a perfect network.

Each one is recorded in `depth_gaps` with `cause = 'reconnect'`, so the loss
is visible, never silent.

**Fix:** open the replacement connection a few minutes *before* the 24 h
mark, run both until the new one's sequence overlaps the old one's, then drop
the old connection and dedupe by update ID. That removes the scheduled gap;
unscheduled drops would still need a REST order-book snapshot to resync.

## A database outage stops the consumer

If a flush fails (Postgres down or restarting), `BatchWriter.run` raises and
the TaskGroup stops the whole consumer. Tested 2026-10-02 by stopping the
database container: `psycopg.errors.AdminShutdown`, exit code 1, ~2 s later.
Reconnect logic covers the websocket only. Frames received up to the crash
are in the raw archive, so the data isn't lost and can be replayed in, but
live ingestion stops until restarted.

**Fix:** retry flushes with backoff while the bounded queue absorbs the
backlog; once it is full, backpressure stalls the socket (and Binance will
eventually drop us, which the reconnect path already handles).

## Raw archive has no retention

At 2026-10-02 rates the archive grows ~150 MB/hour uncompressed for two
symbols (37 MB per 15 minutes of recording); gzip cuts that ~10x, to roughly
350 MB/day. Volatile markets will be higher. Nothing deletes old hours yet.

## Up to ~1 s of archive can be lost on a hard crash

Archive writes are buffered and flushed every second. A clean shutdown
(SIGTERM/SIGINT) flushes everything; `kill -9`, a kernel panic or power loss
can lose the last second.
