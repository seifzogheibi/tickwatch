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

## A database outage stops ingestion, including the archive

If a flush fails (Postgres down or restarting), `BatchWriter.run` raises and
the TaskGroup stops the whole consumer (`psycopg.errors.AdminShutdown`, exit
code 1, ~2 s later). Under Compose, Docker restarts it: a database restart
now costs ~5 s (tested 2026-10-03). But for as long as the database stays
down, the consumer crash-loops and *receives nothing* -- the raw archive
stops too, so the window is unrecoverable. That is what turned a Docker VM
failure into a 15-minute hole on 2026-10-02 (`incidents/`).

**Fix:** retry flushes with backoff while the bounded queue absorbs the
backlog, and keep archiving regardless of database health so the window can
be replayed in afterwards.

## Raw archive has no retention

With trades, depth and quotes for two symbols, measured at 01:31 UTC on
2026-10-03 (a quiet night): ~127 MB/hour uncompressed, 3.0 GB/day; gzip cuts
it 9.2x, to ~330 MB/day. Busier markets will be higher. Nothing deletes old
hours yet.

## Up to ~1 s of archive can be lost on a hard crash

Archive writes are buffered and flushed every second. A clean shutdown
(SIGTERM/SIGINT) flushes everything; `kill -9`, a kernel panic or power loss
can lose the last second.

## Detector state is lost on every restart

Both detectors keep their state in memory: the z-score's 5-minute window and
the Isolation Forest's model. After any restart the z-score is silent for 60 s
and the forest for 1 hour (3,630 scoreable buckets) while they rebuild. The
replay harness sidesteps this for analysis -- it rebuilds state from the
archive -- but live flags have holes after each restart.

**Fix:** persist the forest model and z-score window on shutdown (or rebuild
them at startup by replaying the last hour of archive).

## Not deployed

So far it has run only on a development laptop, which slept for 2 h 14 min
on 2026-10-02 (`incidents/2026-10-02-laptop-sleep.md`). The Compose setup is
ready for an always-on host (`deploy.md`) but has not been run on one, and the
benchmarks in `benchmarks/` are from Docker Desktop on a Mac.
