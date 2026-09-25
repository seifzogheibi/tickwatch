# 2026-10-02: 15-minute ingestion outage (Docker Desktop VM unreachable)

**Impact:** no data collected from 21:15:55 to 21:31:20 UTC (15 min 25 s), for
both symbols. Those frames were never received, so they are not in the raw
archive either and cannot be replayed. No stored data was lost: the database
volume was intact afterwards (1,078,150 trades, all features and flags).

**Environment:** development laptop, Docker Desktop 29.8.1 on macOS. Not yet
the deployment host.

## Timeline (UTC)

| Time | Event |
|---|---|
| ~21:11 | `docker compose up -d` run to add Prometheus and Grafana. The db service's image tag had changed from `latest-pg16` to the identical `2.30.2-pg16`, so Compose recreated the db container. |
| 21:15:55 | Last feature bucket written. The consumer lost its DB connection and exited (code 1), as documented in `known-limitations.md`. |
| ~21:16-21:30 | All three containers stuck in `Created`; `docker compose up` hung; even `docker run alpine` timed out. Docker's backend log: `connect tcp 192.168.65.7:2376: no route to host`, guest services refusing connections -- the Linux VM was unreachable while the Desktop app stayed up (so `quit app "Docker"` had no effect). |
| 21:30:55 | `docker desktop restart`; engine up immediately, test container ran. |
| 21:31:20 | Stack up, db healthy, consumer restarted; data flowing again. |

## Root cause

Not determined. The VM became unreachable during the `compose up` that
recreated the db container, but the logs don't show why, and correlation
isn't cause. Disk was not the issue (270 GB free; Docker using 4.6 GB images,
1.0 GB volumes).

## What made it worse

1. **The consumer exits when the database goes away** rather than retrying.
   With the DB down for ~15 minutes, it would have lost data anyway (the queue
   holds ~10,000 rows), but it also stopped archiving, so even the raw frames
   for the window are gone.
2. **Nothing restarted anything.** The consumer runs by hand on the host.
3. **No alerting.** The outage was found by checking, not by being told.

## Follow-ups

- Stage 7: run the consumer under Compose with `restart: unless-stopped` and a
  health check, so a dead process comes back on its own.
- Keep archiving while the database is unavailable: decouple the archive from
  DB health so raw frames survive a DB outage and can be replayed in later
  (see `known-limitations.md`, "A database outage stops the consumer").
- Alert on `time() - tickwatch_last_frame_timestamp_seconds > 60` once alerting
  exists; the dashboard's "Seconds since last frame" panel shows it today.
- Changing an image tag recreates the container even when the image is
  byte-identical. Do tag changes deliberately, with the consumer stopped first.
