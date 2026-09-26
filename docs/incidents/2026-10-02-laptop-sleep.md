# 2026-10-02/03: 2 h 14 min ingestion outage (development laptop slept)

**Impact:** no data from 21:49:40 to 00:03:52 UTC (2 h 14 min), both symbols.
Not in the raw archive either, so unrecoverable. Second outage of the day;
see also `2026-10-02-docker-vm-outage.md`.

**Environment:** the consumer was running on a MacBook, by hand, while the
deployment host doesn't exist yet.

## Timeline (UTC)

| Time | Event |
|---|---|
| 21:31:20 | Consumer restarted after the Docker VM outage. |
| 21:49:40 | Last feature bucket written. |
| 21:51:42 | `ConnectionClosedError: keepalive ping timeout` -- websockets' ping/pong caught the dead connection. |
| 21:51:43 | Reconnect attempts begin failing with `gaierror: nodename nor servname provided`: DNS gone, i.e. no network. |
| 21:51-23:31 | Report lines that should be 10 s apart jump by 3, 17 and 18 minutes: the machine was asleep. Backoff kept retrying whenever it was awake (attempts 1-8, delays 0.5-29 s). |
| ~00:01 | Consumer process stopped by the session harness's 2-hour limit on background commands. |
| 00:03:52 | Consumer restarted by hand; connected first time. |

## Root cause

The host went to sleep. A laptop is not a 24/7 host.

## What worked

The Stage 3 resilience code behaved exactly as designed against a real
network loss, not a simulated one: keepalive detected the half-open
connection, DNS failures were treated as retryable connection errors, and
backoff stayed bounded. It would have reconnected within seconds of the
network returning.

## Follow-ups

- Stage 7: run on an always-on host under Compose with a restart policy.
- Until then, data collected on the laptop is not a basis for long-window
  analysis. The Stage 5 detector comparison needs >= 3 h of continuous data
  (1 h Isolation Forest warm-up, then the comparison window); it will be
  produced from the deployment host instead.
