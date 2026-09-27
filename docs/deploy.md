# Deploying and running tickwatch

Everything runs under Docker Compose: TimescaleDB, the consumer, Prometheus
and Grafana. Every service restarts unless deliberately stopped and rotates
its container logs (5 x 10 MB).

## One-command bring-up

Prerequisites: Docker with Compose v2, and a `.env` (copy `.env.example` and
set the three passwords: `PGPASSWORD`, `GRAFANA_ADMIN_PASSWORD`,
`GRAFANA_DB_PASSWORD`).

```bash
docker compose up -d --build
```

On start the consumer applies the (idempotent) schema, creates the read-only
`grafana_reader` role, then connects to Binance. It reports healthy once a
frame has arrived in the last 60 s.

| What | Where |
|---|---|
| Grafana | http://localhost:3000 (user `admin`, `GRAFANA_ADMIN_PASSWORD`) |
| Prometheus | http://localhost:9090 |
| Consumer metrics | inside the compose network at `consumer:8000/metrics` |
| Postgres | `localhost:5432` (for psql, replay, benchmarks) |

All ports bind to 127.0.0.1. On a remote host, reach them through an SSH
tunnel (`ssh -L 3000:localhost:3000 host`) rather than opening them.

## Everyday commands

```bash
docker compose ps                         # status and health
docker compose logs -f consumer           # live consumer log
docker compose restart consumer           # restart (clean shutdown first)
docker compose stop consumer              # stop; it stays stopped
docker compose up -d --build consumer     # deploy a code change
```

## Behaviour verified 2026-10-03 (on the development Mac)

| Scenario | Result |
|---|---|
| Fresh `up -d --build` | All four services up; consumer healthy in < 45 s; Prometheus target `consumer:8000` up |
| Database restarted under the consumer | Consumer exited (`AdminShutdown`), Docker restarted it, writing again ~5 s after the DB restart. The same event caused a 15-minute outage before this setup (`docs/incidents/2026-10-02-docker-vm-outage.md`). |
| `docker compose stop consumer` | Drained and exited 0 in 2.3 s, inside the 20 s grace period |
| Health check | Healthy with a fresh frame; unhealthy if the last frame is > 60 s old, none has arrived, or the endpoint is down (`tests/test_healthcheck.py`) |

Note: Docker never restarts a container you stop or kill yourself
(`docker kill`, `docker compose stop`); restart policies apply to crashes.
Also, an *unhealthy* container is reported but not restarted by plain
Docker; the consumer's own reconnect loop handles a lost stream.

## Replaying the archive

The live archive is in the `rawdata` volume. To replay it, e.g. into a
scratch database:

```bash
docker compose exec db psql -U tickwatch -c "CREATE DATABASE tickwatch_replay"
docker compose run --rm -e PGDATABASE=tickwatch_replay consumer \
    sh -c "python -m tickwatch.db init && tickwatch-replay /data/raw"
```

Archives recorded before the consumer moved into Compose (2026-10-03
01:11 UTC) are in `./data/raw` on the development Mac; mount both to replay
across the move (the reader orders files chronologically across roots):

```bash
docker compose run --rm -v "$PWD/data/raw:/data/host-raw:ro" -e PGDATABASE=tickwatch_replay \
    consumer tickwatch-replay /data/host-raw /data/raw
```

## On a VPS (not yet done)

Planned for an Ubuntu 24.04 host with ~2 vCPU / 4 GB RAM. This section will
be filled in with what actually happens when it's done.

1. Install Docker Engine and the Compose plugin (Docker's apt repository).
2. `git clone` the repo, create `.env`, `docker compose up -d --build`.
3. Leave all ports on 127.0.0.1; use an SSH tunnel for Grafana.
4. Re-run the Stage 1/2 benchmarks there: the numbers in `benchmarks/` are
   from Docker Desktop on a Mac and won't transfer.
