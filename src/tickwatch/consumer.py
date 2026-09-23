"""Consumer: read the Binance combined stream, archive it, parse it, write it to Postgres.

The socket reader and the database writer run as separate tasks joined by
BatchWriter's bounded queue. The reader reconnects forever with jittered
exponential backoff: connections drop on network blips, and Binance closes
every connection after 24 hours regardless.
"""

import asyncio
import logging
import signal
import time
from datetime import UTC, datetime

import websockets

from tickwatch.archive import RawArchive
from tickwatch.backoff import Backoff
from tickwatch.config import Settings, load_settings
from tickwatch.db import connect
from tickwatch.pipeline import Pipeline, Stats
from tickwatch.writer import BatchWriter, flush_copy

log = logging.getLogger("tickwatch.consumer")

LOG_EVERY_S = 10.0
# A connection that has stayed up this long resets the backoff schedule.
HEALTHY_AFTER_S = 60.0
# Binance doesn't answer a client's CLOSE frame, so websockets would wait its
# default 10 s close_timeout on every shutdown -- as long as Docker's stop grace
# period. Measured 2026-10-02: CLOSE sent, EOF only after exactly 10.0 s.
CLOSE_TIMEOUT_S = 2.0
# Errors that mean "the connection is gone, try again", as opposed to bugs.
CONNECTION_ERRORS = (websockets.WebSocketException, OSError, TimeoutError)


async def read_stream(
    settings: Settings, pipeline: Pipeline, stats: Stats, backoff: Backoff | None = None
) -> None:
    backoff = backoff or Backoff()
    while True:
        was_connected = False
        try:
            log.info("connecting to %s", settings.ws_url)
            async with websockets.connect(settings.ws_url, close_timeout=CLOSE_TIMEOUT_S) as ws:
                log.info("connected")
                pipeline.on_connected(datetime.now(UTC))
                connected_at = time.monotonic()
                # Set once a connection is up, so the except/close paths below
                # count a lost connection once, not once per failed retry.
                was_connected = True
                async for raw in ws:
                    await pipeline.on_frame(datetime.now(UTC), raw)
                    if backoff.attempt and time.monotonic() - connected_at > HEALTHY_AFTER_S:
                        backoff.reset()
            log.warning("connection closed by server")
        except CONNECTION_ERRORS as e:
            log.warning("connection lost: %s: %s", type(e).__name__, e)
        if was_connected:
            stats.disconnects += 1
        stats.reconnect_attempts += 1
        delay = backoff.next_delay()
        log.info("reconnect attempt #%d in %.1fs", stats.reconnect_attempts, delay)
        await asyncio.sleep(delay)


async def report(writer: BatchWriter, stats: Stats) -> None:
    last = time.monotonic()
    while True:
        await asyncio.sleep(LOG_EVERY_S)
        now = time.monotonic()
        log.info(
            "wrote %d rows in %d flushes (%.1f rows/s), queue=%d, "
            "disconnects=%d reconnect_attempts=%d gaps=%d parse_errors=%d flags=%s",
            stats.rows,
            stats.flushes,
            stats.rows / (now - last),
            writer.queue.qsize(),
            stats.disconnects,
            stats.reconnect_attempts,
            stats.gaps,
            stats.parse_errors,
            stats.flags,
        )
        stats.rows = stats.flushes = 0
        last = now


async def run() -> None:
    settings = load_settings()
    stats = Stats()
    archive = RawArchive(settings.raw_dir)
    archive.compress_stale()

    # SIGTERM (docker stop, systemd) and Ctrl-C both mean "stop cleanly":
    # stop reading, flush what's queued, close the archive.
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    try:
        async with await connect(settings) as conn:
            writer = BatchWriter(conn, flush=flush_copy, on_flush=stats.on_flush)
            async with asyncio.TaskGroup() as tg:
                writer_task = tg.create_task(writer.run())
                others = [
                    tg.create_task(report(writer, stats)),
                    tg.create_task(archive.flush_periodically()),
                    tg.create_task(read_stream(settings, Pipeline(writer, stats, archive), stats)),
                ]
                await stop.wait()
                log.info("stopping: flushing %d queued items", writer.queue.qsize())
                for t in others:
                    t.cancel()
                await writer.close()
                await writer_task
    finally:
        await archive.close()
    log.info(
        "stopped cleanly: disconnects=%d reconnect_attempts=%d gaps=%d parse_errors=%d",
        stats.disconnects,
        stats.reconnect_attempts,
        stats.gaps,
        stats.parse_errors,
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
