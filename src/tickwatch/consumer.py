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
from dataclasses import dataclass
from datetime import UTC, datetime

import websockets

from tickwatch.archive import RawArchive
from tickwatch.backoff import Backoff
from tickwatch.config import Settings, load_settings
from tickwatch.db import connect
from tickwatch.gaps import GapDetector
from tickwatch.parse import DepthUpdate, parse_message
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


@dataclass
class Stats:
    rows: int = 0  # rows flushed since the last report
    flushes: int = 0
    disconnects: int = 0  # established connections lost, cumulative
    reconnect_attempts: int = 0  # includes failed attempts during an outage
    gaps: int = 0
    parse_errors: int = 0

    def on_flush(self, rows: int, _seconds: float) -> None:
        self.rows += rows
        self.flushes += 1


async def read_stream(
    settings: Settings,
    writer: BatchWriter,
    archive: RawArchive,
    stats: Stats,
    backoff: Backoff | None = None,
) -> None:
    backoff = backoff or Backoff()
    detector = GapDetector()
    connected_once = False
    while True:
        was_connected = False
        try:
            log.info("connecting to %s", settings.ws_url)
            async with websockets.connect(settings.ws_url, close_timeout=CLOSE_TIMEOUT_S) as ws:
                log.info("connected")
                if connected_once:
                    detector.mark_reconnect()
                connected_once = True
                connected_at = time.monotonic()
                # Set once a connection is up, so the except/close paths below
                # count a lost connection once, not once per failed retry.
                was_connected = True
                async for raw in ws:
                    received_at = datetime.now(UTC)
                    archive.write(received_at, raw)
                    try:
                        item = parse_message(raw, received_at)
                    except (ValueError, KeyError, TypeError):
                        # The frame is already archived; skip it rather than
                        # take the pipeline down over one malformed message.
                        stats.parse_errors += 1
                        log.exception("unparseable frame: %.200r", raw)
                        continue
                    if isinstance(item, DepthUpdate) and (gap := detector.check(item)):
                        stats.gaps += 1
                        log.warning(
                            "depth gap %s cause=%s missing_update_ids=%d (u=%d -> U=%d)",
                            gap.symbol,
                            gap.cause,
                            gap.missing_update_ids,
                            gap.prev_final_update_id,
                            gap.first_update_id,
                        )
                        await writer.put(gap)
                    if item is not None:
                        await writer.put(item)
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
            "disconnects=%d reconnect_attempts=%d gaps=%d parse_errors=%d",
            stats.rows,
            stats.flushes,
            stats.rows / (now - last),
            writer.queue.qsize(),
            stats.disconnects,
            stats.reconnect_attempts,
            stats.gaps,
            stats.parse_errors,
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
                    tg.create_task(read_stream(settings, writer, archive, stats)),
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
