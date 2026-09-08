"""Consumer: read the Binance combined stream, parse each message, write it to Postgres.

The socket reader and the database writer run as separate tasks joined by
BatchWriter's bounded queue, so a slow flush no longer stalls the socket
until the queue fills. Still no reconnect, and rows still queued when the
process is killed are lost.
"""

import asyncio
import logging
import time
from datetime import UTC, datetime

import websockets

from tickwatch.config import Settings, load_settings
from tickwatch.db import connect
from tickwatch.parse import parse_message
from tickwatch.writer import BatchWriter, flush_copy

log = logging.getLogger("tickwatch.consumer")

LOG_EVERY_S = 10.0


class _FlushStats:
    def __init__(self) -> None:
        self.rows = 0
        self.flushes = 0

    def __call__(self, rows: int, _seconds: float) -> None:
        self.rows += rows
        self.flushes += 1


async def read_stream(settings: Settings, writer: BatchWriter) -> None:
    log.info("connecting to %s", settings.ws_url)
    async with websockets.connect(settings.ws_url) as ws:
        log.info("connected")
        async for raw in ws:
            item = parse_message(raw, datetime.now(UTC))
            if item is not None:
                await writer.put(item)
    # Socket closed cleanly: let the writer flush what's queued, then stop.
    await writer.close()


async def report(writer: BatchWriter, stats: _FlushStats) -> None:
    last = time.monotonic()
    while True:
        await asyncio.sleep(LOG_EVERY_S)
        now = time.monotonic()
        log.info(
            "wrote %d rows in %d flushes (%.1f rows/s), queue=%d",
            stats.rows,
            stats.flushes,
            stats.rows / (now - last),
            writer.queue.qsize(),
        )
        stats.rows = stats.flushes = 0
        last = now


async def run() -> None:
    settings = load_settings()
    stats = _FlushStats()
    async with await connect(settings) as conn:
        writer = BatchWriter(conn, flush=flush_copy, on_flush=stats)
        async with asyncio.TaskGroup() as tg:
            writer_task = tg.create_task(writer.run())
            reporter = tg.create_task(report(writer, stats))
            tg.create_task(read_stream(settings, writer))
            await writer_task
            reporter.cancel()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
