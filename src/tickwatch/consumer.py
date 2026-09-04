"""Consumer: read the Binance combined stream, parse each message, write it to Postgres.

Still naive: no reconnect, and each row is written synchronously before the
next message is read, so a slow database stalls the socket.
"""

import asyncio
import logging
import time
from datetime import UTC, datetime

import websockets

from tickwatch.config import load_settings
from tickwatch.db import connect
from tickwatch.parse import parse_message
from tickwatch.writer import NaiveWriter

log = logging.getLogger("tickwatch.consumer")

LOG_EVERY_S = 10.0


async def run() -> None:
    settings = load_settings()
    async with await connect(settings) as conn:
        writer = NaiveWriter(conn)
        log.info("connecting to %s", settings.ws_url)
        async with websockets.connect(settings.ws_url) as ws:
            log.info("connected")
            written = 0
            last_log = time.monotonic()
            async for raw in ws:
                item = parse_message(raw, datetime.now(UTC))
                if item is None:
                    continue
                await writer.write(item)
                written += 1
                now = time.monotonic()
                if now - last_log >= LOG_EVERY_S:
                    log.info("wrote %d rows (%.1f/s)", written, written / (now - last_log))
                    written = 0
                    last_log = now


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
