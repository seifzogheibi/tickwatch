"""Minimal consumer: connect to the Binance combined stream and print every message.

Deliberately naive. No reconnect, no parsing, no persistence -- those come later.
"""

import asyncio
import logging

import websockets

from tickwatch.config import load_settings

log = logging.getLogger("tickwatch.consumer")


async def run() -> None:
    settings = load_settings()
    log.info("connecting to %s", settings.ws_url)
    async with websockets.connect(settings.ws_url) as ws:
        log.info("connected")
        async for raw in ws:
            print(raw, flush=True)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
