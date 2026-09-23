"""Replay archived raw frames through the live pipeline, deterministically.

Usage: tickwatch-replay data/raw/2026-10-02 [more files or dirs ...]

Frames go through the same Pipeline (parse -> gap check -> BatchWriter ->
COPY) as the live consumer, with each frame's *recorded* receive time, so the
same archive always produces the same rows and the same gaps. Writes are
idempotent, so replaying into a database that already holds the data adds
nothing. Target database comes from the usual PG* settings.
"""

import argparse
import asyncio
import logging
import time
from collections.abc import Iterable
from pathlib import Path

from tickwatch.archive import CONNECTED_MARKER, iter_archive
from tickwatch.config import load_settings
from tickwatch.db import connect
from tickwatch.pipeline import Pipeline, Stats
from tickwatch.writer import BatchWriter, flush_copy

log = logging.getLogger("tickwatch.replay")


async def replay(paths: Iterable[Path], stats: Stats) -> None:
    settings = load_settings()
    async with await connect(settings) as conn:
        writer = BatchWriter(conn, flush=flush_copy, on_flush=stats.on_flush)
        pipeline = Pipeline(writer, stats)  # no archive: the input already is one
        async with asyncio.TaskGroup() as tg:
            writer_task = tg.create_task(writer.run())
            for received_at, payload in iter_archive(paths):
                if payload == CONNECTED_MARKER:
                    pipeline.on_connected(received_at)
                elif not payload.startswith("#"):
                    await pipeline.on_frame(received_at, payload)
            await writer.close()
            await writer_task


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("paths", nargs="+", type=Path, help="archive files or directories")
    args = p.parse_args()
    stats = Stats()
    t0 = time.perf_counter()
    asyncio.run(replay(args.paths, stats))
    elapsed = time.perf_counter() - t0
    log.info(
        "replayed %d frames in %.1fs (%.0f frames/s): %d rows submitted in %d flushes "
        "(duplicates skipped), gaps=%d parse_errors=%d flags=%s",
        stats.frames,
        elapsed,
        stats.frames / elapsed if elapsed else 0,
        stats.rows,
        stats.flushes,
        stats.gaps,
        stats.parse_errors,
        stats.flags,
    )


if __name__ == "__main__":
    main()
