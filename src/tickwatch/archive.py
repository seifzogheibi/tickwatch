"""Append every raw frame to disk as it arrives, before parsing.

Layout: <root>/YYYY-MM-DD/HH.tsv (UTC hour of receipt), one frame per line:

    <received_at as integer ns since epoch>\\t<frame exactly as received>

Binance frames are compact single-line JSON, so tab and newline never occur
unescaped inside one. When the hour rolls over, the finished file is gzipped
in a worker thread. Writes are buffered and flushed every `flush_every_s`,
so a hard crash can lose at most that much of the archive.
"""

import asyncio
import gzip
import logging
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

log = logging.getLogger("tickwatch.archive")


def _hour_path(root: Path, t: datetime) -> Path:
    return root / t.strftime("%Y-%m-%d") / t.strftime("%H.tsv")


def _gzip_and_remove(path: Path) -> None:
    gz = path.with_suffix(path.suffix + ".gz")
    tmp = gz.with_suffix(".gz.tmp")
    with path.open("rb") as src, gzip.open(tmp, "wb") as dst:
        shutil.copyfileobj(src, dst)
    tmp.rename(gz)  # atomic: a .gz never exists half-written
    path.unlink()


class RawArchive:
    def __init__(self, root: Path, flush_every_s: float = 1.0) -> None:
        self.root = root
        self.flush_every_s = flush_every_s
        self._path: Path | None = None
        self._file: TextIO | None = None
        self._compressions: set[asyncio.Task] = set()

    def write(self, received_at: datetime, frame: str | bytes) -> None:
        path = _hour_path(self.root, received_at)
        if path != self._path:
            self._rotate(path)
        if isinstance(frame, bytes):
            frame = frame.decode()
        ns = int(received_at.timestamp()) * 1_000_000_000 + received_at.microsecond * 1000
        self._file.write(f"{ns}\t{frame}\n")

    def _rotate(self, path: Path) -> None:
        finished = self._path
        if self._file is not None:
            self._file.close()
        path.parent.mkdir(parents=True, exist_ok=True)
        # Append: restarting within the same hour continues the same file.
        self._file = path.open("a", encoding="utf-8")
        self._path = path
        if finished is not None:
            self._compress_later(finished)

    def _compress_later(self, path: Path) -> None:
        task = asyncio.get_running_loop().create_task(asyncio.to_thread(_gzip_and_remove, path))
        self._compressions.add(task)
        task.add_done_callback(self._compression_done)

    def _compression_done(self, task: asyncio.Task) -> None:
        self._compressions.discard(task)
        if not task.cancelled() and task.exception() is not None:
            log.error("archive compression failed", exc_info=task.exception())

    def compress_stale(self) -> None:
        """Compress finished hours left uncompressed, e.g. by a crash. Call at startup."""
        current = _hour_path(self.root, datetime.now(UTC))
        for path in sorted(self.root.glob("*/*.tsv")):
            if path != current:
                self._compress_later(path)

    async def flush_periodically(self) -> None:
        while True:
            await asyncio.sleep(self.flush_every_s)
            if self._file is not None:
                self._file.flush()

    async def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None
        if self._compressions:
            await asyncio.gather(*self._compressions, return_exceptions=True)
