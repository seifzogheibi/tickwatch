"""Append every raw frame to disk as it arrives, before parsing.

Layout: <root>/YYYY-MM-DD/HH.tsv (UTC hour of receipt), one frame per line:

    <received_at as integer ns since epoch>\\t<frame exactly as received>

Binance frames are compact single-line JSON, so tab and newline never occur
unescaped inside one. Lines whose payload starts with "#" are markers written
by tickwatch itself, not frames -- currently only CONNECTED_MARKER, written
each time a websocket connection is established.

When the hour rolls over, the finished file is gzipped in a worker thread.
Writes are buffered and flushed every `flush_every_s`, so a hard crash can
lose at most that much of the archive.
"""

import asyncio
import gzip
import heapq
import itertools
import logging
import shutil
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

log = logging.getLogger("tickwatch.archive")

CONNECTED_MARKER = "#connected"


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


def _hour_key(f: Path) -> tuple[str, str]:
    return (f.parent.name, f.name.split(".")[0])  # (YYYY-MM-DD, HH)


def _archive_files(paths: Iterable[Path]) -> list[Path]:
    files = []
    for p in paths:
        if p.is_dir():
            files.extend(p.rglob("*.tsv"))
            files.extend(p.rglob("*.tsv.gz"))
        else:
            files.append(p)
    # By (date, hour) across all roots, then path so ties are deterministic.
    return sorted(set(files), key=lambda f: (*_hour_key(f), str(f)))


def _parse_ns(ns: int) -> datetime:
    # Integer arithmetic is exact by construction. (ns / 1e9 also round-trips
    # microseconds at current epochs -- checked on 1M random timestamps,
    # 2020-2040 -- but only because fromtimestamp rounds to the nearest us.)
    seconds, rem = divmod(ns, 1_000_000_000)
    return datetime.fromtimestamp(seconds, UTC).replace(microsecond=rem // 1000)


def _read_lines(f: Path) -> Iterator[tuple[int, str]]:
    opener = gzip.open if f.suffix == ".gz" else open
    with opener(f, "rt", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            ns, sep, payload = line.rstrip("\n").partition("\t")
            if not sep or not ns.isdigit() or not payload:
                log.warning("skipping malformed archive line %s:%d", f, lineno)
                continue
            yield int(ns), payload


def iter_archive(paths: Iterable[Path]) -> Iterator[tuple[datetime, str]]:
    """Yield (received_at, payload) for every line of the given files/dirs, in time order.

    A payload is a raw frame or a "#" marker. Lines that aren't
    '<digits>\\t<payload>' -- e.g. a line cut short by a hard crash -- are
    logged and skipped.

    Several roots can hold the same hour: e.g. 01.tsv written on the host
    until a cutover and 01.tsv in a container volume after it. Files for the
    same hour are therefore merged line by line on receive time rather than
    concatenated, so replay never sees time run backwards. Only one hour's
    files are open at a time.
    """
    for _, group in itertools.groupby(_archive_files(paths), key=_hour_key):
        streams = [_read_lines(f) for f in group]
        for ns, payload in heapq.merge(*streams, key=lambda r: r[0]):
            yield _parse_ns(ns), payload
