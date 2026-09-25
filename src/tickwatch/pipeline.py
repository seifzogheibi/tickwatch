"""What happens to a frame after it is received: shared by live and replay.

The live consumer and the replay harness both drive a Pipeline with the same
two calls -- `on_connected` and `on_frame` -- so replay exercises exactly the
code that processed the data live. Only the source of frames differs.
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime

from tickwatch import metrics
from tickwatch.archive import CONNECTED_MARKER, RawArchive
from tickwatch.detectors import IsolationForestDetector, ZScoreDetector
from tickwatch.features import FeatureBuilder
from tickwatch.gaps import GapDetector
from tickwatch.parse import PARSE_ERRORS, BookTicker, DepthUpdate, Trade, parse_message
from tickwatch.writer import BatchWriter

log = logging.getLogger("tickwatch.pipeline")

_STREAM_LABEL = {Trade: "trade", DepthUpdate: "depth", BookTicker: "bookticker"}


@dataclass
class Stats:
    rows: int = 0  # rows flushed since the last report
    flushes: int = 0
    flags: dict[str, int] = field(default_factory=dict)  # cumulative, per detector
    frames: int = 0  # cumulative frames handled
    disconnects: int = 0  # established connections lost, cumulative
    reconnect_attempts: int = 0  # includes failed attempts during an outage
    gaps: int = 0
    parse_errors: int = 0

    def on_flush(self, rows: int, _seconds: float) -> None:
        self.rows += rows
        self.flushes += 1


class Pipeline:
    def __init__(
        self, writer: BatchWriter, stats: Stats, archive: RawArchive | None = None
    ) -> None:
        self.writer = writer
        self.stats = stats
        # Live passes the archive; replay doesn't (its input *is* the archive).
        self.archive = archive
        self.detector = GapDetector()
        self.features = FeatureBuilder()
        self.detectors = [ZScoreDetector(), IsolationForestDetector()]
        self._connected_once = False

    def on_connected(self, at: datetime) -> None:
        """A websocket connection was (re-)established at `at`."""
        if self.archive is not None:
            # Recorded so replay can tell a reconnect gap from a stream gap.
            self.archive.write(at, CONNECTED_MARKER)
        if self._connected_once:
            self.detector.mark_reconnect()
        self._connected_once = True

    async def on_frame(self, received_at: datetime, raw: str | bytes) -> None:
        self.stats.frames += 1
        if self.archive is not None:
            self.archive.write(received_at, raw)
        try:
            item = parse_message(raw, received_at)
        except PARSE_ERRORS:
            # The frame is already archived; skip it rather than take the
            # pipeline down over one malformed message.
            self.stats.parse_errors += 1
            metrics.PARSE_ERRORS.inc()
            metrics.FRAMES.labels("other").inc()
            log.exception("unparseable frame: %.200r", raw)
            return
        metrics.FRAMES.labels(_STREAM_LABEL.get(type(item), "other")).inc()
        if isinstance(item, DepthUpdate) and (gap := self.detector.check(item)):
            self.stats.gaps += 1
            metrics.GAPS.labels(gap.symbol, gap.cause).inc()
            log.warning(
                "depth gap %s cause=%s missing_update_ids=%d (u=%d -> U=%d)",
                gap.symbol,
                gap.cause,
                gap.missing_update_ids,
                gap.prev_final_update_id,
                gap.first_update_id,
            )
            await self.writer.put(gap)
        if isinstance(item, Trade | BookTicker):
            for f in self.features.on_event(item):
                await self.writer.put(f)
                for detector in self.detectors:
                    t0 = time.perf_counter()
                    flag = await detector.score(f)
                    metrics.DETECTOR_SECONDS.labels(detector.name).observe(time.perf_counter() - t0)
                    if flag:
                        self.stats.flags[flag.detector] = self.stats.flags.get(flag.detector, 0) + 1
                        metrics.FLAGS.labels(flag.symbol, flag.detector).inc()
                        await self.writer.put(flag)
        if isinstance(item, Trade | DepthUpdate):
            # Quotes (BookTicker) aren't stored row-by-row: ~90/s for two
            # symbols, and the raw archive already keeps every one.
            await self.writer.put(item)
