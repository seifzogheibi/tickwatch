"""Detect sequence breaks in the Binance diff-depth stream.

Binance's rule for a diff-depth stream: each event's first update ID `U`
must equal the previous event's final update ID `u` + 1. Anything else
means the local order book can no longer be trusted from diffs alone.
"""

from dataclasses import dataclass
from datetime import datetime

from tickwatch.parse import DepthUpdate


@dataclass(frozen=True, slots=True)
class Gap:
    time: datetime  # event time of the update that revealed the gap
    symbol: str
    prev_final_update_id: int  # u of the last update before the break
    first_update_id: int  # U of the update after it
    # Positive: update IDs skipped (U - prev_u - 1). Zero or negative: the new
    # update overlaps or repeats IDs we've already seen.
    missing_update_ids: int
    # "stream": broke mid-connection. "reconnect": first update after we
    # reconnected, so the gap spans the time we were disconnected.
    cause: str
    received_at: datetime


class GapDetector:
    def __init__(self) -> None:
        self._last_final: dict[str, int] = {}
        self._after_reconnect: set[str] = set()

    def mark_reconnect(self) -> None:
        """Call on reconnect: the next break per symbol is attributed to it."""
        self._after_reconnect = set(self._last_final)

    def check(self, update: DepthUpdate) -> Gap | None:
        prev = self._last_final.get(update.symbol)
        self._last_final[update.symbol] = update.final_update_id
        reconnected = update.symbol in self._after_reconnect
        self._after_reconnect.discard(update.symbol)
        if prev is None or update.first_update_id == prev + 1:
            return None
        return Gap(
            time=update.time,
            symbol=update.symbol,
            prev_final_update_id=prev,
            first_update_id=update.first_update_id,
            missing_update_ids=update.first_update_id - prev - 1,
            cause="reconnect" if reconnected else "stream",
            received_at=update.received_at,
        )
