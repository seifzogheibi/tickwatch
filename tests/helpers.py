"""Shared test data. Real frames and a real archive captured 2026-10-02."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from tickwatch.parse import DepthUpdate

# Real archive captured while a proxy was killed for ~4 s: 1,165 frames
# (841 trades, 324 depth updates) and two '#connected' markers, the second
# followed by one reconnect gap per symbol (BTCUSDT 1,821 and ETHUSDT 771
# missing update IDs).
ARCHIVE = Path(__file__).parent / "fixtures" / "archive"

TRADE_FRAME = (
    '{"stream":"btcusdt@trade","data":{"e":"trade","E":1790960630914,"s":"BTCUSDT",'
    '"t":6732650448,"p":"84922.04000000","q":"0.00023000","T":1790960630913,'
    '"m":true,"M":true}}'
)
DEPTH_FRAME = (
    '{"stream":"ethusdt@depth@100ms","data":{"e":"depthUpdate","E":1790960631015,'
    '"s":"ETHUSDT","U":81705043969,"u":81705044008,'
    '"b":[["2678.74000000","1.20000000"],["2678.50000000","0.00000000"]],'
    '"a":[["2678.75000000","3.10000000"]]}}'
)

T0 = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)


def depth(symbol: str, first: int, final: int, offset_ms: int = 0) -> DepthUpdate:
    t = T0 + timedelta(milliseconds=offset_ms)
    return DepthUpdate(
        time=t,
        symbol=symbol,
        first_update_id=first,
        final_update_id=final,
        bids=[],
        asks=[],
        received_at=t,
    )
