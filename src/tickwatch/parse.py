"""Parse Binance combined-stream messages into typed records."""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation


@dataclass(frozen=True, slots=True)
class Trade:
    time: datetime  # trade time (T)
    symbol: str
    trade_id: int
    price: Decimal
    qty: Decimal
    buyer_is_maker: bool
    event_time: datetime  # event time (E)
    received_at: datetime


@dataclass(frozen=True, slots=True)
class DepthUpdate:
    time: datetime  # event time (E)
    symbol: str
    first_update_id: int  # U
    final_update_id: int  # u
    bids: list[list[str]]  # [[price, qty], ...]; qty "0" removes the level
    asks: list[list[str]]
    received_at: datetime


# Everything parse_message raises on a malformed frame: bad JSON
# (ValueError), missing fields (KeyError), wrong shapes (TypeError), and
# non-numeric prices/quantities (decimal.InvalidOperation, which is an
# ArithmeticError, not a ValueError). Callers that skip bad frames catch this.
PARSE_ERRORS = (ValueError, KeyError, TypeError, InvalidOperation)


def _ms(ts: int) -> datetime:
    return datetime.fromtimestamp(ts / 1000, tz=UTC)


def parse_message(raw: str | bytes, received_at: datetime) -> Trade | DepthUpdate | None:
    """Parse one combined-stream frame. Returns None for event types we don't store."""
    data = json.loads(raw)["data"]
    match data["e"]:
        case "trade":
            return Trade(
                time=_ms(data["T"]),
                symbol=data["s"],
                trade_id=data["t"],
                price=Decimal(data["p"]),
                qty=Decimal(data["q"]),
                buyer_is_maker=data["m"],
                event_time=_ms(data["E"]),
                received_at=received_at,
            )
        case "depthUpdate":
            return DepthUpdate(
                time=_ms(data["E"]),
                symbol=data["s"],
                first_update_id=data["U"],
                final_update_id=data["u"],
                bids=data["b"],
                asks=data["a"],
                received_at=received_at,
            )
        case _:
            return None
