"""Runtime settings, read from the environment (and a local .env file if present)."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True, slots=True)
class Settings:
    binance_ws_base: str
    symbols: tuple[str, ...]
    depth_interval_ms: int
    pg_host: str
    pg_port: int
    pg_user: str
    pg_password: str
    pg_db: str

    @property
    def streams(self) -> list[str]:
        """Combined-stream names: one trade and one diff-depth stream per symbol."""
        out = []
        for s in self.symbols:
            out.append(f"{s}@trade")
            out.append(f"{s}@depth@{self.depth_interval_ms}ms")
        return out

    @property
    def ws_url(self) -> str:
        return f"{self.binance_ws_base}/stream?streams={'/'.join(self.streams)}"

    @property
    def pg_dsn(self) -> str:
        return (
            f"host={self.pg_host} port={self.pg_port} user={self.pg_user} "
            f"password={self.pg_password} dbname={self.pg_db}"
        )


def load_settings() -> Settings:
    load_dotenv()
    symbols = tuple(
        s.strip().lower() for s in os.environ.get("SYMBOLS", "btcusdt,ethusdt").split(",") if s
    )
    if not symbols:
        raise ValueError("SYMBOLS must name at least one symbol")
    return Settings(
        binance_ws_base=os.environ.get("BINANCE_WS_BASE", "wss://stream.binance.com:9443"),
        symbols=symbols,
        depth_interval_ms=int(os.environ.get("DEPTH_INTERVAL_MS", "100")),
        pg_host=os.environ.get("PGHOST", "localhost"),
        pg_port=int(os.environ.get("PGPORT", "5432")),
        pg_user=os.environ.get("PGUSER", "tickwatch"),
        pg_password=os.environ["PGPASSWORD"],
        pg_db=os.environ.get("PGDATABASE", "tickwatch"),
    )
