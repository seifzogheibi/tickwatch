"""Replay the real archive fixture into a throwaway Postgres database.

Needs the compose database running (docker compose up -d); skipped otherwise.
"""

import asyncio
import hashlib
from collections.abc import Iterator
from dataclasses import dataclass

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from tickwatch.archive import iter_archive
from tickwatch.config import load_settings
from tickwatch.db import apply_schema
from tickwatch.pipeline import Stats
from tickwatch.replay import replay

from .helpers import ARCHIVE

pytestmark = pytest.mark.integration

TEST_DB = "tickwatch_test"

# Whole-table contents in a fixed order, for hashing.
TABLES = [
    "trades ORDER BY symbol, trade_id",
    "depth_updates ORDER BY symbol, final_update_id",
    "depth_gaps ORDER BY symbol, first_update_id",
    "features_1s ORDER BY symbol, time",
    "anomaly_flags ORDER BY symbol, detector, time",
]


def _admin_dsn() -> str:
    try:
        dsn = load_settings().pg_dsn
        psycopg.connect(dsn, connect_timeout=2).close()
    except (KeyError, psycopg.OperationalError) as e:
        pytest.skip(f"Postgres not available: {e}")
    return dsn


def _recreate(admin_dsn: str) -> str:
    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        conn.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TEST_DB))
        )
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEST_DB)))
    dsn = make_conninfo(admin_dsn, dbname=TEST_DB)

    async def schema() -> None:
        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            await apply_schema(conn)

    asyncio.run(schema())
    return dsn


def _content_hash(dsn: str) -> str:
    h = hashlib.sha256()
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        for t in TABLES:
            with cur.copy(f"COPY (SELECT * FROM {t}) TO STDOUT") as copy:
                for chunk in copy:
                    h.update(chunk)
    return h.hexdigest()


def _replay() -> Stats:
    stats = Stats()
    asyncio.run(replay([ARCHIVE], stats))
    return stats


@dataclass(frozen=True)
class ScratchDB:
    dsn: str
    admin_dsn: str  # captured before PGDATABASE is pointed at the test DB


@pytest.fixture
def test_db(monkeypatch: pytest.MonkeyPatch) -> Iterator[ScratchDB]:
    admin = _admin_dsn()
    db = ScratchDB(dsn=_recreate(admin), admin_dsn=admin)
    # replay() reads its target from the environment, like the CLI does.
    monkeypatch.setenv("PGDATABASE", TEST_DB)
    yield db
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute(
            sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TEST_DB))
        )


def test_replay_stores_every_frame_and_the_reconnect_gaps(test_db: ScratchDB) -> None:
    stats = _replay()
    assert (stats.frames, stats.parse_errors, stats.gaps) == (1165, 0, 2)
    with psycopg.connect(test_db.dsn) as conn:
        assert conn.execute("SELECT count(*) FROM trades").fetchone() == (841,)
        assert conn.execute("SELECT count(*) FROM depth_updates").fetchone() == (324,)
        gaps = conn.execute(
            "SELECT symbol, cause, missing_update_ids FROM depth_gaps ORDER BY symbol"
        ).fetchall()
    assert gaps == [("BTCUSDT", "reconnect", 1821), ("ETHUSDT", "reconnect", 771)]


def test_rows_carry_the_recorded_receive_time(test_db: ScratchDB) -> None:
    _replay()
    first_frame_time = next(t for t, p in iter_archive([ARCHIVE]) if not p.startswith("#"))
    with psycopg.connect(test_db.dsn) as conn:
        (earliest,) = conn.execute(
            "SELECT least((SELECT min(received_at) FROM trades),"
            " (SELECT min(received_at) FROM depth_updates))"
        ).fetchone()
    assert earliest == first_frame_time


def test_replay_is_deterministic_and_idempotent(test_db: ScratchDB) -> None:
    _replay()
    first = _content_hash(test_db.dsn)

    _replay()  # again, into the same database: duplicates are skipped
    assert _content_hash(test_db.dsn) == first

    fresh = _recreate(test_db.admin_dsn)  # and into a brand-new database
    _replay()
    assert _content_hash(fresh) == first
