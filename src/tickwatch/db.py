"""Database connection and schema management.

Usage: python -m tickwatch.db init
"""

import asyncio
import os
import sys
from importlib.resources import files

import psycopg
from psycopg import sql

from tickwatch.config import Settings, load_settings


async def connect(settings: Settings) -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(settings.pg_dsn)


async def apply_schema(conn: psycopg.AsyncConnection) -> None:
    schema = files("tickwatch").joinpath("schema.sql").read_text()
    await conn.execute(schema)
    await conn.commit()


READER_ROLE = "grafana_reader"


async def ensure_reader_role(conn: psycopg.AsyncConnection, dbname: str, password: str) -> None:
    """A login role that can only SELECT, for dashboards. Idempotent."""
    role = sql.Identifier(READER_ROLE)
    cur = await conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (READER_ROLE,))
    verb = "ALTER" if await cur.fetchone() else "CREATE"
    await conn.execute(
        sql.SQL(verb + " ROLE {} LOGIN PASSWORD {}").format(role, sql.Literal(password))
    )
    await conn.execute(
        sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(dbname), role)
    )
    await conn.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role))
    await conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA public TO {}").format(role))
    # Tables created later (and TimescaleDB chunks) are readable too.
    await conn.execute(
        sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO {}").format(
            role
        )
    )
    await conn.commit()


async def _init() -> None:
    settings = load_settings()
    async with await connect(settings) as conn:
        await apply_schema(conn)
        print("schema applied")
        if password := os.environ.get("GRAFANA_DB_PASSWORD"):
            await ensure_reader_role(conn, settings.pg_db, password)
            print(f"read-only role {READER_ROLE} ready")


def main() -> None:
    if sys.argv[1:] != ["init"]:
        sys.exit("usage: python -m tickwatch.db init")
    asyncio.run(_init())


if __name__ == "__main__":
    main()
