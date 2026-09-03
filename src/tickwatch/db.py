"""Database connection and schema management.

Usage: python -m tickwatch.db init
"""

import asyncio
import sys
from importlib.resources import files

import psycopg

from tickwatch.config import Settings, load_settings


async def connect(settings: Settings) -> psycopg.AsyncConnection:
    return await psycopg.AsyncConnection.connect(settings.pg_dsn)


async def apply_schema(conn: psycopg.AsyncConnection) -> None:
    sql = files("tickwatch").joinpath("schema.sql").read_text()
    await conn.execute(sql)
    await conn.commit()


async def _init() -> None:
    async with await connect(load_settings()) as conn:
        await apply_schema(conn)
    print("schema applied")


def main() -> None:
    if sys.argv[1:] != ["init"]:
        sys.exit("usage: python -m tickwatch.db init")
    asyncio.run(_init())


if __name__ == "__main__":
    main()
