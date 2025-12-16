#!/usr/bin/env python3
"""
Utility to migrate selected tables from an existing PBS Monitor database
into a freshly initialized one. The primary use-case is copying Jobs and
Reservations (plus their history tables) from an older schema into the new
compact node-snapshot schema.

Example:
    python scripts/migrate_db.py \
        --source /path/to/pbs_monitor.db_2025-12-15 \
        --dest /path/to/pbs_monitor.db
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Sequence

from sqlalchemy import MetaData, Table, create_engine, select, text
from sqlalchemy.engine import Engine

DEFAULT_TABLES: Sequence[str] = (
    "jobs",
    "job_history",
    "reservations",
    "reservation_history",
    "reservation_utilization",
)


def normalize_db_url(value: str) -> str:
    """Return a SQLAlchemy URL string for the given value."""
    if "://" in value:
        return value
    abs_path = Path(value).expanduser().resolve()
    return f"sqlite:///{abs_path}"


def disable_foreign_keys(engine: Engine) -> None:
    """Disable SQLite FK checks for faster bulk loads."""
    with engine.connect() as conn:
        conn.execute(text("PRAGMA foreign_keys=OFF;"))


def enable_foreign_keys(engine: Engine) -> None:
    """Re-enable SQLite FK checks."""
    with engine.connect() as conn:
        conn.execute(text("PRAGMA foreign_keys=ON;"))


def reflect_table(engine: Engine, table_name: str) -> Table:
    """Reflect a table definition from the given engine."""
    metadata = MetaData()
    return Table(table_name, metadata, autoload_with=engine)


def copy_table(
    source_engine: Engine,
    dest_engine: Engine,
    table_name: str,
    chunk_size: int,
    truncate: bool,
) -> int:
    """Copy all rows from table_name in the source DB to the destination."""
    src_table = reflect_table(source_engine, table_name)
    dest_table = reflect_table(dest_engine, table_name)

    if truncate:
        with dest_engine.begin() as dest_conn:
            dest_conn.execute(dest_table.delete())

    total = 0
    stmt = select(src_table)
    pk_columns = list(src_table.primary_key.columns)
    if pk_columns:
        stmt = stmt.order_by(*pk_columns)

    with source_engine.connect() as src_conn:
        result = src_conn.execute(stmt)
        while True:
            rows = result.fetchmany(chunk_size)
            if not rows:
                break
            payload = [dict(row._mapping) for row in rows]
            with dest_engine.begin() as dest_conn:
                dest_conn.execute(dest_table.insert(), payload)
            total += len(rows)
    return total


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Copy selected tables between PBS Monitor databases.")
    parser.add_argument("--source", required=True, help="Path or SQLAlchemy URL of the source database.")
    parser.add_argument("--dest", required=True, help="Path or SQLAlchemy URL of the destination database.")
    parser.add_argument(
        "--tables",
        nargs="+",
        default=list(DEFAULT_TABLES),
        help=f"Tables to copy (default: {', '.join(DEFAULT_TABLES)})",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Number of rows to write per batch.",
    )
    parser.add_argument(
        "--keep-existing",
        action="store_true",
        help="Do not truncate destination tables before inserting data.",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    source_url = normalize_db_url(args.source)
    dest_url = normalize_db_url(args.dest)

    source_engine = create_engine(source_url)
    dest_engine = create_engine(dest_url)

    print(f"Copying tables from {source_url} -> {dest_url}")
    disable_foreign_keys(dest_engine)
    try:
        for table in args.tables:
            print(f"  > {table} ... ", end="", flush=True)
            copied = copy_table(
                source_engine,
                dest_engine,
                table,
                chunk_size=args.chunk_size,
                truncate=not args.keep_existing,
            )
            print(f"{copied} rows copied")
    finally:
        enable_foreign_keys(dest_engine)
        source_engine.dispose()
        dest_engine.dispose()


if __name__ == "__main__":
    main()
