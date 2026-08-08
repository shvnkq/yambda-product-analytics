"""Shared metric definitions and DuckDB settings for the first stage."""

from pathlib import Path

import duckdb

DAY_SECONDS = 86_400
SESSION_GAP_SECONDS = 1_800
LISTEN_PLUS_THRESHOLD = 50


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def connect(database_path: Path, *, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path), read_only=read_only)
    if not read_only:
        temp_dir = database_path.parent / "_duckdb_tmp"
        temp_dir.mkdir(exist_ok=True)
        connection.execute("SET memory_limit='8GB'")
        connection.execute(f"SET temp_directory='{sql_path(temp_dir)}'")
        connection.execute("SET preserve_insertion_order=false")
        connection.execute("SET threads=4")
    return connection
