"""Bridge between dbt python models and the existing filter scripts.

Two jobs:

1. load_module(): import the filter scripts by file path so the business rules
   stay in exactly one place (filter0/filter1/filter2), never copied.
2. relation_sql(): resolve a built dbt model to a quoted name on the live
   DuckDB connection. Inside a duckdb python model dbt.ref() hands back a
   loaded dataframe rather than a relation, which would double memory on an
   11M-row model, so the models query the built relation directly.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

AKBAR_ROOT = Path(os.environ.get("AKBAR_ROOT", r"C:\Coding\GROUP1\AKBAR")).resolve()

DBT_SCHEMA = os.environ.get("DBT_SCHEMA", "akbar_dbt")


def load_module(alias: str, relative_path: str):
    """Import a filter script by file path (they are not a package)."""
    key = f"akbar_{alias}"
    if key in sys.modules:
        return sys.modules[key]

    path = AKBAR_ROOT / relative_path
    if not path.is_file():
        raise FileNotFoundError(f"Filter script not found: {path}")

    spec = importlib.util.spec_from_file_location(key, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load filter script: {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[key] = module
    spec.loader.exec_module(module)
    return module


def _parts_of(session, table_name: str) -> tuple[str, str, str]:
    """(catalog, schema, table) of a built dbt model, preferring the dbt schema."""
    rows = session.execute(
        "select table_catalog, table_schema, table_name "
        "from information_schema.tables where table_name = ?",
        [table_name],
    ).fetchall()
    if not rows:
        raise RuntimeError(
            f"Relation {table_name!r} not found - is the upstream model built?"
        )
    for row in rows:
        if row[1] == DBT_SCHEMA:
            return row
    return rows[0]


def relation_sql(session, table_name: str) -> str:
    """Quoted catalog.schema.table for a built dbt model."""
    catalog, schema, table = _parts_of(session, table_name)
    return f'"{catalog}"."{schema}"."{table}"'


def columns_of(session, table_name: str) -> list[str]:
    """Column names of a built dbt model, in ordinal order."""
    catalog, schema, table = _parts_of(session, table_name)
    rows = session.execute(
        "select column_name from information_schema.columns "
        "where table_catalog = ? and table_schema = ? and table_name = ? "
        "order by ordinal_position",
        [catalog, schema, table],
    ).fetchall()
    return [r[0] for r in rows]
