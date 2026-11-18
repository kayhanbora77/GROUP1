"""Filter 1b: build journeys (ConnectionID / LegNo / LastLegAirport) and map
clean rows onto the standard target schema.

Reuses filter1/f13_build_connections.py so the leg-chaining rules stay in one
place. Runs on clean rows only, which is why a rejected or duplicate leg can
never split a real connection.
"""

import sys
from pathlib import Path

import polars as pl

AKBAR_ROOT = Path(r"C:\Coding\GROUP1\AKBAR")
sys.path.insert(0, str(AKBAR_ROOT / "dbt" / "lib"))


def model(dbt, session):
    dbt.config(materialized="table")

    import akbar_lib

    f13 = akbar_lib.load_module("connections", r"filter1\f13_build_connections.py")

    relation = akbar_lib.relation_sql(session, "int_akbar_cleaned")
    df = pl.from_arrow(session.sql(f"select * from {relation}").arrow())
    print(f"int_akbar_connections: {df.height:,} clean rows in")

    out = f13.add_connections(df)
    if out.height != df.height:
        raise RuntimeError(f"Row-count mismatch: in={df.height:,}, out={out.height:,}")

    out = f13.transform_to_target(out)
    print(f"int_akbar_connections: {out.height:,} legs -> {out['ConnectionID'].n_unique():,} connections")
    return out.to_arrow()
