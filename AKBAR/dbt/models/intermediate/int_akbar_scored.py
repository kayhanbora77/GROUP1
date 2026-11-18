"""Filter 1 as one pass: every raw row classified as clean or rejected.

Reuses filter1/f11_rejection.py (its rule order, duplicate detection and
FlightNo normalisation) instead of re-implementing the rules in SQL.

Output: all 21 raw columns, FLIGHTNO normalised for clean rows, and
RejectionReason - NULL means the row passed every rule.
"""

import sys
from pathlib import Path

import polars as pl

AKBAR_ROOT = Path(r"C:\Coding\GROUP1\AKBAR")
sys.path.insert(0, str(AKBAR_ROOT / "dbt" / "lib"))


def model(dbt, session):
    dbt.config(materialized="table")

    import akbar_lib

    f11 = akbar_lib.load_module("rejection", r"filter1\f11_rejection.py")

    relation = akbar_lib.relation_sql(session, "stg_akbar_raw")
    columns = akbar_lib.columns_of(session, "stg_akbar_raw")
    # The rules operate on strings only: casting once here is what f11.process_table
    # does internally and keeps timestamps/ticket numbers from being re-rendered.
    casts = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in columns)
    schema = {c: pl.String for c in columns}

    reader = session.cursor()
    reader.execute(f"SELECT {casts} FROM {relation}")

    frames: list[pl.DataFrame] = []
    seen_keys: set = set()
    clean_rows = rejected_rows = 0

    while True:
        batch = reader.fetchmany(f11.BATCH_SIZE)
        if not batch:
            break
        batch_df = pl.DataFrame(batch, schema=schema, orient="row")
        clean_df, reject_df, seen_keys = f11.process_batch(batch_df, seen_keys)

        if not clean_df.is_empty():
            frames.append(
                clean_df.with_columns(pl.lit(None, dtype=pl.String).alias("RejectionReason"))
            )
            clean_rows += clean_df.height
        if not reject_df.is_empty():
            frames.append(reject_df)
            rejected_rows += reject_df.height
    reader.close()

    print(f"int_akbar_scored: {clean_rows:,} clean / {rejected_rows:,} rejected")

    if not frames:
        return pl.DataFrame(schema={**schema, "RejectionReason": pl.String}).to_arrow()
    return pl.concat(frames, how="vertical").to_arrow()
