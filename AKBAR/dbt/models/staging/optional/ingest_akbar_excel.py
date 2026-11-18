"""Filter 0 as a dbt python model: Excel files -> one typed dataframe.

Lives outside models/ and joins the project only when the Excel ingest is
requested, because a full reload of the ten C2R workbooks takes hours and
re-creates 11.2M rows:

    dbt run --select ingest_akbar_excel --vars '{enable_excel_ingest: true}'

The normal entry point of the pipeline is main.AKBAR_RAW, which f01 produces.
"""

import sys
from pathlib import Path

import polars as pl

AKBAR_ROOT = Path(r"C:\Coding\GROUP1\AKBAR")
sys.path.insert(0, str(AKBAR_ROOT / "dbt" / "lib"))


def model(dbt, session):
    dbt.config(materialized="table")

    import akbar_lib

    f01 = akbar_lib.load_module("ingest", r"filter0\f01_create_table.py")

    files = f01.list_excel_files(f01.EXCEL_FOLDER, f01.EXCEL_GLOB)
    print(f"ingest_akbar_excel: {len(files)} workbook(s) in {f01.EXCEL_FOLDER}")

    frames = []
    for path in files:
        name = Path(path).name
        df = f01.read_excel_file(path)
        if df.height == 0:
            print(f"  {name}: empty, skipped")
            continue
        df = f01.clean_text_columns(df)
        df = f01.add_uuid_and_source(df, name)
        df = f01.process_flight_columns(df)
        df = f01.parse_dates(df, name)
        df = f01.add_flight_time(df, name)
        frames.append(df)

    if not frames:
        raise RuntimeError("All workbooks were empty - nothing to load.")

    combined = pl.concat(frames, how="diagonal")
    print(f"ingest_akbar_excel: {combined.height:,} rows combined")
    return combined.to_arrow()
