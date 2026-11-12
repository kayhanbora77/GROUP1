"""
f03_build_connections.py
========================
Runs AFTER the rejection step (f02). Reads SOURCE_TABLE_NAME (AKBAR_CLEANED),
builds connections, maps columns to the standard schema, and writes the result
to TARGET_TABLE (TA_STANDARD_AKBAR).

Adds:
    ConnectionID    uuid shared by consecutive legs of one journey
    LegNo           1..n within the connection, ordered by FLIGHTTIME
    LastLegAirport  AIRPORT2 of the connection's last leg, on every row

Maps source columns to TARGET_COLUMNS (e.g., PAXNAME -> PaxName, FLIGHTNO -> FlightNumber).
Any target columns not mapped from the source are added as NULL.

Grouping key (per row) depends on which identifiers are valid:
    1. TDNR valid    -> TDNR
    2. TDNR invalid  -> PNRR + PAXNAME (if both valid)
    3. Fallback      -> PNRR, PAXNAME, or unique Id

Within a key, ordered by FLIGHTTIME, a new connection starts when:
    - The FLIGHTDATE gap to the previous leg is > 2 days, OR
    - The legs do not chain (previous AIRPORT2 != this AIRPORT1, case/whitespace insensitive), OR
    - The previous leg was the LAST leg (STPO is NULL, empty, or '0').
      (STPO = 'X' indicates a first/middle leg).
"""

import uuid

import duckdb
import polars as pl

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE_NAME = "AKBAR_CLEANED"
TARGET_TABLE = "TA_STANDARD_AKBAR"

MAX_GAP_DAYS = 2
NEW_COLS = ["ConnectionID", "LEGNO", "LASTLEGAIRPORT"]

# Standard target schema for TA_STANDARD_AKBAR
TARGET_COLUMNS = [
    "Id", "ConnectionID", "PaxName", "AgencyRefNumber", "ETicketNo", "FlightNumber",
    "DepartureDate", "FileName", "BookingRef", "AirlineCode", "FromAirport",
    "ToAirport", "LastLegAirport", "EUEligible", "EUEligibleDuration", "ExtraNote",
    "FlightFound", "LegNo", "IsTimeLimitL1", "IsTimeLimitL2", "EUFlights_Id",
    "Link_Id", "DelayInSecond", "Status", "IsSingleFlight", "IsMultiSegment",
    "OperatingFlightNo", "ScheduledDeparture", "ScheduledArrival", "ActualDeparture",
    "ActualArrival", "SourceData"
]

# Map source columns (AKBAR_CLEANED) to target columns (TA_STANDARD_AKBAR)
COLUMN_MAPPING = {
    "Id":"Id",
    "PAXNAME": "PaxName",
    "TDNR": "ETicketNo",
    "FLIGHTNO": "FlightNumber",
    "FLIGHTDATE": "DepartureDate",
    "SOURCEFILE": "FileName",
    "PNRR": "BookingRef",
    "AIRLINECODE": "AirlineCode",
    "AIRPORT1": "FromAirport",
    "AIRPORT2": "ToAirport",    
}

# Keep in sync with f02.
DATE_LIKE_RE = r"^(\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}|\d{1,2}-[A-Za-z]{3}-\d{2,4})$"
_DT_FORMATS = [
    "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
]


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _as_datetime(col: str) -> pl.Expr:
    """Datetime expression for a column that is either VARCHAR or already a timestamp."""
    return pl.coalesce([
        pl.col(col).cast(pl.String).str.strptime(
            pl.Datetime, format=f, strict=False, exact=True
        )
        for f in _DT_FORMATS
    ])


def add_connections(df: pl.DataFrame) -> pl.DataFrame:
    df = df.drop([c for c in NEW_COLS if c in df.columns])

    # 1. Determine valid identifiers
    tdnr_ok = (
        pl.col("TDNR").is_not_null()
        & (pl.col("TDNR").str.len_chars() >= 3)
        & ~pl.col("TDNR").str.contains(DATE_LIKE_RE)
    ).fill_null(False)
    
    pnr_ok = (pl.col("PNRR").is_not_null() & (pl.col("PNRR") != "/")).fill_null(False)
    pax_ok = (pl.col("PAXNAME").is_not_null() & (pl.col("PAXNAME") != "")).fill_null(False)

    SEP = "\x1f"
    key_pnr_pax = pl.concat_str([pl.col("PNRR"), pl.col("PAXNAME")], separator=SEP)

    # 2. Build grouping key (TDNR takes priority if valid)
    df = df.with_columns(
        pl.when(tdnr_ok).then(pl.col("TDNR"))
          .when(pnr_ok & pax_ok).then(key_pnr_pax)
          .when(pnr_ok).then(pl.col("PNRR"))
          .when(pax_ok).then(pl.col("PAXNAME"))
          .otherwise(pl.col("Id"))        # no usable key -> own connection
          .alias("__key"),
        _as_datetime("FLIGHTDATE").alias("__fd"),
        _as_datetime("FLIGHTTIME").alias("__ft"),
    )

    df = (
        df.with_row_index("__row")
          .sort(["__key", "__ft", "__row"], nulls_last=True, maintain_order=True)
    )

    # 3. Determine connection boundaries
    fdate = pl.col("__fd").dt.date()
    prev_date = fdate.shift(1).over("__key")
    
    # Clean airport codes for chaining check (strip whitespace, uppercase to avoid case mismatches)
    # Ensures: Leg N AIRPORT2 == Leg N+1 AIRPORT1
    prev_to = pl.col("AIRPORT2").shift(1).over("__key").cast(pl.String).str.strip_chars().str.to_uppercase()
    curr_from = pl.col("AIRPORT1").cast(pl.String).str.strip_chars().str.to_uppercase()
    
    # STPO logic: 'X' = first/middle leg, NULL/''/'0' = last leg
    stpo = pl.col("STPO").cast(pl.String).fill_null("").str.strip_chars()
    prev_stpo = stpo.shift(1).over("__key").fill_null("")

    new_conn = (
        prev_date.is_null()                                     # First row in group
        | ((fdate - prev_date).dt.total_days() > MAX_GAP_DAYS)  # Date gap > 2 days
        | (prev_to != curr_from)                                # Airports don't chain
        | prev_stpo.is_in(["", "0"])                            # Previous leg was the last leg
    ).fill_null(True)

    df = df.with_columns(new_conn.cum_sum().alias("__conn"))

    # 4. Generate UUIDs and Leg numbers
    n_conn = df["__conn"].max() or 0
    uuids = pl.Series("ConnectionID", [str(uuid.uuid4()) for _ in range(n_conn)], dtype=pl.String)
    df = df.with_columns(uuids.gather(df["__conn"] - 1).alias("ConnectionID"))

    df = df.with_columns(
        (pl.int_range(pl.len()).over("__conn") + 1).alias("LEGNO"),
        pl.col("AIRPORT2").last().over("__conn").alias("LastLegAirport"),
    )

    null_cnt = df["LastLegAirport"].null_count()
    if null_cnt:
        print(f"\nWARNING: {null_cnt:,} row(s) have no AIRPORT2 in their connection "
              f"(LastLegAirport is NULL)")
    bad_dt = df.filter(pl.col("__ft").is_null()).height
    if bad_dt:
        print(f"\nWARNING: {bad_dt:,} row(s) have an unparseable FLIGHTTIME "
              f"(they are ordered last within their key, so LEGNO may be off)")

    return df.drop(["__row", "__conn", "__key", "__fd", "__ft"])


def transform_to_target(df: pl.DataFrame) -> pl.DataFrame:
    """Renames mapped columns, adds missing target columns as NULL, and enforces column order."""
    # 1. Rename mapped source columns
    rename_map = {src: tgt for src, tgt in COLUMN_MAPPING.items() if src in df.columns}
    df = df.rename(rename_map)
    
    # 2. Rename generated LEGNO to LegNo
    if "LEGNO" in df.columns:
        df = df.rename({"LEGNO": "LegNo"})
        
    # 3. Add missing target columns as NULL (cast to String/VARCHAR)
    missing_cols = [
        pl.lit(None).cast(pl.String).alias(col) 
        for col in TARGET_COLUMNS if col not in df.columns
    ]
    if missing_cols:
        df = df.with_columns(missing_cols)
        
    # 4. Select and order columns exactly as TARGET_COLUMNS
    df = df.select(TARGET_COLUMNS)
    return df


def build_connections(
    db_path: str = DB_PATH, 
    source_table: str = SOURCE_TABLE_NAME, 
    target_table: str = TARGET_TABLE
) -> None:
    con = duckdb.connect(db_path)
    try:
        src_tbl = quote_identifier(source_table)
        tgt_tbl = quote_identifier(target_table)
        
        # Use con.sql().to_arrow_table() to avoid the extra copy warning
        arrow_tbl = con.sql(f"SELECT * FROM {src_tbl}").to_arrow_table()
        df = pl.from_arrow(arrow_tbl)
        print(f"Read {df.height:,} rows from {source_table}")

        out = add_connections(df)
        if out.height != df.height:
            raise RuntimeError(f"Row-count mismatch: in={df.height:,}, out={out.height:,}")

        # Transform to the standard target schema
        out = transform_to_target(out)

        con.register("conn_df", out.to_arrow(compat_level=pl.CompatLevel.oldest()))
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"CREATE OR REPLACE TABLE {tgt_tbl} AS SELECT * FROM conn_df")
            after = con.execute(f"SELECT COUNT(*) FROM {tgt_tbl}").fetchone()[0]
            if after != out.height:
                raise RuntimeError(f"Row-count mismatch after write: {after:,} vs {out.height:,}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.unregister("conn_df")

        n_conn = con.execute(f'SELECT COUNT(DISTINCT "ConnectionID") FROM {tgt_tbl}').fetchone()[0]
        print(f"\nSuccessfully created/updated: {target_table}")
        print(f"Rows:         {after:,}")
        print(f"Connections:  {n_conn:,}")
    finally:
        con.close()


if __name__ == "__main__":
    build_connections()