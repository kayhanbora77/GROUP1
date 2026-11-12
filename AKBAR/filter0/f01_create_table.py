"""
process_flight_excel.py  (Multi-file Excel version - No IDATE)

1. Reads EVERY *.xlsx / *.xls file in EXCEL_FOLDER using Polars.
2. Forces all columns to String/VARCHAR to prevent scientific notation, 
   truncation of large ticket numbers, or loss of leading zeros.
3. Trims all text, turns blank cells into NULL.
4. Adds a UUID per row ("Id") and the source file name ("SourceFile").
5. Extracts AirlineCode from FLIGHTNO and removes whitespace from flight numbers.
6. Parses dates (FlightDate1..6).
7. Loads all files into one DuckDB table.
"""

import glob
import os
import re
import uuid

import duckdb
import polars as pl

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

EXCEL_FOLDER = r"C:\Users\cagri\Desktop\Group1_REAnalyse\Akbar\filter0"
EXCEL_GLOB = "*.xlsx"  # Change to "*.xls" if your files use that extension

DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "AKBAR_RAW"

TRUNCATE_BEFORE_LOAD = True

FLIGHT_NUMBER_COLS = ["FLIGHTNO"]

# Updated: Removed IDATE since it does not exist in your Excel files
DATE_COLS = ["DAIS", "FLIGHTDATE"]

FLIGHTDATE_FORMATS = [
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y",   # 5/9/2018 = 9 May 2018
    "%d-%b-%y", "%d-%b-%Y",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%d/%m/%Y", "%Y-%m-%d %H:%M:%S%.f", # last resort
]
DATE_FORMATS = {col: FLIGHTDATE_FORMATS for col in DATE_COLS}

# These columns must remain VARCHAR in DuckDB.
VARCHAR_COLS = {
    "TDNR", "CJCP", "STAT", "PNRR", "PAXNAME","ORIT","ORIGINALTKTNO", "STPO",
    "FLIGHTNO", "AIRPORT1", "AIRPORT2", 
}

NULL_WORDS = ["nan", "none", "null"]
NO_NULLWORD_COLS = {"AIRPORT1", "AIRPORT2"}   # "NAN" = Nadi is a valid IATA code

# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------

def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def validate_table_name(name: str) -> None:
    parts = name.split(".")
    if not parts or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts):
        raise ValueError(f"Invalid DuckDB table name: {name!r}")


# ----------------------------------------------------------------------
# STEP 1 - FIND + READ EXCEL FILES
# ----------------------------------------------------------------------

def list_excel_files(folder: str, pattern: str) -> list[str]:
    if not os.path.isdir(folder):
        raise FileNotFoundError(f"Folder not found: {folder}")
    files = sorted(glob.glob(os.path.join(folder, pattern)))
    if not files:
        raise FileNotFoundError(f"No files matching {pattern!r} in {folder}")
    return files


def read_excel_file(path: str) -> pl.DataFrame:
    """Read Excel file and cast all columns to String immediately to prevent number truncation/scientific notation."""
    df = pl.read_excel(path, engine="calamine", read_options={"dtypes":"string"})
    
    # Normalise header names first: trim + upper-case
    df = df.rename({c: c.strip().upper() for c in df.columns})

    # Cast every single column to String so TicketNumber, codes, etc., never lose digits or leading zeros
    df = df.with_columns([pl.col(c).cast(pl.String) for c in df.columns])

    print(f"  {os.path.basename(path)}: {df.height:,} rows, {df.width} cols")
    return df


# ----------------------------------------------------------------------
# STEP 2 - TRIM TEXT, BLANK -> NULL, ADD Id / SourceFile
# ----------------------------------------------------------------------
def clean_text_columns(df: pl.DataFrame) -> pl.DataFrame:
    exprs = []
    for c in df.columns:
        s = pl.col(c).str.strip_chars()
        if c in NO_NULLWORD_COLS:
            is_null = (s == "") | (s == "NULL")        # exact upper-case NULL text only
        else:
            is_null = (s == "") | s.str.to_lowercase().is_in(NULL_WORDS)
        exprs.append(pl.when(is_null).then(None).otherwise(s).alias(c))
    return df.with_columns(exprs)


def add_uuid_and_source(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    if "Id" in df.columns:
        raise ValueError("Source file already contains an 'Id' column.")
    ids = pl.Series("Id", [str(uuid.uuid4()) for _ in range(df.height)], dtype=pl.String)
    df = df.with_columns(ids, pl.lit(source_name).alias("SOURCEFILE"))
    return df.select(["Id", "SOURCEFILE", *[c for c in df.columns if c not in ("Id", "SOURCEFILE")]])


# ----------------------------------------------------------------------
# STEP 3 - AIRLINE CODES + CLEAN FLIGHT NUMBERS
# ----------------------------------------------------------------------

IATA_RE = r"^([A-Za-z][A-Za-z0-9]|[0-9][A-Za-z])"


def process_flight_columns(df: pl.DataFrame) -> pl.DataFrame:
    expressions = []
    for col in FLIGHT_NUMBER_COLS:
        if col not in df.columns:
            continue
        suffix = col.replace("FLIGHTNO", "")

        no_space = pl.col(col).str.replace_all(r"\s+", "")
        cleaned = (
            pl.when(no_space.str.to_lowercase().is_in(NULL_WORDS))
            .then(None)
            .otherwise(no_space)
        )
        expressions.append(cleaned.str.extract(IATA_RE, 1).alias(f"AIRLINECODE{suffix}"))
        expressions.append(cleaned.alias(col))
    return df.with_columns(expressions)


# ----------------------------------------------------------------------
# STEP 4 - PARSE DATES
# ----------------------------------------------------------------------

def parse_dates(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    for col in DATE_COLS:
        if col not in df.columns:
            continue

        parsed = pl.coalesce([
            pl.col(col).str.strptime(pl.Datetime, format=fmt, strict=False, exact=True)
            for fmt in DATE_FORMATS[col]
        ])
        result = df.with_columns(parsed.alias("__parsed"))

        bad = result.filter(pl.col(col).is_not_null() & pl.col("__parsed").is_null()).select(col)
        if bad.height:
            print(f"\nWARNING [{source_name}]: {bad.height:,} invalid date value(s) in {col}:")
            print(bad.unique().head(20))

        df = result.with_columns(pl.col("__parsed").alias(col)).drop("__parsed")
    return df


def add_flight_time(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    """Combine parsed FLIGHTDATE (date) + TIME (HH:MM[:SS] text) into FLIGHTTIME."""
    if "FLIGHTDATE" not in df.columns or "TIME" not in df.columns:
        return df

    t = pl.coalesce([
        pl.col("TIME").str.strptime(pl.Time, format="%H:%M", strict=False, exact=True),
        pl.col("TIME").str.strptime(pl.Time, format="%H:%M:%S", strict=False, exact=True),
    ])
    df = df.with_columns(
        pl.col("FLIGHTDATE").dt.date().dt.combine(t).alias("FLIGHTTIME")
    )

    bad = df.filter(pl.col("TIME").is_not_null() & pl.col("FLIGHTTIME").is_null()).select("TIME")
    if bad.height:
        print(f"\nWARNING [{source_name}]: {bad.height:,} invalid TIME value(s):")
        print(bad.unique().head(20))
    return df


def add_connections(df: pl.DataFrame) -> pl.DataFrame:
    keys = ["TDNR", "PNRR", "PAXNAME"]
    # safety net: the connection logic needs these columns
    df = df.filter(pl.all_horizontal(pl.col(c).is_not_null() for c in REQUIRED_COLS))

    df = (
        df.with_row_index("__row")
          .sort(keys + ["FLIGHTTIME", "__row"], nulls_last=True, maintain_order=True)
    )

    fdate = pl.col("FLIGHTDATE").dt.date()
    prev_date = fdate.shift(1).over(keys)
    prev_to = pl.col("AIRPORT2").shift(1).over(keys)

    # a new connection starts when: first row of the passenger/PNR, gap > 2 days,
    # or the legs do not chain (previous AIRPORT2 != this AIRPORT1)
    new_conn = (
        prev_date.is_null()
        | ((fdate - prev_date).dt.total_days() > 2)
        | (prev_to != pl.col("AIRPORT1"))
    ).fill_null(True)

    df = df.with_columns(new_conn.cum_sum().alias("__conn"))

    n_conn = df["__conn"].max() or 0
    uuids = pl.Series("ConnectionID", [str(uuid.uuid4()) for _ in range(n_conn)], dtype=pl.String)
    df = df.with_columns(uuids.gather(df["__conn"] - 1).alias("ConnectionID"))


    df = df.with_columns(
        (pl.int_range(pl.len()).over("__conn") + 1).alias("LEGNO"),
        # AIRPORT2 of the last leg in the connection, on every row of that connection.
        # drop_nulls() falls back to the last non-null AIRPORT2 if the final leg's is missing.
        pl.col("AIRPORT2").drop_nulls().last().over("__conn").alias("LastLegAirport"),
    )

    null_cnt = df["LastLegAirport"].null_count()
    if null_cnt:
        print(f"\nERROR: {null_cnt:,} row(s) have no AIRPORT2 anywhere in their connection:")
        print(
            df.filter(pl.col("LastLegAirport").is_null())
              .select(["SOURCEFILE", "TDNR", "PNRR", "PAXNAME", "AIRPORT1", "AIRPORT2", "FLIGHTDATE"])
              .head(20)
        )
        raise ValueError("LastLegAirport must be NOT NULL")
    
    return df.drop(["__row", "__conn"])    

REQUIRED_COLS = ["TDNR", "PNRR", "PAXNAME", "AIRPORT1", "AIRPORT2", "FLIGHTDATE"]
REJECT_TABLE = "AKBAR_RAW_REJECTED"

def split_rejects(df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Rows missing any required column go to the reject table with a RejectReason."""
    missing_any = pl.any_horizontal([pl.col(c).is_null() for c in REQUIRED_COLS])
    reason = pl.concat_str(
        [pl.when(pl.col(c).is_null()).then(pl.lit(f"{c};")).otherwise(pl.lit(""))
         for c in REQUIRED_COLS]
    ).alias("RejectReason")

    rejected = df.filter(missing_any).with_columns(reason)
    good = df.filter(~missing_any)
    return good, rejected    
# ----------------------------------------------------------------------
# STEP 5 - INSERT INTO DUCKDB
# ----------------------------------------------------------------------

def insert_into_duckdb(df: pl.DataFrame, db_path: str, table_name: str) -> None:
    validate_table_name(table_name)

    # Note: If an Excel file doesn't have Airport5, Airport6, etc., we can make VARCHAR_COLS check dynamic or ensure columns exist.
    # Let's check only the columns actually present in the dataframe that are also expected:
    active_varchar_cols = VARCHAR_COLS.intersection(df.columns)

    con = duckdb.connect(db_path)
    try:
        con.register("flight_data_df", df)
        table_sql = ".".join(quote_identifier(p) for p in table_name.split("."))
        columns_sql = ", ".join(quote_identifier(c) for c in df.columns)
        select_sql = ", ".join(
            f"CAST({quote_identifier(c)} AS VARCHAR) AS {quote_identifier(c)}"
            if c in active_varchar_cols else quote_identifier(c)
            for c in df.columns
        )

        con.execute("BEGIN TRANSACTION")
        try:
            if TRUNCATE_BEFORE_LOAD:
                con.execute(f"DROP TABLE IF EXISTS {table_sql}")
            con.execute(
                f"CREATE TABLE IF NOT EXISTS {table_sql} AS "
                f"SELECT {select_sql} FROM flight_data_df WHERE FALSE"
            )

            before = con.execute(f"SELECT COUNT(*) FROM {table_sql}").fetchone()[0]
            con.execute(
                f"INSERT INTO {table_sql} ({columns_sql}) SELECT {select_sql} FROM flight_data_df"
            )
            after = con.execute(f"SELECT COUNT(*) FROM {table_sql}").fetchone()[0]

            if after - before != df.height:
                raise RuntimeError(f"Row-count mismatch: source={df.height:,}, "
                                   f"inserted={after - before:,}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        print(f"\nSuccessfully loaded: {table_name}")
        print(f"Source rows:   {df.height:,}")
        print(f"Target rows:   {after:,}")
    finally:
        con.close()


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main() -> None:
    files = list_excel_files(EXCEL_FOLDER, EXCEL_GLOB)
    print(f"Found {len(files)} Excel file(s) in {EXCEL_FOLDER}")

    frames: list[pl.DataFrame] = []
    for path in files:
        name = os.path.basename(path)
        df = read_excel_file(path)
        if df.height == 0:
            print(f"  {name}: empty, skipped")
            continue
        df = clean_text_columns(df)
        df = add_uuid_and_source(df, name)
        df = process_flight_columns(df)
        df = parse_dates(df, name)
        df = add_flight_time(df, name)
        frames.append(df)

    if not frames:
        raise RuntimeError("All files were empty - nothing to load.")

    combined = pl.concat(frames, how="diagonal")

    combined, rejected = split_rejects(combined)          # must be assigned, and before add_connections
    print(f"Rejected rows: {rejected.height:,}")
    if rejected.height:
        insert_into_duckdb(rejected, DB_PATH, REJECT_TABLE)

    combined = add_connections(combined)
    print(f"\nProcessed rows (all files combined): {combined.height:,}")

    insert_into_duckdb(combined, DB_PATH, TABLE_NAME)
    


if __name__ == "__main__":
    main()