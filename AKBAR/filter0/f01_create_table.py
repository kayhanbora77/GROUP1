"""
process_flight_excel.py (Multi-file Excel Loader for DuckDB)

1. Reads every Excel file matching EXCEL_GLOB from EXCEL_FOLDER using Polars.
2. Normalizes column names (trim + uppercase).
3. Converts all columns to String to preserve ticket numbers, codes,
   leading zeros, and prevent scientific notation.
4. Trims text values and converts blank/null-like values to NULL.
5. Adds a UUID ("Id") and source filename ("SOURCEFILE") to every row.
6. Cleans flight numbers and extracts AIRLINECODE from FLIGHTNO.
7. Parses DAIS and FLIGHTDATE columns into Datetime values.
8. Creates FLIGHTTIME by combining FLIGHTDATE and TIME when available.
9. Combines data from all Excel files into a single Polars DataFrame.
10. Loads the consolidated dataset into a DuckDB table.
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

# Date columns that should be parsed into Datetime values.
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

    # Convert all columns to String to preserve formatting and prevent
    # scientific notation, truncation, or loss of leading zeros.
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
# STEP 3 - NORMALIZE FLIGHT NUMBERS AND EXTRACT AIRLINE CODES
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
    """
    Build FLIGHTTIME using FLIGHTDATE and TIME.

    Supported TIME formats:
        20:05
        9:10
        9:10A
        9:10P
        9:10AM
        12:55PM
        20:05:00

    If TIME cannot be parsed, use HOUR + MIN.

    MIN can contain values such as:
        10
        10A
        55P
    """

    if "FLIGHTDATE" not in df.columns:
        return df

    if "TIME" not in df.columns:
        print(f"WARNING [{source_name}]: TIME column missing.")
        return df

    # Normalize TIME text
    time_text = (
        pl.col("TIME")
        .cast(pl.String)
        .str.strip_chars()
        .str.to_uppercase()
    )

    # Extract hour, minute and optional AM/PM suffix.
    # Examples: 9:10A, 12:55P, 09:10AM, 20:05
    time_hour = time_text.str.extract(
        r"^(\d{1,2})\s*:\s*(\d{1,2})\s*([AP])?\.?M?\.?$",
        1
    ).cast(pl.Int64, strict=False)

    time_minute = time_text.str.extract(
        r"^(\d{1,2})\s*:\s*(\d{1,2})\s*([AP])?\.?M?\.?$",
        2
    ).cast(pl.Int64, strict=False)

    time_suffix = time_text.str.extract(
        r"^(\d{1,2})\s*:\s*(\d{1,2})\s*([AP])?\.?M?\.?$",
        3
    )

    # Fallback: extract HOUR and MIN columns.
    if "HOUR" in df.columns:
        fallback_hour = (
            pl.col("HOUR")
            .cast(pl.String)
            .str.strip_chars()
            .str.extract(r"^(\d{1,2})", 1)
            .cast(pl.Int64, strict=False)
        )
    else:
        fallback_hour = pl.lit(None, dtype=pl.Int64)

    if "MIN" in df.columns:
        min_text = (
            pl.col("MIN")
            .cast(pl.String)
            .str.strip_chars()
            .str.to_uppercase()
        )

        fallback_minute = (
            min_text
            .str.extract(r"^(\d{1,2})", 1)
            .cast(pl.Int64, strict=False)
        )

        fallback_suffix = min_text.str.extract(
            r"^\d{1,2}\s*([AP])\.?M?\.?$",
            1
        )
    else:
        fallback_minute = pl.lit(None, dtype=pl.Int64)
        fallback_suffix = pl.lit(None, dtype=pl.String)

    # Prefer TIME; otherwise use HOUR + MIN.
    hour = pl.coalesce([time_hour, fallback_hour])
    minute = pl.coalesce([time_minute, fallback_minute])

    suffix = pl.coalesce([time_suffix, fallback_suffix])

    # Convert 12-hour time into 24-hour time.
    hour_24 = (
        pl.when(suffix == "A")
        .then(hour % 12)
        .when(suffix == "P")
        .then((hour % 12) + 12)
        .otherwise(hour)
    )

    # Validate time values.
    valid_time = (
        hour_24.is_not_null()
        & minute.is_not_null()
        & (hour_24 >= 0)
        & (hour_24 <= 23)
        & (minute >= 0)
        & (minute <= 59)
    )

    # Construct a complete datetime string.
    date_text = pl.col("FLIGHTDATE").dt.strftime("%Y-%m-%d")

    datetime_text = pl.concat_str(
        [
            date_text,
            hour_24.cast(pl.String).str.pad_start(2, "0"),
            minute.cast(pl.String).str.pad_start(2, "0"),
        ],
        separator=" "
    )

    result = df.with_columns(
        pl.when(valid_time)
        .then(
            datetime_text.str.strptime(
                pl.Datetime,
                format="%Y-%m-%d %H %M",
                strict=False
            )
        )
        .otherwise(None)
        .alias("FLIGHTTIME")
    )

    # Report invalid records.
    bad = result.filter(
        pl.col("FLIGHTDATE").is_not_null()
        & pl.col("FLIGHTTIME").is_null()
        & (
            pl.col("TIME").is_not_null()
            | (
                pl.col("HOUR").is_not_null()
                if "HOUR" in df.columns
                else pl.lit(False)
            )
        )
    )

    if bad.height:
        print(
            f"\nWARNING [{source_name}]: "
            f"{bad.height:,} FLIGHTTIME values could not be parsed."
        )

        diagnostic_cols = [
            c for c in [
                "FLIGHTNO",
                "FLIGHTDATE",
                "HOUR",
                "MIN",
                "TIME"
            ]
            if c in bad.columns
        ]

        print(bad.select(diagnostic_cols).unique().head(20))

    return result

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
    
    print(f"\nProcessed rows (all files combined): {combined.height:,}")
    insert_into_duckdb(combined, DB_PATH, TABLE_NAME)
    

if __name__ == "__main__":
    main()