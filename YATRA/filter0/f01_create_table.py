"""
process_flight_csv.py  (Multi-file CSV version - No IDATE)

1. Reads EVERY *.csv file in CSV_FOLDER using Polars (delimiter auto-detected:
   tab, comma, semicolon or pipe).
2. Forces all columns to String/VARCHAR to prevent scientific notation,
   truncation of large ticket numbers, or loss of leading zeros.
3. Trims all text, turns blank cells into NULL.
4. Adds a UUID per row ("Id") and the source file name ("SOURCEFILE").
5. Extracts AIRLINECODE from FLIGHTNUMBER and removes whitespace from flight numbers.
6. Parses dates (DEPARTUREDATE, ARRIVALDATE, BOOKINGDATE).
7. Loads all files into one DuckDB table.
"""

import glob
import io
import os
import re
import uuid

import duckdb
import polars as pl

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

CSV_FOLDER = r"C:\Users\cagri\Desktop\Group1\Yatra\filter0"
CSV_GLOB = "*.csv"

DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "YATRA_RAW"

TRUNCATE_BEFORE_LOAD = True

FLIGHT_NUMBER_COLS = ["FLIGHTNUMBER"]

DATE_COLS = ["DEPARTUREDATE", "ARRIVALDATE", "BOOKINGDATE"]

# Order matters: first successful format wins (coalesce).
# Day-first formats with explicit separators/month names are listed before
# the ambiguous ones.
FLIGHTDATE_FORMATS = [
    "%d-%m-%Y %I:%M %p",                                  # 19-04-2020 10:25 PM  (Yatra)
    "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y",
    "%d-%b-%y", "%d-%b-%Y",                               # 1-Apr-20
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y",    # 5/9/2018 = 9 May 2018
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%d/%m/%Y",                                           # last resort
]
DATE_FORMATS = {col: FLIGHTDATE_FORMATS for col in DATE_COLS}

# These columns must remain VARCHAR in DuckDB.
VARCHAR_COLS = {
    "BOOKINGREF", "SUPERPNR", "PAXNAME", "PNRNO", "AIRLINENAME",
    "TICKETNO", "FLIGHTNUMBER", "DEPARTUREAIRPORT", "ARRIVALAIRPORT",
}

NULL_WORDS = ["nan", "none", "null"]


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
# STEP 1 - FIND + READ CSV FILES
# ----------------------------------------------------------------------

def list_csv_files(folder: str, pattern: str) -> list[str]:
    if not os.path.isdir(folder):
        raise FileNotFoundError(f"Folder not found: {folder}")
    files = sorted(glob.glob(os.path.join(folder, pattern)))
    if not files:
        raise FileNotFoundError(f"No files matching {pattern!r} in {folder}")
    return files


def _decode(raw: bytes) -> str:
    """UTF-8 (BOM-safe) first, fall back to Windows-1252."""
    try:
        return raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw.decode("cp1252", errors="replace")


def _detect_separator(text: str) -> str:
    header = text.split("\n", 1)[0]
    candidates = ["\t", ",", ";", "|"]
    return max(candidates, key=header.count)


def read_csv_file(path: str) -> pl.DataFrame:
    """Read CSV with every column as String (no type inference at all)."""
    with open(path, "rb") as f:
        text = _decode(f.read())

    sep = _detect_separator(text)
    df = pl.read_csv(
        io.BytesIO(text.encode("utf-8")),
        separator=sep,
        infer_schema=False,            # everything -> String, nothing is guessed
        truncate_ragged_lines=True,
        null_values=None,
    )

    # Normalise header names: trim + upper-case
    df = df.rename({c: c.strip().upper() for c in df.columns})

    # Safety net: guarantee String dtype everywhere
    df = df.with_columns([pl.col(c).cast(pl.String) for c in df.columns])

    sep_name = {"\t": "TAB", ",": "COMMA", ";": "SEMICOLON", "|": "PIPE"}[sep]
    print(f"  {os.path.basename(path)}: {df.height:,} rows, {df.width} cols (sep={sep_name})")
    return df


# ----------------------------------------------------------------------
# STEP 2 - TRIM TEXT, BLANK -> NULL, ADD Id / SourceFile
# ----------------------------------------------------------------------

def clean_text_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.when(pl.col(c).str.strip_chars() == "")
        .then(None)
        .otherwise(pl.col(c).str.strip_chars())
        .alias(c)
        for c in df.columns
    )


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
        # FLIGHTNUMBER -> AIRLINECODE ; FLIGHTNO3 -> AIRLINECODE3
        suffix = col.replace("FLIGHTNUMBER", "").replace("FLIGHTNO", "")

        no_space = pl.col(col).str.replace_all(r"\s+", "")
        cleaned = (
            pl.when(no_space.str.to_lowercase().is_in(NULL_WORDS))
            .then(None)
            .otherwise(no_space)
        )
        # Purely numeric flight numbers (e.g. "32") carry no airline prefix -> NULL code
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


# ----------------------------------------------------------------------
# STEP 5 - INSERT INTO DUCKDB
# ----------------------------------------------------------------------

def insert_into_duckdb(df: pl.DataFrame, db_path: str, table_name: str) -> None:
    validate_table_name(table_name)

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
    files = list_csv_files(CSV_FOLDER, CSV_GLOB)
    print(f"Found {len(files)} CSV file(s) in {CSV_FOLDER}")

    frames: list[pl.DataFrame] = []
    for path in files:
        name = os.path.basename(path)
        df = read_csv_file(path)
        if df.height == 0:
            print(f"  {name}: empty, skipped")
            continue
        df = clean_text_columns(df)
        df = add_uuid_and_source(df, name)
        df = process_flight_columns(df)
        df = parse_dates(df, name)
        frames.append(df)

    if not frames:
        raise RuntimeError("All files were empty - nothing to load.")

    combined = pl.concat(frames, how="diagonal")
    print(f"\nProcessed rows (all files combined): {combined.height:,}")

    insert_into_duckdb(combined, DB_PATH, TABLE_NAME)


if __name__ == "__main__":
    main()