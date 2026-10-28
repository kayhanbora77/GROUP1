"""
process_flight_csv.py  (BlueStar multi-file version)

1. Reads EVERY *.csv file in CSV_FOLDER with Polars (tab / comma / semicolon
   delimiter auto-detected per file).
2. Trims all text, turns blank cells into NULL.
3. Adds a UUID per row ("Id") and the source file name ("SourceFile").
4. Extracts AirlineCode1..4 from FLIGHTNO1..4 and removes whitespace from the
   flight numbers ("6P 419" -> AirlineCode "6P", FLIGHTNO "6P419").
5. Parses dates:
     IDATE        26-Apr-18  (day-Mon-yy)
     FLIGHTDATE*  5/9/2018   (MONTH/DAY/YEAR - confirmed by 4/28/2018 in the data)
6. Loads all files into one DuckDB table (text columns stay VARCHAR).

Configuration is hardcoded below.
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

# Raw string (r"...") - a normal string with \U would be a SyntaxError.
CSV_FOLDER = r"C:\Users\cagri\Desktop\Group1\BlueStar\filter0\CSV"
CSV_GLOB = "*.csv"          # also try "*.txt" if some exports use that extension

DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "BLUESTAR_RAW"

# None = auto-detect per file (tab / comma / semicolon). Or force "\t" / "," / ";".
CSV_SEP = None

TRUNCATE_BEFORE_LOAD = True

FLIGHT_NUMBER_COLS = ["FLIGHTNO1", "FLIGHTNO2", "FLIGHTNO3", "FLIGHTNO4"]

DATE_COLS = ["IDATE", "FLIGHTDATE1", "FLIGHTDATE2", "FLIGHTDATE3", "FLIGHTDATE4"]

# Formats tried in order (first one that parses wins).
IDATE_FORMATS = [
    "%d-%b-%y", "%d-%b-%Y",                      # 26-Apr-18
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d/%m/%Y",
    "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M", "%d-%m-%Y",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
]
FLIGHTDATE_FORMATS = [
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y",   # 5/9/2018 = 9 May 2018
    "%d-%b-%y", "%d-%b-%Y",
    "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
    "%d/%m/%Y",                                          # last resort, only if month-first fails
]
DATE_FORMATS = {col: (IDATE_FORMATS if col == "IDATE" else FLIGHTDATE_FORMATS)
                for col in DATE_COLS}

# These columns must remain VARCHAR in DuckDB.
VARCHAR_COLS = {
    "PAXNAME", "PNRNO", "AIRLINENAME", "TICKETNO",
    "FLIGHTNO1", "FLIGHTNO2", "FLIGHTNO3", "FLIGHTNO4",
    "SNAME", "PAYTYPE", "ILREF",
    "AIRPORT1", "AIRPORT2", "AIRPORT3", "AIRPORT4", "AIRPORT5",   # (comma was missing before)
}

# Decoding is done in Python so every codec name is valid.
ENCODING_CHAIN = ["utf-8-sig", "cp1252", "latin-1"]

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
        raise FileNotFoundError(f"CSV folder not found: {folder}")
    files = sorted(glob.glob(os.path.join(folder, pattern)))
    if not files:
        raise FileNotFoundError(f"No files matching {pattern!r} in {folder}")
    return files


def detect_separator(header_line: str) -> str:
    counts = {"\t": header_line.count("\t"),
              ",": header_line.count(","),
              ";": header_line.count(";")}
    return max(counts, key=counts.get)


def read_csv_with_fallback(path: str, sep: str | None) -> pl.DataFrame:
    """Read every column as String (keeps leading zeros, no numeric inference)."""
    with open(path, "rb") as fh:
        raw = fh.read()

    text, used = None, None
    for enc in ENCODING_CHAIN:
        try:
            text, used = raw.decode(enc), enc
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise RuntimeError(f"Could not decode {path} with {ENCODING_CHAIN}")

    first_line = text.split("\n", 1)[0]
    use_sep = sep or detect_separator(first_line)

    df = pl.read_csv(
        io.BytesIO(text.encode("utf-8")),
        separator=use_sep,
        has_header=True,
        infer_schema=False,                 # everything as String
        quote_char=None if use_sep == "\t" else '"',   # stray " in tab files must not break parsing
        truncate_ragged_lines=True,         # tolerate trailing tabs / short rows
        ignore_errors=False,
        raise_if_empty=False,
    )

    # Normalise header names: trim + upper-case (FlightNo1 / flightno1 -> FLIGHTNO1).
    df = df.rename({c: c.strip().upper() for c in df.columns})

    print(f"  {os.path.basename(path)}: {df.height:,} rows, {df.width} cols, "
          f"encoding={used}, sep={'TAB' if use_sep == chr(9) else use_sep!r}")
    return df


# ----------------------------------------------------------------------
# STEP 2 - TRIM TEXT, BLANK -> NULL, ADD Id / SourceFile
# ----------------------------------------------------------------------

def clean_text_columns(df: pl.DataFrame) -> pl.DataFrame:
    """'PI8Z6K  ' -> 'PI8Z6K'; '   ' -> NULL (AIRPORT3..5, PAYTYPE are often blank)."""
    return df.with_columns(
        pl.when(pl.col(c).str.strip_chars() == "")
        .then(None)
        .otherwise(pl.col(c).str.strip_chars())
        .alias(c)
        for c in df.columns
    )


def add_uuid_and_source(df: pl.DataFrame, source_name: str) -> pl.DataFrame:
    if "Id" in df.columns:
        raise ValueError("Source CSV already contains an 'Id' column.")
    ids = pl.Series("Id", [str(uuid.uuid4()) for _ in range(df.height)], dtype=pl.String)
    df = df.with_columns(ids, pl.lit(source_name).alias("SourceFile"))
    return df.select(["Id", "SourceFile", *[c for c in df.columns if c not in ("Id", "SourceFile")]])


# ----------------------------------------------------------------------
# STEP 3 - AIRLINE CODES + CLEAN FLIGHT NUMBERS  (vectorised, no map_elements)
# ----------------------------------------------------------------------

# IATA designator = 2 chars with at least one letter: 'BA', '6E', '9W', 'G8'
IATA_RE = r"^([A-Za-z][A-Za-z0-9]|[0-9][A-Za-z])"


def process_flight_columns(df: pl.DataFrame) -> pl.DataFrame:
    expressions = []
    for col in FLIGHT_NUMBER_COLS:
        if col not in df.columns:
            raise ValueError(f"Required flight column missing: {col}")
        suffix = col[-1]

        no_space = pl.col(col).str.replace_all(r"\s+", "")
        cleaned = (
            pl.when(no_space.str.to_lowercase().is_in(NULL_WORDS))
            .then(None)
            .otherwise(no_space)
        )
        expressions.append(cleaned.str.extract(IATA_RE, 1).alias(f"AirlineCode{suffix}"))
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

    if df.columns.count("Id") != 1:
        raise ValueError("Expected exactly one Id column.")

    missing = VARCHAR_COLS.difference(df.columns)
    if missing:
        raise ValueError(f"Required VARCHAR columns missing from CSV: {sorted(missing)}")

    con = duckdb.connect(db_path)
    try:
        con.register("flight_data_df", df)
        table_sql = ".".join(quote_identifier(p) for p in table_name.split("."))
        columns_sql = ", ".join(quote_identifier(c) for c in df.columns)
        select_sql = ", ".join(
            f"CAST({quote_identifier(c)} AS VARCHAR) AS {quote_identifier(c)}"
            if c in VARCHAR_COLS else quote_identifier(c)
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

            target_types = {r[0]: r[1].upper()
                            for r in con.execute(f"DESCRIBE SELECT * FROM {table_sql}").fetchall()}
            for col in VARCHAR_COLS:
                if target_types.get(col) != "VARCHAR":
                    raise TypeError(f"{table_name}.{col} must be VARCHAR, "
                                    f"but is {target_types.get(col)!r}.")

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
    print(f"Found {len(files)} file(s) in {CSV_FOLDER}")

    frames: list[pl.DataFrame] = []
    for path in files:
        name = os.path.basename(path)
        df = read_csv_with_fallback(path, CSV_SEP)
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

    # Different files may have slightly different column sets: keep all, fill gaps with NULL.
    combined = pl.concat(frames, how="diagonal")
    print(f"\nProcessed rows (all files): {combined.height:,}")

    insert_into_duckdb(combined, DB_PATH, TABLE_NAME)


if __name__ == "__main__":
    main()