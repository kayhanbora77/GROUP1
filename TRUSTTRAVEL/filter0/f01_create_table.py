
import glob
import os
import re
import uuid

import duckdb
import polars as pl


# ----------------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------------

EXCEL_FOLDER = (
    r"C:\Users\cagri\Desktop\Group1_NewAnalyse\TrustTravel\filter0"
)

DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "TRUSTTRAVEL_RAW"

TRUNCATE_BEFORE_LOAD = True

# All names are normalized to uppercase.
FLIGHT_NUMBER_COLS = [
    "FLIGHTNO1",
    "FLIGHTNO2",
    "FLIGHTNO3",
    "FLIGHTNO4",
]

DATE_COLS = [
    "BILLDATE",
    "FLIGHTDATE1",
    "FLIGHTDATE2",
    "FLIGHTDATE3",
    "FLIGHTDATE4",
]

# IMPORTANT:
# Your previous comment says 5/9/2018 means 9 May 2018.
# Therefore, the default slash-date convention is DD/MM/YYYY.
#
# If your source uses US dates (May 9, 2018), change this to:
# "%m/%d/%Y"

AMBIGUOUS_SLASH_DATE_FORMAT = "%d/%m/%Y"

DATE_FORMATS = [
    "%d-%b-%y",                 # 1-Jan-22
    "%d-%b-%Y",                 # 1-Jan-2022

    "%Y-%m-%d",                 # 2025-04-01
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%d %H:%M",

    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",

    AMBIGUOUS_SLASH_DATE_FORMAT,
]

NULL_WORDS = ["nan", "none", "null"]

IATA_RE = r"^([A-Z0-9]{2})"


# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------

def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def validate_table_name(name: str) -> None:
    parts = name.split(".")

    if not parts or any(
        not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p)
        for p in parts
    ):
        raise ValueError(f"Invalid DuckDB table name: {name!r}")


# ----------------------------------------------------------------------
# STEP 1 - FIND ALL EXCEL FILES
# ----------------------------------------------------------------------

def list_excel_files(folder: str) -> list[str]:

    if not os.path.isdir(folder):
        raise FileNotFoundError(f"Folder not found: {folder}")

    files = []

    for extension in ("*.xlsx", "*.xls"):
        files.extend(
            glob.glob(os.path.join(folder, extension))
        )

    files = sorted(set(files))

    if not files:
        raise FileNotFoundError(
            f"No Excel files found in {folder}"
        )

    return files


# ----------------------------------------------------------------------
# STEP 2 - READ EVERY WORKSHEET
# ----------------------------------------------------------------------

def read_excel_sheets(path: str) -> dict[str, pl.DataFrame]:
    """
    Read ALL worksheets using Polars + Calamine.

    sheet_id=0 means all worksheets.
    """

    sheets = pl.read_excel(
        path,
        engine="calamine",
        sheet_id=0,
    )

    # Polars returns a dictionary when all sheets are requested.
    if isinstance(sheets, pl.DataFrame):
        sheets = {"Sheet1": sheets}

    if not isinstance(sheets, dict):
        raise TypeError(
            f"Unexpected Excel result for {path}: {type(sheets)}"
        )

    result = {}

    for sheet_name, df in sheets.items():

        # Normalize column names.
        df = df.rename({
            c: c.strip().upper()
            for c in df.columns
        })

        # Reject duplicate column names after normalization.
        if len(df.columns) != len(set(df.columns)):
            raise ValueError(
                f"Duplicate column names after normalization: "
                f"{path} / {sheet_name}"
            )

        # Prevent reserved-column collisions.
        reserved = {"ID", "SOURCEFILE", "SOURCESHEET"}

        collisions = reserved.intersection(df.columns)

        if collisions:
            raise ValueError(
                f"Reserved columns already exist: {collisions} "
                f"in {path} / {sheet_name}"
            )

        # Cast all imported values to String.
        # Note: Excel must store identifiers as text to preserve
        # original leading zeros and digits beyond Excel's precision.
        df = df.with_columns([
            pl.col(c).cast(pl.String, strict=False).alias(c)
            for c in df.columns
        ])

        print(
            f"  {os.path.basename(path)} | "
            f"{sheet_name}: {df.height:,} rows, "
            f"{df.width} columns"
        )

        result[sheet_name] = df

    return result


# ----------------------------------------------------------------------
# STEP 3 - CLEAN TEXT
# ----------------------------------------------------------------------

def clean_text_columns(df: pl.DataFrame) -> pl.DataFrame:

    expressions = []

    for col in df.columns:

        cleaned = (
            pl.col(col)
            .str.replace_all("\u00A0", " ")
            .str.strip_chars()
        )

        expressions.append(
            pl.when(
                cleaned == ""
            )
            .then(None)
            .when(
                cleaned.str.to_lowercase().is_in(NULL_WORDS)
            )
            .then(None)
            .otherwise(cleaned)
            .alias(col)
        )

    return df.with_columns(expressions)


# ----------------------------------------------------------------------
# STEP 4 - ADD UUID AND SOURCE INFORMATION
# ----------------------------------------------------------------------

def add_uuid_and_source(
    df: pl.DataFrame,
    source_name: str,
    sheet_name: str,
) -> pl.DataFrame:

    ids = pl.Series(
        "ID",
        [str(uuid.uuid4()) for _ in range(df.height)],
        dtype=pl.String,
    )

    df = df.with_columns([
        ids,
        pl.lit(source_name).alias("SOURCEFILE"),
        pl.lit(sheet_name).alias("SOURCESHEET"),
    ])

    ordered = [
        "ID",
        "SOURCEFILE",
        "SOURCESHEET",
    ]

    ordered.extend([
        c for c in df.columns
        if c not in ordered
    ])

    return df.select(ordered)



# ----------------------------------------------------------------------
# STEP 5 - NORMALIZE FLIGHT NUMBERS + AIRLINE CODES
# ----------------------------------------------------------------------

def process_flight_columns(df: pl.DataFrame) -> pl.DataFrame:

    expressions = []

    for col in FLIGHT_NUMBER_COLS:

        if col not in df.columns:
            continue

        suffix = col.replace("FLIGHTNO", "")

        # Remove all whitespace.
        no_space = (
            pl.col(col)
            .str.replace_all(r"\s+", "")
            .str.to_uppercase()
        )

        # Convert blank / NULL words to NULL.
        cleaned = (
            pl.when(
                no_space.is_null()
                | (no_space == "")
                | no_space.str.to_lowercase().is_in(NULL_WORDS)
            )
            .then(None)
            .otherwise(no_space)
        )

        # Split airline code and numeric flight number.
        airline = cleaned.str.extract(
            r"^([A-Z0-9]{2})",
            1
        )

        flight_digits = cleaned.str.extract(
            r"^[A-Z0-9]{2}([0-9]+)$",
            1
        )

        # Remove leading zeros.
        normalized_digits = (
            flight_digits.str.replace(r"^0+", "")
        )

        # Preserve a zero if the entire numeric portion is zero.
        normalized_digits = (
            pl.when(normalized_digits == "")
            .then(pl.lit("0"))
            .otherwise(normalized_digits)
        )

        # Rebuild normalized flight number.
        normalized = (
            pl.when(
                airline.is_not_null()
                & flight_digits.is_not_null()
            )
            .then(
                airline + normalized_digits
            )
            .otherwise(cleaned)
        )

        expressions.extend([
            normalized.alias(col),
            airline.alias(f"AIRLINECODE{suffix}"),
        ])

    if expressions:
        df = df.with_columns(expressions)

    return df

# ----------------------------------------------------------------------
# STEP 6 - PARSE DATES
# ----------------------------------------------------------------------

def parse_dates(
    df: pl.DataFrame,
    source_name: str,
    sheet_name: str,
) -> pl.DataFrame:

    for col in DATE_COLS:

        if col not in df.columns:
            continue

        original = pl.col(col)

        parsed = pl.coalesce([
            original.str.strptime(
                pl.Datetime,
                format=fmt,
                strict=False,
                exact=True,
            )
            for fmt in DATE_FORMATS
        ])

        temp_col = "__PARSED_DATE"

        result = df.with_columns(
            parsed.alias(temp_col)
        )

        # Identify non-empty values that could not be parsed.
        bad = result.filter(
            original.is_not_null()
            & pl.col(temp_col).is_null()
        )

        if bad.height:

            print(
                f"\nWARNING: Invalid dates detected"
                f"\nFile: {source_name}"
                f"\nSheet: {sheet_name}"
                f"\nColumn: {col}"
                f"\nInvalid rows: {bad.height:,}"
            )

            print(
                bad.select(col)
                .unique()
                .head(20)
            )

        df = (
            result
            .with_columns(
                pl.col(temp_col).alias(col)
            )
            .drop(temp_col)
        )

    return df


# ----------------------------------------------------------------------
# STEP 7 - LOAD INTO DUCKDB
# ----------------------------------------------------------------------

def insert_into_duckdb(
    df: pl.DataFrame,
    db_path: str,
    table_name: str,
) -> None:

    validate_table_name(table_name)

    con = duckdb.connect(db_path)

    try:

        con.register("flight_data_df", df)

        table_sql = ".".join(
            quote_identifier(p)
            for p in table_name.split(".")
        )

        columns_sql = ", ".join(
            quote_identifier(c)
            for c in df.columns
        )

        con.execute("BEGIN TRANSACTION")

        try:

            if TRUNCATE_BEFORE_LOAD:

                con.execute(
                    f"DROP TABLE IF EXISTS {table_sql}"
                )

            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_sql} AS
                SELECT *
                FROM flight_data_df
                WHERE FALSE
                """
            )

            before = con.execute(
                f"SELECT COUNT(*) FROM {table_sql}"
            ).fetchone()[0]

            con.execute(
                f"""
                INSERT INTO {table_sql} ({columns_sql})
                SELECT {columns_sql}
                FROM flight_data_df
                """
            )

            after = con.execute(
                f"SELECT COUNT(*) FROM {table_sql}"
            ).fetchone()[0]

            inserted = after - before

            if inserted != df.height:
                raise RuntimeError(
                    f"Row-count mismatch: "
                    f"source={df.height:,}, "
                    f"inserted={inserted:,}"
                )

            con.execute("COMMIT")

        except Exception:

            con.execute("ROLLBACK")
            raise

        print("\nSuccessfully loaded into DuckDB")
        print(f"Table: {table_name}")
        print(f"Source rows: {df.height:,}")
        print(f"Inserted rows: {inserted:,}")
        print(f"Total target rows: {after:,}")

        # Show parsed date column types.
        print("\nDuckDB column types:")

        for row in con.execute(
            f"DESCRIBE {table_sql}"
        ).fetchall():

            if row[0] in DATE_COLS:
                print(f"  {row[0]}: {row[1]}")

    finally:

        con.close()


# ----------------------------------------------------------------------
# STEP 8 - MAIN
# ----------------------------------------------------------------------

def main():

    files = list_excel_files(EXCEL_FOLDER)

    print("=" * 70)
    print(f"Excel files found: {len(files)}")
    print("=" * 70)

    frames = []

    total_sheets = 0
    total_rows = 0

    for path in files:

        filename = os.path.basename(path)

        print(f"\nProcessing workbook: {filename}")

        sheets = read_excel_sheets(path)

        for sheet_name, df in sheets.items():

            total_sheets += 1

            if df.height == 0:

                print(
                    f"  Empty worksheet skipped: {sheet_name}"
                )
                continue

            df = clean_text_columns(df)

            df = add_uuid_and_source(
                df,
                filename,
                sheet_name,
            )

            df = process_flight_columns(df)

            df = parse_dates(
                df,
                filename,
                sheet_name,
            )

            total_rows += df.height

            frames.append(df)

    if not frames:

        raise RuntimeError(
            "All Excel worksheets were empty. Nothing to load."
        )

    # Support different columns in different worksheets.
    combined = pl.concat(
        frames,
        how="diagonal_relaxed",
    )

    print("\n" + "=" * 70)
    print("EXCEL PROCESSING SUMMARY")
    print("=" * 70)

    print(f"Excel files: {len(files):,}")
    print(f"Worksheets: {total_sheets:,}")
    print(f"Processed rows: {total_rows:,}")
    print(f"Combined rows: {combined.height:,}")
    print(f"Combined columns: {combined.width:,}")

    if combined.height != total_rows:
        raise RuntimeError(
            "Row count mismatch during concatenation."
        )

    insert_into_duckdb(
        combined,
        DB_PATH,
        TABLE_NAME,
    )


if __name__ == "__main__":
    main()
