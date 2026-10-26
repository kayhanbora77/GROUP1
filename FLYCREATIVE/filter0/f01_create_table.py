"""
process_flight_csv.py

1. Reads the source CSV using Polars.
2. Adds a UUID per row.
3. Extracts AirlineCode1 / AirlineCode2 from FlightNo1 / FlightNo2.
4. Removes whitespace from flight numbers before inserting.
5. Parses dates safely, prioritizing day-first formats.
6. Inserts the result into DuckDB.
7. Uses Polars instead of pandas.
8. Creates TicketNumber and IATA as VARCHAR in DuckDB.

Configuration is hardcoded below.
"""

import re
import uuid

import duckdb
import polars as pl


# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

CSV_PATH = (
    r"C:\Users\cagri\Desktop\Group1\FlyCreative"
    r"\filter0\FlyCreative Data_FY 2024_25_CSV.csv"
)

DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE_NAME = "FLYCREATIVE_RAW"

# Use "\t" for tab-separated files or "," for comma-separated files.
CSV_SEP = ","

TRUNCATE_BEFORE_LOAD = True

FLIGHT_NUMBER_COLS = ["FlightNo1", "FlightNo2", "FlightNo3", "FlightNo4"]

DATE_COLS = [
    "FlightDate1",
    "FlightDate2",
    "FlightDate3",
    "FlightDate4",
    "TicketDate",
]

# These columns must remain VARCHAR in DuckDB.
VARCHAR_COLS = {
    "TicketNumber",
    "IATA",
    "PaxName",
    "Sector",
    "FlightNo1",
    "FlightNo2",
    "FlightNo3",
    "FlightNo4",
    "FlightSector1",
    "FlightSector2",
    "FlightSector3",
    "FlightSector4",
    "AirlineName",
    "PNR",
    "Airport1",
    "Airport2",
    "Airport3",
    "Airport4",
    "Airport5",
}

# Polars-supported encoding names.
ENCODING_CHAIN = ["utf8", "windows-1252", "iso8859-1"]


# ----------------------------------------------------------------------
# HELPERS
# ----------------------------------------------------------------------

def quote_identifier(name: str) -> str:
    """Safely quote a DuckDB identifier."""
    return '"' + name.replace('"', '""') + '"'


def validate_table_name(name: str) -> None:
    """Accept a single table name, optionally schema-qualified."""
    parts = name.split(".")

    if not parts or any(
        not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part)
        for part in parts
    ):
        raise ValueError(f"Invalid DuckDB table name: {name!r}")


# ----------------------------------------------------------------------
# STEP 1 - READ CSV WITH ENCODING FALLBACK
# ----------------------------------------------------------------------

def read_csv_with_fallback(path: str, sep: str) -> pl.DataFrame:
    """
    Read every source column as String.

    This preserves leading zeros in ticket numbers and IATA values
    and avoids accidental numeric inference.
    """
    if len(sep) != 1:
        raise ValueError("CSV_SEP must be exactly one character.")

    last_err = None

    for encoding in ENCODING_CHAIN:
        try:
            df = pl.read_csv(
                path,
                separator=sep,
                infer_schema=False,
                encoding=encoding,
                try_parse_dates=False,
                ignore_errors=False,
                truncate_ragged_lines=False,
                raise_if_empty=False,
            )

            print(f"CSV encoding: {encoding}")
            print(f"Source rows: {df.height:,}")
            print(f"Source columns: {df.width:,}")

            return df

        except (UnicodeError, OSError, pl.exceptions.PolarsError) as exc:
            last_err = exc

    raise RuntimeError(
        f"Could not read CSV using encodings {ENCODING_CHAIN}: "
        f"{last_err}"
    )


# ----------------------------------------------------------------------
# STEP 2 - ADD UUID PER ROW
# ----------------------------------------------------------------------

def add_uuid_column(df: pl.DataFrame) -> pl.DataFrame:
    if "Id" in df.columns:
        raise ValueError(
            "Source CSV already contains an 'Id' column. "
            "Rename or remove it before generating UUIDs."
        )

    ids = pl.Series(
        "Id",
        [str(uuid.uuid4()) for _ in range(df.height)],
        dtype=pl.String,
    )

    return df.insert_column(0, ids)


# ----------------------------------------------------------------------
# STEP 3 - EXTRACT AIRLINE CODES AND CLEAN FLIGHT NUMBERS
# ----------------------------------------------------------------------

def extract_airline_code(flight_number: str | None) -> str | None:
    """
    Examples:
        'BA 2065' -> 'BA'
        'BA2065'  -> 'BA'
        '6E 1342' -> '6E'
        ''        -> None
    """
    if flight_number is None:
        return None

    value = flight_number.strip()

    if not value or value.lower() in {"nan", "none", "null"}:
        return None

    if any(char.isspace() for char in value):
        return value.split()[0]

    match = re.match(r"^([A-Za-z]+)", value)

    return match.group(1) if match else value


def clean_flight_number(flight_number: str | None) -> str | None:
    """
    Remove whitespace from flight numbers.

    'BA 2065' -> 'BA2065'
    '6E 1342' -> '6E1342'
    ''        -> None
    """
    if flight_number is None:
        return None

    value = flight_number.strip()

    if not value or value.lower() in {"nan", "none", "null"}:
        return None

    return re.sub(r"\s+", "", value)


def process_flight_columns(df: pl.DataFrame) -> pl.DataFrame:
    expressions = []

    for col in FLIGHT_NUMBER_COLS:
        if col not in df.columns:
            raise ValueError(f"Required flight column missing: {col}")

        suffix = col[-1]

        expressions.extend([
            pl.col(col)
            .map_elements(
                extract_airline_code,
                return_dtype=pl.String,
            )
            .alias(f"AirlineCode{suffix}"),

            pl.col(col)
            .map_elements(
                clean_flight_number,
                return_dtype=pl.String,
            )
            .alias(col),
        ])

    return df.with_columns(expressions)


# ----------------------------------------------------------------------
# STEP 4 - PARSE DATES, DAY-FIRST AND BLANK-SAFE
# ----------------------------------------------------------------------

def parse_dates(df: pl.DataFrame) -> pl.DataFrame:
    """
    Accept common date formats, prioritizing day-first interpretation.

    Examples:
        31/12/2024
        1/2/2025
        31-12-2024
        2025-12-31
        31/12/2024 14:30:00

    Blank or whitespace-only values become NULL.
    Invalid nonblank values are reported.
    """
    expressions = []

    date_formats = [
        "%d-%b-%y",  # 1-Apr-24 -> 2024-04-01
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]
    for col in DATE_COLS:
        if col not in df.columns:
            continue

        raw = pl.col(col).cast(pl.String).str.strip_chars()
        normalized = pl.when(
            raw == ""
        ).then(
            pl.lit(None, dtype=pl.String)
        ).otherwise(raw)

        parsed_candidates = [
            normalized.str.strptime(
                pl.Datetime,
                format=fmt,
                strict=False,
                exact=True,
            )
            for fmt in date_formats
        ]

        parsed = pl.coalesce(parsed_candidates)

        expressions.append(
            parsed.alias(f"__parsed_{col}")
        )

    if not expressions:
        return df

    result = df.with_columns(expressions)

    for col in DATE_COLS:
        if col not in df.columns:
            continue

        raw = pl.col(col).cast(pl.String).str.strip_chars()
        parsed_col = f"__parsed_{col}"

        bad = result.filter(
            raw.is_not_null()
            & (raw != "")
            & pl.col(parsed_col).is_null()
        ).select(col)

        if bad.height:
            print(
                f"\nWARNING: {bad.height:,} invalid date value(s) "
                f"in {col}:"
            )
            print(bad.head(20))

            if bad.height > 20:
                print("Only the first 20 invalid values are displayed.")

    # Replace original date strings with parsed datetime columns.
    for col in DATE_COLS:
        parsed_col = f"__parsed_{col}"

        if parsed_col in result.columns:
            result = result.with_columns(
                pl.col(parsed_col).alias(col)
            ).drop(parsed_col)

    return result


# ----------------------------------------------------------------------
# STEP 5 - INSERT INTO DUCKDB
# ----------------------------------------------------------------------

def insert_into_duckdb(
    df: pl.DataFrame,
    db_path: str,
    table_name: str,
) -> None:
    validate_table_name(table_name)

    if df.columns.count("Id") != 1:
        raise ValueError("Expected exactly one Id column.")

    missing_varchar_cols = VARCHAR_COLS.difference(df.columns)

    if missing_varchar_cols:
        raise ValueError(
            "Required VARCHAR columns missing from CSV: "
            f"{sorted(missing_varchar_cols)}"
        )

    con = duckdb.connect(db_path)

    try:
        con.register("flight_data_df", df)

        table_sql = ".".join(
            quote_identifier(part) for part in table_name.split(".")
        )

        columns_sql = ", ".join(
            quote_identifier(col) for col in df.columns
        )

        select_expressions = []

        for col in df.columns:
            quoted = quote_identifier(col)

            if col in VARCHAR_COLS:
                select_expressions.append(
                    f"CAST({quoted} AS VARCHAR) AS {quoted}"
                )
            else:
                select_expressions.append(quoted)

        select_sql = ", ".join(select_expressions)

        con.execute("BEGIN TRANSACTION")

        try:
            if TRUNCATE_BEFORE_LOAD:
                con.execute(f"DROP TABLE IF EXISTS {table_sql}")

            con.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_sql} AS
                SELECT {select_sql}
                FROM flight_data_df
                WHERE FALSE
                """
            )

            # Confirm that existing target columns have the required types.
            schema = con.execute(
                f"DESCRIBE SELECT * FROM {table_sql}"
            ).fetchall()

            target_types = {
                row[0]: row[1].upper()
                for row in schema
            }

            for col in VARCHAR_COLS:
                if target_types.get(col) != "VARCHAR":
                    raise TypeError(
                        f"{table_name}.{col} must be VARCHAR, "
                        f"but its current type is "
                        f"{target_types.get(col)!r}. "
                        "Recreate or migrate the target table."
                    )

            con.execute(
                f"""
                INSERT INTO {table_sql} ({columns_sql})
                SELECT {select_sql}
                FROM flight_data_df
                """
            )

            inserted_count = con.execute(
                f"SELECT COUNT(*) FROM {table_sql}"
            ).fetchone()[0]

            if inserted_count != df.height:
                raise RuntimeError(
                    f"Row-count mismatch: source={df.height:,}, "
                    f"target={inserted_count:,}"
                )

            con.execute("COMMIT")

        except Exception:
            con.execute("ROLLBACK")
            raise

        print(f"\nSuccessfully loaded: {table_name}")
        print(f"Source rows:   {df.height:,}")
        print(f"Target rows:   {inserted_count:,}")
        print(f"TicketNumber:  VARCHAR")
        print(f"IATA:          VARCHAR")

    finally:
        con.close()


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main() -> None:
    df = read_csv_with_fallback(CSV_PATH, CSV_SEP)

    df = add_uuid_column(df)
    df = process_flight_columns(df)
    df = parse_dates(df)

    print(f"\nProcessed rows: {df.height:,}")

    insert_into_duckdb(
        df=df,
        db_path=DB_PATH,
        table_name=TABLE_NAME,
    )


if __name__ == "__main__":
    main()