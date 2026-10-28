"""
Flight Row Splitter  —  optimized for 5M+ rows
================================================
Rules
-----
1. count(FlightNo) > count(FlightDate)  → insert into TBO_REJECTION
2. count(FlightNo) <= count(FlightDate) → trim to count(FlightNo) flights/dates,
                                           keep other columns (DAIS, TRNN, etc.) unchanged
3. count(Airport)  = count(FlightNo) + 1 (trim airports accordingly)
4. Any consecutive date gap > 1 day     → split at that boundary
   All consecutive date gaps <= 1 day   → do NOT split
5. Use polars instead of pandas   
"""

import duckdb
import uuid
import os
import time
import polars as pl
import re
import math
from datetime import datetime

# ============================================================================
# CONFIG
# ============================================================================

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "BLUESTAR_RAW"
TARGET_TABLE = "BLUESTAR_SPLIT"
REJECT_TABLE = "BLUESTAR_REJECT"

MAX_FLIGHTS = 4
MAX_DATES = 4
MAX_AIRPORTS = 5

BATCH_SIZE = 200_000

FLIGHT_PREFIX = "FLIGHTNO"
DATE_PREFIX = "FLIGHTDATE"
AIRPORT_PREFIX ="AIRPORT"

# ============================================================================
# COLUMN LISTS  —  match THOMASCOOK_RAW schema exactly
# ============================================================================

FLIGHT_COLS = [f"{FLIGHT_PREFIX}{i + 1}" for i in range(MAX_FLIGHTS)]  # FlightNo1..4
DATE_COLS = [
    f"{DATE_PREFIX}{i + 1}" for i in range(MAX_DATES)
]  # DepartureDate1..4
AIRPORT_COLS = [f"{AIRPORT_PREFIX}{i + 1}" for i in range(MAX_AIRPORTS)]  # Airport1..5
DYNAMIC_COLS = FLIGHT_COLS + DATE_COLS + AIRPORT_COLS

_RE_FLTNO = re.compile(r"^[A-Z]{2,3}\d+$")  # Fixed: only letters, not alphanumeric
_RE_SCI_NOTATION = re.compile(r"^(\d+)(?:\.0+)?E\+?(\d+)$", re.IGNORECASE)
_RE_FLTNO_WITH_SPACE = re.compile(r"^([A-Z]{1,3})\s+(\d+)$", re.IGNORECASE)  # Handle "G 217", "K 1475"

STATIC_COLS = [
    "SourceFile",
    "IDATE",
	"PAXNAME",
	"PNRNO",
	"AIRLINENAME",
	"TICKETNO",
    "SNAME",
    "PAYTYPE",
    "ILREF",
    "AirlineCode1"
]

COL_IDX: dict = {}
# ============================================================================
# HELPERS
# ============================================================================


def is_null_value(val) -> bool:
    """Pandas-free null check for values fetched from DuckDB."""
    if val is None:
        return True
    if isinstance(val, float) and math.isnan(val):
        return True
    # Polars/Arrow may expose NaT-like values as strings in source data.
    return False


def clean_null_value(val):
    """Convert null-like text sentinels to Python None before building Polars frames."""
    if isinstance(val, uuid.UUID):
        return str(val)
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    if isinstance(val, str) and val.strip().lower() in ("", "nat", "none", "nan", "null"):
        return None
    return val


def _isna(val) -> bool:
    """True if val is None, NaN, None, or blank/NaT string after strip."""
    if val is None or is_null_value(val):
        return True
    if isinstance(val, str) and val.strip().lower() in ("", "nat", "none", "nan"):
        return True
    return False


def is_valid(val):
    if val is None or is_null_value(val):
        return False
    if isinstance(val, str):
        s = val.strip().lower()
        return s not in ("", "nat", "none", "nan")
    return True


def _fix_scientific_notation(fn: str) -> str:
    """Collapse Excel-mangled scientific notation, e.g. '6.00E+78' -> '6E78'."""
    if not isinstance(fn, str):
        fn = str(fn)
    m = _RE_SCI_NOTATION.fullmatch(fn.strip())
    if not m:
        return fn
    mantissa, exponent = m.group(1), m.group(2)
    return f"{mantissa}E{exponent}"


def normalize_flight_numbers(row: dict) -> dict:
    """
    Normalize flight numbers:
    - Remove spaces: "G 217" -> "G217", "K 1475" -> "K1475"
    - Strip leading zeros: "SV0020" -> "SV20"
    - Fix scientific notation: "6.00E+78" -> "6E78"
    """
    row = dict(row)
    for i in range(1, MAX_FLIGHTS + 1):
        fn = row.get(f"{FLIGHT_PREFIX}{i}")
        if not _isna(fn):
            fn_str = str(fn).strip()
            
            # Fix scientific notation first
            fn_str = _fix_scientific_notation(fn_str)
            
            # Remove spaces between letters and numbers: "G 217" -> "G217"
            fn_str = re.sub(r'\s+', '', fn_str)
            
            fn_upper = fn_str.upper()
            
            # Check if it matches the pattern with space removed
            if _RE_FLTNO.fullmatch(fn_upper):
                row[f"{FLIGHT_PREFIX}{i}"] = _normalize_flightno(fn_upper)
            else:
                # Try to extract airline code and number even if format is slightly off
                normalized = _normalize_any_flightno(fn_upper)
                if normalized:
                    row[f"{FLIGHT_PREFIX}{i}"] = normalized
                else:
                    row[f"{FLIGHT_PREFIX}{i}"] = fn_upper
    return row


def _normalize_flightno(fn: str) -> str:
    """
    Remove leading zeros between the alphabetic prefix and the numeric suffix.
    E.g.  SV0020 → SV20,  AF0459 → AF459,  EK001 → EK1
    """
    m = re.fullmatch(r"([A-Z]{2,3})(\d+)", fn.upper().strip())
    if not m:
        return fn
    prefix, digits = m.group(1), m.group(2)
    normalized_digits = digits.lstrip("0") or "0"
    return prefix + normalized_digits


def _normalize_any_flightno(fn: str) -> str:
    """
    Normalize any flight number format:
    - "G217" -> "G217" (already normalized)
    - "G 217" -> "G217"
    - "K1475" -> "K1475"
    - "SV0020" -> "SV20"
    - "EK001" -> "EK1"
    """
    if not fn:
        return None
    
    # Remove all spaces
    fn = re.sub(r'\s+', '', fn)
    
    # Try to match pattern: letters followed by digits
    m = re.fullmatch(r"([A-Z]{1,3})(\d+)", fn)
    if m:
        prefix, digits = m.group(1), m.group(2)
        # Remove leading zeros from digits
        normalized_digits = digits.lstrip("0") or "0"
        return prefix + normalized_digits
    
    return None


def parse_dt(val):
    """
    Robustly parse datetime strings. 
    Handles formats like '2019-01-01 - 00:45' by extracting the YYYY-MM-DD prefix.
    """
    if val is None or is_null_value(val):
        return None
    if isinstance(val, datetime):
        return val
    if hasattr(val, "year") and hasattr(val, "month") and hasattr(val, "day"):
        return datetime(val.year, val.month, val.day)
    
    s = str(val).strip()
    if not s or s.lower() in ("nat", "none", "nan"):
        return None

    # 1. Extract YYYY-MM-DD using regex to handle formats like "2019-01-01 - 00:45"
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d")
        except ValueError:
            pass

    # 2. Fallback for other standard formats
    s_trimmed = s[:19]
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%d - %H:%M"):
        try:
            return datetime.strptime(s_trimmed, fmt)
        except ValueError:
            pass
            
    return None


def day_gap(d1, d2):
    a, b = parse_dt(d1), parse_dt(d2)
    if a is None or b is None:
        return None
    return abs((b.date() - a.date()).days)


# ============================================================================
# RULE 1 & 2: count flights vs dates, then trim
# ============================================================================

def extract_and_validate(row_list):
    """
    Returns:
        flights   — list of (FlightNo, DepartureDate) tuples [Rule 2]
        airports  — list of airport strings                            [Rule 3]
        cnt_no    — count of flights
        is_reject — boolean indicating if row should be rejected      [Rule 1]
        reject_reason — reason string if rejected, else None
    """
    flight_nos = []
    for i in range(MAX_FLIGHTS):
        v = row_list[COL_IDX[f"{FLIGHT_PREFIX}{i + 1}"]]
        if is_valid(v):
            flight_nos.append(str(v).strip())

    flight_dates = []
    for i in range(MAX_DATES):
        v = row_list[COL_IDX[f"{DATE_PREFIX}{i + 1}"]]
        if is_valid(v):
            flight_dates.append(str(v).strip())

    cnt_no = len(flight_nos)
    cnt_date = len(flight_dates)

    # Rule 1: reject if more flights than dates
    if cnt_no > cnt_date:
        return None, None, cnt_no, True, f"ROUTE_OVERFLOW: {cnt_no} flights > {cnt_date} dates"

    # Rule 2: trim dates to match flight count
    trimmed_dates = flight_dates[:cnt_no]
    flights = list(zip(flight_nos, trimmed_dates))

    # Rule 3: trim airports to flight count + 1
    all_airports = []
    for i in range(MAX_AIRPORTS):
        v = row_list[COL_IDX[f"{AIRPORT_PREFIX}{i + 1}"]]
        if is_valid(v):
            all_airports.append(str(v).strip())

    airports = all_airports[:cnt_no + 1] if cnt_no > 0 else []

    # NEW: Reject if no flight numbers at all (but dates/airports may exist)
    if cnt_no == 0:
        return None, None, 0, True, "NO_VALID_FLIGHT_NUMBERS"

    return flights, airports, cnt_no, False, None


# ============================================================================
# RULE 4: split-point detection (date gap only)
# ============================================================================


def find_split_points(flights):
    split_points = []
    for i in range(len(flights) - 1):
        gap = day_gap(flights[i][1], flights[i + 1][1])
        if gap is not None and gap > 1:
            split_points.append(i + 1)
    return split_points


# ============================================================================
# BUILD CHILD ROW
# ============================================================================


def build_child_row(parent_list, flights_slice, airports_slice, parent_id):
    """Clone parent, clear dynamic cols, fill in segment data."""
    child = list(parent_list)

    for c in DYNAMIC_COLS:
        child[COL_IDX[c]] = None

    for i, (fn, fd) in enumerate(flights_slice):
        child[COL_IDX[f"{FLIGHT_PREFIX}{i + 1}"]] = fn
        child[COL_IDX[f"{DATE_PREFIX}{i + 1}"]] = fd

    for i, ap in enumerate(airports_slice):
        child[COL_IDX[f"{AIRPORT_PREFIX}{i + 1}"]] = ap

    child[COL_IDX["Id"]] = str(uuid.uuid4())
    child[COL_IDX["ParentId"]] = str(parent_id)

    return child


# ============================================================================
# BATCH PROCESSOR
# ============================================================================


def process_batch(rows_df, all_cols):
    unsplit_rows = []
    child_rows = []
    rejection_rows = []
    rejection_reasons = []  # NEW: parallel list for reasons

    records = rows_df

    for row_list in records:
        # Rule 7: normalize FlightNumber values
        row_dict = dict(zip(all_cols, row_list))
        row_dict = normalize_flight_numbers(row_dict)
        row_list = [clean_null_value(row_dict[c]) for c in all_cols]

        flights, airports, cnt_no, is_reject, reject_reason = extract_and_validate(row_list)

        # Rule 1 (or no flights): reject
        if is_reject:
            rejection_rows.append(list(row_list))
            rejection_reasons.append(reject_reason)  # NEW
            continue

        # Create trimmed row with only valid flights/dates/airports
        trimmed_row = list(row_list)
        for c in DYNAMIC_COLS:
            trimmed_row[COL_IDX[c]] = None
            
        for i, (fn, fd) in enumerate(flights):
            trimmed_row[COL_IDX[f"{FLIGHT_PREFIX}{i + 1}"]] = fn
            trimmed_row[COL_IDX[f"{DATE_PREFIX}{i + 1}"]] = fd
            
        for i, ap in enumerate(airports):
            trimmed_row[COL_IDX[f"{AIRPORT_PREFIX}{i + 1}"]] = ap

        # Rule 4: check for splits
        split_points = find_split_points(flights)

        if not split_points:
            unsplit_rows.append(trimmed_row)
            continue

        # Split the row
        parent_id = row_list[COL_IDX["Id"]]
        boundaries = [0] + split_points + [len(flights)]

        for k in range(len(boundaries) - 1):
            f_start = boundaries[k]
            f_end = boundaries[k + 1]
            a_start = f_start
            a_end = f_end + 1

            seg_flights = flights[f_start:f_end]
            seg_airports = airports[a_start:min(a_end, len(airports))]

            if not seg_flights:
                continue

            child_rows.append(
                build_child_row(trimmed_row, seg_flights, seg_airports, parent_id)
            )

    empty = pl.DataFrame(schema=all_cols)
    unsplit_df = pl.DataFrame(unsplit_rows, schema=all_cols, orient="row", infer_schema_length=None) if unsplit_rows else empty
    children_df = pl.DataFrame(child_rows, schema=all_cols, orient="row", infer_schema_length=None) if child_rows else empty
    
    # Build rejection_df with reasons
    if rejection_rows:
        rejection_df = pl.DataFrame(rejection_rows, schema=all_cols, orient="row", infer_schema_length=None)
        rejection_df = rejection_df.with_columns(pl.Series("RejectionReason", rejection_reasons, dtype=pl.String))
    else:
        rejection_df = pl.DataFrame(schema=all_cols + ["RejectionReason"])

    # Clean None and string "NaT" values so DuckDB receives proper NULLs
    for df in (unsplit_df, children_df, rejection_df):
        if not df.is_empty():
            pass  # NULL-like source values are normalized before output-frame creation.

    return unsplit_df, children_df, rejection_df

# ============================================================================
# DB HELPERS
# ============================================================================


def col_names(con, table):
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}' ORDER BY ordinal_position"
    ).fetchall()
    return [r[0] for r in rows]


def ensure_parent_id_column(con, table):
    cols = col_names(con, table)
    if "ParentId" not in cols:
        con.execute(f'ALTER TABLE "{table}" ADD COLUMN "ParentId" UUID')
        print("  Added ParentId column.")


def ensure_target_table(con, source_table, target_table):
    con.execute(f'DROP TABLE IF EXISTS "{target_table}"')
    con.execute(
        f'CREATE TABLE "{target_table}" AS SELECT * FROM "{source_table}" WHERE 1=0'
    )
    print(f"  Recreated table '{target_table}'.")


def ensure_rejection_table(con, source_table, reject_table):
    con.execute(f'DROP TABLE IF EXISTS "{reject_table}"')
    con.execute(
        f'CREATE TABLE "{reject_table}" AS SELECT * FROM "{source_table}" WHERE 1=0'
    )
    con.execute(f'ALTER TABLE "{reject_table}" ADD COLUMN "RejectionReason" VARCHAR')
    print(f"  Recreated table '{reject_table}' with RejectionReason column.")


# ============================================================================
# MAIN
# ============================================================================


def process_table(db_path=DB_PATH, table=SOURCE_TABLE, batch_size=BATCH_SIZE):
    con = duckdb.connect(db_path)
    con.execute(f"PRAGMA threads={os.cpu_count()}")
    try:
        con.execute("SET memory_limit='16GB'")
    except Exception:
        pass

    ensure_parent_id_column(con, table)
    ensure_target_table(con, table, TARGET_TABLE)
    ensure_rejection_table(con, table, REJECT_TABLE)

    all_cols = col_names(con, table)

    global COL_IDX
    COL_IDX = {c: i for i, c in enumerate(all_cols)}

    col_list = ", ".join(f'"{c}"' for c in all_cols)
    total = con.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]

    unsplit_total = 0
    split_count = 0
    child_count = 0
    reject_count = 0
    t0 = time.time()

    cursor = con.cursor()
    cursor.execute(f'SELECT {col_list} FROM "{table}" WHERE "ParentId" IS NULL')

    while True:
        raw = cursor.fetchmany(batch_size)
        if not raw:
            break

        unsplit_df, children_df, rejection_df = process_batch(raw, all_cols)

        if not unsplit_df.is_empty():
            con.execute(
                f'INSERT INTO "{TARGET_TABLE}" ({col_list}) SELECT * FROM unsplit_df'
            )
            unsplit_total += len(unsplit_df)

        if not children_df.is_empty():
            con.execute(
                f'INSERT INTO "{TARGET_TABLE}" ({col_list}) SELECT * FROM children_df'
            )
            child_count += len(children_df)
            split_count += children_df.get_column("ParentId").n_unique()

        if not rejection_df.is_empty():
            rej_cols = all_cols + ["RejectionReason"]
            rej_col_list = ", ".join(f'"{c}"' for c in rej_cols)
            con.execute(
                f'INSERT INTO "{REJECT_TABLE}" ({rej_col_list}) SELECT * FROM rejection_df'
            )
            reject_count += len(rejection_df)
            
        elapsed = time.time() - t0
        scanned = unsplit_total + split_count + reject_count
        rate = scanned / elapsed if elapsed > 0 else 0
        print(
            f"  {scanned:>10,} / {total:,} scanned  |"
            f"  {split_count:>6,} splits  |"
            f"  {child_count:>8,} children  |"
            f"  {reject_count:>6,} rejected  |  {rate:>8,.0f} rows/sec"
        )

    cursor.close()

    final_split = con.execute(f'SELECT COUNT(*) FROM "{TARGET_TABLE}"').fetchone()[0]
    final_reject = con.execute(f'SELECT COUNT(*) FROM "{REJECT_TABLE}"').fetchone()[0]
    con.close()

    elapsed = time.time() - t0
    print(f"\n{'=' * 70}")
    print(f"DONE  ({elapsed:.1f}s)")
    print(f"  Source rows scanned  : {total:,}")
    print(f"  Rows NOT split       : {unsplit_total:,}")
    print(f"  Rows split           : {split_count:,}")
    print(f"  Children added       : {child_count:,}")
    print(f"  Rows rejected        : {reject_count:,}")
    print(f"  Expected in SPLIT    : {unsplit_total + child_count:,}")
    print(f"  Actual   in SPLIT    : {final_split:,}")
    print(f"  Actual   in REJECT   : {final_reject:,}")
    print(f"{'=' * 70}\n")


if __name__ == "__main__":
    process_table()