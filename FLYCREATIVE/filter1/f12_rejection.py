r"""
Flight Row Rejection  -  optimized for 5M+ rows
=================================================
Reads SOURCE_TABLE in batches, applies rejection rules, and routes rows:
  * Rejected rows  -> REJECTION_TABLE  (with a RejectionReason column)
  * Clean rows     -> TARGET_TABLE     (with normalized FlightNo values)

Rejection Rules (checked in this order; first failing rule wins)
-----------------------------------------------------------------
1.  Missing required FlightNo1 + FlightDate1
    -> MISSING_REQUIRED_SLOT
2.  FlightNo validation
    -> FN_NULL, FD_NULL, FD_EMPTY, FN_EMPTY, FN_PURELY_NUMERIC,
       FN_TOO_LONG, FN_ALL_ZEROS, FN_ALL_ALPHA_AFTER_STRIP, FN_BAD_FORMAT
3.  Consecutive duplicate FlightNo
    -> FN_CONSECUTIVE_DUPLICATE
4.  FlightNo count != FlightDate count
    -> ROUTE_OVERFLOW
5.  FlightDate validation (year range DATE_YEAR_MIN-DATE_YEAR_MAX)
    -> FD_BAD_FORMAT, FD_OUT_OF_RANGE
6.  FlightDate chronological order
    -> FD_DECREASING
7.  Airline code validation (2-3 alphanumeric characters)
    -> AC_BAD_FORMAT
8.  Airport validation (exactly 3 letters)
    -> AP_BAD_FORMAT
9.  Duplicate segment detection (across batches, first occurrence kept)
    -> DUPLICATE_SEGMENT, using DUP_KEY_COLS
    (BATCH_DUPLICATE is defined but not emitted; both kinds of duplicate
    use DUPLICATE_SEGMENT.)
"""

import math
import os
import re
import time
from datetime import date as _date
from datetime import datetime
from enum import StrEnum

import duckdb
import polars as pl

# ============================================================================
# CONFIG
# ============================================================================
DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE = "FLYCREATIVE_SPLIT"      # input table (read-only)
TARGET_TABLE = "FLYCREATIVE_SPLIT2"     # output table for clean rows
REJECTION_TABLE = "FLYCREATIVE_REJECT"  # output table for rejected rows

CLEAR_REJECTION_TABLE = False  # True: empty the rejection table at start of each run

MAX_FLIGHTS = 4
MAX_FLTNO_LEN = 7
BATCH_SIZE = 200_000

DATE_YEAR_MIN = 2015
DATE_YEAR_MAX = 2030

AIRLINE_ALPHA_COL = "AirlineCode1"  # set to None to skip airline validation
FLIGHT_PREFIX = "FlightNo"
DATE_PREFIX = "FlightDate"

# Columns that define a duplicate segment
DUP_KEY_COLS = [
    "PaxName",
    "PNR",
    "FlightNo1", "FlightNo2", "FlightNo3", "FlightNo4",
    "FlightDate1", "FlightDate2", "FlightDate3", "FlightDate4",
    "Airport1", "Airport2", "Airport3", "Airport4", "Airport5",
]


# ============================================================================
# REJECTION REASONS
# ============================================================================
class Reason(StrEnum):
    FN_NULL = "FlightNumber NULL"
    FN_EMPTY = "FlightNumber EMPTY"
    FN_PURELY_NUMERIC = "FlightNumber PURELY_NUMERIC"
    FN_TOO_LONG = "FlightNumber TOO_LONG"
    FN_ALL_ZEROS = "FlightNumber ALL_ZEROS"
    FN_ALL_ALPHA_AFTER_STRIP = "FlightNumber ALL_ALPHA_AFTER_STRIP"
    FN_BAD_FORMAT = "FlightNumber BAD_FORMAT"
    FD_NULL = "FlightDate NULL"
    FD_EMPTY = "FlightDate EMPTY"
    FD_BAD_FORMAT = "FlightDate BAD_FORMAT"
    FD_OUT_OF_RANGE = "FlightDate OUT_OF_RANGE"
    FD_DECREASING = "FlightDate OUT_OF_CHRONOLOGICAL_ORDER"
    ROUTE_OVERFLOW = "ROUTE OVERFLOW"
    DUPLICATE_SEGMENT = "DUPLICATE SEGMENT"
    BATCH_DUPLICATE = "BATCH DUPLICATE"
    MISSING_REQUIRED_SLOT = "MISSING REQUIRED SLOT (FlightNo1/FlightDate1)"
    AC_BAD_FORMAT = "AirlineCode BAD_FORMAT"
    AP_BAD_FORMAT = "Airport BAD_FORMAT"
    FN_CONSECUTIVE_DUPLICATE = "FlightNumber CONSECUTIVE_DUPLICATE"


# ============================================================================
# HELPERS
# ============================================================================
_RE_SCI_NOTATION = re.compile(r"^(\d+)(?:\.0+)?E\+?(\d+)$", re.IGNORECASE)
_RE_FLTNO = re.compile(r"^[A-Z0-9]{2,3}\d+$")
_RE_AIRLINECODE_23 = re.compile(r"^[A-Za-z0-9]{2,3}$")
_RE_AIRPORT_3 = re.compile(r"^[A-Za-z]{3}$")

_DATE_FMTS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y%m%d",
    "%d%m%Y",
    "%d.%m.%Y",
    "%Y.%m.%d",
    "%b %d %Y",
    "%d %b %Y",
    "%B %d %Y",
    "%d %B %Y",
]


def _isna(val) -> bool:
    """True if val is None, NaN, or blank / null-like string."""
    if val is None:
        return True
    if isinstance(val, str):
        return val.strip().lower() in ("", "nat", "none", "nan", "null")
    if isinstance(val, float):
        return math.isnan(val)
    return False


def _fix_scientific_notation(fn: str) -> str:
    """Collapse Excel-mangled scientific notation, e.g. '6.00E+78' -> '6E78'."""
    m = _RE_SCI_NOTATION.fullmatch(fn.strip())
    if not m:
        return fn
    return f"{m.group(1)}E{m.group(2)}"


def _fix_row_flightnos(row: dict) -> dict:
    row = dict(row)
    for i in range(1, MAX_FLIGHTS + 1):
        key = f"{FLIGHT_PREFIX}{i}"
        fn = row.get(key)
        if not _isna(fn):
            row[key] = _fix_scientific_notation(str(fn).strip())
    return row


def _normalize_flightno(fn: str) -> str:
    m = re.fullmatch(r"([A-Z0-9]{2,3}?)(\d+)", fn.upper().strip())
    if not m:
        return fn
    prefix, digits = m.group(1), m.group(2)
    return prefix + (digits.lstrip("0") or "0")


def _parse_date(dt_str):
    if isinstance(dt_str, datetime):
        return dt_str
    if isinstance(dt_str, _date):
        return datetime(dt_str.year, dt_str.month, dt_str.day)
    s = str(dt_str).strip()
    if not s:
        return None
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================
def is_valid_flightno(fn, dt):
    fn_na = _isna(fn)
    dt_na = _isna(dt)
    if fn_na and dt_na:
        return False, Reason.FN_NULL, f"fn={fn!r}, dt={dt!r}"
    if fn_na:
        return False, Reason.FN_NULL, f"fn={fn!r}"
    if dt_na:
        return False, Reason.FD_NULL, f"dt={dt!r}"
    dt = str(dt).strip()
    if not dt:
        return False, Reason.FD_EMPTY, f"dt={dt!r}"
    fn = str(fn).strip()
    if not fn:
        return False, Reason.FN_EMPTY, f"fn={fn!r}"
    if fn.isdigit():
        return False, Reason.FN_PURELY_NUMERIC, f"fn={fn!r}"
    if len(fn) > MAX_FLTNO_LEN:
        return False, Reason.FN_TOO_LONG, f"fn={fn!r} len={len(fn)}"
    stripped = fn.rstrip("0")
    if not stripped:
        return False, Reason.FN_ALL_ZEROS, f"fn={fn!r}"
    if stripped.isalpha():
        return False, Reason.FN_ALL_ALPHA_AFTER_STRIP, f"fn={fn!r}"
    fn_upper = fn.upper()
    if not _RE_FLTNO.fullmatch(fn_upper):
        return False, Reason.FN_BAD_FORMAT, f"fn={fn_upper!r}"
    return True, None, None


def check_missing_required_slot(row):
    fn1_missing = _isna(row.get(f"{FLIGHT_PREFIX}1"))
    dt1_missing = _isna(row.get(f"{DATE_PREFIX}1"))
    if fn1_missing and dt1_missing:
        return (
            True,
            f"{Reason.MISSING_REQUIRED_SLOT} "
            f"(both {FLIGHT_PREFIX}1 and {DATE_PREFIX}1 are empty)",
        )
    return False, None


def check_flightno_validity(row):
    reasons = []
    for i in range(1, MAX_FLIGHTS + 1):
        fn = row.get(f"{FLIGHT_PREFIX}{i}")
        dt = row.get(f"{DATE_PREFIX}{i}")
        # A slot with only one side populated is left to check_route_overflow.
        if _isna(fn) or _isna(dt):
            continue
        ok, reason, detail = is_valid_flightno(fn, dt)
        if not ok:
            reasons.append(f"Slot{i}: {reason} ({detail})")
    if reasons:
        return True, "; ".join(reasons)
    return False, None


def check_consecutive_duplicate_flightno(row):
    """Reject if any two consecutive FlightNo slots are identical."""
    reasons = []
    for i in range(1, MAX_FLIGHTS):
        fn_a = row.get(f"{FLIGHT_PREFIX}{i}")
        fn_b = row.get(f"{FLIGHT_PREFIX}{i + 1}")
        if _isna(fn_a) or _isna(fn_b):
            continue
        a = str(fn_a).strip().upper()
        b = str(fn_b).strip().upper()
        if a and a == b:
            reasons.append(
                f"Slot{i}/Slot{i + 1}: {Reason.FN_CONSECUTIVE_DUPLICATE} "
                f"({FLIGHT_PREFIX}{i}={a!r} == {FLIGHT_PREFIX}{i + 1}={b!r})"
            )
    if reasons:
        return True, "; ".join(reasons)
    return False, None


def check_route_overflow(row):
    """FlightNo count must equal FlightDate count."""
    fn_count = sum(
        1 for i in range(1, MAX_FLIGHTS + 1)
        if not _isna(row.get(f"{FLIGHT_PREFIX}{i}"))
    )
    dt_count = sum(
        1 for i in range(1, MAX_FLIGHTS + 1)
        if not _isna(row.get(f"{DATE_PREFIX}{i}"))
    )
    if fn_count != dt_count:
        return (
            True,
            f"{Reason.ROUTE_OVERFLOW} "
            f"({FLIGHT_PREFIX}={fn_count}, {DATE_PREFIX}={dt_count})",
        )
    return False, None


def check_flightdate_format_and_range(row):
    reasons = []
    for i in range(1, MAX_FLIGHTS + 1):
        dt = row.get(f"{DATE_PREFIX}{i}")
        if _isna(dt):
            continue
        dt_str = str(dt).strip()
        if not dt_str:
            continue
        parsed = _parse_date(dt_str)
        if parsed is None:
            reasons.append(f"Slot{i}: {Reason.FD_BAD_FORMAT} ({dt_str!r})")
            continue
        if not (DATE_YEAR_MIN <= parsed.year <= DATE_YEAR_MAX):
            reasons.append(
                f"Slot{i}: {Reason.FD_OUT_OF_RANGE} "
                f"(year={parsed.year}, allowed {DATE_YEAR_MIN}-{DATE_YEAR_MAX})"
            )
    if reasons:
        return True, "; ".join(reasons)
    return False, None


def check_flightdate_chronology(row):
    """
    FlightDate slots must be non-decreasing in slot order. Blank slots are
    skipped; malformed dates are caught by check_flightdate_format_and_range.
    """
    reasons = []
    prev_dt = None
    prev_idx = None
    for i in range(1, MAX_FLIGHTS + 1):
        dt = row.get(f"{DATE_PREFIX}{i}")
        if _isna(dt):
            continue
        parsed = _parse_date(dt)
        if parsed is None:
            continue
        if prev_dt is not None and parsed < prev_dt:
            reasons.append(
                f"Slot{prev_idx}->Slot{i}: {Reason.FD_DECREASING} "
                f"({DATE_PREFIX}{prev_idx}={prev_dt} > {DATE_PREFIX}{i}={parsed})"
            )
        prev_dt = parsed
        prev_idx = i
    if reasons:
        return True, "; ".join(reasons)
    return False, None


def check_airline_code(row):
    if AIRLINE_ALPHA_COL is None:
        return False, None
    ac = row.get(AIRLINE_ALPHA_COL)
    if _isna(ac):
        return False, None
    ac_str = str(ac).strip()
    if not _RE_AIRLINECODE_23.fullmatch(ac_str):
        return True, f"{Reason.AC_BAD_FORMAT} col={AIRLINE_ALPHA_COL!r} val={ac_str!r}"
    return False, None


def check_airport(row):
    reasons = []
    for col, val in row.items():
        if "airport" in col.lower() and not _isna(val):
            v = str(val).strip()
            if v and not _RE_AIRPORT_3.fullmatch(v):
                reasons.append(f"{col}: {Reason.AP_BAD_FORMAT} ({v!r})")
    if reasons:
        return True, "; ".join(reasons)
    return False, None


_CHECKS = [
    check_missing_required_slot,
    check_flightno_validity,
    check_consecutive_duplicate_flightno,
    check_route_overflow,
    check_flightdate_format_and_range,
    check_flightdate_chronology,
    check_airline_code,
    check_airport,
]


# ============================================================================
# NORMALIZATION
# ============================================================================
def normalize_flight_numbers(row: dict) -> dict:
    row = dict(row)
    for i in range(1, MAX_FLIGHTS + 1):
        key = f"{FLIGHT_PREFIX}{i}"
        fn = row.get(key)
        if not _isna(fn):
            fn_upper = str(fn).strip().upper()
            if _RE_FLTNO.fullmatch(fn_upper):
                row[key] = _normalize_flightno(fn_upper)
    return row


# ============================================================================
# TABLE HELPERS
# ============================================================================
def _sanitize_col(col: str) -> str:
    return col.strip().replace('"', '""')


def ensure_rejection_table(con, source_cols, rejection_table, clear=False):
    """Create the rejection table if missing; optionally empty it."""
    exists = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [rejection_table],
    ).fetchone()[0]

    if exists:
        cols = {
            r[0]
            for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = ?",
                [rejection_table],
            ).fetchall()
        }
        if "RejectionReason" not in cols:
            con.execute(
                f'ALTER TABLE "{rejection_table}" ADD COLUMN "RejectionReason" VARCHAR'
            )
            print(f"  Added RejectionReason column to {rejection_table}.")
        if clear:
            con.execute(f'DELETE FROM "{rejection_table}"')
            print(f"  Cleared {rejection_table}.")
        else:
            print(f"  Rejection table '{rejection_table}' already exists (appending).")
        return

    col_defs = ", ".join(f'"{_sanitize_col(c)}" VARCHAR' for c in source_cols)
    con.execute(
        f'CREATE TABLE "{rejection_table}" ({col_defs}, "RejectionReason" VARCHAR)'
    )
    print(f"  Created {rejection_table}.")


def ensure_target_table(con, source_cols, target_table):
    """Drop and recreate the target table fresh on every run."""
    con.execute(f'DROP TABLE IF EXISTS "{target_table}"')
    col_defs = ", ".join(f'"{_sanitize_col(c)}" VARCHAR' for c in source_cols)
    con.execute(f'CREATE TABLE "{target_table}" ({col_defs})')
    print(f"  Dropped and recreated {target_table}.")


def col_names(con, table: str) -> list[str]:
    rows = con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        [table],
    ).fetchall()
    return [r[0] for r in rows]


def insert_df(con, table, cols, df):
    """Insert a Polars frame into a DuckDB table via an explicit Arrow registration."""
    col_list = ", ".join(f'"{c}"' for c in cols)
    df = df.select([pl.col(c).cast(pl.String) for c in cols])
    con.register("tmp_df", df.to_arrow(compat_level=pl.CompatLevel.oldest()))
    try:
        con.execute(f'INSERT INTO "{table}" ({col_list}) SELECT * FROM tmp_df')
    finally:
        con.unregister("tmp_df")


# ============================================================================
# DUPLICATE DETECTION
# ============================================================================
def _dup_key(row) -> int:
    """Hash of the duplicate-key columns (keeps seen_keys small)."""
    return hash(
        tuple(
            None if _isna(row.get(c)) else str(row.get(c)).strip()
            for c in DUP_KEY_COLS
        )
    )


def find_batch_duplicates(batch: list[dict], seen_keys: set):
    local_seen: set = set()
    dup_indices: list[int] = []
    for idx, row in enumerate(batch):
        key = _dup_key(row)
        if key in seen_keys or key in local_seen:
            dup_indices.append(idx)
        else:
            local_seen.add(key)
    seen_keys.update(local_seen)
    return dup_indices, seen_keys


# ============================================================================
# PROCESS ONE BATCH
# ============================================================================
def process_batch(batch_df: pl.DataFrame, seen_keys: set):
    str_schema = {c: pl.String for c in batch_df.columns}
    records = batch_df.to_dicts()

    reject_rows: list[dict] = []
    reject_reasons: list[str] = []
    reject_indices: set[int] = set()

    for idx, row in enumerate(records):
        row = _fix_row_flightnos(row)
        records[idx] = row
        for check_fn in _CHECKS:
            rejected, reason = check_fn(row)
            if rejected:
                reject_indices.add(idx)
                reject_rows.append(row)
                reject_reasons.append(str(reason))
                break

    # Duplicate detection on rows that passed all checks
    clean_idx_map = [i for i in range(len(records)) if i not in reject_indices]
    clean_records = [records[i] for i in clean_idx_map]
    dup_indices_local, seen_keys = find_batch_duplicates(clean_records, seen_keys)
    for local_idx in dup_indices_local:
        orig_idx = clean_idx_map[local_idx]
        reject_indices.add(orig_idx)
        reject_rows.append(records[orig_idx])
        reject_reasons.append(str(Reason.DUPLICATE_SEGMENT))

    if reject_rows:
        reject_df = pl.DataFrame(reject_rows, schema=str_schema)
        reject_df = reject_df.with_columns(
            pl.Series("RejectionReason", reject_reasons, dtype=pl.String)
        )
    else:
        reject_df = pl.DataFrame()

    clean_indices = [i for i in range(len(records)) if i not in reject_indices]
    if clean_indices:
        clean_records_normalized = [
            normalize_flight_numbers(records[i]) for i in clean_indices
        ]
        clean_df = pl.DataFrame(clean_records_normalized, schema=str_schema)
    else:
        clean_df = pl.DataFrame(schema=str_schema)

    return clean_df, reject_df, seen_keys


# ============================================================================
# MAIN
# ============================================================================
def process_table(
    db_path=DB_PATH,
    source=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    rejection_table=REJECTION_TABLE,
    batch_size=BATCH_SIZE,
):
    con = duckdb.connect(db_path)
    con.execute(f"PRAGMA threads={os.cpu_count()}")
    try:
        con.execute("SET memory_limit='16GB'")
    except Exception:
        pass

    src_cols = col_names(con, source)
    print(f"  Detected {len(src_cols)} columns.")

    ensure_rejection_table(con, src_cols, rejection_table, clear=CLEAR_REJECTION_TABLE)
    ensure_target_table(con, src_cols, target_table)

    rej_cols = src_cols + ["RejectionReason"]
    str_schema = {c: pl.String for c in src_cols}

    # Cast every column to VARCHAR at the source so Python only sees str/None
    col_list_cast = ", ".join(
        f'CAST("{_sanitize_col(c)}" AS VARCHAR) AS "{_sanitize_col(c)}"'
        for c in src_cols
    )

    total = con.execute(f'SELECT COUNT(*) FROM "{source}"').fetchone()[0]
    print(f"\n{'=' * 65}")
    print(f"FLIGHT ROW REJECTION  -  {total:,} rows")
    print(f"  Source     : {source}")
    print(f"  Target     : {target_table}")
    print(f"  Rejection  : {rejection_table}")
    print(f"{'=' * 65}")

    processed = 0
    rejected_tot = 0
    clean_tot = 0
    seen_keys: set = set()
    t0 = time.time()

    reader = con.cursor()
    writer = con.cursor()
    reader.execute(f'SELECT {col_list_cast} FROM "{source}"')
    while True:
        raw = reader.fetchmany(batch_size)
        if not raw:
            break

        batch_df = pl.DataFrame(raw, schema=str_schema, orient="row")
        clean_df, reject_df, seen_keys = process_batch(batch_df, seen_keys)

        if not reject_df.is_empty():
            insert_df(writer, rejection_table, rej_cols, reject_df)
            rejected_tot += reject_df.height

        if not clean_df.is_empty():
            insert_df(writer, target_table, src_cols, clean_df)
            clean_tot += clean_df.height

        processed += len(raw)
        elapsed = time.time() - t0
        rate = processed / elapsed if elapsed > 0 else 0
        print(
            f"  {processed:>10,} / {total:,} scanned  |"
            f"  {clean_tot:>8,} clean  |"
            f"  {rejected_tot:>8,} rejected  |  {rate:>8,.0f} rows/sec"
        )
    reader.close()
    writer.close()

    rej_count = con.execute(f'SELECT COUNT(*) FROM "{rejection_table}"').fetchone()[0]
    target_count = con.execute(f'SELECT COUNT(*) FROM "{target_table}"').fetchone()[0]

    print(f"\n{'=' * 65}")
    print("REJECTION SUMMARY")
    print(f"{'=' * 65}")
    summary = con.execute(f"""
        SELECT "RejectionReason", COUNT(*) AS cnt
        FROM "{rejection_table}"
        GROUP BY "RejectionReason"
        ORDER BY cnt DESC
    """).fetchall()
    for reason, cnt in summary:
        print(f"  {cnt:>10,}  {reason}")

    con.close()
    elapsed = time.time() - t0
    pass_rate = (target_count / total * 100) if total else 0.0
    print(f"\n{'=' * 65}")
    print(f"DONE  ({elapsed:.1f}s)")
    print(f"  Source rows    : {total:,}")
    print(f"  Clean rows     : {target_count:,}  -> {target_table}")
    print(f"  Rejected rows  : {rej_count:,}  -> {rejection_table}")
    print(f"  Pass rate      : {pass_rate:.1f}%")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    process_table()