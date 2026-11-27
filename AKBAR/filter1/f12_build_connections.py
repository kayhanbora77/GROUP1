"""
f12_build_connections.py
========================
Runs AFTER the rejection step (f11). Reads SOURCE_TABLE_NAME (AKBAR_CLEANED),
builds connections with the STPO rule, maps columns to the standard schema,
and writes the result to TARGET_TABLE (TA_STANDARD_AKBAR).

Adds:
    ConnectionID    uuid shared by consecutive legs of one journey
    LegNo           1..n within the connection
    LastLegAirport  AIRPORT2 of the connection's last leg, on every row

STPO rule (per ticket, after ordering legs):
    A leg gets the PREVIOUS leg's ConnectionID iff ALL hold:
        a) 0 <= (this FLIGHTDATE - prev FLIGHTDATE).days <= MAX_GAP_DAYS
        b) prev AIRPORT2 == this AIRPORT1
        c) prev STPO == 'X'   (current leg's STPO is never used)
    Otherwise the leg starts a NEW ConnectionID.
    The first leg of a ticket always starts a new ConnectionID.

Ordering inside a ticket:
    - Sort by FLIGHTTIME when not null, otherwise by FLIGHTDATE.
    - When two legs tie (same date, no FLIGHTTIME) break the tie by airport
      chaining: leg N AIRPORT2 == leg N+1 AIRPORT1.

Two code paths:
    A) Keys with no tied sort keys  -> fully vectorised (Polars).
    B) Keys with tied sort keys      -> Python loop with airport chaining
       (only for the tied clusters inside each key).
"""

import itertools
import uuid
from datetime import datetime

import duckdb
import polars as pl

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE_NAME = "AKBAR_CLEANED"
TARGET_TABLE = "TA_STANDARD_AKBAR"

MAX_GAP_DAYS = 1
NEW_COLS = ["ConnectionID", "LEGNO", "LastLegAirport"]

# Standard target schema for TA_STANDARD_AKBAR
TARGET_COLUMNS = [
    "Id", "ConnectionID", "PaxName", "AgencyRefNumber", "ETicketNo", "FlightNumber",
    "DepartureDate", "FileName", "BookingRef", "AirlineCode", "FromAirport",
    "STPO",
    "ToAirport", "LastLegAirport", "EUEligible", "EUEligibleDuration", "ExtraNote",
    "FlightFound", "LegNo", "IsTimeLimitL1", "IsTimeLimitL2", "EUFlights_Id",
    "Link_Id", "DelayInSecond", "Status", "IsSingleFlight", "IsMultiSegment",
    "OperatingFlightNo", "ScheduledDeparture", "ScheduledArrival", "ActualDeparture",
    "ActualArrival", "SourceData"
]

# Map source columns (AKBAR_CLEANED) to target columns (TA_STANDARD_AKBAR)
COLUMN_MAPPING = {
    "Id": "Id",
    "PAXNAME": "PaxName",
    "TDNR": "ETicketNo",
    "FLIGHTNO": "FlightNumber",
    "FLIGHTDATE": "DepartureDate",
    "SOURCEFILE": "FileName",
    "PNRR": "BookingRef",
    "AIRLINECODE": "AirlineCode",
    "AIRPORT1": "FromAirport",
    "AIRPORT2": "ToAirport",
    "STPO": "STPO",
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


def _clean_code(col: str) -> pl.Expr:
    """Airport code, whitespace-stripped and upper-cased for chaining comparisons."""
    return pl.col(col).cast(pl.String).str.strip_chars().str.to_uppercase()


def _normalize_stpo(col: str = "STPO") -> pl.Expr:
    """Trim + upper-case. Only 'X' and 'O' are meaningful; NULL/empty/'0'/other -> NULL."""
    s = pl.col(col).cast(pl.String).str.strip_chars().str.to_uppercase()
    return pl.when(s.is_in(["X", "O"])).then(s).otherwise(None)


# --------------------------------------------------------------------------- #
# Path A: keys with no tied sort keys -> vectorised, time ordered
# --------------------------------------------------------------------------- #
def _label_fast(df: pl.DataFrame) -> pl.DataFrame:
    df = df.sort(["__key", "__sort"], maintain_order=True)

    fd = pl.col("__fd")
    prev_fd = fd.shift(1).over("__key")
    prev_a2 = pl.col("__a2").shift(1).over("__key")
    prev_stpo = pl.col("__stpo_norm").shift(1).over("__key")

    # Date gap in days (Int64; null for the first row of each key).
    gap = (fd - prev_fd).dt.total_days()

    # Start a new connection iff:
    #   - first row of the key (prev_fd is null), OR
    #   - date gap out of [0, MAX_GAP_DAYS], OR
    #   - airports do not chain (prev AIRPORT2 != this AIRPORT1), OR
    #   - previous STPO is not 'X'.
    # The current leg's STPO is intentionally NOT used.
    new_conn = (
        prev_fd.is_null()
        | (gap < 0)
        | (gap > MAX_GAP_DAYS)
        | (prev_a2 != pl.col("__a1"))
        | (prev_stpo != "X")
    ).fill_null(True)

    df = df.with_columns(new_conn.cum_sum().cast(pl.Int64).alias("__conn"))
    return df.with_columns(
        (pl.int_range(pl.len()).over("__conn") + 1).cast(pl.Int64).alias("LEGNO")
    )


# --------------------------------------------------------------------------- #
# Path B: keys with tied sort keys -> Python loop with airport chaining
# --------------------------------------------------------------------------- #
def _can_precede(a, b) -> bool:
    """True if leg a can immediately precede leg b (a.AIRPORT2 == b.AIRPORT1)."""
    return a["a2"] is not None and b["a1"] is not None and a["a2"] == b["a1"]


def _chain_cluster(legs):
    """Order a tied cluster by airport chaining.

    Deterministic tie-break by (a1, a2) so the result never depends on input
    row order. Head = a leg that no other leg can precede; pure cycles fall
    back to the alphabetically smallest leg.
    """
    n = len(legs)
    if n <= 1:
        return list(legs)

    def key(i):
        return (legs[i]["a1"] or "", legs[i]["a2"] or "")

    head_candidates = [
        i for i in range(n)
        if not any(j != i and _can_precede(legs[j], legs[i]) for j in range(n))
    ]
    head = min(head_candidates, key=key) if head_candidates else min(range(n), key=key)

    chain = [head]
    remaining = set(range(n)) - {head}
    while remaining:
        tail = legs[chain[-1]]
        valid = [i for i in remaining if _can_precede(tail, legs[i])]
        nxt = min(valid, key=key) if valid else min(remaining, key=key)
        chain.append(nxt)
        remaining.discard(nxt)
    return [legs[i] for i in chain]


def _sort_key_of(leg):
    sk = leg["__sort"]
    return (sk if sk is not None else datetime.max, leg["a1"] or "", leg["a2"] or "")


def _sort_eq(a, b) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return a == b


def _order_key_legs(legs):
    """Order legs within a key: by sort_key, with airport chaining for tied clusters."""
    legs = sorted(legs, key=_sort_key_of)
    result = []
    i, n = 0, len(legs)
    while i < n:
        sk = legs[i]["__sort"]
        j = i + 1
        while j < n and _sort_eq(legs[j]["__sort"], sk):
            j += 1
        cluster = legs[i:j]
        if len(cluster) == 1:
            result.append(cluster[0])
        else:
            result.extend(_chain_cluster(cluster))
        i = j
    return result


def _gap_days(curr, prev):
    """Integer day gap between two FLIGHTDATE datetimes; None if either is missing."""
    fd, pfd = curr["__fd"], prev["__fd"]
    if fd is None or pfd is None:
        return None
    if hasattr(fd, "date"):
        fd = fd.date()
    if hasattr(pfd, "date"):
        pfd = pfd.date()
    return (fd - pfd).days


def _label_slow(df: pl.DataFrame, offset: int) -> pl.DataFrame:
    work = df.select(
        pl.col("__key"),
        pl.col("__row"),
        pl.col("__fd"),
        pl.col("__sort"),
        pl.col("__stpo_norm"),
        pl.col("__a1").alias("a1"),
        pl.col("__a2").alias("a2"),
    ).sort(["__key", "__sort", "a1", "a2"], nulls_last=True)

    rows_out, conns, legnos = [], [], []
    next_conn = offset

    for _, grp in itertools.groupby(work.iter_rows(named=True), key=lambda r: r["__key"]):
        ordered = _order_key_legs(list(grp))
        prev = None
        for leg in ordered:
            if prev is None:
                next_conn += 1
                pos = 1
            else:
                gap = _gap_days(leg, prev)
                same = (
                    gap is not None and 0 <= gap <= MAX_GAP_DAYS
                    and prev["a2"] is not None and leg["a1"] is not None
                    and prev["a2"] == leg["a1"]
                    and prev["__stpo_norm"] == "X"
                )
                if same:
                    pos += 1
                else:
                    next_conn += 1
                    pos = 1
            rows_out.append(leg["__row"])
            conns.append(next_conn)
            legnos.append(pos)
            prev = leg

    labels = pl.DataFrame(
        {"__row": rows_out, "__conn": conns, "LEGNO": legnos},
        schema={"__row": pl.UInt32, "__conn": pl.Int64, "LEGNO": pl.Int64},
    )
    out = df.join(labels, on="__row", how="left")
    if out["__conn"].null_count():
        raise RuntimeError("Chain labelling left rows without a connection")
    return out


# --------------------------------------------------------------------------- #
# Main entry: build ConnectionID / LegNo / LastLegAirport on a Polars frame
# --------------------------------------------------------------------------- #
def add_connections(df: pl.DataFrame) -> pl.DataFrame:
    df = df.drop([c for c in NEW_COLS if c in df.columns])

    # 1. Grouping key: TDNR + PNRR + PAXNAME when TDNR is valid, else PNRR + PAXNAME.
    tdnr_ok = (
        pl.col("TDNR").is_not_null()
        & (pl.col("TDNR").cast(pl.String).str.len_chars() >= 3)
        & ~pl.col("TDNR").cast(pl.String).str.contains(DATE_LIKE_RE)
    ).fill_null(False)

    pnr_ok = (pl.col("PNRR").is_not_null()
              & (pl.col("PNRR").cast(pl.String) != "/")).fill_null(False)
    pax_ok = (pl.col("PAXNAME").is_not_null()
              & (pl.col("PAXNAME").cast(pl.String) != "")).fill_null(False)

    SEP = "\x1f"
    key_pnr_pax = pl.concat_str([pl.col("PNRR"), pl.col("PAXNAME")], separator=SEP)
    key_all = pl.concat_str(
        [pl.col("TDNR").fill_null(""), pl.col("PNRR").fill_null(""),
         pl.col("PAXNAME").fill_null("")],
        separator=SEP,
    )

    df = df.with_columns(
        pl.when(tdnr_ok).then(key_all)
          .when(pnr_ok & pax_ok).then(key_pnr_pax)
          .when(pnr_ok).then(pl.col("PNRR"))
          .when(pax_ok).then(pl.col("PAXNAME"))
          .otherwise(pl.col("Id"))
          .alias("__key"),
    ).with_row_index("__row")

    # 2. Parse dates, STPO, airports.
    df = df.with_columns(_as_datetime("FLIGHTDATE").alias("__fd"))
    if "FLIGHTTIME" in df.columns:
        df = df.with_columns(_as_datetime("FLIGHTTIME").alias("__ft"))
        df = df.with_columns(pl.coalesce(["__ft", "__fd"]).alias("__sort"))
        df = df.drop("__ft")
    else:
        df = df.with_columns(pl.col("__fd").alias("__sort"))

    df = df.with_columns(
        _clean_code("AIRPORT1").alias("__a1"),
        _clean_code("AIRPORT2").alias("__a2"),
    )
    if "STPO" in df.columns:
        df = df.with_columns(_normalize_stpo("STPO").alias("__stpo_norm"))
    else:
        df = df.with_columns(pl.lit(None).cast(pl.String).alias("__stpo_norm"))

    # 3. Keys whose sort key has duplicates need the airport-chaining tie-break.
    needs_chain = (
        (pl.len().over("__key") > 1)
        & (pl.col("__sort").n_unique().over("__key") < pl.len().over("__key"))
    )
    df = df.with_columns(needs_chain.alias("__chain"))

    fast = df.filter(~pl.col("__chain"))
    slow = df.filter(pl.col("__chain"))

    parts, offset = [], 0
    if fast.height:
        fast = _label_fast(fast)
        offset = int(fast["__conn"].max() or 0)
        parts.append(fast)
    if slow.height:
        n_keys = slow["__key"].n_unique()
        slow = _label_slow(slow, offset)
        parts.append(slow)
        print(f"\nINFO: {slow.height:,} row(s) in {n_keys:,} key(s) have tied sort keys "
              f"-> ordered by airport chaining")

    cols = parts[0].columns
    df = pl.concat([p.select(cols) for p in parts], how="vertical")

    # 4. Sort by connection so LastLegAirport picks the true last leg, then UUIDs.
    df = df.sort(["__conn", "LEGNO"], maintain_order=True)

    n_conn = int(df["__conn"].max() or 0)
    uuids = pl.Series(
        "ConnectionID",
        [str(uuid.uuid4()) for _ in range(n_conn)],
        dtype=pl.String,
    )
    df = df.with_columns(
        uuids.gather(df["__conn"] - 1).alias("ConnectionID"),
        pl.col("AIRPORT2").last().over("__conn").alias("LastLegAirport"),
    )

    null_cnt = df["LastLegAirport"].null_count()
    if null_cnt:
        print(f"\nWARNING: {null_cnt:,} row(s) have no AIRPORT2 in their connection "
              f"(LastLegAirport is NULL)")

    return df.drop(
        ["__row", "__conn", "__key", "__fd", "__sort",
         "__stpo_norm", "__a1", "__a2", "__chain"]
    )


def transform_to_target(df: pl.DataFrame) -> pl.DataFrame:
    """Renames mapped columns, adds missing target columns with correct types,
    and enforces column order.
    """
    rename_map = {
        src: tgt
        for src, tgt in COLUMN_MAPPING.items()
        if src in df.columns
    }
    df = df.rename(rename_map)
    if "LEGNO" in df.columns:
        df = df.rename({"LEGNO": "LegNo"})
    # Boolean columns
    bool_cols = {
        "EUEligible",
        "FlightFound",
        "IsSingleFlight",
        "IsMultiSegment",
        "IsTimeLimitL1",
        "IsTimeLimitL2",
    }
    # Timestamp columns
    timestamp_cols = {
        "DepartureDate",
        "ScheduledDeparture",
        "ScheduledArrival",
        "ActualDeparture",
        "ActualArrival",
    }
    missing_exprs = []

    for col in TARGET_COLUMNS:
        if col in df.columns:
            continue
        if col in bool_cols:
            missing_exprs.append(
                pl.lit(None).cast(pl.Boolean).alias(col)
            )
        elif col in timestamp_cols:
            missing_exprs.append(
                pl.lit(None).cast(pl.Datetime).alias(col)
            )
        elif col == "LegNo":
            missing_exprs.append(
                pl.lit(None).cast(pl.Int64).alias(col)
            )
        else:
            missing_exprs.append(
                pl.lit(None).cast(pl.String).alias(col)
            )
    if missing_exprs:
        df = df.with_columns(missing_exprs)

    return df.select(TARGET_COLUMNS)


def build_connections(
    db_path: str = DB_PATH,
    source_table: str = SOURCE_TABLE_NAME,
    target_table: str = TARGET_TABLE
) -> None:
    con = duckdb.connect(db_path)
    try:
        src_tbl = quote_identifier(source_table)
        tgt_tbl = quote_identifier(target_table)

        arrow_tbl = con.sql(f"SELECT * FROM {src_tbl}").to_arrow_table()
        df = pl.from_arrow(arrow_tbl)
        print(f"Read {df.height:,} rows from {source_table}")

        out = add_connections(df)
        if out.height != df.height:
            raise RuntimeError(f"Row-count mismatch: in={df.height:,}, out={out.height:,}")

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