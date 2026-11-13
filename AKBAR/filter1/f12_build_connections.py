"""
f03_build_connections.py
========================
Runs AFTER the rejection step (f02). Reads SOURCE_TABLE_NAME (AKBAR_CLEANED),
builds connections, maps columns to the standard schema, and writes the result
to TARGET_TABLE (TA_STANDARD_AKBAR).

Adds:
    ConnectionID    uuid shared by consecutive legs of one journey
    LegNo           1..n within the connection
    LastLegAirport  AIRPORT2 of the connection's last leg, on every row

Maps source columns to TARGET_COLUMNS (e.g., PAXNAME -> PaxName, FLIGHTNO -> FlightNumber).
Any target columns not mapped from the source are added as NULL.

Grouping key (per row) depends on which identifiers are valid:
    1. TDNR valid    -> TDNR
    2. TDNR invalid  -> PNRR + PAXNAME (if both valid)
    3. Fallback      -> PNRR, PAXNAME, or unique Id

Two ways of ordering legs inside a key:

  A) Every row of the key has a parseable FLIGHTTIME (fast, vectorised path)
     Ordered by FLIGHTTIME. A new connection starts when:
        - The FLIGHTDATE gap to the previous leg is > 2 days, OR
        - The legs do not chain (previous AIRPORT2 != this AIRPORT1), OR
        - The previous leg was the LAST leg (STPO is NULL, empty, or '0').
          (STPO = 'X' indicates a first/middle leg).

  B) At least one row of the key has NULL / unparseable FLIGHTTIME
     FLIGHTTIME cannot be trusted for ordering, so legs are chained by route:
        leg N AIRPORT2 == leg N+1 AIRPORT1
     A leg may follow another leg only if:
        - the previous leg is not a last leg (STPO not NULL/''/'0'),
        - AIRPORT2 of the previous leg == AIRPORT1 of this leg,
        - FLIGHTDATE gap is between 0 and 2 days,
        - if both legs have FLIGHTTIME, the previous one is not later.
     The chain starts at the leg that no other leg can precede, so the order
     comes from the route itself rather than from the row order.
"""

import itertools
import uuid
from collections import namedtuple
from datetime import date, datetime

import duckdb
import polars as pl

DB_PATH = r"C:\DuckDB\my_db.duckdb"
SOURCE_TABLE_NAME = "AKBAR_CLEANED"
TARGET_TABLE = "TA_STANDARD_AKBAR"

MAX_GAP_DAYS = 2
MAX_CHAIN_GROUP = 300   # larger groups (junk keys) use a cheap sequential chain
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
}

# Keep in sync with f02.
DATE_LIKE_RE = r"^(\d{1,4}[-/.]\d{1,2}[-/.]\d{1,4}|\d{1,2}-[A-Za-z]{3}-\d{2,4})$"
_DT_FORMATS = [
    "%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d",
]

Leg = namedtuple("Leg", "row fd ft a1 a2 cont")


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


def _stpo_continues() -> pl.Expr:
    """True when the leg is NOT a last leg (STPO is not NULL / '' / '0')."""
    stpo = pl.col("STPO").cast(pl.String).fill_null("").str.strip_chars()
    return ~stpo.is_in(["", "0"])


# --------------------------------------------------------------------------- #
# Path A: groups where every leg has a FLIGHTTIME -> vectorised, time ordered
# --------------------------------------------------------------------------- #
def _label_by_time(df: pl.DataFrame) -> pl.DataFrame:
    df = df.sort(["__key", "__ft", "__row"], nulls_last=True, maintain_order=True)

    fdate = pl.col("__fd").dt.date()
    prev_date = fdate.shift(1).over("__key")
    prev_to = _clean_code("AIRPORT2").shift(1).over("__key")
    curr_from = _clean_code("AIRPORT1")
    prev_cont = _stpo_continues().shift(1).over("__key").fill_null(False)

    new_conn = (
        prev_date.is_null()                                     # First row in group
        | ((fdate - prev_date).dt.total_days() > MAX_GAP_DAYS)  # Date gap > 2 days
        | (prev_to != curr_from)                                # Airports don't chain
        | ~prev_cont                                            # Previous leg was the last leg
    ).fill_null(True)

    df = df.with_columns(new_conn.cum_sum().cast(pl.Int64).alias("__conn"))
    return df.with_columns(
        (pl.int_range(pl.len()).over("__conn") + 1).cast(pl.Int64).alias("LEGNO")
    )


# --------------------------------------------------------------------------- #
# Path B: groups with missing FLIGHTTIME -> order by airport chaining
# --------------------------------------------------------------------------- #
def _can_follow(p: Leg, c: Leg) -> bool:
    """May leg `c` directly follow leg `p` in the same journey?"""
    if not p.cont:                                   # previous leg was a last leg
        return False
    if p.a2 is None or c.a1 is None or p.a2 != c.a1:  # route must chain
        return False
    if p.fd is None or c.fd is None:                 # unknown date -> don't join (as before)
        return False
    gap = (c.fd - p.fd).days
    if gap < 0 or gap > MAX_GAP_DAYS:
        return False
    if p.ft is not None and c.ft is not None and p.ft > c.ft:
        return False
    return True


def _build_chains(legs: list) -> list:
    """Return a list of chains; each chain is a list of indexes into `legs`, in leg order."""
    n = len(legs)
    order = sorted(
        range(n),
        key=lambda i: (legs[i].fd or date.max, legs[i].ft or datetime.max, legs[i].row),
    )

    # Very large groups come from junk keys: avoid O(n^2) work, chain sequentially.
    if n > MAX_CHAIN_GROUP:
        chains: list = []
        for i in order:
            if chains and _can_follow(legs[chains[-1][-1]], legs[i]):
                chains[-1].append(i)
            else:
                chains.append([i])
        return chains

    chains = []
    unassigned = order[:]
    while unassigned:
        # Head = a leg that no other unassigned leg can precede.
        # (Pure cycles, e.g. A->B / B->A with STPO 'X', fall back to the earliest leg.)
        head = next(
            (i for i in unassigned
             if not any(j != i and _can_follow(legs[j], legs[i]) for j in unassigned)),
            unassigned[0],
        )
        chain = [head]
        unassigned.remove(head)
        while True:
            tail = legs[chain[-1]]
            nxt = next((j for j in unassigned if _can_follow(tail, legs[j])), None)
            if nxt is None:
                break
            chain.append(nxt)
            unassigned.remove(nxt)
        chains.append(chain)
    return chains


def _label_by_chain(df: pl.DataFrame, offset: int) -> pl.DataFrame:
    work = df.select(
        pl.col("__key"),
        pl.col("__row"),
        pl.col("__fd").dt.date().alias("fd"),
        pl.col("__ft").alias("ft"),
        _clean_code("AIRPORT1").alias("a1"),
        _clean_code("AIRPORT2").alias("a2"),
        _stpo_continues().alias("cont"),
    ).sort(["__key", "__row"])

    rows, conns, legnos = [], [], []
    next_conn = offset
    for _, grp in itertools.groupby(work.iter_rows(), key=lambda r: r[0]):
        legs = [Leg(*r[1:]) for r in grp]
        for chain in _build_chains(legs):
            next_conn += 1
            for pos, idx in enumerate(chain, start=1):
                rows.append(legs[idx].row)
                conns.append(next_conn)
                legnos.append(pos)

    labels = pl.DataFrame(
        {"__row": rows, "__conn": conns, "LEGNO": legnos},
        schema={"__row": pl.UInt32, "__conn": pl.Int64, "LEGNO": pl.Int64},
    )
    out = df.join(labels, on="__row", how="left")
    if out["__conn"].null_count():
        raise RuntimeError("Chain labelling left rows without a connection")
    return out


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
    ).with_row_index("__row")

    bad_dt = df.filter(pl.col("__ft").is_null()).height

    # 3. Keys with any missing FLIGHTTIME (and more than one leg) need route chaining
    needs_chain = (
        (pl.col("__ft").is_null().sum().over("__key") > 0)
        & (pl.len().over("__key") > 1)
    )
    df = df.with_columns(needs_chain.alias("__chain"))
    fast = df.filter(~pl.col("__chain"))
    slow = df.filter(pl.col("__chain"))

    parts, offset = [], 0
    if fast.height:
        fast = _label_by_time(fast)
        offset = fast["__conn"].max() or 0
        parts.append(fast)
    if slow.height:
        n_keys = slow["__key"].n_unique()
        slow = _label_by_chain(slow, offset)
        parts.append(slow)
        print(f"\nINFO: {slow.height:,} row(s) in {n_keys:,} key(s) have a missing FLIGHTTIME "
              f"-> ordered by airport chaining (AIRPORT2 -> next AIRPORT1)")

    cols = parts[0].columns
    df = pl.concat([p.select(cols) for p in parts], how="vertical")
    df = df.sort(["__conn", "LEGNO"])

    # 4. Generate UUIDs and last-leg airport
    n_conn = df["__conn"].max() or 0
    uuids = pl.Series("ConnectionID", [str(uuid.uuid4()) for _ in range(n_conn)], dtype=pl.String)
    df = df.with_columns(
        uuids.gather(df["__conn"] - 1).alias("ConnectionID"),
        pl.col("AIRPORT2").last().over("__conn").alias("LastLegAirport"),
    )

    null_cnt = df["LastLegAirport"].null_count()
    if null_cnt:
        print(f"\nWARNING: {null_cnt:,} row(s) have no AIRPORT2 in their connection "
              f"(LastLegAirport is NULL)")
    if bad_dt:
        print(f"\nNOTE: {bad_dt:,} row(s) have a NULL/unparseable FLIGHTTIME")

    return df.drop(["__row", "__conn", "__key", "__fd", "__ft", "__chain"])


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