import time
import tempfile
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

# ────────────────────────────────────────────────
# CONFIG
# ────────────────────────────────────────────────
DB_PATH = r"C:\DuckDB\my_db.duckdb"
THREADS = 8
MEMORY_LIMIT = "12GB"
TEMP_DIR = Path(tempfile.gettempdir()) / "duckdb_temp"
SOURCE_TABLE = "TA_STANDARD_AKBAR"

# ────────────────────────────────────────────────
# CONSTANTS
# ────────────────────────────────────────────────
SPECIAL_NON_EU_TIME_LIMITS = {
    "BA": (6, 6),
    "TK": (2, 2),
    "PC": (2, 2),
    "JU": (2, 2),
    "FH": (2, 2),
    "VF": (2, 2),
    "VS": (6, 6),
    "XQ": (2, 2),
}

SPECIAL_L1_MAP = {code: lim[0] for code, lim in SPECIAL_NON_EU_TIME_LIMITS.items()}
SPECIAL_L2_MAP = {code: lim[1] for code, lim in SPECIAL_NON_EU_TIME_LIMITS.items()}

REFERENCE_DATE = pd.Timestamp("2026-12-15")


def log(msg: str) -> None:
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def get_connection() -> duckdb.DuckDBPyConnection:
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(DB_PATH)
    con.execute(f"SET threads = {THREADS}")
    con.execute(f"SET memory_limit = '{MEMORY_LIMIT}'")
    con.execute("SET preserve_insertion_order = false")
    con.execute(f"SET temp_directory = '{TEMP_DIR}'")
    return con


# ────────────────────────────────────────────────
# DATA LOADING
# ────────────────────────────────────────────────
def get_eueligible_data(con: duckdb.DuckDBPyConnection) -> Optional[pd.DataFrame]:
    log("Loading EU eligible data...")

    # Count what we are about to skip so it never disappears silently
    null_conn = con.execute(f"""
        SELECT COUNT(*) FROM {SOURCE_TABLE}
        WHERE EUEligible IS TRUE AND ConnectionID IS NULL
    """).fetchone()[0]
    if null_conn:
        log(f"WARNING: skipping {null_conn:,} row(s) with NULL ConnectionID")

    query = f"""
        SELECT
            t.ConnectionID,
            t."LegNo"                               AS "LegNo",
            t.AirlineCode,
            t.DepartureDate,
            depAirport.NameCountry                  AS depCountry,
            arrAirport.NameCountry                  AS arrCountry,
            COALESCE(depLimits.LimitL1, 0)::DOUBLE  AS depL1,
            COALESCE(depLimits.LimitL2, 0)::DOUBLE  AS depL2,
            COALESCE(arrLimits.LimitL1, 0)::DOUBLE  AS arrL1,
            COALESCE(arrLimits.LimitL2, 0)::DOUBLE  AS arrL2
        FROM {SOURCE_TABLE} t
        LEFT JOIN AIRPORTS depAirport
            ON t.FromAirport = depAirport.CodeIataAirport
        LEFT JOIN AIRPORTS arrAirport
            ON t.ToAirport = arrAirport.CodeIataAirport
        LEFT JOIN TIME_LIMITS depLimits
            ON depAirport.NameCountry = depLimits.Country
        LEFT JOIN TIME_LIMITS arrLimits
            ON arrAirport.NameCountry = arrLimits.Country
        WHERE t.EUEligible IS TRUE
          AND t.ConnectionID IS NOT NULL       -- NULLs would collapse into one fake group
    """
    df = con.execute(query).df()   # let errors fail the run loudly

    if df.empty:
        log("No EU eligible records found.")
        return None

    df["LegNo"] = pd.to_numeric(df["LegNo"], errors="coerce")
    # One bad leg invalidates the WHOLE connection: dropping single legs would
    # silently change which leg counts as first / last
    bad = df["LegNo"].isna() | df["DepartureDate"].isna()
    if bad.any():
        bad_ids = df.loc[bad, "ConnectionID"].unique()
        log(f"WARNING: skipping {len(bad_ids):,} connection(s) with a missing "
            f"LegNo or DepartureDate")
        df = df[~df["ConnectionID"].isin(bad_ids)]
        if df.empty:
            log("No usable records left.")
            return None

    log(f"Loaded {len(df):,} legs")
    return df


# ────────────────────────────────────────────────
# BUSINESS LOGIC
# ────────────────────────────────────────────────
def _add_years(dates: pd.Series, years: pd.Series) -> pd.Series:
    """Calendar-exact deadline: date + N years (leap-safe), NaT where years == 0."""
    months = (years * 12).round().astype("Int64")
    out = pd.Series(pd.NaT, index=dates.index, dtype="datetime64[ns]")
    ok = months.notna() & (months > 0) & dates.notna()
    out.loc[ok] = [
        d + pd.DateOffset(months=int(m))
        for d, m in zip(dates.loc[ok].dt.normalize(), months.loc[ok])
    ]
    return out


def calculate_timelimits_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ConnectionID", "IsTimeLimitL1", "IsTimeLimitL2"])

    df = df.copy()
    limit_cols = ["depL1", "arrL1", "depL2", "arrL2"]
    df[limit_cols] = df[limit_cols].fillna(0.0)

    # Order by LegNo -> first row is truly leg 1, last row is the final leg.
    # drop_duplicates keeps NULLs as NULLs: a NULL first-leg country is NOT
    # back-filled from a later leg.
    legs = df.sort_values(["ConnectionID", "LegNo"], kind="stable", na_position="last")
    first = legs.drop_duplicates("ConnectionID", keep="first").set_index("ConnectionID")
    last  = legs.drop_duplicates("ConnectionID", keep="last").set_index("ConnectionID")

    # Journey date and carrier come from leg 1
    agg = first[["AirlineCode", "DepartureDate"]].copy()

    # EVERY leg is checked: flag German airports on each side of each leg
    legs_dep_de = legs["depCountry"].eq("Germany").fillna(False).astype(bool)
    legs_arr_de = legs["arrCountry"].eq("Germany").fillna(False).astype(bool)
    any_de = (
        (legs_dep_de | legs_arr_de)
        .groupby(legs["ConnectionID"]).any()
        .reindex(agg.index)
    )

    # German courts rule: deadline = Dec 31 of (flight year + 3)
    ger_deadline = pd.to_datetime({
        "year":  agg["DepartureDate"].dt.year + 3,
        "month": 12,
        "day":   31,
    })

    # Special carriers apply only when no airport on any leg has any limit
    has_limit = (
        legs[limit_cols].gt(0).any(axis=1)
        .groupby(legs["ConnectionID"]).any()
        .reindex(agg.index)
    )
    no_limit = ~any_de & ~has_limit

    for level, special in (("L1", SPECIAL_L1_MAP), ("L2", SPECIAL_L2_MAP)):
        # Max limit over all NON-German airports of all legs
        # (German airports use the year-end rule instead)
        max_limit = (
            pd.concat(
                [legs[f"dep{level}"].mask(legs_dep_de, 0.0),
                 legs[f"arr{level}"].mask(legs_arr_de, 0.0)],
                axis=1,
            )
            .max(axis=1)
            .groupby(legs["ConnectionID"]).max()
            .reindex(agg.index)
        )
        deadline = _add_years(agg["DepartureDate"], max_limit)

        # Germany on any leg: the later of the two deadlines wins
        german = ger_deadline.where(any_de)
        deadline = pd.concat([deadline, german], axis=1).max(axis=1)

        special_deadline = _add_years(
            agg["DepartureDate"], agg["AirlineCode"].map(special).fillna(0.0)
        )
        deadline = deadline.mask(no_limit, special_deadline)

        # No deadline at all (NaT) compares as False
        agg[f"IsTimeLimit{level}"] = REFERENCE_DATE <= deadline

    return agg.reset_index()[["ConnectionID", "IsTimeLimitL1", "IsTimeLimitL2"]]


# ────────────────────────────────────────────────
# DATABASE UPDATE
# ────────────────────────────────────────────────
def set_time_limits(con: duckdb.DuckDBPyConnection, df_updates: pd.DataFrame) -> None:
    if df_updates.empty:
        log("No updates to apply")
        return

    log(f"Updating {len(df_updates):,} connection groups...")
    con.register("temp_updates", df_updates)
    try:
        con.execute("BEGIN")
        # Clear earlier results so skipped rows don't keep stale values
        con.execute(f"""
            UPDATE {SOURCE_TABLE}
               SET IsTimeLimitL1 = NULL, IsTimeLimitL2 = NULL
             WHERE EUEligible IS TRUE
        """)
        con.execute(f"""
            UPDATE {SOURCE_TABLE} t
               SET IsTimeLimitL1 = u.IsTimeLimitL1,
                   IsTimeLimitL2 = u.IsTimeLimitL2
              FROM temp_updates u
             WHERE t.ConnectionID = u.ConnectionID
               AND t.EUEligible IS TRUE
        """)
        con.execute("COMMIT")
        log("Committed successfully")
    except Exception:
        con.execute("ROLLBACK")
        log("UPDATE failed — rolled back")
        raise
    finally:
        con.unregister("temp_updates")


# ────────────────────────────────────────────────
# MAIN
# ────────────────────────────────────────────────
def main():
    start_time = time.time()
    log("Starting process")

    con = get_connection()
    try:
        df = get_eueligible_data(con)
        if df is not None:
            df_updates = calculate_timelimits_vectorized(df)
            set_time_limits(con, df_updates)
            log("──────────── DONE ────────────")
            log(f"Updated {len(df_updates):,} connection groups")
            log(f"Processed {len(df):,} legs")
            log(f"Finished in {time.time() - start_time:.2f} seconds")
        else:
            log("No data to process")
    finally:
        con.close()


if __name__ == "__main__":
    main()