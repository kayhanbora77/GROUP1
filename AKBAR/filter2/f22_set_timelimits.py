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

# Pre-split into separate mappings for vectorized lookup
SPECIAL_L1_MAP = {
    code: limits[0] for code, limits in SPECIAL_NON_EU_TIME_LIMITS.items()
}
SPECIAL_L2_MAP = {
    code: limits[1] for code, limits in SPECIAL_NON_EU_TIME_LIMITS.items()
}

REFERENCE_DATE = pd.Timestamp("2026-09-15")

# ────────────────────────────────────────────────
# UTILITIES
# ────────────────────────────────────────────────
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

    # Optimized: Removed unused columns (depCountry, arrCountry, LegNo)
    # Removed ORDER BY (not needed for aggregation)
    # Keep DepartureDate as native TIMESTAMP (no strftime roundtrip)
    query = f"""
        SELECT            
            t.ConnectionID,
            t.AirlineCode,
            t.DepartureDate,
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
    """

    try:
        df = con.execute(query).df()
    except Exception as e:
        log(f"Error fetching data: {e}")
        return None

    if df.empty:
        log("No EU eligible records found.")
        return None

    log(f"Loaded {len(df):,} legs")
    return df


# ────────────────────────────────────────────────
# BUSINESS LOGIC
# ────────────────────────────────────────────────
def calculate_timelimits_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["ConnectionID", "IsTimeLimitL1", "IsTimeLimitL2"])

    df = df.copy()

    # Ensure numeric types (SQL cast handles most, this is safety net)
    limit_cols = ["depL1", "arrL1", "depL2", "arrL2"]
    df[limit_cols] = df[limit_cols].fillna(0.0)
    # Before the groupby:
    df_sorted = df.sort_values("DepartureDate", ascending=True)
    # Aggregate per ConnectionID (no redundant group_key needed)
    agg = (
        df_sorted.groupby("ConnectionID", sort=False, dropna=False)
        .agg(
            AirlineCode=("AirlineCode", "first"),
            DepartureDate=("DepartureDate", "min"),
            depL1=("depL1", "max"),
            arrL1=("arrL1", "max"),
            depL2=("depL2", "max"),
            arrL2=("arrL2", "max"),
        )
        .reset_index()
    )

    # Max limit across dep/arr airports (vectorized)
    agg["limitL1"] = agg[["depL1", "arrL1"]].max(axis=1)
    agg["limitL2"] = agg[["depL2", "arrL2"]].max(axis=1)

    # Special non-EU carrier fallback (fully vectorized - no lambda)
    needs_special = (agg["limitL1"] == 0) & (agg["limitL2"] == 0)
    if needs_special.any():
        agg.loc[needs_special, "limitL1"] = (
            agg.loc[needs_special, "AirlineCode"].map(SPECIAL_L1_MAP).fillna(0.0)
        )
        agg.loc[needs_special, "limitL2"] = (
            agg.loc[needs_special, "AirlineCode"].map(SPECIAL_L2_MAP).fillna(0.0)
        )

    # Calculate year difference from reference date
    diff_years = (REFERENCE_DATE - agg["DepartureDate"]).dt.days / 365.25

    # Time limit check
    agg["IsTimeLimitL1"] = agg["limitL1"] >= diff_years
    agg["IsTimeLimitL2"] = agg["limitL2"] >= diff_years

    return agg[["ConnectionID", "IsTimeLimitL1", "IsTimeLimitL2"]]


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
