"""
f04_eu_eligibility.py
=====================
Runs AFTER f03. TA_STANDARD_AKBAR already has ConnectionID, LegNo and
LastLegAirport. This step only fills EUEligible (BOOLEAN) on every leg row.

Eligibility is decided per journey (ConnectionID), so every leg of a journey
gets the same value.

    FirstLegAirport = FromAirport of the leg with the lowest LegNo
    LastLegAirport  = existing column (ToAirport of the last leg)
    Airline1        = airline of the first leg

Rule priority (highest wins):
    1. FirstLegAirport is TR and LastLegAirport is TR           -> NOT eligible
    2. FirstLegAirport is TR and Airline1 in (LH, XQ, QR)       -> eligible
    3. Journey starts in Serbia (BEG, INI, KVO) and Airline1=JU -> eligible
    4. ANY leg's airline in (BA, TK, PC, JU, FH, VF, VS, XQ)    -> eligible
    5. FirstLegAirport and LastLegAirport both non-EU           -> NOT eligible
    6. FirstLegAirport is EU                                    -> eligible
    7. Otherwise                                                -> NOT eligible

"EU airport" = AIRPORTS row whose CodeIso2Country is not TR / MA.
"TR airport" = AIRPORTS row whose CodeIso2Country is TR.

The table is rebuilt with the same columns in the same order; EUEligible
becomes BOOLEAN (it is a NULL VARCHAR column after f03).
"""

import logging

import duckdb

DB_PATH = r"C:\DuckDB\my_db.duckdb"
TABLE = "TA_STANDARD_AKBAR"
THREADS = 8
MEMORY_LIMIT = "8GB"
TEMP_DIR = r"C:\DuckDB\temp"

SPECIAL_NON_EU_CARRIERS = frozenset({"BA", "TK", "PC", "JU", "FH", "VF", "VS", "XQ"})
SPECIAL_CARRIERS = frozenset({"LH", "XQ", "QR"})   # TR departure + one of these on leg 1
SRB_CARRIERS = frozenset({"JU"})
SRB_AIRPORTS = frozenset({"BEG", "INI", "KVO"})

REQUIRED_COLUMNS = {"ConnectionID", "LegNo", "FromAirport", "LastLegAirport",
                    "AirlineCode", "FlightNumber", "EUEligible"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s",
                    datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _in_list(codes) -> str:
    return ", ".join(f"'{c}'" for c in sorted(codes))


def _eligibility_sql(table: str) -> str:
    return f"""
    CREATE OR REPLACE TEMP TABLE _elig AS
    WITH eu AS (
        SELECT DISTINCT UPPER(TRIM(CodeIataAirport)) AS code
        FROM AIRPORTS
        WHERE CodeIso2Country NOT IN ('TR', 'MA') AND CodeIataAirport IS NOT NULL
    ),
    tr AS (
        SELECT DISTINCT UPPER(TRIM(CodeIataAirport)) AS code
        FROM AIRPORTS
        WHERE CodeIso2Country = 'TR' AND CodeIataAirport IS NOT NULL
    ),
    legs AS (
        SELECT ConnectionID,
               TRY_CAST(LegNo AS BIGINT)        AS leg_no,
               UPPER(TRIM(FromAirport))         AS from_ap,
               UPPER(TRIM(LastLegAirport))      AS last_ap,
               NULLIF(COALESCE(
                   NULLIF(UPPER(TRIM(AirlineCode)), ''),
                   regexp_extract(UPPER(TRIM(FlightNumber)), '^([A-Z]{{2,3}})', 1)
               ), '')                           AS airline
        FROM {table}
        WHERE ConnectionID IS NOT NULL
    ),
    journey AS (
        SELECT ConnectionID,
               arg_min(from_ap, leg_no) AS first_ap,
               max(last_ap)             AS last_ap,
               arg_min(airline, leg_no) AS airline1,
               COALESCE(bool_or(airline IN ({_in_list(SPECIAL_NON_EU_CARRIERS)})), FALSE)
                                        AS any_special_non_eu
        FROM legs
        GROUP BY ConnectionID
    ),
    flags AS (
        SELECT j.ConnectionID, j.airline1, j.any_special_non_eu,
               fe.code IS NOT NULL                    AS first_eu,
               le.code IS NOT NULL                    AS last_eu,
               ft.code IS NOT NULL                    AS first_tr,
               lt.code IS NOT NULL                    AS last_tr,
               j.first_ap IN ({_in_list(SRB_AIRPORTS)}) AS first_srb
        FROM journey j
        LEFT JOIN eu fe ON fe.code = j.first_ap
        LEFT JOIN eu le ON le.code = j.last_ap
        LEFT JOIN tr ft ON ft.code = j.first_ap
        LEFT JOIN tr lt ON lt.code = j.last_ap
    )
    SELECT ConnectionID,
           CASE
               WHEN first_tr AND last_tr                                         THEN FALSE  -- 1
               WHEN first_tr AND airline1 IN ({_in_list(SPECIAL_CARRIERS)})      THEN TRUE   -- 2
               WHEN first_srb AND airline1 IN ({_in_list(SRB_CARRIERS)})         THEN TRUE   -- 3
               WHEN any_special_non_eu                                           THEN TRUE   -- 4
               WHEN NOT first_eu AND NOT last_eu                                 THEN FALSE  -- 5
               WHEN first_eu                                                     THEN TRUE   -- 6
               ELSE FALSE                                                                    -- 7
           END AS eligible
    FROM flags
    """


def calc_eu_eligibility(db_path: str = DB_PATH, table_name: str = TABLE,
                        temp_dir: str = TEMP_DIR) -> None:
    con = duckdb.connect(db_path)
    try:
        con.execute(f"SET threads={THREADS}")
        con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
        con.execute(f"SET temp_directory='{temp_dir}'")

        cols = {r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
            [table_name]).fetchall()}
        missing = REQUIRED_COLUMNS - cols
        if missing:
            raise RuntimeError(f"{table_name} is missing column(s): {sorted(missing)}")

        tbl = quote_identifier(table_name)
        tmp = quote_identifier(f"{table_name}__eu_tmp")

        before = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        no_key = con.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE ConnectionID IS NULL").fetchone()[0]
        if no_key:
            logger.warning(f"{no_key:,} row(s) have no ConnectionID -> EUEligible stays NULL")

        con.execute(_eligibility_sql(tbl))

        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"""
                CREATE OR REPLACE TABLE {tmp} AS
                SELECT t.* REPLACE (e.eligible AS EUEligible)
                FROM {tbl} t
                LEFT JOIN _elig e ON t.ConnectionID = e.ConnectionID
            """)
            after = con.execute(f"SELECT COUNT(*) FROM {tmp}").fetchone()[0]
            if after != before:
                raise RuntimeError(f"Row-count mismatch: before={before:,}, after={after:,}")
            con.execute(f"DROP TABLE {tbl}")
            con.execute(f"ALTER TABLE {tmp} RENAME TO {quote_identifier(table_name)}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        logger.info(f"Updated EUEligible on {after:,} rows of {table_name}")
        for val, n in con.execute(
                f"SELECT EUEligible, COUNT(*) FROM {tbl} GROUP BY 1 ORDER BY 1").fetchall():
            logger.info(f"  EUEligible={val}: {n:,} rows")
    finally:
        con.close()


if __name__ == "__main__":
    calc_eu_eligibility()