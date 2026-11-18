# AKBAR claim-refund pipeline

Excel booking files arrive from the agency, get flattened into `AKBAR_RAW`,
validated, turned into journeys (connections), and finally flagged for EU
compensation eligibility and claim time limits.

This repo now runs that pipeline on **dbt-core + dbt-duckdb** (both free, no
server, no JVM). The original scripts in `filter0/`, `filter1/` and `filter2/`
are still the source of truth for the business rules; the dbt project imports
them instead of copying them.

---

## 1. What the review found

Current warehouse state, measured on `C:/DuckDB/my_db.duckdb` (29 GB file):

| table | rows |
|---|---|
| `main.AKBAR_RAW` | 11,240,942 |
| `main.AKBAR_CLEANED` | 11,240,154 |
| `main.AKBAR_REJECTED` | 788 |
| `main.TA_STANDARD_AKBAR` | 11,240,154 (7,653,040 connections) |

### Bugs and risks

1. **The table is in a half-finished state right now.** `EUEligible`,
   `IsTimeLimitL1` and `IsTimeLimitL2` are **NULL on all 11,240,154 rows**.
   `f13_build_connections.py` recreates `TA_STANDARD_AKBAR` with those columns
   as NULL (`transform_to_target` adds every unmapped column as NULL), so
   re-running f13 after f21/f22 silently wipes the eligibility work. Nothing
   enforces the step order.
2. **`filter2/f22_set_timelimits.py` cannot run against today's table.**
   `(REFERENCE_DATE - agg["DepartureDate"]).dt.days` needs a datetime, but
   `DepartureDate` is `VARCHAR` in `TA_STANDARD_AKBAR`. Root cause: `insert_df()`
   in f11 casts *every* column to `VARCHAR` for both the clean and rejected
   tables, so the `TIMESTAMP` types produced by f01 are lost one step later.
   The same casting is why the flag columns are `VARCHAR` instead of `BOOLEAN`.
3. **f22 fans out legs when it joins `AIRPORTS`.** It joins `AIRPORTS` twice
   without deduplicating on `CodeIataAirport`; if the lookup ever holds two rows
   for one IATA code, every leg is duplicated and the time-limit aggregates
   silently change. The dbt model deduplicates the lookup.
4. **f22 applies time limits to non-eligible journeys.** It reads all legs but
   only `UPDATE`s rows where `EUEligible IS TRUE`; anything else keeps NULLs.
   The dbt model reproduces that gate explicitly (`test assert_flags_are_populated`
   caught the first version of the model getting this wrong).
5. **No configuration.** Paths (`C:\DuckDB\my_db.duckdb`), `REFERENCE_DATE`
   (`2026-09-15`), the 16 GB memory limit, the country/airline lists and the
   Excel folder are all hardcoded across five scripts.
6. **Memory:** f11 keeps a Python `set` of a 16-byte hash for every row of the
   whole table to detect duplicates across batches (~1 GB+ at 11.2M rows), on
   top of the batch DataFrames. `Reason.BATCH_DUPLICATE` is defined but never used.
7. **`filter1/f12_update_flightno.txt` is manual surgery on production data.**
   It `UPDATE`s `AKBAR_RAW` and `DELETE`s from `AKBAR_REJECTED` by
   `RejectionReason LIKE ...`, is not executed by anything, and its own footer
   ("run f11 again but do not run f12") documents a workflow that depends on
   human memory. A correction like this belongs in the pipeline as a versioned
   model, not in a text file.
8. **No tests, no lineage, no scheduler.** Nothing verifies that clean + rejected
   equals raw, that every journey has contiguous leg numbers, or that the flags
   are populated — which is exactly how issue 1 shipped.

### What is good and was preserved

The rule design itself is sound and is now reused unchanged: f11's ordered
rejection rules and cross-batch duplicate detection, f13's two-path leg chaining
(time-ordered when every leg has `FLIGHTTIME`, route-chained when it does not),
f21's seven-rule EU priority (including the `TR`/`MA` exclusions and the
`LH/XQ/QR/JU` carrier carve-outs) and f22's per-country time limits with the
special-carrier fallback.

---

## 2. The new project

```
dbt/
  dbt_project.yml            config, vars (reference_date, row_limit), pragmas
  profiles.yml               project-local profile -> C:/DuckDB/my_db.duckdb, schema akbar_dbt
  lib/akbar_lib.py           imports the filter scripts by path; resolves relations
  models/
    staging/
      _akbar__sources.yml    AKBAR_RAW / AIRPORTS / TIME_LIMITS + source tests
      stg_akbar_raw.sql      view over AKBAR_RAW (optional row_limit)
      optional/
        ingest_akbar_excel.py  filter0 as a model, disabled by default
    intermediate/
      int_akbar_scored.py    filter1: one pass, every row + RejectionReason
      int_akbar_cleaned.sql  clean rows (= legacy AKBAR_CLEANED)
      int_akbar_rejected.sql rejected rows (= legacy AKBAR_REJECTED)
      int_akbar_connections.py filter1b: ConnectionID / LegNo / LastLegAirport + schema map
    marts/
      int_akbar_eu_eligibility.sql  filter2a in SQL, keeps the deciding rule
      ta_standard_akbar.sql         filter2b in SQL -> final 32-column mart
      akbar_pipeline_health.sql     row counts per stage
  tests/                     4 singular tests (see below)
orchestration/
  run_pipeline.ps1           Windows runner + logging (Task Scheduler friendly)
  airflow/                   docker-compose, DAG, container profile
.venv-dbt/                   isolated Python 3.13 environment for dbt
```

Lineage: `stg_akbar_raw → int_akbar_scored → (cleaned | rejected) →
int_akbar_connections → int_akbar_eu_eligibility → ta_standard_akbar →
akbar_pipeline_health`.

Everything is written to the **`akbar_dbt` schema** of the existing warehouse.
`main.AKBAR_RAW` and `main.TA_STANDARD_AKBAR` are untouched, so the legacy
scripts keep working while you cut over.

### Tests

| test | guarantees |
|---|---|
| `not_null` / `unique` on `Id` (staging, cleaned, connections, mart) | no duplicated or orphaned legs |
| `assert_stage_balance` | clean rows + rejected rows = raw rows |
| `assert_legno_is_contiguous` | every journey numbers its legs 1..n with no gaps |
| `assert_flags_are_populated` | no journey has NULL flags, and no time-limit flag exists without EU eligibility |
| source tests | `AKBAR_RAW.Id` unique/not null, airport and country lookups not null |

---

## 3. Running it

```bash
# one-time: the venv that holds dbt
py -3.13 -m venv .venv-dbt
./.venv-dbt/Scripts/python -m pip install dbt-duckdb polars pyarrow pandas fastexcel

cd dbt

# fast run on a 20k-row slice (seconds) - use this while changing rules
../.venv-dbt/Scripts/dbt build --profiles-dir . --vars "{row_limit: 20000}"

# full pipeline over all 11.2M rows (writes ~11M-row tables into akbar_dbt)
../.venv-dbt/Scripts/dbt build --profiles-dir .

# single model / downstream of a model
../.venv-dbt/Scripts/dbt build --select int_akbar_eu_eligibility --profiles-dir .
../.venv-dbt/Scripts/dbt build --select +ta_standard_akbar --profiles-dir .

# docs + lineage graph
../.venv-dbt/Scripts/dbt docs generate --profiles-dir . && ../.venv-dbt/Scripts/dbt docs serve --profiles-dir .

# the Excel ingest, only when you actually want to reload from the workbooks
../.venv-dbt/Scripts/dbt run --select ingest_akbar_excel --profiles-dir .   # enable models/staging/optional first
```

Vars: `reference_date` (default `2026-09-15`) and `row_limit` (default null =
full table). Both live in `dbt_project.yml`, so nothing is hardcoded in SQL.

On Windows you can also use:

```powershell
.\orchestration\run_pipeline.ps1 -RowLimit 20000   # or no argument for full
```

It logs to `orchestration/logs/` and returns dbt's exit code, so Task Scheduler
fails the run when a model or test fails.

---

## 4. Orchestration choices (free tools only)

**dbt is the orchestrator.** It already knows the dependency graph and runs the
models + tests in order, so a separate scheduler is only needed to decide *when*
to run. Three options, in order of effort:

| option | when to use | how |
|---|---|---|
| Windows Task Scheduler + `run_pipeline.ps1` | this machine, now | create a daily task pointing at the script; no extra software |
| Airflow in Docker | you need retries, history, alerting, several pipelines | `cd orchestration/airflow && docker compose up -d` → http://localhost:8080 (`airflow` / `airflow`), DAG `akbar_claim_pipeline` runs daily 03:00 |
| dbt Cloud / any cron runner | moving off this PC | point it at `dbt build` |

Airflow is not running yet: starting it pulls the ~1.5 GB image and creates a
container against the shared 29 GB warehouse. The compose file and DAG are
ready; start it when you want scheduling. DuckDB allows one writer, so never run
the host pipeline and the Airflow pipeline at the same time.

**Why not Apache Spark:** there is no JDK on this machine (Spark needs Java), the
data already lives in a single DuckDB file that DuckDB processes in-process with
all cores, and 11.2M rows is well inside single-node territory. Adding Spark
would mean exporting the warehouse, running a JVM cluster and re-importing
results — more moving parts and slower for this volume. Revisit it if the
warehouse must merge many agencies' files concurrently, or outgrows one disk.

---

## 5. Verification performed

* `dbt debug` — connection to `C:/DuckDB/my_db.duckdb` OK.
* `dbt build --vars "{row_limit: 20000}"` — **37/37 nodes pass** (8 models,
  29 tests). Python models run the real f11/f13 code.
* `dbt build --vars "{row_limit: 200000}"` — **37/37 pass in 33 s**, so the
  Python models scale linearly; the full 11.2M-row build is therefore roughly
  25-35 minutes (extrapolated, not executed).
* Result sanity check on the slice: 20,000 rows → 2,623 EU-eligible journeys,
  686 inside time limit L1, and zero journeys with a time-limit flag but no EU
  eligibility (the invariant that was violated before the fix).

Not performed: the full 11.2M-row build (it adds multi-GB tables to a shared
warehouse — run it deliberately), the Airflow container (not started), and any
BI/downstream consumer of `TA_STANDARD_AKBAR`.

---

## 6. Suggested next steps

1. Decide when to promote: point consumers at `akbar_dbt.ta_standard_akbar`, then
   retire the f11/f13 direct table writes.
2. Fix f11's `insert_df` so the legacy tables keep real types (`TIMESTAMP`,
   `BOOLEAN`) — this is what breaks f22 today.
3. Convert `f12_update_flightno.txt` into a versioned model or seed so manual
   `UPDATE`/`DELETE` on `AKBAR_RAW` disappears.
4. Move the remaining config (paths, carrier lists, Excel folder) into vars.
