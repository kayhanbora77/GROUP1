"""AKBAR claim-refund pipeline, scheduled by Airflow.

The DAG is deliberately thin: dbt owns the model dependencies and the tests, so
Airflow only decides *when* to run and what to do on failure. Each task is one
dbt selector instead of one task per table, which keeps the Airflow graph small
and avoids duplicating the lineage that dbt already knows.

Runs daily at 03:00 Istanbul time. Set the `row_limit` Airflow variable to run
on a slice (useful while developing).
"""

from __future__ import annotations

import datetime as dt

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/akbar/dbt"
PROFILES_DIR = "/opt/airflow/dbt_profiles"

DBT = f"dbt --no-write-json --project-dir {PROJECT_DIR} --profiles-dir {PROFILES_DIR}"

default_args = {
    "owner": "data",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=10),
    "email_on_failure": False,
}


def dbt_args() -> str:
    """Extra CLI args shared by every dbt task."""
    row_limit = Variable.get("akbar_row_limit", default_var="")
    if row_limit:
        return f"--vars '{{row_limit: {row_limit}}}'"
    return ""


with DAG(
    dag_id="akbar_claim_pipeline",
    description="AKBAR raw -> cleaned -> connections -> EU eligibility + time limits",
    start_date=dt.datetime(2026, 1, 1),
    schedule="0 3 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["akbar", "dbt", "claims"],
) as dag:

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build {dbt_args()}",
        append_env=True,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT} docs generate --static",
    )

    dbt_build >> dbt_docs
