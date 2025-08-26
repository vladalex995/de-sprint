# projects/a_lakehouse/airflow_home/dags/daily_orders_dag.py
# Purpose:
# - Define a daily Airflow pipeline (DAG) that runs four steps:
#   1) extract (operational DB → raw CSV)
#   2) bronze (CSV → Parquet, partitioned by date)
#   3) silver (build warehouse tables)
#   4) health check (assert today's rows exist)
#
# How tasks run:
# - We use BashOperator to call your EXISTING project scripts
#   with your project's Python (.venv), so we don't reinstall everything in Airflow's venv.

from __future__ import annotations                    # allows modern type hints
from pathlib import Path                              # build absolute paths safely
from datetime import timedelta                        # used for retry delays
import pendulum                                      # timezone library Airflow uses

from airflow import DAG                               # DAG = pipeline definition
from airflow.operators.bash import BashOperator       # run shell commands (bash)

# ---- CONFIGURE YOUR PATHS (EDIT THESE TWO LINES) -----------------------------
PROJECT_DIR = Path("/ABSOLUTE/PATH/TO/de-sprint")     # <<< CHANGE to your de-sprint folder absolute path
PROJECT_PY  = PROJECT_DIR / ".venv" / "bin" / "python" # your project's Python interpreter

# Why absolute paths?
# - Airflow runs tasks from its own working directory; absolute paths prevent "file not found".

# ---- DAG SETTINGS ------------------------------------------------------------
tz = pendulum.timezone("Europe/Bucharest")            # run & show times in your local timezone

default_args = {
    "retries": 2,                                     # if a task fails, retry it twice
    "retry_delay": timedelta(minutes=2),              # wait 2 minutes between retries
}

with DAG(
    dag_id="daily_orders_airflow",                    # unique pipeline name in the UI
    description="Extract→Bronze→Silver→Health (daily)",
    schedule="10 6 * * *",                            # CRON: run every day at 06:10 local time
    start_date=pendulum.datetime(2025, 8, 1, tz=tz),  # the earliest logical date we allow
    catchup=False,                                    # don't auto-run for all past dates
    default_args=default_args,                        # apply retries/delays to all tasks
    tags=["de-sprint", "orders"],                     # grouping label in the UI
    timezone=tz,                                      # ensure the DAG uses your timezone
) as dag:
    # {{ ds }} is an Airflow template: the execution date as 'YYYY-MM-DD'.
    # We pass it to your scripts as --date so they write/read the correct partitions.

    extract = BashOperator(
        task_id="extract_raw",                        # task name in the UI
        bash_command=(
            f"{PROJECT_PY} {PROJECT_DIR}/projects/a_lakehouse/extract_orders.py "
            "--date {{ ds }}"
        ),
    )

    bronze = BashOperator(
        task_id="bronze_parquet",
        bash_command=(
            f"{PROJECT_PY} {PROJECT_DIR}/projects/a_lakehouse/bronze_orders_parquet.py "
            "--date {{ ds }}"
        ),
    )

    silver = BashOperator(
        task_id="silver_build",
        bash_command=(
            # Run the whole silver build (drops/creates tables)
            f"{PROJECT_PY} {PROJECT_DIR}/projects/a_lakehouse/silver_build.py"
        ),
    )

    health = BashOperator(
        task_id="health_check",
        bash_command=(
            f"{PROJECT_PY} {PROJECT_DIR}/projects/a_lakehouse/health_check.py "
            "--date {{ ds }}"
        ),
    )

    # Define dependencies (order): extract → bronze → silver → health
    extract >> bronze >> silver >> health
