"""
iot_summary_analytics.py

Airflow DAG that runs the batch summary job (jobs/transformations/iot_batch_summary.py)
on a recurring schedule against whatever curated Parquet data the streaming
consumer has written so far, producing time-windowed averages in storage/summary.

Schedule: every 15 minutes by default.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "streamflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

PROJECT_ROOT = "/opt/airflow/project"
SUMMARY_WINDOW = "15 minutes"

with DAG(
    dag_id="iot_summary_analytics",
    description="Scheduled batch summary of curated IoT telemetry into time-windowed aggregates.",
    default_args=DEFAULT_ARGS,
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["streamflow", "batch", "analytics"],
) as dag:

    run_batch_summary = BashOperator(
        task_id="run_batch_summary",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"spark-submit jobs/transformations/iot_batch_summary.py "
            f'--window "{SUMMARY_WINDOW}" '
            f"--curated-path storage/curated "
            f"--summary-path storage/summary"
        ),
    )

    run_batch_summary