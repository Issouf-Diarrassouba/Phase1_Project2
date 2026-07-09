"""
StreamFlow IoT — daily batch-summary DAG (fixed for the Phase 1 repo).

Airflow orchestrates ONLY bounded work here. The two long-running pieces —
mock_iot_producer.py and spark_iot_consumer.py — are NOT run by Airflow;
they run continuously in their own containers (see docker-compose.yml).
This DAG operates on the Parquet the streaming consumer has already written.

Tasks (run in order):
  1. check_raw_data     — sensor: wait until storage/raw/events has parquet files
  2. run_batch_summary  — spark-submit jobs/transformations/iot_batch_summary.py
  3. validate_summary   — confirm the summary parquet outputs exist & are non-empty
  4. report             — print a short human-readable run summary to the log

Path model (matches docker-compose.yml):
  The Airflow container mounts the repo's ./jobs at /opt/streamflow/jobs and
  ./storage at /opt/streamflow/storage — the SAME host ./storage directory the
  Spark consumer container writes into (it sees it as /storage). So the sensor,
  the batch job, and the validator all look at one physical directory.

Configuration is via env vars so the same DAG works on a laptop or in Docker:
  IOT_PROJECT_DIR   root of the project inside this container (default /opt/streamflow)
  IOT_RAW_PATH      where the consumer writes valid events (default storage/raw/events)
  IOT_SUMMARY_DIR   where the batch job writes summaries (default storage/summary)
  SPARK_SUBMIT      path to spark-submit (default: "spark-submit" on PATH)

Trigger it manually from the Airflow UI, or let it run on the daily schedule.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor

# --- configuration (override with env vars in your Airflow environment) ---
PROJECT_DIR = os.environ.get("IOT_PROJECT_DIR", "/opt/streamflow")
# FIX: the consumer writes to storage/raw/events (not data/raw/events)
RAW_PATH = os.environ.get("IOT_RAW_PATH", os.path.join(PROJECT_DIR, "storage/raw/events"))
SUMMARY_DIR = os.environ.get("IOT_SUMMARY_DIR", os.path.join(PROJECT_DIR, "storage/summary"))
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")
# FIX: the batch job lives in jobs/transformations/, not jobs/batch/
BATCH_JOB = os.path.join(PROJECT_DIR, "jobs/transformations/iot_batch_summary.py")

default_args = {
    "owner": "streamflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _raw_data_ready() -> bool:
    """True once at least one .parquet file exists under the raw events path."""
    if not os.path.isdir(RAW_PATH):
        print(f"[sensor] raw path does not exist yet: {RAW_PATH}")
        return False
    found = False
    for root, _dirs, files in os.walk(RAW_PATH):
        # ignore Spark's _spark_metadata bookkeeping when deciding readiness
        if "_spark_metadata" in root:
            continue
        if any(f.endswith(".parquet") for f in files):
            found = True
            break
    print(f"[sensor] parquet present under {RAW_PATH}: {found}")
    return found


def _validate_summary() -> None:
    """Fail the task if the expected summary outputs are missing or empty."""
    expected = ["event_summary", "metric_summary", "room_summary"]
    problems = []
    for name in expected:
        path = os.path.join(SUMMARY_DIR, name)
        if not os.path.isdir(path):
            problems.append(f"missing summary output: {path}")
            continue
        has_parquet = any(
            f.endswith(".parquet")
            for _r, _d, fs in os.walk(path)
            for f in fs
        )
        if not has_parquet:
            problems.append(f"summary output has no parquet files: {path}")
    if problems:
        raise RuntimeError("Summary validation failed:\n  " + "\n  ".join(problems))
    print(f"[validate] all summary outputs present under {SUMMARY_DIR}: {expected}")


def _report() -> None:
    print("=" * 55)
    print("STREAMFLOW IoT BATCH SUMMARY — run complete")
    print(f"  raw source : {RAW_PATH}")
    print(f"  summaries  : {SUMMARY_DIR}")
    for name in ("event_summary", "metric_summary", "room_summary"):
        path = os.path.join(SUMMARY_DIR, name)
        status = "ok" if os.path.isdir(path) else "MISSING"
        print(f"    - {name}: {status}")
    print("=" * 55)


with DAG(
    dag_id="iot_batch_summary",
    description="Bounded Spark batch summary over the persisted IoT stream output",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 1 * * *",  # daily at 01:00 UTC; also triggerable manually
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["streamflow", "iot", "spark", "batch"],
) as dag:

    check_raw_data = PythonSensor(
        task_id="check_raw_data",
        python_callable=_raw_data_ready,
        poke_interval=15,
        timeout=60 * 10,
        mode="reschedule",  # frees the worker slot between pokes
    )

    run_batch_summary = BashOperator(
        task_id="run_batch_summary",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"{SPARK_SUBMIT} --master local[2] {BATCH_JOB}"
        ),
        # FIX: the batch job now reads its paths from env instead of
        # hard-coding /storage/... — point it at THIS container's mounts.
        env={
            "IOT_BATCH_INPUT": RAW_PATH,
            "IOT_BATCH_OUTPUT": SUMMARY_DIR,
        },
        append_env=True,  # keep PATH/JAVA_HOME etc. from the container
    )

    validate_summary = PythonOperator(
        task_id="validate_summary",
        python_callable=_validate_summary,
    )

    report = PythonOperator(
        task_id="report",
        python_callable=_report,
    )

    check_raw_data >> run_batch_summary >> validate_summary >> report
