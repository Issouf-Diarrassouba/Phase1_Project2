"""
iot_bounded_producer.py

Airflow DAG that triggers a fixed-batch run of mock_iot_producer.py --
i.e. it sends an exact, reproducible number of synthetic sensor messages
rather than streaming indefinitely. Useful for demos, load testing, and
generating deterministic fixtures for the curated/summary layers.

Schedule: manual/on-demand by default (schedule=None). Trigger via the
Airflow UI or CLI:

    airflow dags trigger iot_bounded_producer
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "streamflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

PROJECT_ROOT = "/opt/airflow/project"  # mounted project root inside the Airflow container
MESSAGE_COUNT = 500
MESSAGE_RATE = 25  # messages/sec

with DAG(
    dag_id="iot_bounded_producer",
    description="Fixed-batch synthetic IoT sensor run for testing/demo purposes.",
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["streamflow", "producer", "iot"],
) as dag:

    run_bounded_producer = BashOperator(
        task_id="run_bounded_producer",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python jobs/producers/mock_iot_producer.py "
            f"--count {MESSAGE_COUNT} --rate {MESSAGE_RATE}"
        ),
    )

    run_bounded_producer