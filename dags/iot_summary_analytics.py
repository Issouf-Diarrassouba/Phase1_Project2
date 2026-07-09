import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/project")

default_args = {
    "owner": "streamflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="iot_summary_analytics",
    default_args=default_args,
    description="Runs the IoT batch summary job over data in storage/raw/events",
    schedule_interval=None,
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["streamflow", "batch"],
) as dag:

    run_batch_summary = DockerOperator(
        task_id="run_batch_summary",
        image="apache/spark:4.0.3",
        api_version="auto",
        auto_remove=True,
        command="/opt/spark/bin/spark-submit /jobs/transformations/iot_batch_summary.py",
        network_mode="phase1_project2_default",
        mounts=[
            Mount(source=f"{PROJECT_ROOT}/jobs", target="/jobs", type="bind"),
            Mount(source=f"{PROJECT_ROOT}/storage", target="/storage", type="bind"),
        ],
        mount_tmp_dir=False,
    )

    run_batch_summary