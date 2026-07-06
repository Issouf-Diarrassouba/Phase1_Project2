# Streamflow Phase 1: IoT Telemetry Lakehouse Pipeline

An end-to-end streaming data pipeline that ingests continuous synthetic IoT sensor events, processes them using structured streaming micro-batches, and orchestrates periodic batch transformations over a local Parquet data lakehouse.

## Architecture & Capabilities

* **Event Broker:** Redpanda (Kafka-API compatible) handles real-time telemetry events.
* **Stream Processing:** Apache Spark Structured Streaming ingests events, handles deduplication, filters corrupted text via a Dead-Letter Queue (DLQ), and saves raw backups and curated records.
* **Orchestration:** Apache Airflow automates metadata migrations and schedules batch jobs.
* **Batch Analytics:** SparkSQL scripts run time-window aggregations over Parquet layers to generate summary reports.

---

## Path Directory Blueprint

```text
streamflow-phase1/
├── .github/                # CI/CD pipelines
│   └── workflows/ci-cd.yml
├── config/                 # Infrastructure engine settings
│   ├── airflow.cfg
│   └── spark-defaults.conf
├── dags/                   # Airflow Orchestration Workflows
│   ├── __init__.py
│   ├── iot_bounded_producer.py      # Triggers fixed-batch sensor validation runs
│   └── iot_summary_analytics.py     # Triggers batch summaries over Parquet files
├── docker/                 # Custom container images
│   ├── airflow/Dockerfile           # Extends Airflow with JDK & PySpark tooling
│   └── spark/Dockerfile             # Extends Spark from Bitnami legacy open-source layers
├── jobs/                   # Central Code Base
│   ├── __init__.py
│   ├── consumers/          # Spark Structured Streaming Engines
│   │   ├── __init__.py
│   │   └── spark_iot_consumer.py    # Ingests, processes, and writes Redpanda streams
│   ├── producers/          # Data generation tools
│   │   ├── __init__.py
│   │   └── mock_iot_producer.py     # Continuous synthetic loop script
│   ├── schemas/            # Strict blueprint layouts
│   │   ├── __init__.py
│   │   └── iot_sensor_schema.py     # PySpark StructType wide/flat schema
│   └── transformations/    # Analytical logic scripts
│       ├── __init__.py
│       └── iot_batch_summary.py     # SparkSQL script for time-window averages
├── scripts/                # Automation scripts
│   └── init-redpanda-topics.sh      # Internal Docker script to auto-create topics
├── storage/                # Persistent Local Lakehouse Storage Volumes
│   ├── checkpoint/         # Streaming state management storage (handles crash recovery)
│   ├── raw/                # Untouched real-time stream backups
│   ├── curated/            # Cleaned, validated time-series Parquet files
│   ├── dlq/                # Dead-Letter Queue (corrupted or malformed events)
│   └── summary/            # Aggregated metrics (Star schema / analytical reporting files)
├── tests/                  # Integrity checks
│   ├── __init__.py
│   ├── test_iot_generator.py
│   └── test_iot_validation.py
├── .env                    # Local environment settings (Ignored by Git)
├── .env.example            # Public placeholder template for setup instructions
├── .gitignore              # Git exclusion list
├── docker-compose.yml      # Multi-container cluster layout manifest
└── requirements.txt        # Local python application dependencies
```

---

## Local Environment Configurations (`.env`)

Before starting the pipeline, ensure you have a local `.env` file at your root directory. 

> ⚠️ **Security Warning:** Never commit your actual `.env` file containing database passwords or secret keys to a remote repository. Ensure `.env` is listed in your `.gitignore`. 

Use the following generic blueprint configuration for local testing:

```env
# Infrastructure Addresses
# .env -- Local Docker Compose development configuration
# This file is for LOCAL DEV ONLY. Do not commit real secrets here;
# it is listed in .gitignore.

# --- Redpanda / Kafka ---
BROKER_ADDRESS=redpanda:9092
BROKER_ADDRESS_HOST=localhost:19092
TOPIC_NAME=iot-telemetry
TOPIC_PARTITIONS=3
TOPIC_REPLICATION_FACTOR=1

# --- Spark ---
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2g
SPARK_KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

# --- Storage paths (mounted volumes, relative to project root inside containers) ---
RAW_PATH=storage/raw
CURATED_PATH=storage/curated
DLQ_PATH=storage/dlq
SUMMARY_PATH=storage/summary
CHECKPOINT_ROOT=storage/checkpoint

# --- Streaming tuning ---
WATERMARK_DELAY=2 minutes
TRIGGER_INTERVAL=10 seconds
SUMMARY_WINDOW=15 minutes

# --- Airflow ---
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=false
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__WEBSERVER__SECRET_KEY=dev-only-local-secret-key-change-me

# --- Airflow metadata Postgres ---
POSTGRES_USER=airflow
POSTGRES_PASSWORD=password_here
POSTGRES_DB=airflow
```

---

## Step-by-Step Execution Guide

### 1. Build and Launch the Infrastructure
Ensure Docker Desktop is open and active on your system. Run this command from the root directory to download image tags and launch all background service containers:

```bash
docker compose up -d --build
```
*Note: The `redpanda-init` container will automatically run the topic initialization script to safely spin up the `iot-telemetry` topic once the broker becomes healthy.*

### 2. Activate Your Local Python Environment
Activate your local virtual environment and install the script dependencies on your laptop machine to run the producer:

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the Live Data Generator
Launch the continuous mock streaming script to begin publishing events to the active broker. Use the module flag (`-m`) to prevent file resolution path issues across host system boundaries:

```bash
export PYTHONPATH=\$PWD
python -m jobs.producers.mock_iot_producer
```
*(Press `Ctrl + C` inside this terminal whenever you want to stop generating new events).*

### 4. Run the Spark Streaming Engine
Open a separate terminal window and submit your main stream processing script using the explicit Spark-Kafka connection connector package:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2 jobs/consumers/spark_iot_consumer.py
```
* Once active, you will see structured data streams landing directly inside `storage/raw/`, `storage/curated/`, and `storage/dlq/`.
* View live micro-batch timelines by tracking the Spark UI dashboard at `http://localhost:4040/streaming`.

### 5. Orchestrate Analytical Aggregations
1. Open your web browser and navigate to the Airflow UI at `http://localhost:8080`.
2. Login with credentials: Username: `airflow` / Password: `airflow`.
3. Locate the **`iot_summary_analytics`** DAG and toggle its status to **Active**.
4. Click **Trigger DAG** (the Play button) to process batch analytics over your Parquet data and populate your **`storage/summary/`** directory.

### 6. Clean Shutdown
To safely turn off all services, stop the local terminal streams using `Ctrl + C`, then shut down the background containers:

```bash
docker compose down
```
