# Path Directory
streamflow-phase1/
├── .github/                         # CI/CD pipelines
│   └── workflows/
│       └── ci-cd.yml
├── config/                          # Infrastructure settings
│   ├── airflow.cfg
│   └── spark-defaults.conf
├── dags/                            # Airflow Orchestration Workflows
│   ├── __init__.py
│   ├── iot_bounded_producer.py      # DAG to trigger fixed-batch sensor runs
│   └── iot_summary_analytics.py     # DAG to run batch summaries over Parquet files
├── docker/                          # Custom container building blocks
│   ├── airflow/
│   │   └── Dockerfile
│   └── spark/
│       └── Dockerfile
├── jobs/                            # Code Base (PySpark, Redpanda, Schemas)
│   ├── __init__.py
│   ├── consumers/                   # Spark Structured Streaming Engines
│   │   ├── __init__.py
│   │   └── spark_iot_consumer.py    # Main streaming ingestion code
│   ├── producers/                   # Data generation engines
│   │   ├── __init__.py
│   │   └── mock_iot_producer.py     # Continuous synthetic sensor stream script
│   ├── schemas/                     # Strict layout blueprints
│   │   ├── __init__.py
│   │   └── iot_sensor_schema.py     # PySpark StructType wide/flat schema
│   └── transformations/             # Analytical logic scripts
│       ├── __init__.py
│       └── iot_batch_summary.py     # SparkSQL script for time-window averages
├── scripts/                         # Automation tools
│   └── init-redpanda-topics.sh      # Shell script to auto-create "iot-telemetry" topic
├── storage/                         # Persistent Local Lakehouse Storage Volumes
│   ├── checkpoint/                  # Streaming state management storage
│   ├── raw/                         # Untouched stream backups
│   ├── curated/                     # Cleaned, validated time-series Parquet files
│   ├── dlq/                         # Dead-Letter Queue (corrupted/bad raw text files)
│   └── summary/                     # Aggregated batch metrics (Star schema / reports)
├── tests/                           # Integrity checks
│   ├── __init__.py
│   ├── test_iot_generator.py
│   └── test_iot_validation.py
├── .env                             # Local environment configurations (BROKER_ADDRESS)
├── .gitignore                       # Git exclusion list
├── README.md                        # Blueprint runbook and hand-off guide
├── docker-compose.yml               # Multi-container cluster layout manifest
└── requirements.txt                 # Application python dependencies

# Quick Start Commands

Reference for running the pipeline locally via Docker Compose.

## 1. Start the whole stack

```bash
docker compose up --build
```
Starts Redpanda, Spark, producer, and consumer together, with logs streaming inline in one terminal.

## 2. Or run pieces individually (useful for debugging)

Start just Redpanda + producer in the background:
```bash
docker compose up -d redpanda producer
```

Watch producer logs live:
```bash
docker compose logs -f producer
```

Run just the consumer (foreground, container auto-removes on exit):
```bash
docker compose run --rm consumer
```

## 3. Check the Kafka topic exists

```bash
docker exec -it redpanda rpk topic list
```

Create it if missing:
```bash
docker exec -it redpanda rpk topic create smart-home.events --partitions 1 --replicas 1
```

## 4. Run the batch summary job

```bash
docker compose run --rm spark /opt/spark/bin/spark-submit /jobs/transformations/iot_batch_summary.py
```

## 5. Verify output

```bash
find ./storage/raw -name "*.parquet"
find ./storage/summary -name "*.parquet"
```

## 6. Shut everything down

```bash
docker compose down
```

## Known gotcha

If the batch summary throws `Unable to infer schema for Parquet` after multiple consumer restarts, it's usually a stale streaming metadata log. Clear it and re-run:

```bash
rm -rf ./storage/raw/events/_spark_metadata
```

This only removes Spark's internal bookkeeping for exactly-once writes — it does not touch any actual `.parquet` data files.