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
