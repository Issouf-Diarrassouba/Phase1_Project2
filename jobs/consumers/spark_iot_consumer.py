"""
spark_iot_consumer.py

Main streaming ingestion job. Reads raw JSON telemetry events off the
Redpanda topic "iot-telemetry" using Spark Structured Streaming's Kafka
source (Redpanda is wire-compatible with the Kafka protocol), validates
each record against IOT_SENSOR_SCHEMA, and fans records out to three
sinks:

    storage/raw/       -- untouched raw JSON string + Kafka metadata, for replay
    storage/curated/    -- parsed, validated, deduplicated Parquet time-series
    storage/dlq/        -- malformed / schema-violating records, as raw text

Run with:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        jobs/consumers/spark_iot_consumer.py
"""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_json,
    struct,
)

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from jobs.schemas.iot_sensor_schema import (  # noqa: E402
    IOT_SENSOR_SCHEMA,
    EVENT_TIME_COLUMN,
    REQUIRED_FIELDS,
)

BROKER_ADDRESS = os.environ.get("BROKER_ADDRESS", "localhost:19092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "iot-telemetry")

RAW_PATH = os.environ.get("RAW_PATH", "storage/raw")
CURATED_PATH = os.environ.get("CURATED_PATH", "storage/curated")
DLQ_PATH = os.environ.get("DLQ_PATH", "storage/dlq")
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "storage/checkpoint")

WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "2 minutes")
TRIGGER_INTERVAL = os.environ.get("TRIGGER_INTERVAL", "10 seconds")


def build_spark_session(app_name="streamflow-iot-consumer"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_kafka_stream(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", BROKER_ADDRESS)
        .option("subscribe", TOPIC_NAME)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def write_raw_backup(raw_df, query_name="raw_backup"):
    """Persist the untouched raw payload + Kafka metadata for replay/audit."""
    raw_out = raw_df.select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
    )

    return (
        raw_out.writeStream
        .format("parquet")
        .option("path", RAW_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{query_name}")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName(query_name)
        .start()
    )


def parse_and_validate(raw_df):
    """Parse the JSON payload against IOT_SENSOR_SCHEMA.

    Returns a tuple (valid_df, invalid_df):
        valid_df   -- rows that parsed successfully AND have all
                      REQUIRED_FIELDS populated
        invalid_df -- raw text of rows that failed parsing or validation,
                      destined for the DLQ
    """
    parsed = raw_df.select(
        col("value").cast("string").alias("raw_json"),
        from_json(col("value").cast("string"), IOT_SENSOR_SCHEMA).alias("data"),
    )

    # A row is "valid" only if JSON parsing succeeded (data is not null)
    # and every required field survived that parse.
    required_not_null = None
    for field in REQUIRED_FIELDS:
        cond = col(f"data.{field}").isNotNull()
        required_not_null = cond if required_not_null is None else (required_not_null & cond)

    valid_mask = col("data").isNotNull() & required_not_null

    valid_df = (
        parsed.filter(valid_mask)
        .select("data.*")
        .withColumn("_ingested_at", current_timestamp())
    )

    invalid_df = (
        parsed.filter(~valid_mask)
        .select(
            col("raw_json"),
            current_timestamp().alias("_rejected_at"),
        )
    )

    return valid_df, invalid_df


def write_curated(valid_df, query_name="curated_iot"):
    """Deduplicated, watermarked curated sink, partitioned by device_type
    and calendar date for efficient downstream batch queries."""
    from pyspark.sql.functions import to_date

    curated = (
        valid_df
        .withWatermark(EVENT_TIME_COLUMN, WATERMARK_DELAY)
        .dropDuplicates(["device_id", EVENT_TIME_COLUMN])
        .withColumn("event_date", to_date(col(EVENT_TIME_COLUMN)))
    )

    return (
        curated.writeStream
        .format("parquet")
        .option("path", CURATED_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{query_name}")
        .partitionBy("event_date", "device_type")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName(query_name)
        .start()
    )


def write_dlq(invalid_df, query_name="dlq_iot"):
    return (
        invalid_df.writeStream
        .format("json")
        .option("path", DLQ_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/{query_name}")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName(query_name)
        .start()
    )


def main():
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_kafka_df = read_kafka_stream(spark)

    raw_query = write_raw_backup(raw_kafka_df)
    valid_df, invalid_df = parse_and_validate(raw_kafka_df)
    curated_query = write_curated(valid_df)
    dlq_query = write_dlq(invalid_df)

    print("[spark_iot_consumer] streams started: raw_backup, curated_iot, dlq_iot")

    for q in (raw_query, curated_query, dlq_query):
        q.awaitTermination()


if __name__ == "__main__":
    main()