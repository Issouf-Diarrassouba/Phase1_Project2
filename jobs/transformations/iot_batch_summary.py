"""
iot_batch_summary.py

Batch job that reads curated Parquet time-series data (written by
spark_iot_consumer.py) and computes fixed time-window averages per
device_type / location, writing the result as a small star-schema-style
summary table to storage/summary/.

Designed to be run on a schedule by the Airflow DAG
dags/iot_summary_analytics.py, but is also runnable standalone:

    spark-submit jobs/transformations/iot_batch_summary.py \
        --window "15 minutes" --curated-path storage/curated \
        --summary-path storage/summary
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    window,
    avg,
    min as spark_min,
    max as spark_max,
    count,
    col,
)


def build_spark_session(app_name="streamflow-iot-batch-summary"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def load_curated(spark, curated_path):
    return spark.read.parquet(curated_path)


def compute_window_summary(curated_df, window_duration="15 minutes"):
    """Aggregate sensor readings into fixed time windows per device_type
    and location -- the core "star schema" fact table for reporting."""
    return (
        curated_df
        .groupBy(
            window(col("event_time"), window_duration).alias("time_window"),
            col("device_type"),
            col("location"),
        )
        .agg(
            avg("temperature_c").alias("avg_temperature_c"),
            spark_min("temperature_c").alias("min_temperature_c"),
            spark_max("temperature_c").alias("max_temperature_c"),
            avg("humidity_pct").alias("avg_humidity_pct"),
            avg("pressure_hpa").alias("avg_pressure_hpa"),
            avg("battery_pct").alias("avg_battery_pct"),
            count("*").alias("reading_count"),
        )
        .select(
            col("time_window.start").alias("window_start"),
            col("time_window.end").alias("window_end"),
            "device_type",
            "location",
            "avg_temperature_c",
            "min_temperature_c",
            "max_temperature_c",
            "avg_humidity_pct",
            "avg_pressure_hpa",
            "avg_battery_pct",
            "reading_count",
        )
        .orderBy("window_start", "device_type", "location")
    )


def write_summary(summary_df, summary_path):
    (
        summary_df.write
        .mode("overwrite")
        .partitionBy("device_type")
        .parquet(summary_path)
    )


def main():
    parser = argparse.ArgumentParser(description="Compute IoT time-window batch summaries.")
    parser.add_argument("--curated-path", default=os.environ.get("CURATED_PATH", "storage/curated"))
    parser.add_argument("--summary-path", default=os.environ.get("SUMMARY_PATH", "storage/summary"))
    parser.add_argument("--window", default=os.environ.get("SUMMARY_WINDOW", "15 minutes"),
                         help="Time window size, e.g. '15 minutes', '1 hour'")
    args = parser.parse_args()

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    curated_df = load_curated(spark, args.curated_path)
    summary_df = compute_window_summary(curated_df, args.window)
    write_summary(summary_df, args.summary_path)

    row_count = summary_df.count()
    print(f"[iot_batch_summary] wrote {row_count} summary rows to {args.summary_path}")

    spark.stop()


if __name__ == "__main__":
    main()