import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, min, max

# Paths are configurable so the same script works everywhere:
#   - inside the spark container (README command): defaults /storage/... still work
#   - from the Airflow DAG: it sets IOT_BATCH_INPUT / IOT_BATCH_OUTPUT to the
#     paths mounted inside the Airflow container (/opt/streamflow/storage/...)
INPUT_PATH = os.environ.get("IOT_BATCH_INPUT", "/storage/raw/events")
OUTPUT_DIR = os.environ.get("IOT_BATCH_OUTPUT", "/storage/summary")

# Creating the spark session

spark = (
    SparkSession.builder
    .appName("Phase 1: IoT Batch Summary")
    .getOrCreate()
)

print(f"[batch] reading raw events from : {INPUT_PATH}")
print(f"[batch] writing summaries under : {OUTPUT_DIR}")

# Reading the raw parquet files

raw_df = spark.read.parquet(INPUT_PATH)

# Inspecting the data

raw_df.printSchema()
raw_df.show(10, truncate=False)

# Count events by type

event_summary = (
    raw_df
    .groupBy("event_type")
    .count()
)

# Showing the total events by type

event_summary.show()

# Average, minimum, maximum, and total values for each event type

metric_summary = (
    raw_df
    .groupBy("event_type")
    .agg(
        avg("value").alias("average_value"),
        min("value").alias("minimum_value"),
        max("value").alias("maximum_value"),
        count("*").alias("total_events")
    )
)

metric_summary.show()

# Count events by room

room_summary = (
    raw_df
    .groupBy("room")
    .count()
)

room_summary.show()

# Writing the summaries to Parquet

event_summary.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "event_summary"))

metric_summary.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "metric_summary"))

room_summary.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "room_summary"))

print("Batch summary completed successfully.")

spark.stop()
