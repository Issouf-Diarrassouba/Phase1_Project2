import os
import sys
import glob

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, min, max

# Resolve input/output paths in priority order:
#   1. command-line args   (how the Airflow DAG passes them — most reliable)
#   2. environment vars    (IOT_BATCH_INPUT / IOT_BATCH_OUTPUT)
#   3. sensible defaults    (works when run by hand in the spark container)
def _arg(idx, env_name, default):
    if len(sys.argv) > idx and sys.argv[idx].strip():
        return sys.argv[idx].strip()
    val = os.environ.get(env_name, "").strip()
    return val if val else default

INPUT_PATH = _arg(1, "IOT_BATCH_INPUT", "/storage/raw/events")
OUTPUT_DIR = _arg(2, "IOT_BATCH_OUTPUT", "/storage/summary")

if not INPUT_PATH:
    raise SystemExit("[batch] ERROR: no input path resolved (argv/IOT_BATCH_INPUT both empty)")
if not OUTPUT_DIR:
    raise SystemExit("[batch] ERROR: no output path resolved (argv/IOT_BATCH_OUTPUT both empty)")

print(f"[batch] reading raw events from : {INPUT_PATH}")
print(f"[batch] writing summaries under : {OUTPUT_DIR}")

# --- Find the actual Parquet data files, ignoring Spark's streaming metadata ---
# The streaming consumer writes a `_spark_metadata` log alongside the data.
# spark.read.parquet(dir) trusts that log, so a stale/empty log makes a
# non-empty folder look schema-less ("Unable to infer schema at ."). We read
# the part files directly instead, which cannot be tripped up by that log.
part_files = sorted(
    glob.glob(os.path.join(INPUT_PATH, "**", "*.parquet"), recursive=True)
)
# Defensive: never let a metadata file sneak into the read list.
part_files = [f for f in part_files if "_spark_metadata" not in f]

if not part_files:
    raise SystemExit(
        f"[batch] ERROR: no .parquet data files under {INPUT_PATH}. "
        "The streaming consumer probably hasn't committed a batch yet — "
        "start the producer + consumer and let them run ~30s before this job."
    )

print(f"[batch] found {len(part_files)} parquet file(s); reading directly")

spark = (
    SparkSession.builder
    .appName("Phase 1: IoT Batch Summary")
    .getOrCreate()
)

# Read the explicit list of data files (bypasses _spark_metadata entirely).
raw_df = spark.read.parquet(*part_files)

raw_df.printSchema()
raw_df.show(10, truncate=False)

# Count events by type
event_summary = raw_df.groupBy("event_type").count()
event_summary.show()

# Average / min / max / total per event type
metric_summary = (
    raw_df
    .groupBy("event_type")
    .agg(
        avg("value").alias("average_value"),
        min("value").alias("minimum_value"),
        max("value").alias("maximum_value"),
        count("*").alias("total_events"),
    )
)
metric_summary.show()

# Count events by room
room_summary = raw_df.groupBy("room").count()
room_summary.show()

# Write the three summaries to Parquet
event_summary.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "event_summary"))
metric_summary.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "metric_summary"))
room_summary.write.mode("overwrite").parquet(os.path.join(OUTPUT_DIR, "room_summary"))

print("Batch summary completed successfully.")

spark.stop()
