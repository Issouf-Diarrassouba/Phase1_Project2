from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, min, max

# Creating the spark session

spark = (
    SparkSession.builder
    .appName("Phase 1: IoT Batch Summary")
    .getOrCreate()
)

# Reading the raw parquet files

raw_df = spark.read.parquet("storage/raw/events")  # Confirm path with Brian

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

event_summary.write.mode("overwrite").parquet("storage/summary/event_summary")

metric_summary.write.mode("overwrite").parquet("storage/summary/metric_summary")

room_summary.write.mode("overwrite").parquet("storage/summary/room_summary")

print("Batch summary completed successfully.")

spark.stop()