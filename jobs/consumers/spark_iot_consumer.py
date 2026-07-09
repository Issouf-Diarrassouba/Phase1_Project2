import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import from_json, col, when

# ---- 1. Define the schema (must match the producer's flat structure exactly) ----
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_ts", StringType(), False),
    StructField("device_id", StringType(), False),
    StructField("room", StringType(), False),
    StructField("source", StringType(), False),
    StructField("value", DoubleType(), False),
    StructField("unit", StringType(), False),
])

# ---- 2. Validation rules (same ranges as the producer used) ----
VALID_EVENT_TYPES = ["temperature", "humidity", "motion", "smoke", "battery"]

VALUE_RANGES = {
    "temperature": (-20, 130),
    "humidity": (0, 100),
    "motion": (0, 1),
    "smoke": (0, 1),
    "battery": (0, 100),
}

def build_spark_session():
    return SparkSession.builder \
        .appName("StreamFlowIoTConsumer") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.3") \
        .getOrCreate()

def read_from_kafka(spark, bootstrap_servers=None, topic="smart-home.events"):
    # Read the broker from the environment (compose sets KAFKA_BOOTSTRAP_SERVERS
    # to redpanda:9092). Falls back to localhost for running outside Docker.
    if bootstrap_servers is None:
        bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"[consumer] reading from Kafka at {bootstrap_servers}, topic {topic}")
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

def parse_events(raw_df):
    return raw_df.select(
        from_json(col("value").cast("string"), event_schema).alias("data")
    ).select("data.*")

def validate_events(parsed_df):
    # Build one big "is this valid" condition, checking type + value range together
    validity_expr = None
    for event_type, (low, high) in VALUE_RANGES.items():
        condition = (col("event_type") == event_type) & (col("value") >= low) & (col("value") <= high)
        validity_expr = condition if validity_expr is None else (validity_expr | condition)

    labeled_df = parsed_df.withColumn(
        "is_valid",
        when(validity_expr, True).otherwise(False)
    ).withColumn(
        "reject_reason",
        when(validity_expr, None)
        .when(~col("event_type").isin(VALID_EVENT_TYPES), "unknown_event_type")
        .otherwise("value_out_of_range")
    )

    valid_df = labeled_df.filter(col("is_valid") == True).drop("is_valid", "reject_reason")
    rejected_df = labeled_df.filter(col("is_valid") == False).drop("is_valid")

    return valid_df, rejected_df

def write_valid_stream(valid_df, output_path="storage/raw/events", checkpoint_path="storage/checkpoints/events"):
    return valid_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .start()

def write_rejected_stream(rejected_df, output_path="storage/dlq/events", checkpoint_path="storage/checkpoints/dlq"):
    return rejected_df.writeStream \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .outputMode("append") \
        .start()

if __name__ == "__main__":
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_from_kafka(spark)
    parsed_df = parse_events(raw_df)
    valid_df, rejected_df = validate_events(parsed_df)

    valid_query = write_valid_stream(valid_df)
    rejected_query = write_rejected_stream(rejected_df)

    # Surface a failure in EITHER stream instead of silently ignoring the DLQ one.
    spark.streams.awaitAnyTermination()
