"""
iot_sensor_schema.py

Strict PySpark StructType definition for the raw IoT telemetry payload
produced by mock_iot_producer.py and consumed by spark_iot_consumer.py.

Keeping this schema centralized guarantees the producer, the streaming
consumer, and the batch summary job all agree on the wire format. Any
JSON message that doesn't conform to this schema (missing fields, wrong
types) will fail parsing and get routed to the dead-letter queue by the
consumer's corrupt-record handling.
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
)

# Wide/flat schema -- deliberately not nested, for simple downstream SQL access.
IOT_SENSOR_SCHEMA = StructType([
    StructField("device_id", StringType(), nullable=False),
    StructField("device_type", StringType(), nullable=False),
    StructField("location", StringType(), nullable=False),
    StructField("event_time", TimestampType(), nullable=False),
    StructField("temperature_c", DoubleType(), nullable=True),
    StructField("humidity_pct", DoubleType(), nullable=True),
    StructField("pressure_hpa", DoubleType(), nullable=True),
    StructField("battery_pct", DoubleType(), nullable=True),
    StructField("signal_strength_dbm", IntegerType(), nullable=True),
    StructField("firmware_version", StringType(), nullable=True),
])

# Column used for watermarking / late-data handling in the streaming consumer.
EVENT_TIME_COLUMN = "event_time"

# Fields that must be non-null for a record to be considered valid.
# Anything failing this check in the consumer gets routed to storage/dlq.
REQUIRED_FIELDS = ["device_id", "device_type", "location", "event_time"]