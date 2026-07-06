from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from jobs.schemas.schema import event_schema 

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SchemaTest") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

# Fake JSON data (simulates Kafka message)
data = [
    ('{"event_id":"1","event_type":"temperature","event_ts":"2026-07-06T10:00:00Z","source":"simulator","entity_id":"d1","payload":{"device_id":"s1","metric":"temp","value":72.5,"unit":"F","location":"Kitchen"}}',)
]

df = spark.createDataFrame(data, ["value"])

parsed = df.select(
    from_json(col("value"), event_schema).alias("data")
).select("data.*")

parsed.show(truncate=False)