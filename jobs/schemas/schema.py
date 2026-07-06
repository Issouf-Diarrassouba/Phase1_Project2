from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# This file is deifning how Spark should understand the incoming Kafka JSON data 
#----------------------------------------------------------------------------------------------------


# payload schema defines the structure of the nested payload feild inside each event 
# It represents the actual business data of the event 
payload_schema = StructType([
    StructField("device_id", StringType(), True),
    StructField("metric", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("unit", StringType(), True),
    StructField("location", StringType(), True),
])

# Even Schema is defining the full structure of the kafka event 
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_ts", StringType(), False),
    StructField("source", StringType(), False),
    StructField("entity_id", StringType(), True),
    StructField("payload", payload_schema, False), # Calleing the pyload schema to obtsain the important business information 
])