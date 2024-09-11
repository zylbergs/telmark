from pyspark.sql import SparkSession
from pyspark.sql.functions import col
#pyspark import unix_timestamp
from pyspark.sql.functions import unix_timestamp
#pyspark import TimestampType
from pyspark.sql.types import TimestampType
import pandas as pd

# Step 1: Set up the Spark session
spark = SparkSession.builder \
    .appName("Streaming JSON Processing") \
    .getOrCreate()
spark.sql("set spark.sql.streaming.schemaInference=true")
# Step 2: Define the streaming source (e.g., a directory)
input_directory = r"dwh\transaction"

# Step 3: Read the streaming JSON data into a PySpark DataFrame
streaming_df = spark.readStream \
    .format("json") \
    .option("path", input_directory) \
    .option("maxFilesPerTrigger", 1) \
    .load()

processed_df = streaming_df.select("customer_id", "transaction_price")

#write datafram to csv file
processed_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", r"dwh\transaction\stream_out.json") \
    .option("checkpointLocation", "checkpoint") \
    .start().awaitTermination()

# Stop the Spark session
spark.stop()