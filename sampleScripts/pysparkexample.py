from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import from_json, col, count, to_timestamp, window

# Initialize Spark Session for Dataproc
spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .getOrCreate()

# Define schema based on your provided JSON sample
json_schema = StructType([
    StructField("keywords", StringType(), True),
    StructField("title", StringType(), True),
    StructField("source", StringType(), True),
    StructField("author", StringType(), True),
    StructField("description", StringType(), True),
    StructField("date_published", StringType(), True),
    StructField("content", StringType(), True),
])

# Create a streaming DataFrame from Kafka source
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-s-4") \
    .option("checkpointLocation", "file:////home/duty095/chkpt") \
    .option("startingOffsets", "earliest") \
    .load()

# The Kafka source provides messages as binary data in the "value" column
# We need to cast it to string and parse the JSON
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_data", "timestamp") \
    .select(from_json("json_data", json_schema).alias("data"), "timestamp") \
    .select(
        col("data.title").alias("title"),
        col("data.author").alias("author"),
        col("data.source").alias("source"),
        col("timestamp").alias("event_time")  # Ensure it's a timestamp type
    )  # Define watermark BEFORE aggregation

# Now perform the aggregation
aggregated_df = parsed_df \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window(col("event_time"), "1 minute")) \
    .count()

final_df = aggregated_df.withColumn("key", "100") \
    .select(col("key").alias("key"), col("window.start").alias("window_start"), col("window.end").alias("window_end"), col("count"))

# Use append mode with the watermark
query = final_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "file:////home/duty095/chkpt") \
    .option("truncate", "false") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()