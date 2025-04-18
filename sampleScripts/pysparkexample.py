from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from pyspark.sql.functions import from_json, col

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
    .option("subscribe", "test-s-5") \
    .option("checkpointLocation", "file:////home/duty095/chkpt") \
    .option("startingOffsets", "earliest") \
    .load()

# The Kafka source provides messages as binary data in the "value" column
# We need to cast it to string and parse the JSON
parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", json_schema).alias("data")) \
    .select(col("data.title").alias("titulo"),  col("data.author").alias("autor"))

# Display the parsed data
query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "file:////home/duty095/chkpt") \
    .option("truncate", "false") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()