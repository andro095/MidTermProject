from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

# Initialize Spark Session for Dataproc
spark = SparkSession.builder \
    .appName("StreamingExample") \
    .getOrCreate()

# Define schema based on your provided JSON sample
json_schema = StructType([
    StructField("Arrival_Time", LongType(), True),
    StructField("Creation_Time", LongType(), True),
    StructField("Device", StringType(), True),
    StructField("Index", LongType(), True),
    StructField("Model", StringType(), True),
    StructField("User", StringType(), True),
    StructField("gt", StringType(), True),
    StructField("x", DoubleType(), True),
    StructField("y", DoubleType(), True),
    StructField("z", DoubleType(), True)
])

# Create a streaming DataFrame from JSON source
stream_df = spark.readStream \
    .format("json") \
    .option("path", "hdfs:///BigData/logs") \
    .schema(json_schema) \
    .load()

# Simply display the raw data without processing
query = stream_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Wait for the streaming query to terminate
query.awaitTermination()