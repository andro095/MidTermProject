import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, window
from pyspark.sql.types import StructType

load_dotenv()

# Initialize Spark Session for Dataproc
spark = SparkSession.builder \
    .appName(os.getenv('SPARK_APP_NAME')) \
    .getOrCreate()

json_schema = (StructType()
                .add("keywords", "string")
                .add("title", "string")
                .add("source", "string")
                .add("author", "string")
                .add("description", "string")
                .add("date_published", "string")
                .add("content", "string")
               )

raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{os.getenv('GCP_LOCAL_HOST')}:{os.getenv('GCP_KAFKA_PORT')}") \
    .option("subscribe", os.getenv('GCP_KAFKA_TOPIC')) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_data", "timestamp") \
    .select(from_json("json_data", json_schema).alias("data"), "timestamp") \
    .select("data.*", "timestamp") \

source_counts = parsed_stream.withWatermark("timestamp", "1 second") \
    .groupBy(window("timestamp", "30 seconds"), "source") \
    .count()

source_counts = source_counts.select("window.start", "window.end", "source", "count")

query = source_counts.writeStream \
    .format("csv") \
    .option("path", os.getenv('OUTPUT_DIR')) \
    .option("checkpointLocation", os.getenv('CHKPTFILE_DIR')) \
    .start()

query.awaitTermination()