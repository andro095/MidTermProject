from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, from_unixtime, window
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize Spark Session for Dataproc
spark = SparkSession.builder \
    .appName(os.getenv('SPARK_APP_NAME')) \
    .getOrCreate()

news_schema = (StructType()
                .add("keywords", "string")
                .add("title", "string")
                .add("source", "string")
                .add("author", "string")
                .add("description", "string")
                .add("date_published", "string")
                .add("content", "string")
               )

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"{os.getenv('GCP_LOCAL_HOST')}:{os.getenv('GCP_KAFKA_PORT')}") \
    .option("subscribe", os.getenv('GCP_KAFKA_TOPIC')) \
    .option("startingOffsets", "earliest") \
    .load()

parsed_df = raw_df \
    .selectExpr("CAST(value AS STRING) as json_data", "timestamp") \
    .select(from_json("json_data", news_schema).alias("data"), "timestamp") \
    .select("data.*", "timestamp") \

sources = parsed_df.withWatermark("timestamp", "2 seconds") \
    .groupBy(window("timestamp", "25 seconds"), "source") \
    .count()

sources = sources.select("window.start", "window.end", "source", "count")

query = sources.writeStream \
    .outputMode("update") \
    .option("checkpointLocation", os.getenv('CHKPT_DIR')) \
    .format("console") \
    .start()

query.awaitTermination()