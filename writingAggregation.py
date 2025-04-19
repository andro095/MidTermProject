from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col
from dotenv import load_dotenv
import os

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
    .selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", json_schema).alias("data")) \
    .select("data.*")

query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", os.getenv('CHKPT_DIR')) \
    .trigger(processingTime='3 seconds') \
    .start()

query.awaitTermination()