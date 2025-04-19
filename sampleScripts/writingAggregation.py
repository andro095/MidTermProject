from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col

# Initialize Spark Session for Dataproc
spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
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
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "test-s-7") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json("json_data", json_schema).alias("data")) \
    .select("data.*")

query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "/BigData/mchkpt") \
    .trigger(processingTime='3 seconds') \
    .start()