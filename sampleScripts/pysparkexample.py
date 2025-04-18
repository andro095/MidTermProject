from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import StructType

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
    .getOrCreate()

articleSchema = StructType().add("keywords", "string") \
    .add("title", "string") \
    .add("source", "string") \
    .add("author", "string") \
    .add("description", "string") \
    .add("date_published", "string") \
    .add("content", "string")

df_stream = spark.readStream.format("kafka") \
    .schema(articleSchema) \
    .option("kafka.bootstrap.servers", "35.188.143.97:9092") \
    .option("subscribe", "articles") \
    .load()

selected_df = df_stream.select(df_stream.title, df_stream.author.alias("author_name"))

wrote_df = selected_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "35.188.143.97:9092") \
    .option("topic", "iot-sink") \
    .outputMode("append") \
    .start()

# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder \
#     .appName("KafkaTest") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0") \
#     .getOrCreate()
#
# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "35.188.143.97:9092") \
#     .option("subscribe", "iot-sink") \
#     .load()
#
# df.printSchema()
