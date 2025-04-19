from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, window, lit

spark = SparkSession.builder \
    .appName("KafkaStreamingExample") \
    .getOrCreate()

userSchema = (
    StructType()
    .add("Arrival_Time", "string")
    .add("Creation_Time", "string")
    .add("Device", "string")
    .add("gt", "string")
)

iot = (
    spark.readStream
    .format("json")
    .schema(userSchema)
    .option("path", "/BigData/logs")
    .load()
)

iot_event_time = iot.withColumn(
    "event_time",
    (col("Creation_Time").cast("double")).cast("timestamp")
)

iot_group_win = (
    iot_event_time
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "10 minutes")
    )
    .count()
)

iot_key_val = (
    iot_group_win
    .withColumn("key", lit(100))
    .select(
        col("key"),
        col("window.start").alias("window_start"),
        lit(" to "),
        col("window.end").alias("window_end"),
        lit(" "),
        col("count").alias("value")
    )
)

stream = iot_key_val.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .option("checkpointLocation", "/BigData/chkpt") \
    .start()

stream.awaitTermination()