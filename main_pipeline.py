from pyspark.sql import SparkSession
from aditional_pipeline import MySQLDataLoader
from db_config import DBConfig
from dotenv import load_dotenv
import os

if __name__ == '__main__':
    load_dotenv()
    
    spark = SparkSession \
        .builder \
        .appName("Spark to MySQL Data Loading") \
        .getOrCreate()

    db_config = DBConfig(
        format= "jdbc",
        jdbc_url = f"jdbc:mysql://{os.getenv('GCP_LOCAL_HOST')}:3306/news_db",
        table_name = "news",
        user = "training",
        password = "training",
        driver="com.mysql.cj.jdbc.Driver"
    )
    hdfs_path = "/BigData/kafka_output"

    data_loader = MySQLDataLoader(spark,db_config,hdfs_path)

    data_loader.run()