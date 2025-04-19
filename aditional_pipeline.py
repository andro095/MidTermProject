from pyspark.sql import SparkSession
from db_config import DBConfig

class MySQLDataLoader:
    def __init__(self, session: SparkSession, db_config: DBConfig, hdfs_path: str):
        self.session = session
        self.db_config = db_config
        self.hdfs_path = hdfs_path
    
    def load_csv_from_file_sink(self,hdfs_path):
        try:
            df = self.session.read.json(hdfs_path)
            return df
        except Exception as e:
            print(f"Error loading CSV from HDFS: {str(e)}")
            raise
    
    def write_to_mysql(self, df, format, jdbc_url, table_name, user, password, driver, mode="overwrite"):
        try:
            df.write \
                .format(format) \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", user) \
                .option("password", password) \
                .option("driver", driver) \
                .mode(mode) \
                .save()
            
            return True
        except Exception as e:
            print(f"Error writing to MySQL: {str(e)}")
            raise     

    def run(self):
        try:
            df = self.load_csv_from_file_sink(self.hdfs_path)
            self.write_to_mysql(
                df=df,
                format= self.format,
                jdbc_url=self.db_config.jdbc_url,
                table_name=self.db_config.table_name,
                user=self.db_config.user,
                password=self.db_config.password,
                driver= self.driver
            )

            print(f"Data loaded successfully")

        except Exception as e:
            print(str(e))