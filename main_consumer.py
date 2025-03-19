from articles import ArticleDecoder
from hdfs_manager import HDFSWriter
from mkafka import MKafkaConsumer
from user_client import ConsumerConsole
from dotenv import load_dotenv
import os

host = '34.133.137.254'
port = '9092'

if __name__ == '__main__':
    load_dotenv()

    file_name = input('Enter file name to save your news: ')
    topic = input('Enter topic name: ')
    writer = HDFSWriter(os.getenv('GCP_HOST'))

    print(os.getenv('HDFS_FILE_DIRECTORY'))

    if not writer.client.status(os.getenv('HDFS_FILE_DIRECTORY')):
        print(f"Directory {os.getenv('HDFS_FILE_DIRECTORY')} does not exist. Creating it now...")

    consumer = MKafkaConsumer(os.getenv('GCP_HOST'), os.getenv('GCP_KAFKA_PORT'), topic = topic, json_deserializer=True, json_decoder=ArticleDecoder)



    console = ConsumerConsole(consumer, writer, f"{os.getenv('HDFS_FILE_DIRECTORY')}/{file_name}.csv")

    console.run()