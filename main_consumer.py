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

    file_name = input('Enter file name to save your news without the extension: ')
    topic = input('Enter topic name: ')

    consumer = MKafkaConsumer(os.getenv('GCP_HOST'), os.getenv('GCP_KAFKA_PORT'), topic = topic, json_deserializer=True, json_decoder=ArticleDecoder)
    writer = HDFSWriter(os.getenv('GCP_HOST'))

    console = ConsumerConsole(consumer, writer, file_name)

    console.run()