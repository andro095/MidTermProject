from hdfs import InsecureClient

from articles import ArticleDecoder
from mkafka import MKafkaConsumer
from user_client import ConsumerConsole
from dotenv import load_dotenv
import os

if __name__ == '__main__':
    load_dotenv()

    file_name = input('Enter file name to save your news: ')
    topic = input('Enter topic name: ')

    consumer = MKafkaConsumer(os.getenv('GCP_HOST'), os.getenv('GCP_KAFKA_PORT'), topic = topic, json_deserializer=True, json_decoder=ArticleDecoder)

    writer = InsecureClient(f'http://{os.getenv("GCP_HOST")}:{os.getenv("GCP_HDFS_PORT")}', root=os.getenv('HDFS_FILE_DIRECTORY'))

    console = ConsumerConsole(consumer, writer, f"{file_name}.csv")

    console.run()