from articles import ArticleEncoder
from mkafka import MKafkaProducer
from mnews_manager import NewsManager
from user_client import ProducerConsole
from dotenv import load_dotenv
import os

if __name__ == '__main__':
    load_dotenv()

    topic = input('Enter topic name: ')

    news_manager = NewsManager(os.getenv('NEWS_API_KEY'))
    producer = MKafkaProducer(os.getenv('GCP_HOST'), os.getenv('GCP_KAFKA_PORT'), topic, json_serializer=True, json_encoder=ArticleEncoder)

    console = ProducerConsole(producer, news_manager)

    console.run()


