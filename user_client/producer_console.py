from collections import Counter

from articles import Article
from mkafka import MKafkaProducer
from mnews_manager import NewsManager

MENU = [
    'Send news',
    'Change language',
    'Change sources',
    'Change date from',
    'Change date to',
    'Change topic',
    'Exit'
]

class ProducerConsole:
    def __init__(self, producer : MKafkaProducer, news_manager : NewsManager):
        self.producer = producer
        self.news_manager = news_manager

    def change_language(self):
        language = input('Enter the language: ')
        self.news_manager.change_language(language)
        print('Language changed to: ', language)

    def change_sources(self):
        sources = input('Enter the sources (separated by coma): ')
        self.news_manager.change_sources(sources)
        print('Sources changed to: ', sources)

    def change_date_from(self):
        date_from = input('Enter the date from (YYYY-MM-DD): ')
        self.news_manager.change_date_from(date_from)
        print('Date from changed to: ', date_from)

    def change_date_to(self):
        date_to = input('Enter the date to (YYYY-MM-DD): ')
        self.news_manager.change_date_to(date_to)
        print('Date to changed to: ', date_to)

    def change_topic(self):
        topic = input('Enter the topic: ')
        self.producer.change_topic(topic)
        print('Topic changed to: ', topic)

    def fetch_news(self):
        keywords = input('Enter the keywords: ')
        news = self.news_manager.get_news(keywords)

        print(f"Found {len(news['articles'])} articles")

        for article in news['articles']:
            article_transformed : Article = Article.from_dict(article, keywords)
            self.producer.produce(article_transformed)

        print('News sent to be processed')

    def run(self):
        while True:
            print('Menu:')
            for i, option in enumerate(MENU):
                print(f'\t{i+1}. {option}')
            choice = int(input('Enter your choice: '))

            if choice == 1:
                self.fetch_news()
            elif choice == 2:
                self.change_language()
            elif choice == 3:
                self.change_sources()
            elif choice == 4:
                self.change_date_from()
            elif choice == 5:
                self.change_date_to()
            elif choice == 6:
                self.change_topic()
            elif choice == 7:
                break
            else:
                print('Invalid choice')