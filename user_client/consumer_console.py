from collections import Counter

from hdfs import InsecureClient

from articles import ArticleTransformed
from mkafka import MKafkaConsumer


class ConsumerConsole:
    def __init__(self, consumer : MKafkaConsumer, writer: InsecureClient, file_name: str):
        self.consumer = consumer
        self.writer = writer
        self.file_name = file_name

    def run(self):
        write_header = True

        with self.writer.write(self.file_name) as writer:
            for article in self.consumer.process():

                article_transformed = ArticleTransformed.from_article(article, Counter(article.content.split()).most_common(1)[0][0])

                csv_data = article_transformed.to_csv(header=write_header)

                writer.write(csv_data)

                if write_header:
                    write_header = False