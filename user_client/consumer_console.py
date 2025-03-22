from articles import ArticleTransformed, Article
from hdfs_manager import HDFSWriter
from mkafka import MKafkaConsumer

from typing import Generator


class ConsumerConsole:
    def __init__(self, consumer : MKafkaConsumer, writer: HDFSWriter, file_name: str):
        self.consumer = consumer
        self.writer = writer
        self.file_name = file_name

    def run(self):
        write_header = True

        messages : Generator[Article] = self.consumer.process()

        for article in messages:
            article_transformed = ArticleTransformed.from_article(article)
            print(f"Article received: {article_transformed.title} - {article_transformed.source} - {article_transformed.date_published}")

            csv_data = article_transformed.to_csv(header=write_header)

            print(f"Writing to HDFS this data: {csv_data}")
            self.writer.save(self.file_name, csv_data, append=not write_header)

            if write_header:
                write_header = False