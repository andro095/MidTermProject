from collections import Counter

from articles import ArticleTransformed
from hdfs_manager import HDFSWriter
from mkafka import MKafkaConsumer


class ConsumerConsole:
    def __init__(self, consumer : MKafkaConsumer, writer: HDFSWriter, file_name: str):
        self.consumer = consumer
        self.writer = writer
        self.file_name = file_name

    def run(self):
        write_header = True

        for article in self.consumer.process():

            article_transformed = ArticleTransformed.from_article(article, Counter(article.content.split()).most_common(1)[0][0])

            csv_data = article_transformed.to_csv(header=write_header)

            if write_header:
                write_header = False

            self.writer.save(self.file_name, csv_data)

            print(csv_data)