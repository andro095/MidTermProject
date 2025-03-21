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
            most_common_word = Counter(article.content.split()).most_common(1)[0]

            article_transformed = ArticleTransformed.from_article(article, most_common_word=most_common_word[0], most_common_count=most_common_word[1])

            csv_data = article_transformed.to_csv(header=write_header)

            self.writer.save(self.file_name, csv_data, append=not write_header)

            if write_header:
                write_header = False