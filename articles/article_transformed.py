from collections import Counter

from .article import Article
import pandas as pd

class ArticleTransformed(Article):
    def __init__(self, keywords: str,  title: str, source: str, author: str, description: str, date_published: str, content: str, most_common_word: str, most_common_count: int = 0, keyword_count: int = 0):
        super().__init__(keywords, title, source, author, description, date_published, content)
        self.most_common_word = most_common_word
        self.most_common_count = most_common_count
        self.keyword_count = keyword_count

    def __str__(self):
        return f"ArticleTransformed(keywords={self.keywords}, title={self.title}, source={self.source}, author={self.author}, description={self.description}, date_published={self.date_published}, content={self.content}, most_common_word={self.most_common_word}, most_common_count={self.most_common_count}), keyword_count={self.keyword_count}"

    @staticmethod
    def from_article(article: Article):
        content_counter = Counter(article.content.split()) if article.content else Counter()
        most_common_word = content_counter.most_common(1)[0] if content_counter else ('', 0)
        keyword_count = content_counter[article.keywords] if article.keywords else 0
        return ArticleTransformed(
            article.keywords,
            article.title,
            article.source,
            article.author,
            article.description,
            article.date_published,
            article.content,
            most_common_word[0],
            most_common_word[1],
            keyword_count
        )

    def to_dict(self):
        article_dict = super().to_dict()
        article_dict['most_common_word'] = self.most_common_word
        article_dict['most_common_count'] = self.most_common_count
        article_dict['keyword_count'] = self.keyword_count
        return article_dict

    def to_csv(self, header : bool = False):
        return pd.DataFrame([self.to_dict()]).to_csv(header=header, index=False)