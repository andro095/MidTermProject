from .article import Article
import pandas as pd

class ArticleTransformed(Article):
    def __init__(self, keywords: str,  title: str, source: str, author: str, description: str, date_published: str, content: str, most_common_word: str, most_common_count: int = 0):
        super().__init__(keywords, title, source, author, description, date_published, content)
        self.most_common_word = most_common_word
        self.most_common_count = most_common_count

    @staticmethod
    def from_article(article: Article, most_common_word: str, most_common_count: int = 0):
        return ArticleTransformed(article.keywords, article.title, article.source, article.author, article.description, article.date_published, article.content, most_common_word, most_common_count)

    def to_dict(self):
        article_dict = super().to_dict()
        article_dict['most_common_word'] = self.most_common_word
        article_dict['most_common_count'] = self.most_common_count
        return article_dict

    def to_csv(self, header : bool = False):
        return pd.DataFrame([self.to_dict()]).to_csv(header=header, index=False)