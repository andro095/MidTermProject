import json


class Article:
    def __init__(self, keywords : str, title : str, source : str, author : str, description : str, date_published : str, content : str):
        self.keywords = keywords
        self.title = title
        self.source = source
        self.author = author
        self.description = description
        self.date_published = date_published
        self.content = content

    def __str__(self):
        return f'{self.title} - {self.source} - {self.date_published}'

    @staticmethod
    def from_dict(article, keywords):
        return Article(
            keywords=keywords,
            title=article['title'],
            source=article['source']['name'],
            author=article['author'],
            description=article['description'],
            date_published=article['publishedAt'],
            content=article['content']
        )

    def to_dict(self):
        return {
            'keywords': self.keywords,
            'title': self.title,
            'source': self.source,
            'author': self.author,
            'description': self.description,
            'date_published': self.date_published,
            'content': self.content
        }
class ArticleEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Article):
            return o.__dict__
        return super().default(o)

class ArticleDecoder(json.JSONDecoder):
    def decode(self, s):
        article = super().decode(s)
        return Article(**article)