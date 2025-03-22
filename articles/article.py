import json
import re


def clean_text(text):
    return re.sub(r"[^a-zA-Z0-9\s]", '', ' '.join(text.lower().split()))


class Article:
    def __init__(self, keywords : str, title : str, source : str, author : str, description : str, date_published : str, content : str):
        self.keywords = clean_text(keywords)
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
        clean_keywords = clean_text(keywords)
        cleaned_title = clean_text(article['title']) if article['title'] else ''
        cleaned_description = clean_text(article['description']) if article['description'] else ''
        cleaned_content = clean_text(article['content']) if article['content'] else ''

        return Article(
            keywords=clean_keywords,
            title=cleaned_title,
            source=article['source']['name'],
            author=article['author'],
            description=cleaned_description,
            date_published=article['publishedAt'],
            content=cleaned_content
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