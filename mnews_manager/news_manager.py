from newsapi import NewsApiClient

class NewsManager:
    def __init__(self, api_key: str, language : str = 'en', sources : str = None, date_from : str = None, date_to : str = None):
        self.client = NewsApiClient(api_key)
        self.language = language
        self.sources = sources
        self.date_from = date_from
        self.date_to = date_to

    def change_language(self, language):
        self.language = language

    def change_sources(self, sources):
        self.sources = sources

    def change_date_from(self, date_from):
        self.date_from = date_from

    def change_date_to(self, date_to):
        self.date_to = date_to

    def get_news(self, keywords):
        return self.client.get_everything(q=keywords,
                                          sources=self.sources,
                                          language=self.language,
                                          from_param=self.date_from,
                                          to=self.date_to)