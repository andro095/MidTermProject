from mnews_manager import NewsManager
from user_client import NewsFetcher
from dotenv import load_dotenv
import os

if __name__ == '__main__':
    load_dotenv()

    news_manager = NewsManager(os.getenv('NEWS_API_KEY'))

    console = NewsFetcher(news_manager)

    console.run()

