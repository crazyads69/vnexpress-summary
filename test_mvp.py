import requests
import sys
from pathlib import Path
from bs4 import BeautifulSoup
from tqdm import tqdm
import telegram
import logging
from logging.handlers import RotatingFileHandler
import os
from dotenv import load_dotenv
from groq import Groq
import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
import json
from typing import List, Dict, Tuple, Optional, Generator
import schedule
import time
import sqlite3

# Load environment variables
load_dotenv()

# Configure logging with rotation
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        RotatingFileHandler(
            "vnexpress_crawler.log", maxBytes=1024 * 1024, backupCount=5
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class DatabaseManager:
    def __init__(self, db_name="crawled_articles.db"):
        self.conn = sqlite3.connect(db_name)
        self.create_table()

    def create_table(self):
        cursor = self.conn.cursor()
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS articles (
            url TEXT PRIMARY KEY,
            title TEXT,
            category TEXT,
            published_date TEXT,
            crawled_date TEXT,
            summary TEXT
        )
        """
        )
        self.conn.commit()

    def insert_article(self, article: Dict, summary: str):
        cursor = self.conn.cursor()
        cursor.execute(
            """
        INSERT OR REPLACE INTO articles (url, title, category, published_date, crawled_date, summary)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                article["url"],
                article["title"],
                article["category"],
                article["published_date"],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                summary,
            ),
        )
        self.conn.commit()

    def is_article_crawled(self, url: str) -> bool:
        cursor = self.conn.cursor()
        cursor.execute("SELECT url FROM articles WHERE url = ?", (url,))
        return cursor.fetchone() is not None

    def close(self):
        self.conn.close()


class BaseCrawler(ABC):
    @abstractmethod
    def extract_content(self, url):
        pass

    @abstractmethod
    def write_content(self, url, output_fpath):
        pass

    @abstractmethod
    def get_urls_of_type_thread(self, article_type, page_number):
        pass


class VNExpressCrawler(BaseCrawler):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.logger = logger
        self.article_type_dict = {
            0: "thoi-su",
            1: "du-lich",
            2: "the-gioi",
            3: "kinh-doanh",
            4: "khoa-hoc",
            5: "giai-tri",
            6: "the-thao",
            7: "phap-luat",
            8: "giao-duc",
            9: "suc-khoe",
            10: "doi-song",
        }
        self.db_manager = DatabaseManager()

    def write_content(self, url: str, output_fpath: str) -> bool:
        try:
            title, description, paragraphs = self.extract_content(url)
            if not title:
                self.logger.error(f"No content found for URL: {url}")
                return False

            with open(output_fpath, "w", encoding="utf-8") as f:
                f.write(f"Title: {title}\n\n")
                if description:
                    f.write("Description:\n")
                    for desc in description:
                        f.write(f"{desc}\n")
                    f.write("\n")
                if paragraphs:
                    f.write("Content:\n")
                    for para in paragraphs:
                        f.write(f"{para}\n")

            self.logger.info(f"Successfully wrote content from {url} to {output_fpath}")
            return True
        except Exception as e:
            self.logger.error(
                f"Error writing content from {url} to {output_fpath}: {str(e)}"
            )
            return False

    def extract_content(
        self, url: str
    ) -> Tuple[Optional[str], Optional[Generator], Optional[Generator]]:
        try:
            content = requests.get(url, timeout=10).content
            soup = BeautifulSoup(content, "html.parser")

            title = soup.find("h1", class_="title-detail")
            if title is None:
                return None, None, None

            title = title.text.strip()
            description = soup.find("p", class_="description")
            if description:
                description = (
                    p.text.strip() for p in description.contents if p.text.strip()
                )
            else:
                description = None

            paragraphs = (p.text.strip() for p in soup.find_all("p", class_="Normal"))

            return title, description, paragraphs
        except Exception as e:
            self.logger.error(f"Error extracting content from {url}: {str(e)}")
            return None, None, None

    def get_urls_of_type_thread(self, article_type: str, page_number: int) -> List[str]:
        try:
            page_url = f"https://vnexpress.net/{article_type}-p{page_number}"
            content = requests.get(page_url, timeout=10).content
            soup = BeautifulSoup(content, "html.parser")
            titles = soup.find_all(class_="title-news")

            if not titles:
                self.logger.info(f"No news found in {page_url}")
                return []

            articles_urls = []
            for title in titles:
                link = title.find_all("a")[0]
                url = link.get("href")
                if not self.db_manager.is_article_crawled(url):
                    articles_urls.append(url)

            return articles_urls
        except Exception as e:
            self.logger.error(f"Error getting URLs from {page_url}: {str(e)}")
            return []

    def get_latest_articles(self, article_type: str, pages: int = 2) -> List[Dict]:
        articles = []
        try:
            for page in range(1, pages + 1):
                urls = self.get_urls_of_type_thread(article_type, page)
                for url in urls:
                    title, description, paragraphs = self.extract_content(url)
                    if title and (description or paragraphs):
                        content = ""
                        if description:
                            content += " ".join(description)
                        if paragraphs:
                            content += " " + " ".join(paragraphs)

                        articles.append(
                            {
                                "title": title,
                                "content": content,
                                "url": url,
                                "category": article_type,
                                "published_date": datetime.now().strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),
                            }
                        )

                    time.sleep(1)

            return articles
        except Exception as e:
            self.logger.error(
                f"Error getting latest articles for {article_type}: {str(e)}"
            )
            return []


class GroqSummarizer:
    def __init__(self):
        self.client = Groq(api_key=os.getenv("GROQ_API_KEY"))

    def summarize(self, text: str) -> str:
        try:
            prompt = f"""Hãy tóm tắt bài báo tiếng Việt sau đây, giữ nguyên các thông tin và ý chính quan trọng:

            {text}

            Yêu cầu khi tóm tắt:
            - Độ dài: 4-10 câu
            - Giữ nguyên các thông tin quan trọng như: thời gian, địa điểm, nhân vật chính
            - Sắp xếp các sự kiện theo trình tự thời gian
            - Sử dụng ngôn ngữ tự nhiên, dễ hiểu
            - Không thêm thông tin không có trong bài gốc
            - Chỉ trả lời bằng tiếng Việt
            """

            completion = self.client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="mixtral-8x7b-32768",
                temperature=0.3,
                max_tokens=8192,
            )

            summary = completion.choices[0].message.content.strip()

            # Check if summary is less than 4 sentences and retry if necessary
            if len(summary.split(".")) < 4:
                logger.info("Summary too short, retrying...")
                return self.summarize(text)  # Retry summarization

            return summary
        except Exception as e:
            logger.error(f"Error summarizing with Groq: {str(e)}")
            return ""


class TelegramPoster:
    def __init__(self):
        self.bot = telegram.Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")

    async def post_update(self, article: Dict, summary: str):
        try:
            category_emojis = {
                "thoi-su": "📰",
                "du-lich": "✈️",
                "the-gioi": "🌍",
                "kinh-doanh": "💼",
                "khoa-hoc": "🔬",
                "giai-tri": "🎭",
                "the-thao": "⚽",
                "phap-luat": "⚖️",
                "giao-duc": "📚",
                "suc-khoe": "🏥",
                "doi-song": "🌟",
            }

            category_emoji = category_emojis.get(article["category"], "📄")

            message = (
                f"{category_emoji} *{article['title']}*\n\n"
                f"📝 *Tóm tắt:*\n{summary}\n\n"
                f"🔗 [Đọc thêm]({article['url']})\n"
                f"📂 Chuyên mục: {article['category']}\n"
                f"🕒 {article['published_date']}"
            )

            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode="Markdown",
                disable_web_page_preview=False,
            )

        except Exception as e:
            logger.error(f"Error posting to Telegram: {str(e)}")


async def process_articles():
    logger.info("Starting hourly article processing...")

    crawler = VNExpressCrawler(num_workers=5, total_pages=2)
    summarizer = GroqSummarizer()
    poster = TelegramPoster()

    try:
        for category in crawler.article_type_dict.values():
            logger.info(f"Processing category: {category}")
            articles = crawler.get_latest_articles(category)

            for article in articles:
                if article["content"]:
                    summary = summarizer.summarize(article["content"])
                    if summary:
                        await poster.post_update(article, summary)
                        crawler.db_manager.insert_article(article, summary)

                await asyncio.sleep(3)

            await asyncio.sleep(5)

        logger.info("Hourly processing completed successfully")

    except Exception as e:
        logger.error(f"Error in process_articles: {str(e)}")


async def main():
    logger.info("Starting VNExpress news crawler...")

    while True:
        await process_articles()
        await asyncio.sleep(3600)


if __name__ == "__main__":
    required_env = ["GROQ_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"]
    missing_env = [env for env in required_env if not os.getenv(env)]

    if missing_env:
        print(f"Missing required environment variables: {', '.join(missing_env)}")
        print("Please add them to your .env file")
        exit(1)

    asyncio.run(main())
