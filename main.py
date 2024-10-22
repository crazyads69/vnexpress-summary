import asyncio
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import List, Dict, Tuple, Optional

import aiohttp
import aiosqlite
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from groq import Groq
from telegram import Bot

# Load environment variables
load_dotenv()

# Configure logging
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

    def __init__(self, db_name="data/crawled_articles.db"):
        self.db_name = db_name

    async def create_table(self):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute(
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
            await db.commit()

    async def insert_article(self, article: Dict, summary: str):
        async with aiosqlite.connect(self.db_name) as db:
            await db.execute(
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
            await db.commit()

    async def is_article_crawled(self, url: str) -> bool:
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute(
                "SELECT url FROM articles WHERE url = ?", (url,)
            ) as cursor:
                return await cursor.fetchone() is not None


class BaseCrawler(ABC):
    @abstractmethod
    async def extract_content(self, url):
        pass

    @abstractmethod
    async def get_urls_of_type_thread(self, article_type, page_number):
        pass


class VNExpressCrawler(BaseCrawler):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        # self.article_type_dict = {
        #     0: "thoi-su",
        #     1: "du-lich",
        #     2: "the-gioi",
        #     3: "kinh-doanh",
        #     4: "khoa-hoc",
        #     5: "giai-tri",
        #     6: "the-thao",
        #     7: "phap-luat",
        #     8: "giao-duc",
        #     9: "suc-khoe",
        #     10: "doi-song",
        # }
        self.article_type_dict = {
            0: "tin-xem-nhieu",
            1: "tin-nong",
            2: "tin-tuc-24h",
        }
        self.db_manager = DatabaseManager()
        self.session = None

    async def create_session(self):
        self.session = aiohttp.ClientSession()

    async def close_session(self):
        if self.session:
            await self.session.close()

    async def extract_content(
        self, url: str
    ) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        try:
            async with self.session.get(url, timeout=10) as response:
                content = await response.text()
            soup = BeautifulSoup(content, "html.parser")

            title = soup.find("h1", class_="title-detail")
            if title is None:
                return None, None, None

            title = title.text.strip()
            description = soup.find("p", class_="description")
            description = description.text.strip() if description else ""

            paragraphs = " ".join(
                [p.text.strip() for p in soup.find_all("p", class_="Normal")]
            )

            return title, description, paragraphs
        except Exception as e:
            logger.error(f"Error extracting content from {url}: {str(e)}")
            return None, None, None

    async def get_urls_of_type_thread(
        self, article_type: str, page_number: int
    ) -> List[str]:
        try:
            page_url = f"https://vnexpress.net/{article_type}-p{page_number}"
            async with self.session.get(page_url, timeout=10) as response:
                content = await response.text()
            soup = BeautifulSoup(content, "html.parser")
            titles = soup.find_all(class_="title-news")

            if not titles:
                logger.info(f"No news found in {page_url}")
                return []

            articles_urls = []
            for title in titles:
                link = title.find_all("a")[0]
                url = link.get("href")
                if not await self.db_manager.is_article_crawled(url):
                    articles_urls.append(url)

            return articles_urls
        except Exception as e:
            logger.error(f"Error getting URLs from {page_url}: {str(e)}")
            return []

    async def get_latest_articles(
        self, article_type: str, pages: int = 2
    ) -> List[Dict]:
        articles = []
        for page in range(1, pages + 1):
            urls = await self.get_urls_of_type_thread(article_type, page)
            tasks = [self.process_article(url, article_type) for url in urls]
            articles.extend(await asyncio.gather(*tasks))
        return [article for article in articles if article is not None]

    async def process_article(self, url: str, article_type: str) -> Optional[Dict]:
        title, description, paragraphs = await self.extract_content(url)
        if title and (description or paragraphs):
            content = f"{description}\n{paragraphs}"
            return {
                "title": title,
                "content": content,
                "url": url,
                "category": article_type,
                "published_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        return None


class GroqSummarizer:
    def __init__(self):
        self.client = Groq(api_key=os.getenv("GROQ_API_KEY"))
        self.cache = {}

    async def summarize(self, text: str) -> str:
        if text in self.cache:
            return self.cache[text]

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

            completion = await asyncio.to_thread(
                self.client.chat.completions.create,
                messages=[{"role": "user", "content": prompt}],
                model="mixtral-8x7b-32768",
                temperature=0.3,
                max_tokens=8192,
            )

            summary = completion.choices[0].message.content.strip()

            if len(summary.split(".")) < 4:
                logger.info("Summary too short, retrying...")
                return await self.summarize(text)

            self.cache[text] = summary
            return summary
        except Exception as e:
            logger.error(f"Error summarizing with Groq: {str(e)}")
            return ""


class TelegramPoster:
    def __init__(self):
        self.bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
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
                "tin-xem-nhieu": "🏆",
                "tin-tuc-24h": "🕒",
                "tin-nong": "🔥",
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

    crawler = VNExpressCrawler(
        num_workers=5, total_pages=1
    )  # Change total_pages to 1 for only get the latest news
    summarizer = GroqSummarizer()
    poster = TelegramPoster()

    await crawler.db_manager.create_table()
    await crawler.create_session()

    try:
        for category in crawler.article_type_dict.values():
            logger.info(f"Processing category: {category}")
            articles = await crawler.get_latest_articles(category)

            for article in articles:
                if article["content"]:
                    summary = await summarizer.summarize(article["content"])
                    if summary:
                        await poster.post_update(article, summary)
                        await crawler.db_manager.insert_article(article, summary)

                await asyncio.sleep(3)

            await asyncio.sleep(5)

        logger.info("Hourly processing completed successfully")

    except Exception as e:
        logger.error(f"Error in process_articles: {str(e)}")
    finally:
        await crawler.close_session()


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
