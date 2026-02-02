import time
import feedparser
import os
import requests
import mysql.connector
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Загрузка переменных окружения
load_dotenv()

# Настройки
FEEDS_URL = "https://www.federalreserve.gov/feeds/feeds.htm"
CHECK_INTERVAL = 3600  # 1 час
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Глобальный драйвер
_selenium_driver = None


def get_selenium_driver():
    """Ленивая инициализация Selenium."""
    global _selenium_driver
    if _selenium_driver is None:
        print("[Selenium] Инициализация Chrome...")
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument("--log-level=3")

        _selenium_driver = webdriver.Chrome(options=options)
    return _selenium_driver


def cleanup_selenium():
    global _selenium_driver
    if _selenium_driver:
        try:
            _selenium_driver.quit()
        except:
            pass
        _selenium_driver = None


def get_full_text_selenium(url, timeout=10):
    """Fallback: Selenium парсинг."""
    try:
        driver = get_selenium_driver()
        driver.get(url)

        # Ждем контент
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#article, #content, .col-md-8"))
            )
        except:
            pass

        # JS очистка
        driver.execute_script("""
            var trash = document.querySelectorAll('nav, header, footer, .header, .footer, .breadcrumb, .social-share');
            trash.forEach(el => el.remove());
        """)

        text = ""
        # Поиск по приоритетным селекторам
        selectors = ["#article", "#content .col-md-8", "#content", ".data-article"]
        for sel in selectors:
            try:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    t = el.text.strip()
                    if len(t) > 50: text += t + "\n"
                if len(text) > 100: break
            except:
                continue

        if not text:
            text = driver.find_element(By.TAG_NAME, "body").text.strip()

        return text if len(text) > 50 else ""
    except Exception as e:
        print(f" [Selenium Error] {e}")
        return ""


class RSSCollector:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': USER_AGENT})
        self.init_db()

    def get_db_connection(self):
        """Создает подключение к MySQL."""
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )

    def init_db(self):
        """Создаем таблицу в MySQL."""
        conn = None
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()

            # ВАЖНО: entry_guid VARCHAR(190) UNIQUE для совместимости с utf8mb4 и лимитом 767 байт ключа
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vlad_rss_feed_entries (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    feed_title VARCHAR(255),
                    feed_url VARCHAR(255),
                    entry_title VARCHAR(255),
                    entry_link VARCHAR(500),
                    entry_guid VARCHAR(190) UNIQUE,
                    entry_description LONGTEXT,
                    full_text LONGTEXT,
                    published VARCHAR(100),
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            conn.commit()
            print(f"MySQL ({os.getenv('DB_HOST')}): Таблица vlad_rss_feed_entries готова.")
        except mysql.connector.Error as err:
            print(f"Ошибка БД при старте: {err}")
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()

    def clean_html_content(self, soup):
        """Удаляет мусор из HTML."""
        for tag in soup.select(
                'script, style, nav, header, footer, aside, .header, .footer, .breadcrumb, .social-share, .related-links'):
            tag.decompose()
        return soup

    def get_full_text(self, url):
        """Основной метод парсинга (Requests -> Selenium)."""
        try:
            resp = self.session.get(url, timeout=10)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                self.clean_html_content(soup)

                # Ищем контент
                content = None
                for sel in ['div#content', 'div#article', 'div.col-md-8', 'main']:
                    found = soup.select_one(sel)
                    if found:
                        content = found
                        break

                if content:
                    text = content.get_text(separator=' ', strip=True)
                    if "Skip to main content" not in text and len(text) > 100:
                        return text

            return get_full_text_selenium(url)

        except Exception:
            return get_full_text_selenium(url)

    def process_feed(self, feed_url, feed_name):
        try:
            feed = feedparser.parse(feed_url)
            conn = self.get_db_connection()
            cursor = conn.cursor()
            new_count = 0

            for entry in feed.entries:
                # Обрезаем GUID до 190 символов, чтобы не было ошибки БД
                entry_guid = entry.get('id', entry.link)[:190]

                cursor.execute("SELECT id FROM vlad_rss_feed_entries WHERE entry_guid = %s", (entry_guid,))
                if cursor.fetchone():
                    continue

                print(f" [{feed_name}] Новая: {entry.title[:50]}...")

                full_text = ""
                if 'link' in entry:
                    full_text = self.get_full_text(entry.link)

                description = entry.get('description', '')
                if not full_text:
                    full_text = BeautifulSoup(description, 'html.parser').get_text(strip=True)

                published = entry.get('published', entry.get('updated', ''))

                try:
                    cursor.execute("""
                        INSERT INTO vlad_rss_feed_entries 
                        (feed_title, feed_url, entry_title, entry_link, entry_guid, 
                         entry_description, full_text, published)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        feed_name,
                        feed_url,
                        entry.title,
                        entry.link,
                        entry_guid,
                        description,
                        full_text,
                        published
                    ))
                    new_count += 1
                except mysql.connector.Error as err:
                    print(f" Ошибка вставки: {err}")

            if new_count > 0:
                conn.commit()
                print(f" -> Сохранено {new_count} записей.")

            cursor.close()
            conn.close()

        except Exception as e:
            print(f"Ошибка фида {feed_name}: {e}")

    def run_cycle(self):
        print(f"Сканирование фидов с {FEEDS_URL}...")
        try:
            resp = self.session.get(FEEDS_URL, timeout=15)
            soup = BeautifulSoup(resp.text, 'html.parser')
            feeds = []
            seen = set()

            for a in soup.find_all('a', href=True):
                href = a['href']
                if href.endswith('.xml'):
                    full_url = urljoin("https://www.federalreserve.gov", href)
                    if full_url not in seen:
                        seen.add(full_url)
                        feeds.append({'url': full_url, 'name': a.text.strip() or "Fed Feed"})

            print(f"Найдено {len(feeds)} лент.")
            for feed in feeds:
                self.process_feed(feed['url'], feed['name'])

        except Exception as e:
            print(f"Сбой цикла: {e}")

        cleanup_selenium()

if __name__ == "__main__":
    print(f"RSS Collector (MySQL Mode). Интервал: {CHECK_INTERVAL} сек.")
    print(f"БД Host: {os.getenv('DB_HOST')}")
    print("=" * 50)

    collector = RSSCollector()

    try:
        while True:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Обновление...")
            collector.run_cycle()
            print(f"Жду {CHECK_INTERVAL} сек...")
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\nСтоп.")
        cleanup_selenium()
