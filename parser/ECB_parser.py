import os
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import mysql.connector
from dotenv import load_dotenv

# Загрузка переменных из .env файла
load_dotenv()

# Настройки
BASE_URL = "https://www.ecb.europa.eu/home/html/rss.en.html"
ECB_HOST = "https://www.ecb.europa.eu"
CHECK_INTERVAL = 3600  # 1 час


class ECBCollector:
    def __init__(self):
        self.init_db()

    def get_db_connection(self):
        """Создает подключение к MySQL."""
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            # Добавляем порт и преобразуем его в число
            port=int(os.getenv("DB_PORT", 3306)),
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

            # Синтаксис MySQL
            # Используем LONGTEXT для XML, так как они могут быть большими
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vlad_ecb_xml_storage (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    feed_url VARCHAR(255),
                    feed_title VARCHAR(255),
                    saved_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    xml_content LONGTEXT
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            conn.commit()
            print(f"Подключение к MySQL ({os.getenv('DB_HOST')}) успешно. Таблица проверена.")
        except mysql.connector.Error as err:
            print(f"Ошибка БД при инициализации: {err}")
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()

    def get_feed_urls(self):
        """Получаем список ссылок на RSS."""
        print(f"Поиск RSS лент на {BASE_URL}...")
        try:
            resp = requests.get(BASE_URL, timeout=15)
            if resp.status_code != 200:
                print(f"Ошибка доступа к сайту ECB: статус {resp.status_code}")
                return []

            soup = BeautifulSoup(resp.text, 'html.parser')
            feeds = []

            for a in soup.select("a[href]"):
                href = a['href']
                if "/rss/" in href or href.endswith(".xml") or href.endswith(".rss"):
                    if "fxref" in href:
                        continue

                    full_url = urljoin(ECB_HOST, href)
                    title = a.get_text(strip=True) or "ECB Feed"
                    feeds.append((full_url, title))

            unique_feeds = list(set(feeds))
            print(f"Найдено {len(unique_feeds)} лент.")
            return unique_feeds

        except Exception as e:
            print(f"Ошибка при поиске лент: {e}")
            return []

    def download_feeds(self):
        """Скачивает и сохраняет данные в MySQL."""
        feeds = self.get_feed_urls()
        if not feeds:
            print("Ленты не найдены.")
            return

        conn = None
        try:
            conn = self.get_db_connection()
            cursor = conn.cursor()
            count = 0

            print(f"Скачивание {len(feeds)} файлов...")

            for url, title in feeds:
                try:
                    r = requests.get(url, timeout=30)
                    if r.status_code != 200:
                        continue

                    content = r.text

                    # В MySQL плейсхолдеры это %s, а не ?
                    cursor.execute("""
                        INSERT INTO vlad_ecb_xml_storage (feed_url, feed_title, xml_content)
                        VALUES (%s, %s, %s)
                    """, (url, title, content))

                    print(f"Сохранено: {title}")
                    count += 1

                except Exception as e:
                    print(f"Ошибка {url}: {e}")

            conn.commit()
            print(f"Цикл завершен. Сохранено записей в MySQL: {count}")

        except mysql.connector.Error as err:
            print(f"Ошибка соединения с БД: {err}")
        finally:
            if conn and conn.is_connected():
                cursor.close()
                conn.close()


if __name__ == "__main__":
    print(f"Запуск ECB Collector (MySQL Mode)")
    print(f"Хост: {os.getenv('DB_HOST')}")
    print(f"Интервал: {CHECK_INTERVAL} сек.")
    print("=" * 40)

    collector = ECBCollector()

    try:
        while True:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Обновление данных...")
            collector.download_feeds()

            print(f"Жду {CHECK_INTERVAL} сек...")
            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\nСтоп.")
