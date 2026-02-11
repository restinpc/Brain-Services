#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import asyncio
import json
import traceback
from datetime import datetime
import websockets
import aiohttp
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()

# === Конфигурация трассировки ошибок ===
TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "bynance_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

def send_error_trace(exc: Exception, script_name: str = "bynance.py"):
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    payload = {
        "url": "cli_script",
        "node": NODE_NAME,
        "email": EMAIL,
        "logs": logs,
    }
    print(f"\n📤 [POST] Отправляем отчёт об ошибке на {TRACE_URL}")
    try:
        import requests
        response = requests.post(TRACE_URL, data=payload, timeout=10)
        print(f"✅ [POST] Успешно отправлено! Статус: {response.status_code}")
    except Exception as e:
        print(f"⚠️ [POST] Не удалось отправить отчёт: {e}")

# === Аргументы командной строки + .env fallback ===
parser = argparse.ArgumentParser(description="Binance OrderBook Stream → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"), help="Хост базы данных")
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"), help="Порт базы данных")
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"), help="Пользователь БД")
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"), help="Пароль БД")
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"), help="Имя базы данных")
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны все параметры подключения к БД (через аргументы или .env)")
    sys.exit(1)

DB_CONFIG = {
    'host': args.host,
    'port': int(args.port),
    'user': args.user,
    'password': args.password,
    'database': args.database,
    'autocommit': False,
}

def extract_symbol_from_table_name(table_name: str) -> str:
    """Извлекает символ из имени таблицы vlad_binance_{symbol}_orderbook"""
    if not table_name.startswith("vlad_binance_") or not table_name.endswith("_orderbook"):
        raise ValueError("Неверный формат имени таблицы")
    return table_name[len("vlad_binance_"):-len("_orderbook")]

class BinanceOrderBook:
    def __init__(self, table_name: str, symbol: str, dump_interval=5):
        self.table_name = table_name
        self.symbol = symbol.lower()
        self.bids = {}
        self.asks = {}
        self.last_update_id = 0
        self.prev_u = None
        self.ws_url = f"wss://stream.binance.com:9443/ws/{self.symbol}@depth@100ms"
        self.rest_url = f"https://api.binance.com/api/v3/depth?symbol={self.symbol.upper()}&limit=1000"
        self.buffer = []
        self.is_synchronized = False
        self.dump_interval = dump_interval

    async def fetch_snapshot(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.rest_url) as response:
                snapshot = await response.json()
                self.bids = {float(price): float(qty) for price, qty in snapshot['bids']}
                self.asks = {float(price): float(qty) for price, qty in snapshot['asks']}
                self.last_update_id = snapshot['lastUpdateId']
                self.prev_u = self.last_update_id
                print(f"[{self.symbol.upper()}] Снапшот получен.")

    def process_update(self, data):
        U = data['U']
        u = data['u']
        if u <= self.last_update_id:
            return
        if self.is_synchronized and self.prev_u is not None and U != self.prev_u + 1:
            print(f"[{self.symbol.upper()}] !!! РАЗРЫВ ПОТОКА !!!")
        for price_str, qty_str in data['b']:
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
        for price_str, qty_str in data['a']:
            price = float(price_str)
            qty = float(qty_str)
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty
        self.prev_u = u
        self.last_update_id = u

    async def save_to_db(self):
        sorted_bids = sorted(self.bids.items(), reverse=True)[:50]
        sorted_asks = sorted(self.asks.items())[:50]
        best_bid = sorted_bids[0][0] if sorted_bids else 0.0
        best_ask = sorted_asks[0][0] if sorted_asks else 0.0
        spread = round(best_ask - best_bid, 8) if (best_bid and best_ask) else 0.0
        data = {
            "symbol": self.symbol,
            "timestamp": datetime.utcnow(),
            "last_update_id": self.last_update_id,
            "spread": spread,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "bids_json": json.dumps(sorted_bids),
            "asks_json": json.dumps(sorted_asks),
        }
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME(6) NOT NULL,
                    last_update_id BIGINT NOT NULL,
                    spread DECIMAL(20,10) NULL,
                    best_bid DECIMAL(20,10) NULL,
                    best_ask DECIMAL(20,10) NULL,
                    bids_json LONGTEXT NULL,
                    asks_json LONGTEXT NULL,
                    INDEX idx_timestamp (timestamp),
                    INDEX idx_update_id (last_update_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                COMMENT='Binance orderbook stream for {self.symbol}';
            """)
            conn.commit()
            sql = f"""
                INSERT INTO `{self.table_name}`
                (timestamp, last_update_id, spread, best_bid, best_ask, bids_json, asks_json)
                VALUES (%(timestamp)s, %(last_update_id)s, %(spread)s, %(best_bid)s, %(best_ask)s, %(bids_json)s, %(asks_json)s)
            """
            cursor.execute(sql, data)
            conn.commit()
            print(f"[{self.symbol.upper()}] ✅ Записано в БД (UpdateID: {self.last_update_id})")
        except Error as e:
            print(f"[{self.symbol.upper()}] ❌ Ошибка БД: {e}")
        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()

    async def dumper_task(self):
        """Фоновая задача для периодической записи в БД"""
        while True:
            await asyncio.sleep(self.dump_interval)
            if self.is_synchronized:
                await self.save_to_db()

    async def start(self):
        """Основной цикл работы с WebSocket"""
        # Запускаем фоновую задачу записи
        asyncio.create_task(self.dumper_task())

        async with websockets.connect(self.ws_url) as ws:
            print(f"[{self.symbol.upper()}] Подключено к WebSocket")
            # Получаем снапшот
            await self.fetch_snapshot()
            # Основной цикл получения обновлений
            while True:
                msg = await ws.recv()
                data = json.loads(msg)
                if not self.is_synchronized:
                    self.buffer.append(data)
                    # После получения достаточного количества обновлений синхронизируемся
                    if len(self.buffer) > 10:
                        for event in self.buffer:
                            if event['u'] > self.last_update_id:
                                if event['U'] <= self.last_update_id + 1 <= event['u']:
                                    self.process_update(event)
                                    self.prev_u = event['u']
                        self.buffer = []
                        self.is_synchronized = True
                        print(f"[{self.symbol.upper()}] Синхронизация OK. Пишем в БД каждые {self.dump_interval} сек.")
                else:
                    self.process_update(data)

async def main():
    try:
        symbol = extract_symbol_from_table_name(args.table_name)
        orderbook = BinanceOrderBook(args.table_name, symbol)
        await orderbook.start()
    except ValueError as e:
        print(f"❌ Ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
    except SystemExit:
        pass
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)