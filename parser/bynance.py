import asyncio
import json
import aiohttp
import websockets
import aiofiles
import os
from datetime import datetime


class BinanceOrderBook:
    def __init__(self, symbol, dump_interval=5):
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

        # Создаем папку
        os.makedirs("orderbooks", exist_ok=True)

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

        if self.is_synchronized:
            if self.prev_u and U != self.prev_u + 1:
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

        if u % 100 == 0:
            print(f"[{self.symbol.upper()}] Active... UpdateID: {u}")

    async def dumper_task(self):
        while True:
            await asyncio.sleep(self.dump_interval)
            if self.is_synchronized:
                await self.save_to_json()

    async def save_to_json(self):
        # Сортируем топ-50
        sorted_bids = sorted(self.bids.items(), reverse=True)[:50]
        sorted_asks = sorted(self.asks.items())[:50]

        # --- РАСЧЕТ СПРЕДА ---
        best_bid = sorted_bids[0][0] if sorted_bids else 0.0
        best_ask = sorted_asks[0][0] if sorted_asks else 0.0

        # Спред = Лучшая продажа - Лучшая покупка
        # Округляем до 8 знаков, чтобы избежать мусора вроде 0.00000000001
        spread = round(best_ask - best_bid, 8) if (best_bid and best_ask) else 0.0

        data = {
            "symbol": self.symbol,
            "timestamp": datetime.now().isoformat(),
            "last_update_id": self.last_update_id,
            "spread": spread,  # <--- Добавлено поле spread
            "best_bid": best_bid,  # <--- Добавлено для удобства проверки
            "best_ask": best_ask,  # <--- Добавлено для удобства проверки
            "bids": sorted_bids,
            "asks": sorted_asks
        }

        filename = f"orderbooks/{self.symbol}_history.jsonl"

        try:
            async with aiofiles.open(filename, mode='a') as f:
                await f.write(json.dumps(data) + "\n")
        except Exception as e:
            print(f"Ошибка записи JSON: {e}")

    async def start(self):
        asyncio.create_task(self.dumper_task())

        async with websockets.connect(self.ws_url) as ws:
            print(f"[{self.symbol.upper()}] Подключено к WebSocket")
            snapshot_task = asyncio.create_task(self.fetch_snapshot())

            while True:
                msg = await ws.recv()
                data = json.loads(msg)

                if not self.is_synchronized:
                    self.buffer.append(data)
                    if snapshot_task.done():
                        await snapshot_task

                        for event in self.buffer:
                            if event['u'] > self.last_update_id:
                                if event['U'] <= self.last_update_id + 1 <= event['u']:
                                    self.process_update(event)
                                    self.prev_u = event['u']
                                else:
                                    pass

                        self.buffer = []
                        self.is_synchronized = True
                        print(f"[{self.symbol.upper()}] Синхронизация OK. Пишем историю в .jsonl")
                else:
                    self.process_update(data)


async def main():
    btc = BinanceOrderBook("btcusdt")
    eth = BinanceOrderBook("ethusdt")
    await asyncio.gather(btc.start(), eth.start())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
