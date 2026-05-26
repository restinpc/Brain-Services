"""
Описание: ADS-B military flights collector → MySQL.

Таблица: vlad_wm_military_flights

Запуск:
  python WorldMonitor_military_flights.py vlad_wm_military_flights [host] [port] [user] [password] [database]

.env:
  DB_HOST=127.0.0.1
  DB_PORT=3306
  DB_USER=root
  DB_PASSWORD=yourpass
  DB_NAME=brain

  # Необязательно. По умолчанию используется открытый adsb.fi endpoint.
  ADSB_API_BASE=https://opendata.adsb.fi/api
  ADSB_MIL_PATH=/v2/mil
  ADSB_FALLBACK_BASES=https://api.adsb.lol,https://api.adsb.one
"""

import os
import sys
import argparse
import json
import time
import random
import traceback
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "wm_military_flights")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

# Старый https://api.worldmonitor.app/api/military-flights возвращает 404.
# Поэтому используем ADSBExchange-compatible открытые endpoints.
ADSB_API_BASE = os.getenv("ADSB_API_BASE", "https://opendata.adsb.fi/api").rstrip("/")
ADSB_MIL_PATH = os.getenv("ADSB_MIL_PATH", "/v2/mil")
ADSB_FALLBACK_BASES = [
    x.strip().rstrip("/")
    for x in os.getenv("ADSB_FALLBACK_BASES", "https://api.adsb.lol,https://api.adsb.one").split(",")
    if x.strip()
]

DATASETS = {
    "vlad_wm_military_flights": {
        "description": "Military ADS-B flights with classification"
    }
}


def send_error_trace(exc, script_name="WorldMonitor_military_flights.py"):
    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )
    try:
        requests.post(
            TRACE_URL,
            data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs},
            timeout=10,
        )
    except Exception:
        pass


parser = argparse.ArgumentParser(description="ADS-B Military Flights → MySQL")
parser.add_argument("table_name", help="Имя целевой таблицы")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print(" Ошибка: не указаны параметры подключения к БД")
    print(" Использование:")
    print("   python WorldMonitor_military_flights.py vlad_wm_military_flights")
    print("   python WorldMonitor_military_flights.py vlad_wm_military_flights 127.0.0.1 3306 root password brain")
    sys.exit(1)

DB_CONFIG = {
    "host": args.host,
    "port": int(args.port),
    "user": args.user,
    "password": args.password,
    "database": args.database,
}


def build_session():
    s = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": "Brain-Services/1.0 (+ADS-B collector)",
        "Accept": "application/json",
    })
    return s


def _join_url(base, path):
    return f"{base.rstrip('/')}/{path.lstrip('/')}"


def adsb_get(session, base_url, path, params=None, timeout=45):
    url = _join_url(base_url, path)
    last_error = None

    for attempt in range(1, 4):
        try:
            if attempt > 1:
                time.sleep((1.5 ** attempt) + random.uniform(0.2, 0.8))

            resp = session.get(url, params=params, timeout=timeout)

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 10))
                print(f"    Rate-limit 429 для {url}, ждём {wait}s...")
                time.sleep(wait + random.uniform(0.3, 1.5))
                continue

            if resp.status_code in (400, 401, 403, 404):
                print(f"    HTTP {resp.status_code} для {url}")
                return None

            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_error = e
            print(f"    {url}: {e}")

    if last_error:
        print(f"    Нет ответа от {url}: {last_error}")
    return None


def _sf(v):
    if v is None or v == "" or v == "ground":
        return None
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None


def _si(v):
    if v is None or v == "":
        return None
    if isinstance(v, str) and v.lower() == "ground":
        return 0
    try:
        return int(float(str(v).replace(",", "")))
    except Exception:
        return None


def _short(v, max_len, default=""):
    if v is None:
        v = default
    return str(v)[:max_len]


def normalize_aircraft_response(data):
    """Поддерживает ADSBExchange-compatible JSON: {'ac': [...]} и похожие форматы."""
    if not data:
        return []
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []

    for key in ("ac", "aircraft", "flights", "planes", "result", "results"):
        value = data.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    return []


def infer_mission_type(f):
    """Грубая классификация по type/callsign, если источник не отдаёт missionType."""
    direct = f.get("missionType") or f.get("mission") or f.get("classification")
    if direct:
        return _short(direct, 50, "unknown")

    aircraft_type = str(f.get("t") or f.get("type") or f.get("aircraftType") or "").upper()
    callsign = str(f.get("flight") or f.get("callsign") or "").strip().upper()
    text = f"{aircraft_type} {callsign}"

    if any(x in text for x in ["KC", "K35", "K30", "A330MRTT", "TANKER", "RCH"]):
        return "tanker/transport"
    if any(x in text for x in ["E3", "E-3", "E7", "E-7", "AWACS", "NAEW"]):
        return "awacs"
    if any(x in text for x in ["P8", "P-8", "RC", "U2", "U-2", "R135", "ISR", "RECON"]):
        return "isr"
    if any(x in text for x in ["C17", "C-17", "C130", "C-130", "A400", "RCH", "CNV"]):
        return "transport"
    if any(x in text for x in ["F16", "F-16", "F18", "F-18", "F35", "F-35", "EUFI", "TYPH", "RAFALE"]):
        return "fighter"
    if any(x in text for x in ["H60", "UH", "CH", "HH", "HELI"]):
        return "heli"
    return "military"


class MilitaryFlightsCollector:
    def __init__(self, table_name):
        self.table_name = table_name
        self.session = build_session()

    def get_db_connection(self):
        return mysql.connector.connect(**DB_CONFIG)

    def ensure_table(self):
        conn = self.get_db_connection()
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                icao_hex VARCHAR(10) COMMENT 'ICAO 24-bit hex',
                callsign VARCHAR(20),
                country VARCHAR(100) COMMENT 'Страна регистрации',
                aircraft_type VARCHAR(100) COMMENT 'Тип ВС',
                mission_type VARCHAR(50) COMMENT 'tanker/awacs/fighter/isr/transport/heli/unknown',
                latitude DECIMAL(10,6),
                longitude DECIMAL(10,6),
                altitude_ft INT,
                speed_kts INT,
                heading DECIMAL(6,2),
                on_ground TINYINT(1) DEFAULT 0,
                squawk VARCHAR(10),
                theater VARCHAR(50) COMMENT 'Ближайший theater (если определён)',
                confidence VARCHAR(20) COMMENT 'Уверенность классификации',
                raw_json TEXT,
                snapshot_hour DATETIME NOT NULL COMMENT 'Для дедупликации',
                snapshot_at DATETIME NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_hex_hour (icao_hex, snapshot_hour),
                INDEX idx_callsign (callsign),
                INDEX idx_mission (mission_type),
                INDEX idx_country (country),
                INDEX idx_theater (theater),
                INDEX idx_snapshot (snapshot_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='ADS-B: military flights with classification';
        """)
        conn.commit()
        c.close()
        conn.close()

    def fetch_military_flights(self):
        bases = [ADSB_API_BASE] + [b for b in ADSB_FALLBACK_BASES if b != ADSB_API_BASE]

        for base in bases:
            print(f"    Источник: {_join_url(base, ADSB_MIL_PATH)}")
            data = adsb_get(self.session, base, ADSB_MIL_PATH)
            flights = normalize_aircraft_response(data)
            if flights:
                return flights, data, base
            print("    Нет данных от источника, пробуем следующий...")
            time.sleep(1.1)  # публичные endpoints обычно ограничены примерно 1 req/sec

        return [], None, None

    def process(self):
        self.ensure_table()

        flights, data, used_base = self.fetch_military_flights()
        if not flights:
            print("    Нет данных military flights")
            return

        now = datetime.now(timezone.utc)
        snapshot_at = now.strftime("%Y-%m-%d %H:%M:%S")
        snapshot_hour = now.replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

        print(f"    Получено {len(flights)} военных бортов")
        if isinstance(data, dict):
            meta = {k: data.get(k) for k in ("msg", "total", "now", "ctime", "ptime") if k in data}
            if meta:
                print(f"    Meta: {json.dumps(meta, ensure_ascii=False, default=str)[:250]}")

        sql = f"""
            INSERT INTO `{self.table_name}`
            (icao_hex, callsign, country, aircraft_type, mission_type,
             latitude, longitude, altitude_ft, speed_kts, heading, on_ground, squawk,
             theater, confidence, raw_json, snapshot_hour, snapshot_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                callsign = VALUES(callsign),
                country = VALUES(country),
                aircraft_type = VALUES(aircraft_type),
                mission_type = VALUES(mission_type),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                altitude_ft = VALUES(altitude_ft),
                speed_kts = VALUES(speed_kts),
                heading = VALUES(heading),
                on_ground = VALUES(on_ground),
                squawk = VALUES(squawk),
                theater = VALUES(theater),
                confidence = VALUES(confidence),
                raw_json = VALUES(raw_json),
                snapshot_at = VALUES(snapshot_at)
        """

        rows = []
        skipped_without_hex = 0

        for f in flights:
            icao_hex = _short(f.get("hex") or f.get("icao") or f.get("icao_hex"), 10).strip().lower()
            if not icao_hex:
                skipped_without_hex += 1
                continue

            callsign = _short(f.get("flight") or f.get("callsign"), 20).strip()
            country = _short(f.get("country") or f.get("ownOp") or f.get("operator") or f.get("flag"), 100)
            aircraft_type = _short(f.get("t") or f.get("type") or f.get("aircraftType"), 100)
            mission_type = infer_mission_type(f)

            altitude = f.get("alt_baro", f.get("alt_geom", f.get("alt", f.get("altitude"))))
            speed = f.get("gs", f.get("spd", f.get("speed")))
            heading = f.get("track", f.get("hdg", f.get("heading")))
            on_ground_raw = f.get("ground") or f.get("onGround") or f.get("on_ground") or altitude == "ground"

            rows.append((
                icao_hex,
                callsign,
                country,
                aircraft_type,
                mission_type,
                _sf(f.get("lat", f.get("latitude"))),
                _sf(f.get("lon", f.get("lng", f.get("longitude")))),
                _si(altitude),
                _si(speed),
                _sf(heading),
                1 if on_ground_raw else 0,
                _short(f.get("squawk"), 10),
                _short(f.get("theater") or f.get("region"), 50) or None,
                _short(f.get("confidence") or f.get("identMethod") or "adsb_mil", 20),
                json.dumps(f, ensure_ascii=False, default=str)[:3000],
                snapshot_hour,
                snapshot_at,
            ))

        if not rows:
            print("    Нет валидных строк для записи")
            if skipped_without_hex:
                print(f"    Пропущено без ICAO hex: {skipped_without_hex}")
            return

        conn = self.get_db_connection()
        c = conn.cursor()
        affected = 0
        for i in range(0, len(rows), 500):
            c.executemany(sql, rows[i:i + 500])
            if c.rowcount and c.rowcount > 0:
                affected += c.rowcount
        conn.commit()
        c.close()
        conn.close()

        print(f"    Записано/обновлено {len(rows)} строк")
        if skipped_without_hex:
            print(f"    Пропущено без ICAO hex: {skipped_without_hex}")

        mission_counts = {}
        for row in rows:
            mt = row[4] or "unknown"
            mission_counts[mt] = mission_counts.get(mt, 0) + 1

        print("    По типам миссий:")
        for mt, cnt in sorted(mission_counts.items(), key=lambda x: -x[1]):
            print(f"      {mt:18s}: {cnt}")

        print(f"    Использованный источник: {used_base}")


def main():
    if args.table_name not in DATASETS:
        print(f" Неизвестная таблица '{args.table_name}'. Допустимые:")
        for n in DATASETS:
            print(f"  - {n}")
        sys.exit(1)

    print(" WorldMonitor Military Flights Collector (ADS-B)")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Таблица: {args.table_name}")
    print("=" * 60)
    MilitaryFlightsCollector(args.table_name).process()
    print("=" * 60)
    print(" ЗАГРУЗКА ЗАВЕРШЕНА")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n Прервано")
        sys.exit(1)
    except Exception as e:
        print(f"\n Критическая ошибка: {e!r}")
        traceback.print_exc()
        send_error_trace(e)
        sys.exit(1)
