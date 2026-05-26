"""
Описание: AIS Maritime Snapshot → MySQL
Таблица: vlad_wm_ais_snapshot

Запуск:
  python WorldMonitor_ais_maritime.py vlad_wm_ais_snapshot [host] [port] [user] [password] [database]

Источник данных:
  1) Если задан AIS_SNAPSHOT_URL — берётся готовый JSON snapshot из вашего агрегатора.
  2) Иначе используется AISHub JSON API.

.env минимум для AISHub:
  AISHUB_USERNAME=your_aishub_username

Опционально:
  AIS_LATMIN=-90
  AIS_LATMAX=90
  AIS_LONMIN=-180
  AIS_LONMAX=180
  AIS_INTERVAL_MIN=60
  AIS_DENSITY_CELL_DEG=2
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
NODE_NAME = os.getenv("NODE_NAME", "wm_ais_maritime")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

# Старый WorldMonitor endpoint /api/ais-snapshot больше не используется по умолчанию,
# потому что он возвращает 404. Если у вас есть свой рабочий агрегатор, укажите его здесь.
AIS_SNAPSHOT_URL = os.getenv("AIS_SNAPSHOT_URL", "").strip()

# AISHub: https://data.aishub.net/ws.php?username=...&format=1&output=json&compress=0
AISHUB_API_URL = os.getenv("AISHUB_API_URL", "https://data.aishub.net/ws.php").strip()
AISHUB_USERNAME = (os.getenv("AISHUB_USERNAME") or os.getenv("AISHUB_API_KEY") or "").strip()
AIS_INTERVAL_MIN = os.getenv("AIS_INTERVAL_MIN", "60").strip()
AIS_DENSITY_CELL_DEG = float(os.getenv("AIS_DENSITY_CELL_DEG", "2"))

# Bounding box можно задать через .env. Если не задан — AISHub вернёт весь доступный поток.
AIS_LATMIN = os.getenv("AIS_LATMIN")
AIS_LATMAX = os.getenv("AIS_LATMAX")
AIS_LONMIN = os.getenv("AIS_LONMIN")
AIS_LONMAX = os.getenv("AIS_LONMAX")

DATASETS = {
    "vlad_wm_ais_snapshot": {
        "description": "AIS maritime snapshot: vessels + chokepoints + naval candidates"
    }
}

# Простые зоны для оценки загруженности ключевых морских узлов.
CHOKEPOINTS = [
    {"name": "Strait of Hormuz", "latmin": 24.0, "latmax": 28.0, "lonmin": 54.0, "lonmax": 58.5},
    {"name": "Bab el-Mandeb", "latmin": 11.0, "latmax": 14.5, "lonmin": 41.0, "lonmax": 45.5},
    {"name": "Suez Canal", "latmin": 29.0, "latmax": 32.5, "lonmin": 31.0, "lonmax": 33.5},
    {"name": "Panama Canal", "latmin": 8.5, "latmax": 9.7, "lonmin": -80.6, "lonmax": -79.0},
    {"name": "Malacca / Singapore", "latmin": 0.5, "latmax": 3.0, "lonmin": 100.0, "lonmax": 105.5},
    {"name": "Bosporus", "latmin": 40.7, "latmax": 41.4, "lonmin": 28.5, "lonmax": 29.5},
    {"name": "Taiwan Strait", "latmin": 22.0, "latmax": 26.5, "lonmin": 118.0, "lonmax": 122.5},
]

NAVAL_KEYWORDS = (
    "NAVY", "NAVAL", "WARSHIP", "MILITARY", "COAST GUARD", "COASTGUARD",
    "PATROL", "FRIGATE", "DESTROYER", "CORVETTE", "SUBMARINE",
    "USS ", "USNS", "HMS ", "RFA ", "FS ", "INS ", "HMAS ", "TCG ",
)


def send_error_trace(exc, script_name="WorldMonitor_ais_maritime.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    try:
        requests.post(
            TRACE_URL,
            data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs},
            timeout=10,
        )
    except Exception:
        pass


parser = argparse.ArgumentParser(description="AIS Maritime Snapshot → MySQL")
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
    print("   python WorldMonitor_ais_maritime.py vlad_wm_ais_snapshot")
    print("   python WorldMonitor_ais_maritime.py vlad_wm_ais_snapshot 127.0.0.1 3306 root password brain")
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
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "Brain-Services AIS Collector/1.0",
        "Accept": "application/json",
    })
    return s


def _sf(v):
    if v is None or v == "":
        return None
    try:
        return float(str(v).replace(",", ""))
    except Exception:
        return None


def _si(v):
    if v is None or v == "":
        return None
    try:
        return int(float(str(v).replace(",", "")))
    except Exception:
        return None


def _json_limit(obj, limit=50000):
    return json.dumps(obj, ensure_ascii=False, default=str)[:limit]


def _request_json(session, url, params=None, timeout=60):
    for attempt in range(1, 4):
        try:
            if attempt > 1:
                time.sleep((1.5 ** attempt) + random.uniform(0.2, 0.8))
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                print(f"    Rate-limit 429, ждём {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            print(f"    Ошибка запроса {url}: {e}")
    return None


def _extract_list_from_aishub_response(data):
    """AISHub JSON обычно выглядит как: [{meta}, [vessels...]]."""
    if data is None:
        return [], {}

    if isinstance(data, list):
        meta = data[0] if data and isinstance(data[0], dict) else {}
        if len(data) > 1 and isinstance(data[1], list):
            return data[1], meta
        # Иногда API может вернуть сразу список судов.
        vessels = [x for x in data if isinstance(x, dict) and ("MMSI" in x or "mmsi" in x)]
        return vessels, meta

    if isinstance(data, dict):
        for key in ("vessels", "ships", "data", "records", "results", "features"):
            val = data.get(key)
            if isinstance(val, list):
                return val, data
        return [], data

    return [], {}


def _fetch_custom_snapshot(session):
    if not AIS_SNAPSHOT_URL:
        return None
    print(f"    Источник: custom AIS_SNAPSHOT_URL")
    return _request_json(session, AIS_SNAPSHOT_URL, timeout=60)


def _fetch_aishub_vessels(session):
    if not AISHUB_USERNAME:
        print("    Не указан AISHUB_USERNAME в .env")
        print("    Либо укажите AIS_SNAPSHOT_URL для своего агрегатора AIS snapshot")
        return None, {}, "not_configured"

    params = {
        "username": AISHUB_USERNAME,
        "format": 1,
        "output": "json",
        "compress": 0,
    }
    if AIS_INTERVAL_MIN:
        params["interval"] = AIS_INTERVAL_MIN

    # Добавляем bbox только если заданы все 4 границы.
    if all(v is not None and str(v).strip() != "" for v in [AIS_LATMIN, AIS_LATMAX, AIS_LONMIN, AIS_LONMAX]):
        params.update({
            "latmin": AIS_LATMIN,
            "latmax": AIS_LATMAX,
            "lonmin": AIS_LONMIN,
            "lonmax": AIS_LONMAX,
        })
        print(f"    Источник: AISHub, bbox=({AIS_LATMIN},{AIS_LATMAX},{AIS_LONMIN},{AIS_LONMAX})")
    else:
        print("    Источник: AISHub, bbox не задан — запрашиваем доступный общий поток")

    data = _request_json(session, AISHUB_API_URL, params=params, timeout=90)
    vessels, meta = _extract_list_from_aishub_response(data)

    if meta.get("ERROR") in (True, "true", "TRUE", 1, "1"):
        print(f"    AISHub вернул ошибку: {meta}")
        return [], meta, "error"

    return vessels, meta, "ok"


def _vessel_lat(v):
    return _sf(v.get("LATITUDE", v.get("lat", v.get("latitude"))))


def _vessel_lon(v):
    return _sf(v.get("LONGITUDE", v.get("lon", v.get("lng", v.get("longitude")))))


def _vessel_name(v):
    return str(v.get("NAME", v.get("name", v.get("shipname", ""))) or "").strip()


def _vessel_callsign(v):
    return str(v.get("CALLSIGN", v.get("callsign", "")) or "").strip()


def _vessel_type(v):
    return _si(v.get("TYPE", v.get("type", v.get("ship_type"))))


def _inside_box(lat, lon, box):
    if lat is None or lon is None:
        return False
    return box["latmin"] <= lat <= box["latmax"] and box["lonmin"] <= lon <= box["lonmax"]


def _classify_naval(v):
    name = _vessel_name(v).upper()
    callsign = _vessel_callsign(v).upper()
    ship_type = _vessel_type(v)
    text = f"{name} {callsign}"

    # AIS type 35 = Military ops в стандартной таблице типов судов.
    if ship_type == 35:
        return True, "ais_type_35"
    for kw in NAVAL_KEYWORDS:
        if kw in text:
            return True, f"keyword:{kw.strip()}"
    return False, ""


def _build_density(vessels):
    density = {}
    cell = max(AIS_DENSITY_CELL_DEG, 0.1)
    for v in vessels:
        lat = _vessel_lat(v)
        lon = _vessel_lon(v)
        if lat is None or lon is None:
            continue
        lat_cell = int(lat // cell) * cell
        lon_cell = int(lon // cell) * cell
        key = f"{lat_cell:.2f},{lon_cell:.2f}"
        if key not in density:
            density[key] = {"lat": round(lat_cell, 4), "lon": round(lon_cell, 4), "count": 0}
        density[key]["count"] += 1

    # Чтобы LONGTEXT не разрастался бесконечно — храним наиболее плотные клетки.
    return sorted(density.values(), key=lambda x: x["count"], reverse=True)[:1000]


def _build_chokepoint_disruptions(vessels):
    disruptions = []
    for box in CHOKEPOINTS:
        count = 0
        stopped_or_slow = 0
        for v in vessels:
            lat = _vessel_lat(v)
            lon = _vessel_lon(v)
            if not _inside_box(lat, lon, box):
                continue
            count += 1
            speed = _sf(v.get("SOG", v.get("speed", v.get("sog"))))
            if speed is not None and speed <= 1.0:
                stopped_or_slow += 1

        if count == 0:
            severity = "unknown"
            score = 0
        else:
            ratio = stopped_or_slow / count
            score = round(ratio * 100, 2)
            if ratio >= 0.45 and count >= 10:
                severity = "high"
            elif ratio >= 0.25 and count >= 5:
                severity = "medium"
            else:
                severity = "low"

        disruptions.append({
            "chokepoint": box["name"],
            "vessel_count": count,
            "slow_or_stopped": stopped_or_slow,
            "score": score,
            "severity": severity,
        })
    return disruptions


def _build_naval_candidates(vessels):
    candidates = []
    for v in vessels:
        is_naval, reason = _classify_naval(v)
        if not is_naval:
            continue
        candidates.append({
            "mmsi": v.get("MMSI", v.get("mmsi")),
            "imo": v.get("IMO", v.get("imo")),
            "name": _vessel_name(v),
            "callsign": _vessel_callsign(v),
            "type": _vessel_type(v),
            "latitude": _vessel_lat(v),
            "longitude": _vessel_lon(v),
            "speed": _sf(v.get("SOG", v.get("speed", v.get("sog")))),
            "heading": _sf(v.get("HEADING", v.get("heading"))),
            "reason": reason,
            "raw": v,
        })
    return candidates[:1000]


def _normalize_snapshot_from_vessels(vessels, meta):
    now = datetime.now(timezone.utc)
    disruptions = _build_chokepoint_disruptions(vessels)
    density = _build_density(vessels)
    candidates = _build_naval_candidates(vessels)

    return {
        "sequence": int(now.timestamp()),
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "status": "connected" if vessels else "empty",
        "source": "aishub",
        "meta": meta,
        "disruptions": disruptions,
        "density": density,
        "candidateReports": candidates,
        "totalVessels": len(vessels),
    }


class AISCollector:
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
                sequence_id BIGINT COMMENT 'AIS sequence number',
                api_timestamp VARCHAR(50) COMMENT 'Timestamp из API',
                ais_status VARCHAR(50) COMMENT 'connected/degraded/offline/empty',
                disruption_count INT COMMENT 'Кол-во chokepoint records',
                disruptions_json LONGTEXT COMMENT 'Полный JSON disruptions/chokepoints',
                density_json LONGTEXT COMMENT 'Vessel density grid',
                candidate_count INT COMMENT 'Кол-во naval candidates',
                candidates_json LONGTEXT COMMENT 'Полный JSON candidateReports',
                total_vessels INT COMMENT 'Общее кол-во судов',
                snapshot_hour DATETIME NOT NULL COMMENT 'Для дедупликации',
                snapshot_at DATETIME NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE KEY uq_snapshot_hour (snapshot_hour),
                INDEX idx_sequence (sequence_id),
                INDEX idx_snapshot (snapshot_at),
                INDEX idx_disruptions (disruption_count DESC)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='AIS maritime snapshot (chokepoints + naval candidates)';
        """)
        conn.commit()
        c.close()
        conn.close()

    def fetch_snapshot(self):
        custom = _fetch_custom_snapshot(self.session)
        if custom:
            custom["source"] = custom.get("source", "custom")
            return custom

        vessels, meta, status = _fetch_aishub_vessels(self.session)
        if status == "not_configured":
            return None
        if vessels is None:
            return None
        return _normalize_snapshot_from_vessels(vessels, meta)

    def process(self):
        self.ensure_table()

        data = self.fetch_snapshot()
        if not data:
            print("    Нет данных AIS snapshot")
            return

        now = datetime.now(timezone.utc)
        snapshot_at = now.replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
        snapshot_hour = now.replace(minute=0, second=0, microsecond=0, tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")

        disruptions = data.get("disruptions", [])
        if isinstance(disruptions, dict):
            disruptions = disruptions.get("events", disruptions.get("data", []))
        if not isinstance(disruptions, list):
            disruptions = []

        candidates = data.get("candidateReports", data.get("candidates", []))
        if isinstance(candidates, dict):
            candidates = candidates.get("reports", candidates.get("data", []))
        if not isinstance(candidates, list):
            candidates = []

        density = data.get("density", {})
        total_vessels = _si(data.get("totalVessels", data.get("total_vessels")))
        if total_vessels is None:
            if isinstance(density, dict):
                total_vessels = 0
                for cell in density.values():
                    if isinstance(cell, (int, float)):
                        total_vessels += int(cell)
                    elif isinstance(cell, dict):
                        total_vessels += int(cell.get("count", cell.get("vessels", 0)) or 0)
            elif isinstance(density, list):
                total_vessels = sum(int(cell.get("count", cell.get("vessels", 0)) or 0) for cell in density if isinstance(cell, dict))

        sql = f"""INSERT INTO `{self.table_name}`
            (sequence_id, api_timestamp, ais_status, disruption_count, disruptions_json,
             density_json, candidate_count, candidates_json, total_vessels, snapshot_hour, snapshot_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                sequence_id = VALUES(sequence_id),
                api_timestamp = VALUES(api_timestamp),
                ais_status = VALUES(ais_status),
                disruption_count = VALUES(disruption_count),
                disruptions_json = VALUES(disruptions_json),
                density_json = VALUES(density_json),
                candidate_count = VALUES(candidate_count),
                candidates_json = VALUES(candidates_json),
                total_vessels = VALUES(total_vessels),
                snapshot_at = VALUES(snapshot_at)
        """

        row = (
            _si(data.get("sequence", data.get("sequence_id"))),
            str(data.get("timestamp", data.get("fetchedAt", "")))[:50],
            str(data.get("status", "unknown"))[:50],
            len(disruptions),
            _json_limit(disruptions),
            _json_limit(density),
            len(candidates),
            _json_limit(candidates),
            total_vessels if total_vessels and total_vessels > 0 else None,
            snapshot_hour,
            snapshot_at,
        )

        conn = self.get_db_connection()
        c = conn.cursor()
        c.execute(sql, row)
        conn.commit()
        c.close()
        conn.close()

        print("    AIS snapshot записан:")
        print(f"      Source: {data.get('source', 'unknown')}")
        print(f"      Sequence: {data.get('sequence')}")
        print(f"      Status: {data.get('status')}")
        print(f"      Total vessels: {total_vessels if total_vessels is not None else '?'}")
        print(f"      Chokepoints: {len(disruptions)}")
        print(f"      Naval candidates: {len(candidates)}")

        for d in disruptions[:5]:
            name = d.get("chokepoint", d.get("name", d.get("region", "?")))
            score = d.get("severity", d.get("score", d.get("level", "?")))
            count = d.get("vessel_count", "?")
            print(f"       {name}: vessels={count}, severity={score}")


def main():
    if args.table_name not in DATASETS:
        print(" Неизвестная таблица. Допустимые:")
        for n in DATASETS:
            print(f"  - {n}")
        sys.exit(1)

    print(" WorldMonitor AIS Maritime Collector")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f" Таблица: {args.table_name}")
    print("=" * 60)
    AISCollector(args.table_name).process()
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
        send_error_trace(e)
        sys.exit(1)
