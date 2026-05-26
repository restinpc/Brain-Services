#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Описание: AIS Maritime Snapshot → MySQL
Таблица: vlad_wm_ais_snapshot

Запуск:
  python WorldMonitor_ais_maritime_NEW_20260526.py vlad_wm_ais_snapshot [host] [port] [user] [password] [database]

SCRIPT_VERSION = 2026-05-26-wm-rest-custom-aishub-aisstream

Источники данных по порядку:
  1) WorldMonitor Maritime REST API, если задан WM_API_KEY
     По умолчанию пробуются:
       https://api.worldmonitor.app/api/maritime/v1/get-vessel-snapshot
       https://www.worldmonitor.app/api/maritime/v1/get-vessel-snapshot
     Заголовки:
       X-WorldMonitor-Key: <WM_API_KEY>
       Authorization: Bearer <WM_API_KEY>

  2) Legacy WorldMonitor endpoints, если задан WM_TRY_LEGACY=1
       /api/ais-snapshot
       /api/military-flights не используется здесь, только AIS/maritime пути

  3) AIS_SNAPSHOT_URL — ваш собственный JSON snapshot/агрегатор

  4) AISHub JSON API, если задан AISHUB_USERNAME
       https://data.aishub.net/ws.php

  5) AISStream WebSocket, если задан AISSTREAM_API_KEY
       wss://stream.aisstream.io/v0/stream
       Требуется: pip install websockets

.env примеры:
  WM_API_KEY=wm_live_xxx
  WM_AIS_API_URLS=https://api.worldmonitor.app/api/maritime/v1/get-vessel-snapshot,https://www.worldmonitor.app/api/maritime/v1/get-vessel-snapshot

  AIS_SNAPSHOT_URL=https://your-domain/snapshot.json

  AISHUB_USERNAME=your_aishub_username

  AISSTREAM_API_KEY=your_aisstream_key
  AISSTREAM_LISTEN_SECONDS=25
  AISSTREAM_MAX_MESSAGES=2000

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
import asyncio
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

SCRIPT_VERSION = "2026-05-26-wm-rest-custom-aishub-aisstream"

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "wm_ais_maritime")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")

# ---------- WorldMonitor REST ----------
WM_API_KEY = (os.getenv("WM_API_KEY") or os.getenv("WORLDMONITOR_API_KEY") or "").strip()
WM_AIS_API_URLS = [
    u.strip() for u in os.getenv(
        "WM_AIS_API_URLS",
        "https://api.worldmonitor.app/api/maritime/v1/get-vessel-snapshot,"
        "https://www.worldmonitor.app/api/maritime/v1/get-vessel-snapshot"
    ).split(",") if u.strip()
]
WM_TRY_LEGACY = os.getenv("WM_TRY_LEGACY", "0").strip().lower() in ("1", "true", "yes", "y")
WM_LEGACY_URLS = [
    u.strip() for u in os.getenv(
        "WM_LEGACY_URLS",
        "https://api.worldmonitor.app/api/ais-snapshot,"
        "https://www.worldmonitor.app/api/ais-snapshot"
    ).split(",") if u.strip()
]

# ---------- Custom aggregator ----------
AIS_SNAPSHOT_URL = os.getenv("AIS_SNAPSHOT_URL", "").strip()

# ---------- AISHub ----------
AISHUB_API_URL = os.getenv("AISHUB_API_URL", "https://data.aishub.net/ws.php").strip()
AISHUB_USERNAME = (os.getenv("AISHUB_USERNAME") or os.getenv("AISHUB_API_KEY") or "").strip()
AIS_INTERVAL_MIN = os.getenv("AIS_INTERVAL_MIN", "60").strip()
AIS_DENSITY_CELL_DEG = float(os.getenv("AIS_DENSITY_CELL_DEG", "2"))

# ---------- AISStream ----------
AISSTREAM_API_KEY = (os.getenv("AISSTREAM_API_KEY") or "").strip()
AISSTREAM_URL = os.getenv("AISSTREAM_URL", "wss://stream.aisstream.io/v0/stream").strip()
AISSTREAM_LISTEN_SECONDS = int(os.getenv("AISSTREAM_LISTEN_SECONDS", "25"))
AISSTREAM_MAX_MESSAGES = int(os.getenv("AISSTREAM_MAX_MESSAGES", "2000"))

# Bounding box для AISHub/AISStream. Если не задан — весь мир.
AIS_LATMIN = os.getenv("AIS_LATMIN", "-90")
AIS_LATMAX = os.getenv("AIS_LATMAX", "90")
AIS_LONMIN = os.getenv("AIS_LONMIN", "-180")
AIS_LONMAX = os.getenv("AIS_LONMAX", "180")

DATASETS = {
    "vlad_wm_ais_snapshot": {
        "description": "AIS maritime snapshot: vessels + chokepoints + naval candidates"
    }
}

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


def send_error_trace(exc, script_name="WorldMonitor_ais_maritime_NEW_20260526.py"):
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
    print("   python WorldMonitor_ais_maritime_NEW_20260526.py vlad_wm_ais_snapshot")
    print("   python WorldMonitor_ais_maritime_NEW_20260526.py vlad_wm_ais_snapshot 127.0.0.1 3306 root password brain")
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
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.headers.update({
        "User-Agent": "Brain-Services AIS Collector/2.0",
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


def _request_json(session, url, params=None, method="GET", headers=None, timeout=60):
    headers = headers or {}
    for attempt in range(1, 4):
        try:
            if attempt > 1:
                time.sleep((1.5 ** attempt) + random.uniform(0.2, 0.8))
            if method.upper() == "POST":
                resp = session.post(url, json=params or {}, headers=headers, timeout=timeout)
            else:
                resp = session.get(url, params=params, headers=headers, timeout=timeout)
            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 60))
                print(f"    Rate-limit 429, ждём {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code in (401, 403):
                print(f"    HTTP {resp.status_code}: нет доступа к {url}")
                return None
            if resp.status_code == 404:
                print(f"    HTTP 404: endpoint не найден: {url}")
                return None
            resp.raise_for_status()
            try:
                return resp.json()
            except Exception:
                print(f"    Ответ не JSON от {url}: {resp.text[:200]}")
                return None
        except Exception as e:
            print(f"    Ошибка запроса {url}: {e}")
    return None


def _extract_vessels_from_any(data):
    if data is None:
        return [], {}

    if isinstance(data, list):
        meta = data[0] if data and isinstance(data[0], dict) else {}
        if len(data) > 1 and isinstance(data[1], list):
            return data[1], meta
        vessels = [x for x in data if isinstance(x, dict)]
        return vessels, meta

    if isinstance(data, dict):
        for key in (
            "vessels", "ships", "data", "records", "results", "features",
            "candidateReports", "candidates", "ais", "positions", "items"
        ):
            val = data.get(key)
            if isinstance(val, list):
                return val, data

        # GeoJSON FeatureCollection
        if data.get("type") == "FeatureCollection" and isinstance(data.get("features"), list):
            return data["features"], data

    return [], data if isinstance(data, dict) else {}


def _fetch_worldmonitor_snapshot(session):
    if not WM_API_KEY:
        print("    WorldMonitor: WM_API_KEY не задан, пропускаем")
        return None

    headers = {
        "X-WorldMonitor-Key": WM_API_KEY,
        "Authorization": f"Bearer {WM_API_KEY}",
        "Accept": "application/json",
    }
    urls = list(WM_AIS_API_URLS)
    if WM_TRY_LEGACY:
        urls.extend(WM_LEGACY_URLS)

    for url in urls:
        print(f"    Источник: WorldMonitor REST -> {url}")
        data = _request_json(session, url, headers=headers, timeout=60)
        if data:
            data["source"] = data.get("source", "worldmonitor")
            return data
    return None


def _fetch_custom_snapshot(session):
    if not AIS_SNAPSHOT_URL:
        print("    Custom AIS_SNAPSHOT_URL не задан, пропускаем")
        return None
    print("    Источник: custom AIS_SNAPSHOT_URL")
    data = _request_json(session, AIS_SNAPSHOT_URL, timeout=60)
    if data:
        data["source"] = data.get("source", "custom")
    return data


def _fetch_aishub_vessels(session):
    if not AISHUB_USERNAME:
        print("    AISHub: AISHUB_USERNAME не задан, пропускаем")
        return None, {}, "not_configured"

    params = {
        "username": AISHUB_USERNAME,
        "format": 1,
        "output": "json",
        "compress": 0,
    }
    if AIS_INTERVAL_MIN:
        params["interval"] = AIS_INTERVAL_MIN
    if all(str(v).strip() != "" for v in [AIS_LATMIN, AIS_LATMAX, AIS_LONMIN, AIS_LONMAX]):
        params.update({
            "latmin": AIS_LATMIN,
            "latmax": AIS_LATMAX,
            "lonmin": AIS_LONMIN,
            "lonmax": AIS_LONMAX,
        })
        print(f"    Источник: AISHub bbox=({AIS_LATMIN},{AIS_LATMAX},{AIS_LONMIN},{AIS_LONMAX})")
    else:
        print("    Источник: AISHub без bbox")

    data = _request_json(session, AISHUB_API_URL, params=params, timeout=90)
    vessels, meta = _extract_vessels_from_any(data)

    if isinstance(meta, dict) and meta.get("ERROR") in (True, "true", "TRUE", 1, "1"):
        print(f"    AISHub вернул ошибку: {meta}")
        return [], meta, "error"

    return vessels, meta, "ok"


async def _fetch_aisstream_async():
    if not AISSTREAM_API_KEY:
        print("    AISStream: AISSTREAM_API_KEY не задан, пропускаем")
        return None, {}, "not_configured"

    try:
        import websockets
    except Exception as e:
        print(f"    AISStream: модуль websockets не установлен: {e}")
        print("    Установите: pip install websockets")
        return None, {}, "missing_dependency"

    bbox = [[[_sf(AIS_LATMIN) or -90, _sf(AIS_LONMIN) or -180], [_sf(AIS_LATMAX) or 90, _sf(AIS_LONMAX) or 180]]]
    subscription = {
        "APIKey": AISSTREAM_API_KEY,
        "BoundingBoxes": bbox,
        "FilterMessageTypes": ["PositionReport", "ShipStaticData"],
    }

    vessels_by_mmsi = {}
    started = time.time()
    print(f"    Источник: AISStream WebSocket, слушаем {AISSTREAM_LISTEN_SECONDS}s")

    try:
        async with websockets.connect(AISSTREAM_URL, ping_interval=20, ping_timeout=20) as ws:
            await ws.send(json.dumps(subscription))
            while time.time() - started < AISSTREAM_LISTEN_SECONDS and len(vessels_by_mmsi) < AISSTREAM_MAX_MESSAGES:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5)
                    msg = json.loads(raw)
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    continue

                meta = msg.get("MetaData", {}) if isinstance(msg, dict) else {}
                message = msg.get("Message", {}) if isinstance(msg, dict) else {}
                mmsi = meta.get("MMSI") or meta.get("mmsi")
                if not mmsi:
                    continue

                vessel = vessels_by_mmsi.get(mmsi, {"MMSI": mmsi})
                vessel["NAME"] = meta.get("ShipName") or meta.get("ship_name") or vessel.get("NAME", "")
                vessel["CALLSIGN"] = meta.get("CallSign") or vessel.get("CALLSIGN", "")
                vessel["LATITUDE"] = meta.get("latitude") or meta.get("Latitude") or vessel.get("LATITUDE")
                vessel["LONGITUDE"] = meta.get("longitude") or meta.get("Longitude") or vessel.get("LONGITUDE")

                # PositionReport может быть вложен по-разному
                pr = message.get("PositionReport", {}) if isinstance(message, dict) else {}
                if isinstance(pr, dict):
                    vessel["LATITUDE"] = pr.get("Latitude", vessel.get("LATITUDE"))
                    vessel["LONGITUDE"] = pr.get("Longitude", vessel.get("LONGITUDE"))
                    vessel["SOG"] = pr.get("Sog", vessel.get("SOG"))
                    vessel["COG"] = pr.get("Cog", vessel.get("COG"))
                    vessel["HEADING"] = pr.get("TrueHeading", vessel.get("HEADING"))

                ssd = message.get("ShipStaticData", {}) if isinstance(message, dict) else {}
                if isinstance(ssd, dict):
                    vessel["NAME"] = ssd.get("Name", vessel.get("NAME", ""))
                    vessel["CALLSIGN"] = ssd.get("CallSign", vessel.get("CALLSIGN", ""))
                    vessel["TYPE"] = ssd.get("Type", vessel.get("TYPE"))

                vessels_by_mmsi[mmsi] = vessel
    except Exception as e:
        print(f"    AISStream ошибка: {e}")
        return [], {"error": str(e)}, "error"

    vessels = list(vessels_by_mmsi.values())
    return vessels, {"source": "aisstream", "listen_seconds": AISSTREAM_LISTEN_SECONDS}, "ok"


def _fetch_aisstream_vessels():
    return asyncio.run(_fetch_aisstream_async())


def _vessel_lat(v):
    if isinstance(v, dict) and v.get("geometry") and isinstance(v.get("geometry"), dict):
        coords = v["geometry"].get("coordinates")
        if isinstance(coords, list) and len(coords) >= 2:
            return _sf(coords[1])
    return _sf(v.get("LATITUDE", v.get("lat", v.get("latitude", v.get("Latitude")))))


def _vessel_lon(v):
    if isinstance(v, dict) and v.get("geometry") and isinstance(v.get("geometry"), dict):
        coords = v["geometry"].get("coordinates")
        if isinstance(coords, list) and len(coords) >= 2:
            return _sf(coords[0])
    return _sf(v.get("LONGITUDE", v.get("lon", v.get("lng", v.get("longitude", v.get("Longitude"))))))


def _vessel_name(v):
    props = v.get("properties", {}) if isinstance(v, dict) else {}
    return str(v.get("NAME", v.get("name", v.get("shipname", props.get("name", "")))) or "").strip()


def _vessel_callsign(v):
    props = v.get("properties", {}) if isinstance(v, dict) else {}
    return str(v.get("CALLSIGN", v.get("callsign", props.get("callsign", ""))) or "").strip()


def _vessel_type(v):
    props = v.get("properties", {}) if isinstance(v, dict) else {}
    return _si(v.get("TYPE", v.get("type", v.get("ship_type", props.get("type")))))


def _vessel_speed(v):
    return _sf(v.get("SOG", v.get("speed", v.get("sog", v.get("Speed")))))


def _inside_box(lat, lon, box):
    if lat is None or lon is None:
        return False
    return box["latmin"] <= lat <= box["latmax"] and box["lonmin"] <= lon <= box["lonmax"]


def _classify_naval(v):
    name = _vessel_name(v).upper()
    callsign = _vessel_callsign(v).upper()
    ship_type = _vessel_type(v)
    text = f"{name} {callsign}"
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
            speed = _vessel_speed(v)
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
        if not isinstance(v, dict):
            continue
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
            "speed": _vessel_speed(v),
            "heading": _sf(v.get("HEADING", v.get("heading", v.get("Heading")))),
            "reason": reason,
            "raw": v,
        })
    return candidates[:1000]


def _normalize_snapshot_from_vessels(vessels, meta, source):
    now = datetime.now(timezone.utc)
    vessels = [v for v in vessels if isinstance(v, dict)]
    disruptions = _build_chokepoint_disruptions(vessels)
    density = _build_density(vessels)
    candidates = _build_naval_candidates(vessels)
    return {
        "sequence": int(now.timestamp()),
        "timestamp": now.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "status": "connected" if vessels else "empty",
        "source": source,
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
        print("    Источники по порядку: WorldMonitor REST -> legacy WM(optional) -> AIS_SNAPSHOT_URL -> AISHub -> AISStream")

        wm = _fetch_worldmonitor_snapshot(self.session)
        if wm:
            return wm

        custom = _fetch_custom_snapshot(self.session)
        if custom:
            return custom

        vessels, meta, status = _fetch_aishub_vessels(self.session)
        if status == "ok" and vessels is not None and len(vessels) > 0:
            return _normalize_snapshot_from_vessels(vessels, meta, "aishub")
        if status == "ok" and vessels == []:
            print("    AISHub вернул 0 судов, пробуем следующий источник")

        vessels, meta, status = _fetch_aisstream_vessels()
        if status == "ok" and vessels is not None:
            return _normalize_snapshot_from_vessels(vessels, meta, "aisstream")

        return None

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
            vessels, _ = _extract_vessels_from_any(data)
            total_vessels = len(vessels) if vessels else None

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
            severity = d.get("severity", d.get("score", d.get("level", "?")))
            count = d.get("vessel_count", "?")
            print(f"       {name}: vessels={count}, severity={severity}")


def main():
    if args.table_name not in DATASETS:
        print(" Неизвестная таблица. Допустимые:")
        for n in DATASETS:
            print(f"  - {n}")
        sys.exit(1)

    print(" WorldMonitor AIS Maritime Collector")
    print(f" Script version: {SCRIPT_VERSION}")
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
