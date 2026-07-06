#!/usr/bin/env python3
"""
Описание: локальный Swiss Ephemeris / pyswisseph parser → одна большая MySQL таблица.
Запуск:   python astro_local_parser.py <table_name> [host] [port] [user] [password] [database]

Логика без дополнительных CLI-флагов:
  • старт истории: 2007-01-01 00:00:00 UTC;
  • шаг: 1 час;
  • конец: текущий UTC-час, округлённый вниз;
  • режим всегда полный: planet + moon + все pair + aspect + event;
  • инкрементально: берёт MAX(ts_utc) из таблицы и продолжает со следующего часа;
  • подходит для cron каждый час: повторный запуск не создаёт дубли из-за UNIQUE KEY.

Что пишет в одну таблицу:
  row_type='planet'  — строка на планету/час со всеми планетными признаками
  row_type='moon'    — строка на час с фазой Луны и углом Луна-Солнце
  row_type='pair'    — строка на каждую пару тел/час
  row_type='aspect'  — строка на найденный аспект/час
  row_type='event'   — переходы знаков, смена ретроградности, near-station
"""

# ── 1. ИМПОРТЫ ─────────────────────────────────────────────────────────────────
from __future__ import annotations

import os
import sys
import math
import argparse
import traceback
import requests
try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv(*_args, **_kwargs):
        return False
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

try:
    import swisseph as swe
except Exception as exc:  # pragma: no cover
    print("❌ Не установлен pyswisseph. Установи: pip install pyswisseph", file=sys.stderr)
    raise

# ── 2. КОНФИГ ─────────────────────────────────────────────────────────────────
load_dotenv()

_HANDLER   = os.getenv("HANDLER", "https://server.brain-project.online").rstrip("/")
TRACE_URL  = f"{_HANDLER}/trace.php"
NODE_NAME  = os.getenv("NODE_NAME", "astro_local_parser")
EMAIL      = os.getenv("ALERT_EMAIL", "твоя почта")

DEFAULT_START = "2007-01-01 00:00:00"
STEP_HOURS = 1
BATCH_HOURS = 250
# Путь к Swiss Ephemeris файлам можно задать только через .env, без CLI-флага.
EPHE_PATH = os.getenv("ASTRO_EPHE_PATH")

SIGNS = [
    "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces",
]

# element_id: 0 fire, 1 earth, 2 air, 3 water
# modality_id: 0 cardinal, 1 fixed, 2 mutable
# polarity: +1 masculine/positive, -1 feminine/negative
SIGN_META = {
    1: (0, 0, +1),
    2: (1, 1, -1),
    3: (2, 2, +1),
    4: (3, 0, -1),
    5: (0, 1, +1),
    6: (1, 2, -1),
    7: (2, 0, +1),
    8: (3, 1, -1),
    9: (0, 2, +1),
    10: (1, 0, -1),
    11: (2, 1, +1),
    12: (3, 2, -1),
}

SYNODIC_MONTH_DAYS = 29.53058867

# Набор аспектов близок к тому, что обычно отдают astrology API.
# orb можно менять, если нужно больше/меньше сигналов.
ASPECTS = [
    ("Conjunction", 0.0, 8.0),
    ("Opposition", 180.0, 8.0),
    ("Trine", 120.0, 6.0),
    ("Square", 90.0, 6.0),
    ("Sextile", 60.0, 4.0),
    ("Semi-Sextile", 30.0, 2.0),
    ("Quintile", 72.0, 2.0),
    ("Septile", 360.0 / 7.0, 1.5),
    ("Octile", 45.0, 2.0),
    ("Novile", 40.0, 1.5),
    ("Quincunx", 150.0, 3.0),
    ("Sesquiquadrate", 135.0, 2.0),
]

PLANET_ORDER = [
    "Sun",
    "Moon",
    "Mercury",
    "Venus",
    "Mars",
    "Jupiter",
    "Saturn",
    "Uranus",
    "Neptune",
    "Pluto",
    "MeanNode",
    "TrueNode",
    "SouthNode",
    "TrueSouthNode",
    "Chiron",
    "BlackMoonLilith",
]

# ── 3. ТРАССИРОВКА ОШИБОК ─────────────────────────────────────────────────────
def send_error_trace(exc: Exception, script_name: str = "astro_local_parser.py"):
    """Отправляет трассировку в фоновом потоке — не блокирует основной процесс."""
    import threading

    logs = (
        f"Node: {NODE_NAME}\n"
        f"Script: {script_name}\n"
        f"Exception: {repr(exc)}\n\n"
        f"Traceback:\n{traceback.format_exc()}"
    )

    def _send():
        try:
            requests.post(
                TRACE_URL,
                data={"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs},
                timeout=10,
            )
        except Exception:
            pass

    threading.Thread(target=_send, daemon=True).start()

# ── 4. АРГУМЕНТЫ ──────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Local astrology hourly parser → one MySQL table")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))

# Дополнительные флаги намеренно убраны: парсер всегда считает полный набор
# с 2007-01-01 до текущего UTC-часа и сам продолжает с MAX(ts_utc)+1h.
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

DB_CONFIG = {
    "host": args.host,
    "port": int(args.port),
    "user": args.user,
    "password": args.password,
    "database": args.database,
    "autocommit": False,
}

def db_connect():
    try:
        import mysql.connector
    except Exception as exc:
        raise RuntimeError("Не установлен mysql-connector-python. Установи: pip install mysql-connector-python") from exc
    return mysql.connector.connect(**DB_CONFIG)

# ── 5. ТАБЛИЦЫ (table_name → конфиг расчёта) ─────────────────────────────────
DATASETS = {
    "vlad_astro_hour_features": {
        "description": "Local Swiss Ephemeris hourly astrology features: planets, moon, pairs, aspects, events in one table",
    },
}

# ── 6. МАТЕМАТИКА / SWISS EPHEMERIS ──────────────────────────────────────────
@dataclass
class PlanetPos:
    name: str
    longitude: float
    latitude: float
    distance_au: float
    speed_longitude: float
    speed_latitude: float
    speed_distance: float

    @property
    def is_retrograde(self) -> int:
        return 1 if self.speed_longitude < 0 else 0

    @property
    def sign_num(self) -> int:
        return int(self.longitude // 30.0) + 1

    @property
    def sign_name(self) -> str:
        return SIGNS[self.sign_num - 1]

    @property
    def degree_in_sign(self) -> float:
        return self.longitude % 30.0


def _planet_code(name: str) -> Optional[int]:
    mapping = {
        "Sun": swe.SUN,
        "Moon": swe.MOON,
        "Mercury": swe.MERCURY,
        "Venus": swe.VENUS,
        "Mars": swe.MARS,
        "Jupiter": swe.JUPITER,
        "Saturn": swe.SATURN,
        "Uranus": swe.URANUS,
        "Neptune": swe.NEPTUNE,
        "Pluto": swe.PLUTO,
        "MeanNode": swe.MEAN_NODE,
        "TrueNode": swe.TRUE_NODE,
        "Chiron": getattr(swe, "CHIRON", 15),
        "BlackMoonLilith": getattr(swe, "MEAN_APOG", 12),
    }
    return mapping.get(name)


def parse_dt(s: str) -> datetime:
    s = s.strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"Unsupported datetime format: {s!r}")


def now_utc_floor_hour() -> datetime:
    dt = datetime.now(timezone.utc)
    return dt.replace(minute=0, second=0, microsecond=0)


def iter_hours(start: datetime, end: datetime, step_hours: int = 1) -> Iterable[datetime]:
    current = start
    step = timedelta(hours=step_hours)
    while current <= end:
        yield current
        current += step


def dt_to_mysql(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def julian_day_ut(dt: datetime) -> float:
    hour = dt.hour + dt.minute / 60.0 + dt.second / 3600.0
    return swe.julday(dt.year, dt.month, dt.day, hour)


def norm360(x: float) -> float:
    x = x % 360.0
    return x + 360.0 if x < 0 else x


def signed_angle(a: float, b: float) -> float:
    return norm360(a - b)


def min_angle(a: float, b: float) -> float:
    d = abs(a - b) % 360.0
    return min(d, 360.0 - d)


def trig_deg(deg: float) -> Tuple[float, float]:
    r = math.radians(deg)
    return math.sin(r), math.cos(r)


def phase_name(angle: float) -> str:
    if angle < 22.5 or angle >= 337.5:
        return "New Moon"
    if angle < 67.5:
        return "Waxing Crescent"
    if angle < 112.5:
        return "First Quarter"
    if angle < 157.5:
        return "Waxing Gibbous"
    if angle < 202.5:
        return "Full Moon"
    if angle < 247.5:
        return "Waning Gibbous"
    if angle < 292.5:
        return "Last Quarter"
    return "Waning Crescent"

_WARNED_CODES = set()


def safe_calc_ut(jd: float, code: int, flags: int) -> Optional[Tuple[float, float, float, float, float, float]]:
    """Пробуем SWIEPH, потом fallback MOSEPH. Если для тела нет эфемерид — пропускаем."""
    try:
        pos, _ret = swe.calc_ut(jd, code, flags)
        return tuple(float(x) for x in pos[:6])
    except Exception as first_exc:
        try:
            fallback_flags = swe.FLG_MOSEPH | swe.FLG_SPEED
            pos, _ret = swe.calc_ut(jd, code, fallback_flags)
            return tuple(float(x) for x in pos[:6])
        except Exception as second_exc:
            if code not in _WARNED_CODES:
                print(
                    f"⚠️ body code {code} skipped; возможно не хватает ephemeris-файлов. "
                    f"SWIEPH: {first_exc}; MOSEPH: {second_exc}",
                    file=sys.stderr,
                )
                _WARNED_CODES.add(code)
            return None


def calc_planets(dt: datetime, selected_planets: List[str]) -> Dict[str, PlanetPos]:
    jd = julian_day_ut(dt)
    flags = swe.FLG_SWIEPH | swe.FLG_SPEED
    out: Dict[str, PlanetPos] = {}

    for name in selected_planets:
        if name in ("SouthNode", "TrueSouthNode"):
            continue
        code = _planet_code(name)
        if code is None:
            continue
        calc = safe_calc_ut(jd, code, flags)
        if calc is None:
            continue
        lon, lat, dist, speed_lon, speed_lat, speed_dist = calc
        out[name] = PlanetPos(
            name=name,
            longitude=norm360(lon),
            latitude=lat,
            distance_au=dist,
            speed_longitude=speed_lon,
            speed_latitude=speed_lat,
            speed_distance=speed_dist,
        )

    if "SouthNode" in selected_planets and "MeanNode" in out:
        n = out["MeanNode"]
        out["SouthNode"] = PlanetPos(
            name="SouthNode",
            longitude=norm360(n.longitude + 180.0),
            latitude=-n.latitude,
            distance_au=n.distance_au,
            speed_longitude=n.speed_longitude,
            speed_latitude=-n.speed_latitude,
            speed_distance=n.speed_distance,
        )

    if "TrueSouthNode" in selected_planets and "TrueNode" in out:
        n = out["TrueNode"]
        out["TrueSouthNode"] = PlanetPos(
            name="TrueSouthNode",
            longitude=norm360(n.longitude + 180.0),
            latitude=-n.latitude,
            distance_au=n.distance_au,
            speed_longitude=n.speed_longitude,
            speed_latitude=-n.speed_latitude,
            speed_distance=n.speed_distance,
        )

    return {name: out[name] for name in selected_planets if name in out}


def nearest_aspect(angle_abs: float) -> Tuple[str, float, float, float, int]:
    """Возвращает name, exact_angle, orb, max_orb, in_orb."""
    best = None
    for name, exact, max_orb in ASPECTS:
        orb = abs(angle_abs - exact)
        if best is None or orb < best[2]:
            best = (name, exact, orb, max_orb)
    assert best is not None
    name, exact, orb, max_orb = best
    return name, exact, orb, max_orb, 1 if orb <= max_orb else 0

# ── 7. ОДНА УНИВЕРСАЛЬНАЯ ТАБЛИЦА ─────────────────────────────────────────────
INSERT_COLUMNS = [
    "ts_utc", "row_type", "body_a", "body_b", "aspect_name", "event_type",

    "longitude", "latitude", "distance_au",
    "speed_longitude", "speed_latitude", "speed_distance", "abs_speed_longitude",
    "sign_num", "sign_name", "degree_in_sign", "is_retrograde",
    "element_id", "modality_id", "polarity",
    "sin_longitude", "cos_longitude", "sin2_longitude", "cos2_longitude", "sin3_longitude", "cos3_longitude",

    "angle", "angle_abs", "sin_angle", "cos_angle",

    "moon_sun_angle", "moon_sun_angle_abs", "phase_fraction", "illumination",
    "lunar_age_days", "lunar_day", "phase_name",
    "is_new_moon_window", "is_full_moon_window", "is_first_quarter_window", "is_last_quarter_window",

    "nearest_aspect", "nearest_aspect_angle", "nearest_aspect_orb",
    "aspect_angle", "orb", "max_orb", "strength",

    "value_num", "value_text",
]

BASE_DEFAULTS = {c: None for c in INSERT_COLUMNS}
BASE_DEFAULTS.update({
    "body_a": "",
    "body_b": "",
    "aspect_name": "",
    "event_type": "",
})


def make_row(**kwargs) -> dict:
    row = dict(BASE_DEFAULTS)
    row.update(kwargs)
    return row


def planet_row(dt: datetime, p: PlanetPos) -> dict:
    s1, c1 = trig_deg(p.longitude)
    s2, c2 = trig_deg(2.0 * p.longitude)
    s3, c3 = trig_deg(3.0 * p.longitude)
    element_id, modality_id, polarity = SIGN_META[p.sign_num]
    return make_row(
        ts_utc=dt_to_mysql(dt),
        row_type="planet",
        body_a=p.name,
        longitude=p.longitude,
        latitude=p.latitude,
        distance_au=p.distance_au,
        speed_longitude=p.speed_longitude,
        speed_latitude=p.speed_latitude,
        speed_distance=p.speed_distance,
        abs_speed_longitude=abs(p.speed_longitude),
        sign_num=p.sign_num,
        sign_name=p.sign_name,
        degree_in_sign=p.degree_in_sign,
        is_retrograde=p.is_retrograde,
        element_id=element_id,
        modality_id=modality_id,
        polarity=polarity,
        sin_longitude=s1,
        cos_longitude=c1,
        sin2_longitude=s2,
        cos2_longitude=c2,
        sin3_longitude=s3,
        cos3_longitude=c3,
    )


def moon_row(dt: datetime, planets: Dict[str, PlanetPos], window_orb: float = 5.0) -> Optional[dict]:
    sun = planets.get("Sun")
    moon = planets.get("Moon")
    if not sun or not moon:
        return None

    angle = signed_angle(moon.longitude, sun.longitude)
    angle_abs = min(angle, 360.0 - angle)
    phase_fraction = angle / 360.0
    illumination = (1.0 - math.cos(math.radians(angle))) / 2.0
    lunar_age_days = phase_fraction * SYNODIC_MONTH_DAYS
    lunar_day = int(math.floor(lunar_age_days)) + 1
    s, c = trig_deg(angle)

    return make_row(
        ts_utc=dt_to_mysql(dt),
        row_type="moon",
        body_a="Moon",
        body_b="Sun",
        angle=angle,
        angle_abs=angle_abs,
        sin_angle=s,
        cos_angle=c,
        moon_sun_angle=angle,
        moon_sun_angle_abs=angle_abs,
        phase_fraction=phase_fraction,
        illumination=illumination,
        lunar_age_days=lunar_age_days,
        lunar_day=lunar_day,
        phase_name=phase_name(angle),
        is_new_moon_window=1 if min_angle(angle, 0.0) <= window_orb else 0,
        is_full_moon_window=1 if min_angle(angle, 180.0) <= window_orb else 0,
        is_first_quarter_window=1 if min_angle(angle, 90.0) <= window_orb else 0,
        is_last_quarter_window=1 if min_angle(angle, 270.0) <= window_orb else 0,
    )


def pair_row(dt: datetime, a: str, b: str, pa: PlanetPos, pb: PlanetPos) -> dict:
    directed = signed_angle(pa.longitude, pb.longitude)
    angle_abs = min(directed, 360.0 - directed)
    s, c = trig_deg(directed)
    asp, exact, orb, max_orb, in_orb = nearest_aspect(angle_abs)
    strength = max(0.0, 1.0 - (orb / max_orb)) if in_orb and max_orb > 0 else 0.0
    return make_row(
        ts_utc=dt_to_mysql(dt),
        row_type="pair",
        body_a=a,
        body_b=b,
        angle=directed,
        angle_abs=angle_abs,
        sin_angle=s,
        cos_angle=c,
        nearest_aspect=asp,
        nearest_aspect_angle=exact,
        nearest_aspect_orb=orb,
        aspect_angle=exact if in_orb else None,
        orb=orb if in_orb else None,
        max_orb=max_orb if in_orb else None,
        strength=strength if in_orb else None,
        value_num=1.0 if in_orb else 0.0,
        value_text="in_orb" if in_orb else "out_of_orb",
    )


def aspect_rows(dt: datetime, a: str, b: str, pa: PlanetPos, pb: PlanetPos) -> List[dict]:
    directed = signed_angle(pa.longitude, pb.longitude)
    angle_abs = min(directed, 360.0 - directed)
    s, c = trig_deg(directed)
    rows = []

    for asp, exact, max_orb in ASPECTS:
        orb = abs(angle_abs - exact)
        if orb <= max_orb:
            strength = max(0.0, 1.0 - (orb / max_orb)) if max_orb > 0 else 1.0
            rows.append(make_row(
                ts_utc=dt_to_mysql(dt),
                row_type="aspect",
                body_a=a,
                body_b=b,
                aspect_name=asp,
                angle=directed,
                angle_abs=angle_abs,
                sin_angle=s,
                cos_angle=c,
                nearest_aspect=asp,
                nearest_aspect_angle=exact,
                nearest_aspect_orb=orb,
                aspect_angle=exact,
                orb=orb,
                max_orb=max_orb,
                strength=strength,
                value_num=strength,
                value_text=asp,
            ))

    return rows


def event_rows(dt: datetime, planets: Dict[str, PlanetPos], prev: Optional[Dict[str, PlanetPos]]) -> List[dict]:
    if not prev:
        return []

    out = []
    for name, p in planets.items():
        q = prev.get(name)
        if not q:
            continue

        if p.sign_num != q.sign_num:
            out.append(make_row(
                ts_utc=dt_to_mysql(dt),
                row_type="event",
                body_a=name,
                event_type="sign_ingress",
                value_num=float(p.sign_num),
                value_text=p.sign_name,
            ))

        if p.is_retrograde != q.is_retrograde:
            out.append(make_row(
                ts_utc=dt_to_mysql(dt),
                row_type="event",
                body_a=name,
                event_type="retrograde_change",
                value_num=float(p.is_retrograde),
                value_text="retrograde" if p.is_retrograde else "direct",
            ))

        # Условная зона станции: очень маленькая видимая скорость долготы.
        if abs(p.speed_longitude) <= 0.005:
            out.append(make_row(
                ts_utc=dt_to_mysql(dt),
                row_type="event",
                body_a=name,
                event_type="near_station",
                value_num=float(p.speed_longitude),
                value_text="",
            ))

    return out


def build_rows_for_hour(
    dt: datetime,
    selected_planets: List[str],
    prev_planets: Optional[Dict[str, PlanetPos]],
) -> Tuple[List[dict], Dict[str, PlanetPos]]:
    """Полный набор строк за один час: планеты, Луна, все пары, аспекты и события."""
    planets = calc_planets(dt, selected_planets)
    rows: List[dict] = []

    for p in planets.values():
        rows.append(planet_row(dt, p))

    mr = moon_row(dt, planets)
    if mr:
        rows.append(mr)

    names = list(planets.keys())
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            a, b = names[i], names[j]
            pa, pb = planets[a], planets[b]
            rows.append(pair_row(dt, a, b, pa, pb))
            rows.extend(aspect_rows(dt, a, b, pa, pb))

    rows.extend(event_rows(dt, planets, prev_planets))

    return rows, planets

# ── 8. СОЗДАНИЕ ТАБЛИЦЫ ───────────────────────────────────────────────────────
def ensure_table(table_name: str):
    conn = db_connect()
    c = conn.cursor()

    c.execute(f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,

            ts_utc DATETIME NOT NULL,
            row_type VARCHAR(16) NOT NULL,
            body_a VARCHAR(32) NOT NULL DEFAULT '',
            body_b VARCHAR(32) NOT NULL DEFAULT '',
            aspect_name VARCHAR(32) NOT NULL DEFAULT '',
            event_type VARCHAR(32) NOT NULL DEFAULT '',

            longitude DOUBLE NULL,
            latitude DOUBLE NULL,
            distance_au DOUBLE NULL,
            speed_longitude DOUBLE NULL,
            speed_latitude DOUBLE NULL,
            speed_distance DOUBLE NULL,
            abs_speed_longitude DOUBLE NULL,

            sign_num TINYINT NULL,
            sign_name VARCHAR(16) NULL,
            degree_in_sign DOUBLE NULL,
            is_retrograde TINYINT NULL,
            element_id TINYINT NULL,
            modality_id TINYINT NULL,
            polarity TINYINT NULL,

            sin_longitude DOUBLE NULL,
            cos_longitude DOUBLE NULL,
            sin2_longitude DOUBLE NULL,
            cos2_longitude DOUBLE NULL,
            sin3_longitude DOUBLE NULL,
            cos3_longitude DOUBLE NULL,

            angle DOUBLE NULL,
            angle_abs DOUBLE NULL,
            sin_angle DOUBLE NULL,
            cos_angle DOUBLE NULL,

            moon_sun_angle DOUBLE NULL,
            moon_sun_angle_abs DOUBLE NULL,
            phase_fraction DOUBLE NULL,
            illumination DOUBLE NULL,
            lunar_age_days DOUBLE NULL,
            lunar_day TINYINT NULL,
            phase_name VARCHAR(32) NULL,
            is_new_moon_window TINYINT NULL,
            is_full_moon_window TINYINT NULL,
            is_first_quarter_window TINYINT NULL,
            is_last_quarter_window TINYINT NULL,

            nearest_aspect VARCHAR(32) NULL,
            nearest_aspect_angle DOUBLE NULL,
            nearest_aspect_orb DOUBLE NULL,
            aspect_angle DOUBLE NULL,
            orb DOUBLE NULL,
            max_orb DOUBLE NULL,
            strength DOUBLE NULL,

            value_num DOUBLE NULL,
            value_text VARCHAR(64) NULL,

            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

            PRIMARY KEY (id),
            UNIQUE KEY uq_astro_one_row (ts_utc, row_type, body_a, body_b, aspect_name, event_type),
            KEY idx_ts (ts_utc),
            KEY idx_type_ts (row_type, ts_utc),
            KEY idx_body_a_ts (body_a, ts_utc),
            KEY idx_body_pair_ts (body_a, body_b, ts_utc),
            KEY idx_aspect_ts (aspect_name, ts_utc),
            KEY idx_event_ts (event_type, ts_utc)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        COMMENT='{DATASETS[table_name]["description"]}';
    """)

    conn.commit()
    c.close()
    conn.close()

# ── 9. ПОСЛЕДНЯЯ ДАТА В БД ───────────────────────────────────────────────────
def get_latest_ts(table_name: str) -> Optional[datetime]:
    try:
        conn = db_connect()
        c = conn.cursor()
        c.execute(f"SELECT MAX(ts_utc) FROM `{table_name}`")
        row = c.fetchone()
        c.close()
        conn.close()
        if row and row[0]:
            dt = row[0]
            if isinstance(dt, datetime):
                return dt.replace(tzinfo=timezone.utc)
            return parse_dt(str(dt))
        return None
    except Exception:
        return None

# ── 10. ЗАПИСЬ В БД ───────────────────────────────────────────────────────────
def save_rows(table_name: str, rows: List[dict]) -> int:
    if not rows:
        return 0

    conn = db_connect()
    c = conn.cursor()

    cols = ", ".join(f"`{cname}`" for cname in INSERT_COLUMNS)
    vals = ", ".join(f"%({cname})s" for cname in INSERT_COLUMNS)

    sql = f"""
        INSERT IGNORE INTO `{table_name}` ({cols})
        VALUES ({vals})
    """

    c.executemany(sql, rows)
    conn.commit()
    inserted = c.rowcount

    c.close()
    conn.close()
    return int(inserted)

# ── 11. ОСНОВНАЯ ЛОГИКА ───────────────────────────────────────────────────────
def process(table_name: str):
    config = DATASETS[table_name]
    del config  # конфиг пока простой, оставлено в стиле остальных парсеров

    if EPHE_PATH:
        swe.set_ephe_path(EPHE_PATH)

    selected_planets = PLANET_ORDER[:]
    start = parse_dt(DEFAULT_START)
    end = now_utc_floor_hour()

    ensure_table(table_name)

    latest = get_latest_ts(table_name)
    print(f"📅 Последняя дата в БД: {latest.strftime('%Y-%m-%d %H:%M:%S') if latest else 'таблица пуста'}")

    # Всегда инкрементально: если таблица уже заполнена, продолжаем со следующего часа.
    if latest:
        next_ts = latest + timedelta(hours=STEP_HOURS)
        if next_ts > start:
            start = next_ts
            print(f"➡️  Продолжаем с: {dt_to_mysql(start)}")

    if start > end:
        print(f"✅ Новых часов нет. Последний доступный UTC-час: {dt_to_mysql(end)}")
        return

    total_hours = int((end - start).total_seconds() // (STEP_HOURS * 3600)) + 1
    print(f"🧮 Часов к расчёту: {total_hours}")
    print(f"🪐 Тел: {len(selected_planets)}")
    print("🔁 Режим: полный набор = planet + moon + all pairs + aspects + events")
    print(f"🕐 Конец расчёта: {dt_to_mysql(end)} UTC")

    # Чтобы hourly-cron корректно поймал sign_ingress/retrograde_change в первом новом часе,
    # prev_planets считаем по предыдущему часу, а не оставляем None.
    prev_planets: Optional[Dict[str, PlanetPos]] = None
    prev_dt = start - timedelta(hours=STEP_HOURS)
    if prev_dt >= parse_dt(DEFAULT_START):
        try:
            prev_planets = calc_planets(prev_dt, selected_planets)
        except Exception as exc:
            print(f"⚠️ Не удалось посчитать предыдущий час для event-сравнения: {exc!r}")
            prev_planets = None

    batch_rows: List[dict] = []
    batch_hours = 0
    processed_hours = 0
    inserted_total = 0
    generated_total = 0

    for dt in iter_hours(start, end, STEP_HOURS):
        rows, prev_planets = build_rows_for_hour(
            dt,
            selected_planets,
            prev_planets=prev_planets,
        )
        batch_rows.extend(rows)
        generated_total += len(rows)
        batch_hours += 1
        processed_hours += 1

        if batch_hours >= BATCH_HOURS:
            inserted = save_rows(table_name, batch_rows)
            inserted_total += inserted
            print(
                f"✅ batch hours={batch_hours} generated_rows={len(batch_rows)} inserted={inserted} "
                f"progress={processed_hours}/{total_hours} last={dt_to_mysql(dt)}",
                flush=True,
            )
            batch_rows = []
            batch_hours = 0

    if batch_rows:
        inserted = save_rows(table_name, batch_rows)
        inserted_total += inserted
        print(f"✅ final batch hours={batch_hours} generated_rows={len(batch_rows)} inserted={inserted}", flush=True)

    print(f"📦 Сгенерировано строк: {generated_total}")
    print(f"💾 Вставлено новых строк: {inserted_total}")

# ── 12. ТОЧКА ВХОДА ───────────────────────────────────────────────────────────
def main():
    if args.table_name not in DATASETS:
        print(f"❌ Неизвестная таблица '{args.table_name}'. Допустимые:")
        for name in DATASETS:
            print(f"  - {name}")
        sys.exit(1)

    print("🚀 Astro Local Parser")
    print(f"   База: {args.host}:{args.port}/{args.database}")
    print(f"   Таблица: {args.table_name}")
    print(f"   Период UTC: {DEFAULT_START} → current UTC hour")
    print("=" * 60)

    process(args.table_name)

    print("=" * 60)
    print("🏁 ГОТОВО")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n🛑 Прервано")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)
