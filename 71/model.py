"""
Astro Hypothesis Lab — service 71.

The raw astrology parser stores hundreds of rows per hour.  This model first
compacts only the lossless planet rows into one fixed binary snapshot per hour.
Every pair angle, aspect, lunar phase, sign, ingress and retrograde transition
is reconstructed from those positions without loading the enormous pair/aspect
part of the parser table into RAM.

Hypothesis matrix:
    12 types × 12 vars = 144 independent cache/backtest slots.

No future market data is used.  Type 11 projects astronomical positions from
current positions and current Swiss-Ephemeris speeds; that information is known
at the target time and does not read future price candles or future dataset rows.
"""
from __future__ import annotations

import bisect
import itertools
import math
import re
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Dict, Iterable, Iterator, Mapping, MutableMapping, Optional, Sequence, Tuple

import numpy as np
from sqlalchemy import text

# -----------------------------------------------------------------------------
# Service constants
# -----------------------------------------------------------------------------
SERVICE_ID = 71
PORT = 8933
NODE_NAME = "brain-vlad_astro_hypothesis_lab-s71"
SERVICE_TEXT = "Astrology hypothesis laboratory: 144 independent type/var strategies"
DEVELOPER_EMAIL = "vladyurjevitch@yandex.ru"

PARSER_TABLE = "vlad_astro_hour_features"
SNAPSHOT_TABLE = "vlad_astro_hour_snapshots_s71"
CTX_TABLE = "vlad_astro_hypothesis_ctx_s71"
WEIGHTS_TABLE = "vlad_astro_hypothesis_weights_s71"

SNAPSHOT_SCHEMA_VERSION = 1
SNAPSHOT_FIELDS_PER_BODY = 6
DEFAULT_HISTORY_START = datetime(2007, 1, 1)
MAX_OUTPUT_FEATURES = 128

BODY_ORDER = [
    "Ascendant",
    "Sun",
    "Moon",
    "Mars",
    "Mercury",
    "Jupiter",
    "Venus",
    "Saturn",
    "Uranus",
    "Neptune",
    "Pluto",
    "Ceres",
    "Vesta",
    "Juno",
    "Pallas",
    "Chiron",
    "Lilith",
    "Mean Node",
    "True Node",
    "Descendant",
    "MC",
    "IC",
]
BODY_INDEX = {name: i for i, name in enumerate(BODY_ORDER)}

# Pair-heavy families intentionally use the main astronomical bodies.  Angles,
# asteroids and alternate nodes remain available in all single-body families.
CORE_BODIES = [
    "Sun", "Moon", "Mercury", "Venus", "Mars", "Jupiter",
    "Saturn", "Uranus", "Neptune", "Pluto", "Mean Node", "Chiron",
]
CORE_INDEX = [BODY_INDEX[x] for x in CORE_BODIES]
PHYSICAL_BODIES = [x for x in BODY_ORDER if x not in {"Ascendant", "Descendant", "MC", "IC"}]
HARMONICS = (1, 2, 3, 4, 6, 12)
PROJECTION_HOURS = (6, 24, 72)

SIGNS = (
    "Aries", "Taurus", "Gemini", "Cancer", "Leo", "Virgo",
    "Libra", "Scorpio", "Sagittarius", "Capricorn", "Aquarius", "Pisces",
)
ELEMENTS = ("fire", "earth", "air", "water")
MODALITIES = ("cardinal", "fixed", "mutable")
SIGN_ELEMENT = np.array([0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3], dtype=np.int8)
SIGN_MODALITY = np.array([0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2], dtype=np.int8)
SIGN_POLARITY = np.array([1, -1, 1, -1, 1, -1, 1, -1, 1, -1, 1, -1], dtype=np.int8)

PHASE_NAMES = (
    "new", "waxing_crescent", "first_quarter", "waxing_gibbous",
    "full", "waning_gibbous", "last_quarter", "waning_crescent",
)

ASPECTS = (
    ("conjunction", 0.0, 8.0, 0.25),
    ("opposition", 180.0, 8.0, -1.0),
    ("trine", 120.0, 6.0, 1.0),
    ("square", 90.0, 6.0, -1.0),
    ("sextile", 60.0, 4.0, 0.75),
    ("semi_sextile", 30.0, 2.0, 0.20),
    ("quintile", 72.0, 2.0, 0.65),
    ("septile", 360.0 / 7.0, 1.5, 0.10),
    ("octile", 45.0, 2.0, -0.55),
    ("novile", 40.0, 1.5, 0.35),
    ("quincunx", 150.0, 3.0, -0.65),
    ("sesquiquadrate", 135.0, 2.0, -0.70),
)
ASPECT_NAMES = tuple(x[0] for x in ASPECTS)

# Approximate normal geocentric longitude speed, degrees/day.  It is used only
# to normalize continuous speed hypotheses; it never changes the raw data.
NORMAL_SPEED = {
    "Sun": 0.986, "Moon": 13.18, "Mercury": 1.20, "Venus": 1.00,
    "Mars": 0.52, "Jupiter": 0.083, "Saturn": 0.033,
    "Uranus": 0.012, "Neptune": 0.006, "Pluto": 0.004,
    "Ceres": 0.20, "Vesta": 0.25, "Juno": 0.18, "Pallas": 0.20,
    "Chiron": 0.020, "Lilith": 0.111, "Mean Node": 0.053,
    "True Node": 0.053,
}

TYPE_DESCRIPTIONS = {
    0: "Absolute categorical state: signs, decans, retrograde state and current aspects",
    1: "Cyclic harmonics of every longitude (sin/cos for 1,2,3,4,6,12 harmonics)",
    2: "Element, modality, polarity and sign-distribution balance",
    3: "Speed, retrograde, station and normalized motion regimes",
    4: "Current aspect network, centrality, tension and harmony topology",
    5: "Aspect dynamics: applying, separating, forming, breaking and exactness",
    6: "Discrete transitions and their recency: ingress, retrograde turn and station",
    7: "Lunar cycle: phase, lunar day, illumination, distance, latitude and Moon aspects",
    8: "Synodic cycles for all core-body pairs",
    9: "Multi-body geometric patterns: stellium, grand trine, T-square, Yod and clusters",
    10: "Astronomical momentum over short, medium and long lookbacks",
    11: "Projected known ephemeris state at +6h, +24h and +72h",
}
VAR_DESCRIPTIONS = {
    0: "binary presence",
    1: "signed direction/polarity",
    2: "continuous normalized magnitude",
    3: "nonlinear squared magnitude",
    4: "strict/high-confidence subset",
    5: "broad/soft square-root weighting",
    6: "change over 1 market bar",
    7: "change over 6 market bars",
    8: "change over 24 market bars",
    9: "second derivative / acceleration",
    10: "rolling 30-bar z-score",
    11: "competitive L1 normalization of strongest features",
}

# -----------------------------------------------------------------------------
# Small math helpers
# -----------------------------------------------------------------------------
def _clip(value: float, lo: float = -1.0, hi: float = 1.0) -> float:
    if not math.isfinite(value):
        return 0.0
    return max(lo, min(hi, float(value)))


def _norm360(value: float) -> float:
    return float(value) % 360.0


def _circular_delta(a: float, b: float) -> float:
    """Shortest signed angle a-b in [-180, 180)."""
    return ((_norm360(a) - _norm360(b) + 180.0) % 360.0) - 180.0


def _min_angle(a: float, b: float) -> float:
    return abs(_circular_delta(a, b))


def _phase_name(angle: float) -> str:
    idx = int(((_norm360(angle) + 22.5) % 360.0) // 45.0)
    return PHASE_NAMES[idx]


def _nearest_aspect(angle_abs: float) -> tuple[str, float, float, float, float]:
    best = min(ASPECTS, key=lambda x: abs(angle_abs - x[1]))
    name, exact, max_orb, polarity = best
    return name, exact, abs(angle_abs - exact), max_orb, polarity


def _pair_name(a: str, b: str) -> str:
    return f"{a}__{b}"


def _safe_table(name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_]+", str(name or "")):
        raise ValueError(f"Unsafe SQL table name: {name!r}")
    return str(name)


# -----------------------------------------------------------------------------
# Compact hourly snapshot enrichment
# -----------------------------------------------------------------------------
def _pack_snapshot(rows: Sequence[Mapping[str, object]]) -> tuple[bytes, int, int, float]:
    matrix = np.full((len(BODY_ORDER), SNAPSHOT_FIELDS_PER_BODY), np.nan, dtype="<f4")
    mask = 0
    seen = 0
    for row in rows:
        name = str(row.get("body_a") or "")
        idx = BODY_INDEX.get(name)
        if idx is None:
            continue
        vals = (
            row.get("longitude"), row.get("latitude"), row.get("distance_au"),
            row.get("speed_longitude"), row.get("speed_latitude"), row.get("speed_distance"),
        )
        for col, value in enumerate(vals):
            if value is not None:
                try:
                    matrix[idx, col] = float(value)
                except (TypeError, ValueError):
                    pass
        if math.isfinite(float(matrix[idx, 0])):
            mask |= 1 << idx
            seen += 1
    return matrix.tobytes(order="C"), mask, seen, seen / float(len(BODY_ORDER))


async def _ensure_contexts_and_weights(
    engine_vlad,
    ctx_table: str,
    weights_table: str,
) -> dict:
    """Create/verify the static semantic universe with a fast repeat path.

    The feature universe is deterministic.  On normal repeated rebuilds we only
    compare row counts and skip all upserts when both tables are complete.
    First creation still uses bulk executemany for contexts and one
    INSERT..SELECT for every weight.
    """
    ctx_table = _safe_table(ctx_table)
    weights_table = _safe_table(weights_table)

    definitions = [
        {
            "feature_key": key,
            "feature_family": family,
            "name": description,
        }
        for key, family, description in iter_feature_definitions()
    ]
    expected = len(definitions)

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{ctx_table}` (
                id               INT UNSIGNED NOT NULL AUTO_INCREMENT,
                feature_key      VARCHAR(191) NOT NULL,
                feature_family   VARCHAR(48)  NOT NULL,
                name             VARCHAR(255) NOT NULL,
                occurrence_count INT UNSIGNED NOT NULL DEFAULT 0,
                date_added       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                                 ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                UNIQUE KEY uk_feature_key (feature_key),
                KEY idx_feature_family (feature_family)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{weights_table}` (
                id          INT UNSIGNED NOT NULL AUTO_INCREMENT,
                weight_code VARCHAR(64)  NOT NULL,
                ctx_id      INT UNSIGNED NOT NULL,
                mode        TINYINT      NOT NULL DEFAULT 0,
                shift       SMALLINT     NOT NULL DEFAULT 0,
                PRIMARY KEY (id),
                UNIQUE KEY uk_weight_code (weight_code),
                UNIQUE KEY uk_ctx_mode_shift (ctx_id, mode, shift),
                KEY idx_ctx_id (ctx_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

    # Fast path: the universe is static and already complete.
    async with engine_vlad.connect() as conn:
        row = (await conn.execute(text(f"""
            SELECT
              (SELECT COUNT(*) FROM `{ctx_table}`) AS ctx_count,
              (SELECT COUNT(*) FROM `{weights_table}`) AS weight_count,
              (SELECT COUNT(DISTINCT feature_family) FROM `{ctx_table}`) AS families
        """))).fetchone()
    ctx_count = int(row[0] if row else 0)
    weight_count = int(row[1] if row else 0)
    families = int(row[2] if row else 0)
    if ctx_count == expected and weight_count == expected:
        print(
            f"[astro-s71] contexts/weights ready: {ctx_count}/{weight_count}; fast skip",
            flush=True,
        )
        return {
            "ctx_table": ctx_table, "contexts": ctx_count,
            "families": families, "weights_table": weights_table,
            "weights": weight_count, "semantic_fast_path": True,
        }

    print(
        f"[astro-s71] building contexts: existing={ctx_count}, expected={expected}",
        flush=True,
    )
    insert_sql = text(f"""
        INSERT IGNORE INTO `{ctx_table}` (feature_key, feature_family, name)
        VALUES (:feature_key, :feature_family, :name)
    """)
    async with engine_vlad.begin() as conn:
        # 5k rows per executemany reduces round trips while staying packet-safe.
        for i in range(0, expected, 5000):
            await conn.execute(insert_sql, definitions[i:i + 5000])
        await conn.execute(text(f"""
            INSERT IGNORE INTO `{weights_table}`
                (weight_code, ctx_id, mode, shift)
            SELECT CONCAT(id, '_0_0'), id, 0, 0
            FROM `{ctx_table}`
        """))

    async with engine_vlad.connect() as conn:
        row = (await conn.execute(text(f"""
            SELECT
              (SELECT COUNT(*) FROM `{ctx_table}`),
              (SELECT COUNT(*) FROM `{weights_table}`),
              (SELECT COUNT(DISTINCT feature_family) FROM `{ctx_table}`)
        """))).fetchone()
    ctx_count = int(row[0] if row else 0)
    weight_count = int(row[1] if row else 0)
    families = int(row[2] if row else 0)
    print(
        f"[astro-s71] contexts/weights complete: {ctx_count}/{weight_count}",
        flush=True,
    )
    return {
        "ctx_table": ctx_table, "contexts": ctx_count,
        "families": families, "weights_table": weights_table,
        "weights": weight_count, "semantic_fast_path": False,
    }


async def enrich_dataset(engine_vlad, engine_brain) -> dict:
    """Incrementally build one compact row per hour from raw planet rows only.

    The raw parser's pair/aspect rows are deliberately not copied.  All of them
    are deterministic functions of the retained body positions, so this reduces
    tens of millions of raw rows to roughly 170k fixed-size hourly snapshots.
    """
    try:
        from brain_framework import get_service_config
        cfg = get_service_config() or {}
    except Exception:
        cfg = {}

    dcfg = cfg.get("dataset") or {}
    ccfg = cfg.get("ctx") or {}
    raw_table = _safe_table(dcfg.get("parser_table") or PARSER_TABLE)
    target_table = _safe_table(dcfg.get("table") or SNAPSHOT_TABLE)
    ctx_table = _safe_table(ccfg.get("table") or CTX_TABLE)
    weights_table = _safe_table(ccfg.get("weights_table") or WEIGHTS_TABLE)
    parser_engine_name = str(dcfg.get("parser_engine") or "brain").lower()
    source_engine = engine_vlad if parser_engine_name == "vlad" else engine_brain
    tail_hours = max(0, int(dcfg.get("rebuild_tail_hours") or 48))
    chunk_days = max(7, int(dcfg.get("enrich_chunk_days") or 180))

    semantic_stats = await _ensure_contexts_and_weights(
        engine_vlad, ctx_table, weights_table
    )

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{target_table}` (
                date_dt       DATETIME        NOT NULL,
                body_blob     MEDIUMBLOB      NOT NULL,
                body_mask     BIGINT UNSIGNED NOT NULL DEFAULT 0,
                body_count    TINYINT UNSIGNED NOT NULL DEFAULT 0,
                source_rows   SMALLINT UNSIGNED NOT NULL DEFAULT 0,
                quality       DOUBLE          NOT NULL DEFAULT 0,
                schema_version SMALLINT UNSIGNED NOT NULL DEFAULT {SNAPSHOT_SCHEMA_VERSION},
                updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                              ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (date_dt),
                KEY idx_quality_date (quality, date_dt)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))

    async with source_engine.connect() as conn:
        raw_max_row = (await conn.execute(text(f"""
            SELECT MAX(ts_utc) FROM `{raw_table}` WHERE row_type='planet'
        """))).fetchone()
    raw_max = raw_max_row[0] if raw_max_row else None
    if raw_max is None:
        return {
            "status": "empty",
            "raw_table": raw_table,
            "snapshots": 0,
            **semantic_stats,
        }

    async with engine_vlad.connect() as conn:
        target_max_row = (await conn.execute(text(
            f"SELECT MAX(date_dt), MAX(schema_version) FROM `{target_table}`"
        ))).fetchone()
    target_max = target_max_row[0] if target_max_row else None
    existing_version = target_max_row[1] if target_max_row else None
    if existing_version not in (None, SNAPSHOT_SCHEMA_VERSION):
        raise RuntimeError(
            f"{target_table}: schema_version={existing_version}, expected "
            f"{SNAPSHOT_SCHEMA_VERSION}. Drop the table once and rebuild."
        )

    start = DEFAULT_HISTORY_START
    if target_max is not None:
        start = max(DEFAULT_HISTORY_START, target_max - timedelta(hours=tail_hours))
    start = start.replace(minute=0, second=0, microsecond=0)
    end_limit = raw_max.replace(minute=0, second=0, microsecond=0)
    if start > end_limit:
        return {
            "status": "up_to_date", "raw_table": raw_table,
            "snapshot_table": target_table, "latest": end_limit.isoformat(),
            **semantic_stats,
        }

    # The parser already has idx_type_ts(row_type, ts_utc).  Reading only
    # row_type='planet' through that index avoids scanning pair/aspect/event rows.
    # Ordering by body_a is unnecessary for grouping and can force a filesort,
    # therefore we order by ts_utc only.
    read_yield_rows = max(1000, int(dcfg.get("read_yield_rows") or 20000))
    write_batch_hours = max(250, int(dcfg.get("write_batch_hours") or 5000))
    inserted = 0
    source_rows_read = 0
    skipped_incomplete = 0
    chunks = 0

    upsert_sql = text(f"""
        INSERT INTO `{target_table}`
            (date_dt, body_blob, body_mask, body_count,
             source_rows, quality, schema_version)
        VALUES (:dt, :blob, :mask, :cnt, :src, :quality, :ver)
        ON DUPLICATE KEY UPDATE
            body_blob=VALUES(body_blob),
            body_mask=VALUES(body_mask),
            body_count=VALUES(body_count),
            source_rows=VALUES(source_rows),
            quality=VALUES(quality),
            schema_version=VALUES(schema_version)
    """)

    async def write_snapshots(rows: list[dict]) -> int:
        if not rows:
            return 0
        async with engine_vlad.begin() as conn:
            await conn.execute(upsert_sql, rows)
        return len(rows)

    current = start
    while current <= end_limit:
        chunk_end = min(
            current + timedelta(days=chunk_days),
            end_limit + timedelta(hours=1),
        )
        stmt = text(f"""
            SELECT ts_utc, body_a, longitude, latitude, distance_au,
                   speed_longitude, speed_latitude, speed_distance
            FROM `{raw_table}` FORCE INDEX (`idx_type_ts`)
            WHERE row_type='planet'
              AND ts_utc >= :start_dt AND ts_utc < :end_dt
            ORDER BY ts_utc
        """).execution_options(
            stream_results=True,
            yield_per=read_yield_rows,
        )

        pending: list[dict] = []
        group_dt = None
        group_rows: list[Mapping[str, object]] = []
        chunk_raw = 0
        chunk_packed = 0

        def build_group() -> dict | None:
            nonlocal skipped_incomplete
            if group_dt is None or not group_rows:
                return None
            blob, mask, count, quality = _pack_snapshot(group_rows)
            has_sun = bool(mask & (1 << BODY_INDEX["Sun"]))
            has_moon = bool(mask & (1 << BODY_INDEX["Moon"]))
            if count < 10 or not has_sun or not has_moon:
                skipped_incomplete += 1
                return None
            return {
                "dt": group_dt,
                "blob": blob,
                "mask": mask,
                "cnt": count,
                "src": len(group_rows),
                "quality": quality,
                "ver": SNAPSHOT_SCHEMA_VERSION,
            }

        async with source_engine.connect() as conn:
            result = await conn.stream(
                stmt,
                {"start_dt": current, "end_dt": chunk_end},
            )
            async for row in result.mappings():
                chunk_raw += 1
                dt = row["ts_utc"]
                if group_dt is None:
                    group_dt = dt
                elif dt != group_dt:
                    packed = build_group()
                    if packed is not None:
                        pending.append(packed)
                        chunk_packed += 1
                    group_dt = dt
                    group_rows = []
                    if len(pending) >= write_batch_hours:
                        inserted += await write_snapshots(pending)
                        pending.clear()
                group_rows.append(row)

        packed = build_group()
        if packed is not None:
            pending.append(packed)
            chunk_packed += 1
        if pending:
            inserted += await write_snapshots(pending)
            pending.clear()

        source_rows_read += chunk_raw
        chunks += 1
        print(
            f"[astro-s71] snapshots chunk={chunks} period={current}..{chunk_end} "
            f"raw={chunk_raw} packed={chunk_packed} total={inserted} "
            f"skipped={skipped_incomplete}",
            flush=True,
        )
        current = chunk_end

    return {
        "status": "ok",
        "raw_table": raw_table,
        "snapshot_table": target_table,
        "from": start.isoformat(),
        "to": end_limit.isoformat(),
        "snapshot_rows_processed": inserted,
        "source_rows_read": source_rows_read,
        "incomplete_hours_skipped": skipped_incomplete,
        "chunks": chunks,
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
        **semantic_stats,
    }


# -----------------------------------------------------------------------------
# Dataset runtime and decoded state
# -----------------------------------------------------------------------------
class _State:
    __slots__ = (
        "blob", "values", "lon", "lat", "distance", "speed", "speed_lat",
        "speed_distance", "valid", "sign", "element", "modality", "polarity",
        "retro", "aspects", "phase_angle", "phase_name", "illumination",
    )

    def __init__(self, blob: bytes):
        arr = np.frombuffer(blob, dtype="<f4")
        expected = len(BODY_ORDER) * SNAPSHOT_FIELDS_PER_BODY
        if arr.size != expected:
            raise ValueError(f"Invalid body_blob length: {arr.size} floats, expected {expected}")
        arr = arr.reshape((len(BODY_ORDER), SNAPSHOT_FIELDS_PER_BODY))
        self.blob = blob
        self.values = arr
        self.lon = np.mod(arr[:, 0].astype(np.float64), 360.0)
        self.lat = arr[:, 1].astype(np.float64)
        self.distance = arr[:, 2].astype(np.float64)
        self.speed = arr[:, 3].astype(np.float64)
        self.speed_lat = arr[:, 4].astype(np.float64)
        self.speed_distance = arr[:, 5].astype(np.float64)
        self.valid = np.isfinite(self.lon)
        self.sign = np.where(self.valid, np.floor(self.lon / 30.0).astype(np.int16), -1)
        safe_sign = np.clip(self.sign, 0, 11)
        self.element = np.where(self.valid, SIGN_ELEMENT[safe_sign], -1)
        self.modality = np.where(self.valid, SIGN_MODALITY[safe_sign], -1)
        self.polarity = np.where(self.valid, SIGN_POLARITY[safe_sign], 0)
        self.retro = np.isfinite(self.speed) & (self.speed < 0.0)
        self.aspects = _compute_aspects(self)
        sun = BODY_INDEX["Sun"]
        moon = BODY_INDEX["Moon"]
        self.phase_angle = _norm360(self.lon[moon] - self.lon[sun])
        self.phase_name = _phase_name(self.phase_angle)
        self.illumination = (1.0 - math.cos(math.radians(self.phase_angle))) / 2.0


@lru_cache(maxsize=8192)
def _decode_state(blob: bytes) -> _State:
    return _State(blob)


def _compute_aspects(state: _State) -> tuple[tuple, ...]:
    rows = []
    for ia, ib in itertools.combinations(CORE_INDEX, 2):
        if not state.valid[ia] or not state.valid[ib]:
            continue
        a = BODY_ORDER[ia]
        b = BODY_ORDER[ib]
        directed = _norm360(state.lon[ia] - state.lon[ib])
        angle_abs = min(directed, 360.0 - directed)
        name, exact, orb, max_orb, polarity = _nearest_aspect(angle_abs)
        strength = max(0.0, 1.0 - orb / max_orb) if max_orb > 0 else 0.0
        rel_speed = 0.0
        if math.isfinite(state.speed[ia]) and math.isfinite(state.speed[ib]):
            rel_speed = float(state.speed[ia] - state.speed[ib])
        rows.append((a, b, name, exact, orb, max_orb, polarity, strength, directed, rel_speed))
    return tuple(rows)


_RUNTIME_TOKEN = None
_RUNTIME_ROWS: Sequence[Mapping[str, object]] = ()
_RUNTIME_DATES: Sequence[datetime] = ()
_BASE_CACHE: "OrderedDict[tuple[int, int, int], dict[str, float]]" = OrderedDict()
_BASE_CACHE_MAX = 6000
_CTX_CACHE_TOKEN = None
_CTX_BY_FEATURE: dict[str, int] = {}


def _prepare_runtime(dataset, dataset_index) -> tuple[Sequence[Mapping[str, object]], Sequence[datetime], int]:
    global _RUNTIME_TOKEN, _RUNTIME_ROWS, _RUNTIME_DATES
    rows = ((dataset_index or {}).get("full_dataset") if dataset_index else None) or dataset or []
    dates = ((dataset_index or {}).get("dates") if dataset_index else None)
    if dates is None or len(dates) != len(rows):
        dates = [row.get("date") or row.get("date_dt") for row in rows]
    token = id(rows)
    if token != _RUNTIME_TOKEN:
        _RUNTIME_TOKEN = token
        _RUNTIME_ROWS = rows
        _RUNTIME_DATES = dates
        _BASE_CACHE.clear()
    return _RUNTIME_ROWS, _RUNTIME_DATES, token


def _ctx_map(dataset_index) -> dict[str, int]:
    global _CTX_CACHE_TOKEN, _CTX_BY_FEATURE
    ctx = (dataset_index or {}).get("ctx_index") or {}
    token = (id(ctx), len(ctx))
    if token != _CTX_CACHE_TOKEN:
        rev = {}
        for info in ctx.values():
            key = str(info.get("feature_key") or "")
            cid = info.get("id")
            if key and cid is not None:
                rev[key] = int(cid)
        _CTX_BY_FEATURE = rev
        _CTX_CACHE_TOKEN = token
    return _CTX_BY_FEATURE


def _find_idx(dates: Sequence[datetime], target: datetime) -> int:
    idx = bisect.bisect_right(dates, target) - 1
    if idx < 0:
        return -1
    dt = dates[idx]
    if not isinstance(dt, datetime):
        return -1
    # Missing source hours should not make a stale astronomical state persist.
    if target - dt > timedelta(hours=2):
        return -1
    return idx


def _idx_before(dates: Sequence[datetime], idx: int, hours: int) -> int:
    if idx < 0:
        return -1
    target = dates[idx] - timedelta(hours=max(1, int(hours)))
    return bisect.bisect_right(dates, target) - 1


def _row_state(rows: Sequence[Mapping[str, object]], idx: int) -> Optional[_State]:
    if idx < 0 or idx >= len(rows):
        return None
    blob = rows[idx].get("body_blob")
    if isinstance(blob, memoryview):
        blob = blob.tobytes()
    if not isinstance(blob, (bytes, bytearray)):
        return None
    try:
        return _decode_state(bytes(blob))
    except Exception:
        return None


# -----------------------------------------------------------------------------
# Feature families (type 0..11)
# -----------------------------------------------------------------------------
def _type0_absolute(state: _State) -> dict[str, float]:
    out: dict[str, float] = {}
    for i, body in enumerate(BODY_ORDER):
        if not state.valid[i]:
            continue
        sign = int(state.sign[i])
        degree = float(state.lon[i] % 30.0)
        out[f"T0.sign.{body}.{SIGNS[sign]}"] = 0.65
        out[f"T0.decan.{body}.{int(degree // 10) + 1}"] = 0.50
        if math.isfinite(state.speed[i]):
            out[f"T0.motion.{body}.{'retro' if state.retro[i] else 'direct'}"] = 0.55
    out[f"T0.phase.{state.phase_name}"] = 0.80
    for a, b, asp, _exact, orb, max_orb, polarity, strength, _ang, _rel in state.aspects:
        if orb <= max_orb * 1.25:
            out[f"T0.aspect.{_pair_name(a, b)}.{asp}"] = _clip(polarity * max(0.15, strength))
    return out


def _type1_harmonics(state: _State) -> dict[str, float]:
    out = {}
    for i, body in enumerate(BODY_ORDER):
        if not state.valid[i]:
            continue
        lon = float(state.lon[i])
        for harmonic in HARMONICS:
            angle = math.radians((lon * harmonic) % 360.0)
            out[f"T1.h{harmonic}.{body}.sin"] = math.sin(angle)
            out[f"T1.h{harmonic}.{body}.cos"] = math.cos(angle)
    return out


def _entropy_concentration(counts: np.ndarray) -> float:
    total = float(np.sum(counts))
    if total <= 0:
        return 0.0
    p = counts[counts > 0] / total
    entropy = -float(np.sum(p * np.log(p)))
    max_entropy = math.log(len(counts)) if len(counts) > 1 else 1.0
    return _clip(1.0 - entropy / max_entropy, 0.0, 1.0)


def _type2_composition(state: _State) -> dict[str, float]:
    out = {}
    idxs = [BODY_INDEX[x] for x in PHYSICAL_BODIES if state.valid[BODY_INDEX[x]]]
    if not idxs:
        return out
    n = float(len(idxs))
    element_counts = np.bincount(state.element[idxs], minlength=4).astype(float)
    modality_counts = np.bincount(state.modality[idxs], minlength=3).astype(float)
    sign_counts = np.bincount(state.sign[idxs], minlength=12).astype(float)
    positive = float(np.sum(state.polarity[idxs] > 0))
    negative = n - positive
    for i, name in enumerate(ELEMENTS):
        out[f"T2.element.{name}"] = element_counts[i] / n
    for i, name in enumerate(MODALITIES):
        out[f"T2.modality.{name}"] = modality_counts[i] / n
    out["T2.polarity.positive"] = positive / n
    out["T2.polarity.negative"] = negative / n
    for i, sign in enumerate(SIGNS):
        out[f"T2.sign_occupancy.{sign}"] = sign_counts[i] / n
    out[f"T2.element_dominant.{ELEMENTS[int(np.argmax(element_counts))]}"] = 1.0
    out[f"T2.modality_dominant.{MODALITIES[int(np.argmax(modality_counts))]}"] = 1.0
    out[f"T2.polarity_dominant.{'positive' if positive >= negative else 'negative'}"] = 1.0
    out["T2.contrast.fire_minus_water"] = (element_counts[0] - element_counts[3]) / n
    out["T2.contrast.earth_minus_air"] = (element_counts[1] - element_counts[2]) / n
    out["T2.contrast.cardinal_minus_fixed"] = (modality_counts[0] - modality_counts[1]) / n
    out["T2.contrast.mutable_minus_fixed"] = (modality_counts[2] - modality_counts[1]) / n
    out["T2.concentration.element"] = _entropy_concentration(element_counts)
    out["T2.concentration.modality"] = _entropy_concentration(modality_counts)
    out["T2.concentration.sign"] = _entropy_concentration(sign_counts)
    retro_idxs = [i for i in idxs if state.retro[i]]
    for e, name in enumerate(ELEMENTS):
        denom = max(1.0, element_counts[e])
        out[f"T2.retro_by_element.{name}"] = sum(state.element[i] == e for i in retro_idxs) / denom
    return out


def _speed_reference(body: str) -> float:
    return max(1e-5, NORMAL_SPEED.get(body, 0.05))


def _type3_speed(state: _State) -> dict[str, float]:
    out = {}
    retro_count = 0
    station_count = 0
    speed_bodies = 0
    for i, body in enumerate(BODY_ORDER):
        speed = float(state.speed[i])
        if not math.isfinite(speed):
            continue
        speed_bodies += 1
        ref = _speed_reference(body)
        ratio = speed / ref
        abs_ratio = abs(ratio)
        station_score = max(0.0, 1.0 - abs_ratio / 0.15)
        out[f"T3.speed.{body}.signed"] = _clip(ratio / 2.0)
        out[f"T3.speed.{body}.absolute"] = _clip(abs_ratio / 2.0, 0.0, 1.0)
        out[f"T3.speed.{body}.station_score"] = station_score
        out[f"T3.speed.{body}.retro"] = -1.0 if speed < 0 else 1.0
        if speed < 0:
            klass = "retro"
            retro_count += 1
        elif abs_ratio < 0.08:
            klass = "station"
            station_count += 1
        elif abs_ratio < 0.55:
            klass = "slow"
        elif abs_ratio <= 1.55:
            klass = "normal"
        else:
            klass = "fast"
        out[f"T3.regime.{body}.{klass}"] = max(0.25, min(1.0, abs_ratio if klass != "station" else station_score))
    denom = max(1, speed_bodies)
    out["T3.total.retro_count"] = retro_count / denom
    out["T3.total.station_count"] = station_count / denom
    return out


def _type4_network(state: _State) -> dict[str, float]:
    out = {}
    centrality = defaultdict(float)
    active_count = 0
    harmony = 0.0
    tension = 0.0
    aspect_counts = defaultdict(float)
    for a, b, asp, _exact, orb, max_orb, polarity, strength, _ang, _rel in state.aspects:
        if orb > max_orb * 1.50:
            continue
        soft_strength = max(0.0, 1.0 - orb / (max_orb * 1.50))
        value = polarity * soft_strength
        out[f"T4.aspect.{_pair_name(a, b)}.{asp}"] = value
        centrality[a] += soft_strength
        centrality[b] += soft_strength
        aspect_counts[asp] += soft_strength
        active_count += 1
        if polarity >= 0:
            harmony += soft_strength * max(0.2, polarity)
        else:
            tension += soft_strength * abs(polarity)
    max_degree = max(1, len(CORE_BODIES) - 1)
    for body in CORE_BODIES:
        out[f"T4.centrality.{body}"] = _clip(centrality[body] / max_degree, 0.0, 1.0)
    pair_total = max(1, len(CORE_BODIES) * (len(CORE_BODIES) - 1) // 2)
    out["T4.metric.density"] = active_count / pair_total
    out["T4.metric.harmony"] = _clip(harmony / max(1.0, active_count), 0.0, 1.0)
    out["T4.metric.tension"] = _clip(tension / max(1.0, active_count), 0.0, 1.0)
    out["T4.metric.balance"] = _clip((harmony - tension) / max(1.0, harmony + tension))
    if aspect_counts:
        out[f"T4.dominant.{max(aspect_counts, key=aspect_counts.get)}"] = 1.0
    return out


def _aspects_by_pair(state: _State) -> dict[tuple[str, str], tuple]:
    return {(row[0], row[1]): row for row in state.aspects}


def _type5_dynamics(state: _State, previous: Optional[_State]) -> dict[str, float]:
    if previous is None:
        return {}
    out = {}
    prev_map = _aspects_by_pair(previous)
    counts = defaultdict(int)
    for row in state.aspects:
        a, b, asp, _exact, orb, max_orb, _polarity, strength, _ang, _rel = row
        old = prev_map.get((a, b))
        if old is None:
            continue
        old_asp, old_orb, old_max = old[2], old[4], old[5]
        current_near = orb <= max_orb * 1.5
        old_near = old_orb <= old_max * 1.5
        status = None
        magnitude = 0.0
        if current_near and not old_near:
            status = "forming"
            magnitude = max(0.2, strength)
        elif old_near and not current_near:
            status = "breaking"
            magnitude = -max(0.2, 1.0 - old_orb / (old_max * 1.5))
        elif current_near and old_near and asp == old_asp:
            delta = old_orb - orb
            if orb <= 0.50:
                status = "exact"
                magnitude = 1.0
            elif delta > 1e-4:
                status = "applying"
                magnitude = _clip(delta / max(0.05, max_orb * 0.15), 0.05, 1.0)
            elif delta < -1e-4:
                status = "separating"
                magnitude = -_clip(abs(delta) / max(0.05, max_orb * 0.15), 0.05, 1.0)
        if status:
            out[f"T5.aspect.{_pair_name(a, b)}.{asp}.{status}"] = magnitude
            counts[status] += 1
    denom = max(1, sum(counts.values()))
    for status in ("applying", "separating", "forming", "breaking", "exact"):
        out[f"T5.metric.{status}"] = counts[status] / denom
    return out


def _transition_features(current: _State, previous: Optional[_State], prefix: str = "T6") -> dict[str, float]:
    if previous is None:
        return {}
    out = {}
    for i, body in enumerate(BODY_ORDER):
        if not current.valid[i] or not previous.valid[i]:
            continue
        if current.sign[i] != previous.sign[i]:
            out[f"{prefix}.ingress.{body}.{SIGNS[int(current.sign[i])]}"] = 1.0
        cs = float(current.speed[i])
        ps = float(previous.speed[i])
        if math.isfinite(cs) and math.isfinite(ps):
            if (cs < 0) != (ps < 0):
                out[f"{prefix}.retro_turn.{body}.{'retro' if cs < 0 else 'direct'}"] = 1.0
            ref = _speed_reference(body)
            cstation = abs(cs / ref) < 0.08
            pstation = abs(ps / ref) < 0.08
            if cstation != pstation:
                out[f"{prefix}.station.{body}.{'enter' if cstation else 'exit'}"] = 1.0
    return out


def _type6_events(rows, dates, idx: int, step_hours: int) -> dict[str, float]:
    current = _row_state(rows, idx)
    prev_idx = _idx_before(dates, idx, step_hours)
    previous = _row_state(rows, prev_idx)
    if current is None:
        return {}
    out = _transition_features(current, previous)

    # Recency windows are measured in market bars, not raw hours.
    windows = ((1, "r1"), (6, "r6"), (24, "r24"))
    for bars, label in windows:
        start_time = dates[idx] - timedelta(hours=step_hours * bars)
        start_idx = max(0, bisect.bisect_left(dates, start_time))
        latest: dict[str, tuple[datetime, float]] = {}
        for j in range(max(start_idx + 1, 1), idx + 1):
            st = _row_state(rows, j)
            pst = _row_state(rows, j - 1)
            if st is None or pst is None:
                continue
            trans = _transition_features(st, pst)
            for key, value in trans.items():
                latest[key] = (dates[j], value)
        for key, (event_dt, value) in latest.items():
            age = max(0.0, (dates[idx] - event_dt).total_seconds() / 3600.0)
            decay = math.exp(-age / max(1.0, step_hours * bars))
            recent_key = key.replace("T6.", "T6.recent.", 1) + f".{label}"
            out[recent_key] = value * decay
    return out


def _type7_lunar(state: _State) -> dict[str, float]:
    out = {}
    moon_i = BODY_INDEX["Moon"]
    out[f"T7.phase.{state.phase_name}"] = 1.0
    lunar_day = int(math.floor(state.phase_angle / 360.0 * 29.53058867)) + 1
    lunar_day = max(1, min(30, lunar_day))
    out[f"T7.lunar_day.{lunar_day}"] = 1.0
    out[f"T7.moon_sign.{SIGNS[int(state.sign[moon_i])]}" ] = 1.0
    out[f"T7.illumination_bin.{min(9, int(state.illumination * 10))}"] = 1.0
    waxing = state.phase_angle < 180.0
    out[f"T7.direction.{'waxing' if waxing else 'waning'}"] = 1.0 if waxing else -1.0
    out["T7.metric.illumination"] = state.illumination * 2.0 - 1.0
    if math.isfinite(state.distance[moon_i]):
        out["T7.metric.distance"] = _clip((float(state.distance[moon_i]) - 0.00257) / 0.00020)
    if math.isfinite(state.speed[moon_i]):
        out["T7.metric.speed"] = _clip((float(state.speed[moon_i]) - 13.18) / 1.5)
    if math.isfinite(state.lat[moon_i]):
        out["T7.metric.latitude"] = _clip(float(state.lat[moon_i]) / 5.5)
    for a, b, asp, _exact, orb, max_orb, polarity, strength, _ang, _rel in state.aspects:
        if "Moon" not in (a, b) or orb > max_orb * 1.5:
            continue
        other = b if a == "Moon" else a
        out[f"T7.moon_aspect.{other}.{asp}"] = polarity * max(0.05, strength)
    return out


def _type8_synodic(state: _State) -> dict[str, float]:
    out = {}
    for a, b, _asp, _exact, _orb, _max_orb, _polarity, _strength, directed, rel_speed in state.aspects:
        pair = _pair_name(a, b)
        sector = int(directed // 30.0) % 12
        quarter = int(directed // 90.0) % 4
        out[f"T8.sector.{pair}.{sector}"] = 1.0
        out[f"T8.quarter.{pair}.{quarter}"] = 1.0
        angle = math.radians(directed)
        out[f"T8.wave.{pair}.sin"] = math.sin(angle)
        out[f"T8.wave.{pair}.cos"] = math.cos(angle)
        out[f"T8.direction.{pair}.{'increasing' if rel_speed >= 0 else 'decreasing'}"] = _clip(rel_speed / 2.0) or (1.0 if rel_speed >= 0 else -1.0)
    return out


def _angle_for_pair(state: _State, a: str, b: str) -> float:
    ia, ib = BODY_INDEX[a], BODY_INDEX[b]
    return _min_angle(float(state.lon[ia]), float(state.lon[ib]))


def _type9_patterns(state: _State) -> dict[str, float]:
    out = {}
    core_valid = [b for b in CORE_BODIES if state.valid[BODY_INDEX[b]]]
    sign_groups = defaultdict(list)
    element_groups = defaultdict(list)
    for body in core_valid:
        i = BODY_INDEX[body]
        sign_groups[int(state.sign[i])].append(body)
        element_groups[int(state.element[i])].append(body)
    for sign, bodies in sign_groups.items():
        if len(bodies) >= 3:
            out[f"T9.stellium_sign.{SIGNS[sign]}"] = _clip((len(bodies) - 2) / 3.0, 0.33, 1.0)
    for element, bodies in element_groups.items():
        if len(bodies) >= 4:
            out[f"T9.stellium_element.{ELEMENTS[element]}"] = _clip((len(bodies) - 3) / 4.0, 0.25, 1.0)

    opposition_count = 0
    conjunction_count = 0
    for row in state.aspects:
        if row[4] <= row[5]:
            if row[2] == "opposition":
                opposition_count += 1
            elif row[2] == "conjunction":
                conjunction_count += 1
    out["T9.cluster.opposition"] = _clip(opposition_count / 5.0, 0.0, 1.0)
    out["T9.cluster.conjunction"] = _clip(conjunction_count / 5.0, 0.0, 1.0)

    grand_trines = 0
    t_squares = 0
    yods = 0
    for a, b, c in itertools.combinations(core_valid, 3):
        ab = _angle_for_pair(state, a, b)
        ac = _angle_for_pair(state, a, c)
        bc = _angle_for_pair(state, b, c)
        trine_orbs = [abs(ab - 120.0), abs(ac - 120.0), abs(bc - 120.0)]
        if max(trine_orbs) <= 8.0:
            strength = 1.0 - max(trine_orbs) / 8.0
            out[f"T9.grand_trine.{a}__{b}__{c}"] = strength
            elements = [int(state.element[BODY_INDEX[x]]) for x in (a, b, c)]
            if len(set(elements)) == 1:
                out[f"T9.grand_trine_element.{ELEMENTS[elements[0]]}"] = max(
                    out.get(f"T9.grand_trine_element.{ELEMENTS[elements[0]]}", 0.0), strength
                )
            grand_trines += 1

        angles = {(a, b): ab, (a, c): ac, (b, c): bc}
        for apex, x, y in ((a, b, c), (b, a, c), (c, a, b)):
            xy = angles.get((x, y), angles.get((y, x)))
            ax = angles.get((apex, x), angles.get((x, apex)))
            ay = angles.get((apex, y), angles.get((y, apex)))
            if abs(xy - 180.0) <= 8.0 and abs(ax - 90.0) <= 7.0 and abs(ay - 90.0) <= 7.0:
                strength = 1.0 - max(abs(xy - 180.0) / 8.0, abs(ax - 90.0) / 7.0, abs(ay - 90.0) / 7.0)
                out[f"T9.t_square_apex.{apex}"] = max(out.get(f"T9.t_square_apex.{apex}", 0.0), strength)
                t_squares += 1
            if abs(xy - 60.0) <= 6.0 and abs(ax - 150.0) <= 5.0 and abs(ay - 150.0) <= 5.0:
                strength = 1.0 - max(abs(xy - 60.0) / 6.0, abs(ax - 150.0) / 5.0, abs(ay - 150.0) / 5.0)
                out[f"T9.yod_apex.{apex}"] = max(out.get(f"T9.yod_apex.{apex}", 0.0), strength)
                yods += 1
    out["T9.metric.grand_trines"] = _clip(grand_trines / 3.0, 0.0, 1.0)
    out["T9.metric.t_squares"] = _clip(t_squares / 3.0, 0.0, 1.0)
    out["T9.metric.yods"] = _clip(yods / 3.0, 0.0, 1.0)
    return out


def _aspect_density(state: _State) -> tuple[float, float, float]:
    active = harmony = tension = 0.0
    for row in state.aspects:
        orb, max_orb, polarity = row[4], row[5], row[6]
        if orb > max_orb:
            continue
        strength = max(0.0, 1.0 - orb / max_orb)
        active += 1.0
        if polarity >= 0:
            harmony += strength
        else:
            tension += strength
    pair_total = max(1.0, len(state.aspects))
    return active / pair_total, harmony / pair_total, tension / pair_total


def _type10_momentum(rows, dates, idx: int, step_hours: int) -> dict[str, float]:
    current = _row_state(rows, idx)
    if current is None:
        return {}
    out = {}
    for bars, label in ((1, "short"), (6, "medium"), (24, "long")):
        pidx = _idx_before(dates, idx, step_hours * bars)
        previous = _row_state(rows, pidx)
        if previous is None:
            continue
        elapsed_days = max(1e-6, (dates[idx] - dates[pidx]).total_seconds() / 86400.0)
        for i, body in enumerate(PHYSICAL_BODIES):
            bi = BODY_INDEX[body]
            if not current.valid[bi] or not previous.valid[bi]:
                continue
            lon_delta = _circular_delta(float(current.lon[bi]), float(previous.lon[bi]))
            expected = _speed_reference(body) * elapsed_days
            out[f"T10.lon.{body}.{label}"] = _clip(lon_delta / max(0.05, expected * 2.0))
            cs, ps = float(current.speed[bi]), float(previous.speed[bi])
            if math.isfinite(cs) and math.isfinite(ps):
                out[f"T10.speed.{body}.{label}"] = _clip((cs - ps) / _speed_reference(body))
        cur_density, cur_harmony, cur_tension = _aspect_density(current)
        old_density, old_harmony, old_tension = _aspect_density(previous)
        out[f"T10.aggregate.aspect_density.{label}"] = _clip((cur_density - old_density) * 8.0)
        out[f"T10.aggregate.harmony.{label}"] = _clip((cur_harmony - old_harmony) * 8.0)
        out[f"T10.aggregate.tension.{label}"] = _clip((cur_tension - old_tension) * 8.0)
        cur_retro = float(np.sum(current.retro)) / max(1, len(PHYSICAL_BODIES))
        old_retro = float(np.sum(previous.retro)) / max(1, len(PHYSICAL_BODIES))
        out[f"T10.aggregate.retro.{label}"] = _clip((cur_retro - old_retro) * 4.0)
    return out


def _projected_state(state: _State, horizon_hours: int) -> tuple[np.ndarray, np.ndarray]:
    lon = state.lon.copy()
    valid_speed = np.isfinite(state.speed)
    lon[valid_speed] = np.mod(lon[valid_speed] + state.speed[valid_speed] * horizon_hours / 24.0, 360.0)
    signs = np.floor(lon / 30.0).astype(np.int16)
    return lon, signs


def _type11_projection(state: _State) -> dict[str, float]:
    out = {}
    for horizon in PROJECTION_HOURS:
        lon, signs = _projected_state(state, horizon)
        for body in PHYSICAL_BODIES:
            i = BODY_INDEX[body]
            if not state.valid[i] or not math.isfinite(state.speed[i]):
                continue
            if signs[i] != state.sign[i]:
                out[f"T11.ingress.{body}.{SIGNS[int(signs[i])]}.h{horizon}"] = 1.0
        sun_i, moon_i = BODY_INDEX["Sun"], BODY_INDEX["Moon"]
        phase = _norm360(lon[moon_i] - lon[sun_i])
        out[f"T11.moon_phase.{_phase_name(phase)}.h{horizon}"] = 1.0
        for a, b in itertools.combinations(CORE_BODIES, 2):
            ia, ib = BODY_INDEX[a], BODY_INDEX[b]
            if not state.valid[ia] or not state.valid[ib]:
                continue
            directed = _norm360(lon[ia] - lon[ib])
            angle_abs = min(directed, 360.0 - directed)
            asp, _exact, orb, max_orb, polarity = _nearest_aspect(angle_abs)
            if orb <= max_orb * 1.25:
                strength = max(0.0, 1.0 - orb / (max_orb * 1.25))
                out[f"T11.aspect.{_pair_name(a, b)}.{asp}.h{horizon}"] = polarity * max(0.05, strength)
    return out


def _base_features(type_id: int, rows, dates, idx: int, step_hours: int, token: int) -> dict[str, float]:
    cache_key = (int(type_id), int(idx), int(step_hours))
    cached = _BASE_CACHE.get(cache_key)
    if cached is not None:
        _BASE_CACHE.move_to_end(cache_key)
        return cached

    state = _row_state(rows, idx)
    if state is None:
        result = {}
    elif type_id == 0:
        result = _type0_absolute(state)
    elif type_id == 1:
        result = _type1_harmonics(state)
    elif type_id == 2:
        result = _type2_composition(state)
    elif type_id == 3:
        result = _type3_speed(state)
    elif type_id == 4:
        result = _type4_network(state)
    elif type_id == 5:
        pidx = _idx_before(dates, idx, step_hours)
        result = _type5_dynamics(state, _row_state(rows, pidx))
    elif type_id == 6:
        result = _type6_events(rows, dates, idx, step_hours)
    elif type_id == 7:
        result = _type7_lunar(state)
    elif type_id == 8:
        result = _type8_synodic(state)
    elif type_id == 9:
        result = _type9_patterns(state)
    elif type_id == 10:
        result = _type10_momentum(rows, dates, idx, step_hours)
    elif type_id == 11:
        result = _type11_projection(state)
    else:
        result = {}

    result = {k: _clip(v) for k, v in result.items() if math.isfinite(float(v)) and abs(float(v)) > 1e-9}
    _BASE_CACHE[cache_key] = result
    _BASE_CACHE.move_to_end(cache_key)
    while len(_BASE_CACHE) > _BASE_CACHE_MAX:
        _BASE_CACHE.popitem(last=False)
    return result


# -----------------------------------------------------------------------------
# var 0..11 transformations
# -----------------------------------------------------------------------------
def _diff_maps(current: Mapping[str, float], previous: Mapping[str, float]) -> dict[str, float]:
    keys = set(current) | set(previous)
    return {k: _clip(current.get(k, 0.0) - previous.get(k, 0.0)) for k in keys}


def _transform_features(type_id: int, var_id: int, rows, dates, idx: int, step_hours: int, token: int) -> dict[str, float]:
    base = _base_features(type_id, rows, dates, idx, step_hours, token)
    if not base:
        return {}
    if var_id == 0:
        return {k: 1.0 for k in base}
    if var_id == 1:
        return {k: (1.0 if v >= 0 else -1.0) for k, v in base.items()}
    if var_id == 2:
        return dict(base)
    if var_id == 3:
        return {k: math.copysign(abs(v) ** 2, v) for k, v in base.items()}
    if var_id == 4:
        ranked = sorted(base.items(), key=lambda kv: (-abs(kv[1]), kv[0]))
        keep = max(1, min(32, int(math.ceil(len(ranked) * 0.25))))
        return {k: v for k, v in ranked[:keep] if abs(v) >= 0.25}
    if var_id == 5:
        return {k: math.copysign(math.sqrt(abs(v)), v) for k, v in base.items()}
    if var_id in (6, 7, 8):
        bars = {6: 1, 7: 6, 8: 24}[var_id]
        pidx = _idx_before(dates, idx, step_hours * bars)
        previous = _base_features(type_id, rows, dates, pidx, step_hours, token) if pidx >= 0 else {}
        return _diff_maps(base, previous)
    if var_id == 9:
        p1 = _idx_before(dates, idx, step_hours)
        p2 = _idx_before(dates, p1, step_hours) if p1 >= 0 else -1
        f1 = _base_features(type_id, rows, dates, p1, step_hours, token) if p1 >= 0 else {}
        f2 = _base_features(type_id, rows, dates, p2, step_hours, token) if p2 >= 0 else {}
        keys = set(base) | set(f1) | set(f2)
        return {k: _clip(base.get(k, 0.0) - 2.0 * f1.get(k, 0.0) + f2.get(k, 0.0)) for k in keys}
    if var_id == 10:
        samples = []
        for bars in range(1, 31):
            pidx = _idx_before(dates, idx, step_hours * bars)
            if pidx < 0:
                break
            samples.append(_base_features(type_id, rows, dates, pidx, step_hours, token))
        if len(samples) < 5:
            return {}
        keys = set(base)
        keys.update(samples[0])
        out = {}
        for key in keys:
            vals = np.array([m.get(key, 0.0) for m in samples], dtype=np.float64)
            std = float(np.std(vals))
            if std < 1e-6:
                continue
            z = (base.get(key, 0.0) - float(np.mean(vals))) / std
            out[key] = _clip(z / 3.0)
        return out
    if var_id == 11:
        ranked = sorted(base.items(), key=lambda kv: (-abs(kv[1]), kv[0]))[:32]
        denom = sum(abs(v) for _, v in ranked) or 1.0
        return {k: _clip(v / denom * min(8.0, len(ranked))) for k, v in ranked}
    return {}


def _encode_result(features: Mapping[str, float], ctx_by_feature: Mapping[str, int]) -> dict[str, float]:
    ranked = sorted(
        ((key, float(value)) for key, value in features.items() if abs(float(value)) > 1e-6),
        key=lambda kv: (-abs(kv[1]), kv[0]),
    )[:MAX_OUTPUT_FEATURES]
    out = {}
    for key, value in ranked:
        ctx_id = ctx_by_feature.get(key)
        if ctx_id is None:
            continue
        out[f"{ctx_id}_0_0"] = round(_clip(value), 6)
    return out



# -----------------------------------------------------------------------------
# Causal online calibration against historical market candles
# -----------------------------------------------------------------------------
class _OnlineSlot:
    __slots__ = ("stats",)

    def __init__(self):
        # feature_key -> [occurrence_count, sum(x^2), sum(x*y)]
        self.stats: dict[str, list[float]] = {}


class _TypeTrajectory:
    __slots__ = (
        "rates_table", "type_id", "dataset_token", "processed", "last_date",
        "first_date", "slots",
    )

    def __init__(self, rates_table: str, type_id: int, dataset_token: int):
        self.rates_table = rates_table
        self.type_id = int(type_id)
        self.dataset_token = int(dataset_token)
        self.processed = 0
        self.last_date: Optional[datetime] = None
        self.first_date: Optional[datetime] = None
        self.slots = {var: _OnlineSlot() for var in VAR_DESCRIPTIONS}


_TRAJECTORIES: dict[tuple[str, int, int], _TypeTrajectory] = {}
_TYPE_BATCH_CACHE: "OrderedDict[tuple, dict[int, dict[datetime, dict[str, float]]]]" = OrderedDict()
_TYPE_BATCH_CACHE_MAX = 16
_PREDICTION_LRU: "OrderedDict[tuple, dict[str, float]]" = OrderedDict()
_PREDICTION_LRU_MAX = 10000


def _rates_identity(rates, dataset_index) -> str:
    table = str((dataset_index or {}).get("rates_table") or "")
    if table:
        return table
    return f"rates@{id(getattr(rates, '_rows', rates))}"


def _outcome(row: Mapping[str, object]) -> Optional[float]:
    try:
        op = float(row.get("open") or 0.0)
        cl = float(row.get("close") or 0.0)
        hi = float(row.get("max") or max(op, cl))
        lo = float(row.get("min") or min(op, cl))
    except (TypeError, ValueError):
        return None
    if not all(math.isfinite(v) for v in (op, cl, hi, lo)) or op == 0.0:
        return None
    scale = max(abs(hi - lo), abs(op) * 1e-6, 1e-12)
    return _clip((cl - op) / scale)


def _minimum_occurrences(type_id: int, var_id: int) -> int:
    if type_id in (5, 6, 9, 11):
        base = 4
    elif type_id in (0, 4, 7, 8):
        base = 12
    else:
        base = 20
    if var_id in (6, 7, 8, 9, 10):
        base = max(5, base // 2)
    return base


def _predict_slot(
    features: Mapping[str, float],
    slot: _OnlineSlot,
    type_id: int,
    var_id: int,
    ctx_by_feature: Mapping[str, int],
) -> dict[str, float]:
    if not features:
        return {}
    min_occ = _minimum_occurrences(type_id, var_id)
    shrink = float(max(8, min_occ * 2))
    active = max(1.0, math.sqrt(len(features)))
    calibrated: dict[str, float] = {}
    for key, x in features.items():
        stat = slot.stats.get(key)
        if stat is None:
            continue
        n, sx2, sxy = stat
        if n < min_occ or sx2 <= 1e-9:
            continue
        beta = sxy / (sx2 + 2.0)
        reliability = n / (n + shrink)
        contribution = _clip(float(x) * beta * reliability / active)
        if abs(contribution) > 1e-7:
            calibrated[key] = contribution
    return _encode_result(calibrated, ctx_by_feature)


def _update_slot(features: Mapping[str, float], y: float, slot: _OnlineSlot) -> None:
    for key, x in features.items():
        xv = float(x)
        if not math.isfinite(xv) or abs(xv) <= 1e-9:
            continue
        stat = slot.stats.get(key)
        if stat is None:
            slot.stats[key] = [1.0, xv * xv, xv * y]
        else:
            stat[0] += 1.0
            stat[1] += xv * xv
            stat[2] += xv * y


def _reset_trajectory_if_needed(
    trajectory: _TypeTrajectory,
    rates: Sequence[Mapping[str, object]],
) -> _TypeTrajectory:
    first_date = rates[0].get("date") if rates else None
    invalid = (
        trajectory.processed > len(rates)
        or (trajectory.first_date is not None and first_date != trajectory.first_date)
        or (
            trajectory.processed > 0
            and trajectory.last_date is not None
            and trajectory.processed <= len(rates)
            and rates[trajectory.processed - 1].get("date") != trajectory.last_date
        )
    )
    if invalid:
        trajectory = _TypeTrajectory(
            trajectory.rates_table, trajectory.type_id, trajectory.dataset_token
        )
    return trajectory


def _remember_prediction(key: tuple, value: dict[str, float]) -> None:
    _PREDICTION_LRU[key] = value
    _PREDICTION_LRU.move_to_end(key)
    while len(_PREDICTION_LRU) > _PREDICTION_LRU_MAX:
        _PREDICTION_LRU.popitem(last=False)


def _process_type_until(
    *,
    rates: Sequence[Mapping[str, object]],
    rows: Sequence[Mapping[str, object]],
    dates: Sequence[datetime],
    dataset_token: int,
    rates_table: str,
    type_id: int,
    target_dates: set[datetime],
    max_target: datetime,
    step_hours: int,
    ctx_by_feature: Mapping[str, int],
) -> dict[int, dict[datetime, dict[str, float]]]:
    key = (rates_table, int(type_id), int(dataset_token))
    trajectory = _TRAJECTORIES.get(key)
    if trajectory is None:
        trajectory = _TypeTrajectory(rates_table, type_id, dataset_token)
    trajectory = _reset_trajectory_if_needed(trajectory, rates)
    _TRAJECTORIES[key] = trajectory

    captured = {var: {} for var in VAR_DESCRIPTIONS}
    if trajectory.processed == 0 and rates:
        trajectory.first_date = rates[0].get("date")

    # If this exact batch was already processed, use the prediction LRU.  This
    # is the normal path for var=1..11 after var=0 fused all vars of the type.
    all_cached = True
    for var in VAR_DESCRIPTIONS:
        for dt in target_dates:
            pred_key = (rates_table, dataset_token, type_id, var, dt)
            value = _PREDICTION_LRU.get(pred_key)
            if value is None:
                all_cached = False
                break
            captured[var][dt] = value
        if not all_cached:
            break
    if all_cached:
        return captured

    while trajectory.processed < len(rates):
        row = rates[trajectory.processed]
        rate_date = row.get("date")
        if not isinstance(rate_date, datetime):
            trajectory.processed += 1
            continue
        if rate_date > max_target:
            break

        astro_idx = _find_idx(dates, rate_date)
        if astro_idx >= 0:
            y = _outcome(row)
            # One base family is shared by all 12 vars.  The per-var transform
            # may request prior bases, which are served by the bounded cache.
            for var_id in VAR_DESCRIPTIONS:
                features = _transform_features(
                    type_id, var_id, rows, dates, astro_idx,
                    step_hours, dataset_token,
                )
                slot = trajectory.slots[var_id]
                if rate_date in target_dates:
                    result = _predict_slot(
                        features, slot, type_id, var_id, ctx_by_feature
                    )
                    captured[var_id][rate_date] = result
                    _remember_prediction(
                        (rates_table, dataset_token, type_id, var_id, rate_date),
                        result,
                    )
                # Prediction is made first; only then is the current candle used
                # to update statistics.  This is the causal anti-leakage rule.
                if y is not None:
                    _update_slot(features, y, slot)

        trajectory.processed += 1
        trajectory.last_date = rate_date

    # Dates can be absent because a rate or astro snapshot is missing.  Cache an
    # explicit empty result so subsequent var calls do not force a recomputation.
    for var_id in VAR_DESCRIPTIONS:
        for dt in target_dates:
            if dt not in captured[var_id]:
                captured[var_id][dt] = {}
                _remember_prediction(
                    (rates_table, dataset_token, type_id, var_id, dt), {}
                )
    return captured


def _single_from_scratch(
    *,
    rates: Sequence[Mapping[str, object]],
    dataset,
    date: datetime,
    type_id: int,
    var_id: int,
    dataset_index,
) -> dict[str, float]:
    rows, dates, token = _prepare_runtime(dataset, dataset_index)
    ctx = _ctx_map(dataset_index)
    step_hours = 24 if bool((dataset_index or {}).get("is_daily")) else 1
    slot = _OnlineSlot()
    prediction: dict[str, float] = {}
    for row in rates:
        rate_date = row.get("date")
        if not isinstance(rate_date, datetime) or rate_date > date:
            break
        idx = _find_idx(dates, rate_date)
        if idx < 0:
            continue
        features = _transform_features(
            type_id, var_id, rows, dates, idx, step_hours, token
        )
        if rate_date == date:
            prediction = _predict_slot(features, slot, type_id, var_id, ctx)
            break
        y = _outcome(row)
        if y is not None:
            _update_slot(features, y, slot)
    return prediction


def model(
    rates: list[dict],
    dataset: list[dict],
    date: datetime,
    type: int,
    var: int,
    param: str = "",
    dataset_index: Optional[dict] = None,
) -> dict[str, float]:
    type_id, var_id = int(type), int(var)
    if type_id not in TYPE_DESCRIPTIONS or var_id not in VAR_DESCRIPTIONS:
        return {}
    rows, _dates, token = _prepare_runtime(dataset, dataset_index)
    table = _rates_identity(rates, dataset_index)
    cached = _PREDICTION_LRU.get((table, token, type_id, var_id, date))
    if cached is not None:
        return cached
    # Direct /values cache misses can arrive out of chronological order.  A
    # scratch causal pass is slower but prevents a trajectory trained on later
    # dates from leaking information into an older request.
    return _single_from_scratch(
        rates=rates, dataset=dataset, date=date,
        type_id=type_id, var_id=var_id, dataset_index=dataset_index,
    )


def batch_model(
    rates: list[dict],
    dataset: list[dict],
    dates: list[datetime],
    type: int,
    var: int,
    param: str = "",
    dataset_index: Optional[dict] = None,
) -> dict[datetime, dict[str, float]]:
    type_id, var_id = int(type), int(var)
    if not dates or type_id not in TYPE_DESCRIPTIONS or var_id not in VAR_DESCRIPTIONS:
        return {dt: {} for dt in dates}
    rows, astro_dates, token = _prepare_runtime(dataset, dataset_index)
    ctx = _ctx_map(dataset_index)
    table = _rates_identity(rates, dataset_index)
    step_hours = 24 if bool((dataset_index or {}).get("is_daily")) else 1
    batch_key = (table, token, type_id, tuple(dates))
    cached_batch = _TYPE_BATCH_CACHE.get(batch_key)
    if cached_batch is None:
        cached_batch = _process_type_until(
            rates=rates,
            rows=rows,
            dates=astro_dates,
            dataset_token=token,
            rates_table=table,
            type_id=type_id,
            target_dates=set(dates),
            max_target=max(dates),
            step_hours=step_hours,
            ctx_by_feature=ctx,
        )
        _TYPE_BATCH_CACHE[batch_key] = cached_batch
        _TYPE_BATCH_CACHE.move_to_end(batch_key)
        while len(_TYPE_BATCH_CACHE) > _TYPE_BATCH_CACHE_MAX:
            _TYPE_BATCH_CACHE.popitem(last=False)
    return {dt: cached_batch.get(var_id, {}).get(dt, {}) for dt in dates}


# -----------------------------------------------------------------------------
# Static semantic feature universe created by enrich_dataset()
# -----------------------------------------------------------------------------
def iter_feature_definitions() -> Iterator[tuple[str, str, str]]:
    seen: set[str] = set()

    def emit(key: str, family: str, description: str):
        if key not in seen:
            seen.add(key)
            return (key, family, description)
        return None

    # Type 0
    for body in BODY_ORDER:
        for sign in SIGNS:
            x = emit(f"T0.sign.{body}.{sign}", "T0_absolute", f"{body} in {sign}")
            if x: yield x
        for decan in (1, 2, 3):
            x = emit(f"T0.decan.{body}.{decan}", "T0_absolute", f"{body} decan {decan}")
            if x: yield x
        for motion in ("direct", "retro"):
            x = emit(f"T0.motion.{body}.{motion}", "T0_absolute", f"{body} {motion}")
            if x: yield x
    for phase in PHASE_NAMES:
        x = emit(f"T0.phase.{phase}", "T0_absolute", f"Moon phase {phase}")
        if x: yield x
    for a, b in itertools.combinations(CORE_BODIES, 2):
        for asp in ASPECT_NAMES:
            x = emit(f"T0.aspect.{_pair_name(a, b)}.{asp}", "T0_absolute", f"{a}-{b} {asp}")
            if x: yield x

    # Type 1
    for body in BODY_ORDER:
        for harmonic in HARMONICS:
            for component in ("sin", "cos"):
                key = f"T1.h{harmonic}.{body}.{component}"
                x = emit(key, "T1_harmonics", f"{body} harmonic {harmonic} {component}")
                if x: yield x

    # Type 2
    for name in ELEMENTS:
        x = emit(f"T2.element.{name}", "T2_composition", f"Element share: {name}")
        if x: yield x
        x = emit(f"T2.element_dominant.{name}", "T2_composition", f"Dominant element: {name}")
        if x: yield x
        x = emit(f"T2.retro_by_element.{name}", "T2_composition", f"Retrograde share in {name}")
        if x: yield x
    for name in MODALITIES:
        x = emit(f"T2.modality.{name}", "T2_composition", f"Modality share: {name}")
        if x: yield x
        x = emit(f"T2.modality_dominant.{name}", "T2_composition", f"Dominant modality: {name}")
        if x: yield x
    for name in ("positive", "negative"):
        x = emit(f"T2.polarity.{name}", "T2_composition", f"Polarity share: {name}")
        if x: yield x
        x = emit(f"T2.polarity_dominant.{name}", "T2_composition", f"Dominant polarity: {name}")
        if x: yield x
    for sign in SIGNS:
        x = emit(f"T2.sign_occupancy.{sign}", "T2_composition", f"Bodies occupying {sign}")
        if x: yield x
    for name in ("fire_minus_water", "earth_minus_air", "cardinal_minus_fixed", "mutable_minus_fixed"):
        x = emit(f"T2.contrast.{name}", "T2_composition", f"Composition contrast {name}")
        if x: yield x
    for name in ("element", "modality", "sign"):
        x = emit(f"T2.concentration.{name}", "T2_composition", f"{name} concentration")
        if x: yield x

    # Type 3
    for body in BODY_ORDER:
        for metric in ("signed", "absolute", "station_score", "retro"):
            x = emit(f"T3.speed.{body}.{metric}", "T3_speed", f"{body} speed {metric}")
            if x: yield x
        for regime in ("retro", "station", "slow", "normal", "fast"):
            x = emit(f"T3.regime.{body}.{regime}", "T3_speed", f"{body} regime {regime}")
            if x: yield x
    for metric in ("retro_count", "station_count"):
        x = emit(f"T3.total.{metric}", "T3_speed", metric)
        if x: yield x

    # Type 4
    for a, b in itertools.combinations(CORE_BODIES, 2):
        for asp in ASPECT_NAMES:
            x = emit(f"T4.aspect.{_pair_name(a, b)}.{asp}", "T4_network", f"Aspect network {a}-{b} {asp}")
            if x: yield x
    for body in CORE_BODIES:
        x = emit(f"T4.centrality.{body}", "T4_network", f"Aspect centrality {body}")
        if x: yield x
    for metric in ("density", "harmony", "tension", "balance"):
        x = emit(f"T4.metric.{metric}", "T4_network", f"Aspect network {metric}")
        if x: yield x
    for asp in ASPECT_NAMES:
        x = emit(f"T4.dominant.{asp}", "T4_network", f"Dominant aspect {asp}")
        if x: yield x

    # Type 5
    for a, b in itertools.combinations(CORE_BODIES, 2):
        for asp in ASPECT_NAMES:
            for status in ("applying", "separating", "forming", "breaking", "exact"):
                x = emit(f"T5.aspect.{_pair_name(a, b)}.{asp}.{status}", "T5_dynamics", f"{a}-{b} {asp} {status}")
                if x: yield x
    for status in ("applying", "separating", "forming", "breaking", "exact"):
        x = emit(f"T5.metric.{status}", "T5_dynamics", f"Aspect dynamics {status}")
        if x: yield x

    # Type 6
    for body in BODY_ORDER:
        for sign in SIGNS:
            base = f"T6.ingress.{body}.{sign}"
            x = emit(base, "T6_events", f"{body} ingress into {sign}")
            if x: yield x
            for recency in ("r1", "r6", "r24"):
                x = emit(base.replace("T6.", "T6.recent.", 1) + f".{recency}", "T6_events", f"Recent {body} ingress {sign} {recency}")
                if x: yield x
        for direction in ("direct", "retro"):
            base = f"T6.retro_turn.{body}.{direction}"
            x = emit(base, "T6_events", f"{body} turns {direction}")
            if x: yield x
            for recency in ("r1", "r6", "r24"):
                x = emit(base.replace("T6.", "T6.recent.", 1) + f".{recency}", "T6_events", f"Recent {body} turn {direction} {recency}")
                if x: yield x
        for direction in ("enter", "exit"):
            base = f"T6.station.{body}.{direction}"
            x = emit(base, "T6_events", f"{body} station {direction}")
            if x: yield x
            for recency in ("r1", "r6", "r24"):
                x = emit(base.replace("T6.", "T6.recent.", 1) + f".{recency}", "T6_events", f"Recent {body} station {direction} {recency}")
                if x: yield x

    # Type 7
    for phase in PHASE_NAMES:
        x = emit(f"T7.phase.{phase}", "T7_lunar", f"Lunar phase {phase}")
        if x: yield x
    for day in range(1, 31):
        x = emit(f"T7.lunar_day.{day}", "T7_lunar", f"Lunar day {day}")
        if x: yield x
    for sign in SIGNS:
        x = emit(f"T7.moon_sign.{sign}", "T7_lunar", f"Moon in {sign}")
        if x: yield x
    for b in range(10):
        x = emit(f"T7.illumination_bin.{b}", "T7_lunar", f"Lunar illumination bin {b}")
        if x: yield x
    for direction in ("waxing", "waning"):
        x = emit(f"T7.direction.{direction}", "T7_lunar", f"Moon {direction}")
        if x: yield x
    for metric in ("illumination", "distance", "speed", "latitude"):
        x = emit(f"T7.metric.{metric}", "T7_lunar", f"Moon {metric}")
        if x: yield x
    for other in [b for b in CORE_BODIES if b != "Moon"]:
        for asp in ASPECT_NAMES:
            x = emit(f"T7.moon_aspect.{other}.{asp}", "T7_lunar", f"Moon-{other} {asp}")
            if x: yield x

    # Type 8
    for a, b in itertools.combinations(CORE_BODIES, 2):
        pair = _pair_name(a, b)
        for sector in range(12):
            x = emit(f"T8.sector.{pair}.{sector}", "T8_synodic", f"{a}-{b} synodic sector {sector}")
            if x: yield x
        for quarter in range(4):
            x = emit(f"T8.quarter.{pair}.{quarter}", "T8_synodic", f"{a}-{b} synodic quarter {quarter}")
            if x: yield x
        for comp in ("sin", "cos"):
            x = emit(f"T8.wave.{pair}.{comp}", "T8_synodic", f"{a}-{b} synodic {comp}")
            if x: yield x
        for direction in ("increasing", "decreasing"):
            x = emit(f"T8.direction.{pair}.{direction}", "T8_synodic", f"{a}-{b} angle {direction}")
            if x: yield x

    # Type 9
    for sign in SIGNS:
        x = emit(f"T9.stellium_sign.{sign}", "T9_patterns", f"Stellium in {sign}")
        if x: yield x
    for element in ELEMENTS:
        x = emit(f"T9.stellium_element.{element}", "T9_patterns", f"Element cluster {element}")
        if x: yield x
        x = emit(f"T9.grand_trine_element.{element}", "T9_patterns", f"Grand trine in {element}")
        if x: yield x
    for triple in itertools.combinations(CORE_BODIES, 3):
        key = f"T9.grand_trine.{'__'.join(triple)}"
        x = emit(key, "T9_patterns", f"Grand trine {'-'.join(triple)}")
        if x: yield x
    for body in CORE_BODIES:
        x = emit(f"T9.t_square_apex.{body}", "T9_patterns", f"T-square apex {body}")
        if x: yield x
        x = emit(f"T9.yod_apex.{body}", "T9_patterns", f"Yod apex {body}")
        if x: yield x
    for name in ("opposition", "conjunction"):
        x = emit(f"T9.cluster.{name}", "T9_patterns", f"{name} cluster")
        if x: yield x
    for metric in ("grand_trines", "t_squares", "yods"):
        x = emit(f"T9.metric.{metric}", "T9_patterns", metric)
        if x: yield x

    # Type 10
    for body in PHYSICAL_BODIES:
        for label in ("short", "medium", "long"):
            for metric in ("lon", "speed"):
                x = emit(f"T10.{metric}.{body}.{label}", "T10_momentum", f"{body} {metric} momentum {label}")
                if x: yield x
    for metric in ("aspect_density", "harmony", "tension", "retro"):
        for label in ("short", "medium", "long"):
            x = emit(f"T10.aggregate.{metric}.{label}", "T10_momentum", f"Aggregate {metric} momentum {label}")
            if x: yield x

    # Type 11
    for horizon in PROJECTION_HOURS:
        for body in PHYSICAL_BODIES:
            for sign in SIGNS:
                x = emit(f"T11.ingress.{body}.{sign}.h{horizon}", "T11_projection", f"Projected {body} ingress {sign} +{horizon}h")
                if x: yield x
        for phase in PHASE_NAMES:
            x = emit(f"T11.moon_phase.{phase}.h{horizon}", "T11_projection", f"Projected Moon phase {phase} +{horizon}h")
            if x: yield x
        for a, b in itertools.combinations(CORE_BODIES, 2):
            for asp in ASPECT_NAMES:
                x = emit(f"T11.aspect.{_pair_name(a, b)}.{asp}.h{horizon}", "T11_projection", f"Projected {a}-{b} {asp} +{horizon}h")
                if x: yield x


def hypothesis_catalog() -> dict:
    return {
        "types": TYPE_DESCRIPTIONS,
        "vars": VAR_DESCRIPTIONS,
        "combinations": len(TYPE_DESCRIPTIONS) * len(VAR_DESCRIPTIONS),
        "feature_contexts": sum(1 for _ in iter_feature_definitions()),
    }
