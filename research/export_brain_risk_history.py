#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Brain Server -> Risk Manager historical signal exporter (READ ONLY)

Назначение:
    Выгружает исторические сигналы Brain Project из MySQL так, чтобы их потом
    можно было состыковать по времени с историей позиций/тиков RoboForex MT5.

Что выгружается:
    - brain_models
    - текущие конфиги brain_signal{model_id} для выбранной пары/таймфрейма
    - котировки brain_rates_{pair}[_day]
    - исторические финальные значения сигналов:
          brain_signal{model_id}_{signal_id}
      с fallback на:
          brain_signal{model_id}_history_{pair_postfix}[_day]
    - brain_precision{model_id}_{signal_id}
    - агрегированная временная шкала signal_aggregate_*.csv.gz
      (proxy strength: голосование + raw values + precision-weighted votes)
    - JSON summary
    - ZIP со всей выгрузкой

ВАЖНО:
    Скрипт выполняет только SELECT/SHOW/DESCRIBE.
    Он НЕ изменяет Brain Server и НЕ запускает пересчёты моделей.

Установка:
    python3 -m pip install mysql-connector-python python-dotenv

Пример на Brain Server:
    python3 export_brain_risk_history.py \
        --pair 1 \
        --timeframe hour \
        --from-date "2026-03-18 00:00:00" \
        --to-date "2026-08-12 23:59:59" \
        --output /tmp/brain_risk_history_export

Подключение к БД берётся:
    1) из CLI --host/--port/--user/--password/--database
    2) из DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME
    3) затем из MASTER_HOST/MASTER_PORT/MASTER_USER/MASTER_PASSWORD/MASTER_NAME

.env ищется автоматически:
    /brain/Brain-Services/.env
    /brain/Brain-Server/.env
    ./.env

Можно указать явно:
    --env-file /brain/Brain-Services/.env
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import os
import re
import sys
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    print("ERROR: mysql-connector-python is not installed.")
    print("Install: python3 -m pip install mysql-connector-python python-dotenv")
    raise

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


PAIR_MAP = {
    1: "eur_usd",
    3: "btc_usd",
    4: "eth_usd",
}

SAFE_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")
MODEL_SIGNAL_TABLE_RE = re.compile(r"^brain_signal(\d+)$")


def log(message: str) -> None:
    print(message, flush=True)


def parse_dt(value: str) -> datetime:
    value = value.strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(value, fmt)
            if fmt == "%Y-%m-%d":
                dt = dt.replace(hour=0, minute=0, second=0)
            return dt
        except ValueError:
            pass
    raise argparse.ArgumentTypeError(
        f"Invalid datetime {value!r}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"
    )


def dt_to_sql(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.hex()
    return value


def json_default(value: Any) -> Any:
    return serialize(value)


def safe_ident(name: str) -> str:
    if not SAFE_IDENT_RE.fullmatch(name):
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return f"`{name}`"


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(obj, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8-sig")
        return

    fields: List[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                fields.append(key)

    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: serialize(v) for k, v in row.items()})


class GzipCsvWriter:
    def __init__(self, path: Path, fieldnames: List[str]):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = gzip.open(path, "wt", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(
            self._fh,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        self._writer.writeheader()
        self.count = 0

    def writerow(self, row: Dict[str, Any]) -> None:
        self._writer.writerow({k: serialize(v) for k, v in row.items()})
        self.count += 1

    def close(self) -> None:
        self._fh.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class BrainDB:
    """
    Read-only helper. Public query methods allow SELECT / SHOW / DESCRIBE only.
    """

    ALLOWED = ("SELECT", "SHOW", "DESCRIBE", "DESC", "EXPLAIN", "WITH")

    def __init__(self, **cfg):
        self.cfg = cfg
        self.conn = mysql.connector.connect(**cfg)
        self.conn.autocommit = True

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass

    @staticmethod
    def _guard(sql: str) -> None:
        stripped = sql.lstrip().upper()
        if not stripped.startswith(BrainDB.ALLOWED):
            raise RuntimeError(
                "READ-ONLY GUARD: only SELECT/SHOW/DESCRIBE/EXPLAIN/WITH are allowed"
            )

    def query(self, sql: str, params: Optional[Tuple[Any, ...]] = None) -> List[Dict[str, Any]]:
        self._guard(sql)
        cur = self.conn.cursor(dictionary=True)
        try:
            cur.execute(sql, params or ())
            return list(cur.fetchall())
        finally:
            cur.close()

    def iter_query(
        self,
        sql: str,
        params: Optional[Tuple[Any, ...]] = None,
        arraysize: int = 5000,
    ) -> Iterator[Dict[str, Any]]:
        self._guard(sql)
        cur = self.conn.cursor(dictionary=True)
        cur.arraysize = arraysize
        try:
            cur.execute(sql, params or ())
            while True:
                rows = cur.fetchmany(arraysize)
                if not rows:
                    break
                for row in rows:
                    yield row
        finally:
            cur.close()

    def table_exists(self, table: str) -> bool:
        rows = self.query(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = DATABASE()
              AND table_name = %s
            LIMIT 1
            """,
            (table,),
        )
        return bool(rows)

    def columns(self, table: str) -> List[str]:
        if not self.table_exists(table):
            return []
        rows = self.query(f"SHOW COLUMNS FROM {safe_ident(table)}")
        return [str(r["Field"]) for r in rows]


def load_env_files(explicit: Optional[str]) -> List[str]:
    loaded: List[str] = []
    if load_dotenv is None:
        return loaded

    candidates: List[Path] = []
    if explicit:
        candidates.append(Path(explicit))
    candidates.extend(
        [
            Path("/brain/Brain-Services/.env"),
            Path("/brain/Brain-Server/.env"),
            Path.cwd() / ".env",
        ]
    )

    seen = set()
    for p in candidates:
        try:
            resolved = str(p.resolve())
        except Exception:
            resolved = str(p)
        if resolved in seen:
            continue
        seen.add(resolved)

        if p.is_file():
            load_dotenv(p, override=False)
            loaded.append(str(p))
    return loaded


def choose_db_config(args: argparse.Namespace) -> Dict[str, Any]:
    def env_first(primary: str, secondary: str, default=None):
        return os.getenv(primary) or os.getenv(secondary) or default

    # Brain risk history lives in the Brain/master DB. Prefer MASTER_*.
    # DB_* in Brain-Services commonly points to the writable "vlad" database,
    # which does not contain brain_rates_* / brain_signal* tables.
    host = args.host or env_first("MASTER_HOST", "DB_HOST", "127.0.0.1")
    port = args.port or env_first("MASTER_PORT", "DB_PORT", "3306")
    user = args.user or env_first("MASTER_USER", "DB_USER")
    password = (
        args.password
        if args.password is not None
        else env_first("MASTER_PASSWORD", "DB_PASSWORD")
    )
    database = args.database or env_first("MASTER_NAME", "DB_NAME", "brain")

    if not user:
        raise RuntimeError(
            "MySQL user is not set. Use --user or DB_USER/MASTER_USER."
        )
    if password is None:
        raise RuntimeError(
            "MySQL password is not set. Use --password or DB_PASSWORD/MASTER_PASSWORD."
        )

    return {
        "host": host,
        "port": int(port),
        "user": user,
        "password": password,
        "database": database,
        "charset": "utf8mb4",
        "use_unicode": True,
        "connection_timeout": int(args.connect_timeout),
    }


def discover_model_ids(db: BrainDB) -> List[int]:
    ids: set[int] = set()

    if db.table_exists("brain_models"):
        cols = db.columns("brain_models")
        if "id" in cols:
            for row in db.query("SELECT id FROM brain_models ORDER BY id"):
                try:
                    ids.add(int(row["id"]))
                except Exception:
                    pass

    tables = db.query(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
          AND table_name REGEXP '^brain_signal[0-9]+$'
        ORDER BY table_name
        """
    )
    for row in tables:
        name = str(row["table_name"])
        m = MODEL_SIGNAL_TABLE_RE.fullmatch(name)
        if m:
            ids.add(int(m.group(1)))

    return sorted(ids)


def parse_model_filter(value: Optional[str]) -> Optional[set[int]]:
    if not value:
        return None
    result = set()
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        result.add(int(part))
    return result


def get_rate_table(pair: int, timeframe: str) -> str:
    postfix = PAIR_MAP[pair]
    return f"brain_rates_{postfix}" + ("_day" if timeframe == "day" else "")


def get_shared_history_table(model_id: int, pair: int, timeframe: str) -> str:
    postfix = PAIR_MAP[pair] + ("_day" if timeframe == "day" else "")
    return f"brain_signal{model_id}_history_{postfix}"


def export_table_range(
    db: BrainDB,
    table: str,
    date_col: str,
    date_from: datetime,
    date_to: datetime,
    path: Path,
) -> int:
    cols = db.columns(table)
    if not cols:
        return 0
    fields = cols

    order = date_col if date_col in cols else cols[0]
    sql = (
        f"SELECT * FROM {safe_ident(table)} "
        f"WHERE {safe_ident(date_col)} >= %s AND {safe_ident(date_col)} <= %s "
        f"ORDER BY {safe_ident(order)}"
    )
    with GzipCsvWriter(path, fields) as w:
        for row in db.iter_query(sql, (dt_to_sql(date_from), dt_to_sql(date_to))):
            w.writerow(row)
        return w.count


def fetch_signal_configs(
    db: BrainDB,
    model_ids: List[int],
    pair: int,
    timeframe: str,
) -> List[Dict[str, Any]]:
    target_day = 1 if timeframe == "day" else 0
    result: List[Dict[str, Any]] = []

    for model_id in model_ids:
        table = f"brain_signal{model_id}"
        if not db.table_exists(table):
            continue

        cols = db.columns(table)
        if "id" not in cols:
            continue

        where = []
        params: List[Any] = []
        if "pair" in cols:
            where.append("`pair` = %s")
            params.append(pair)
        if "is_day" in cols:
            where.append("`is_day` = %s")
            params.append(target_day)

        sql = f"SELECT * FROM {safe_ident(table)}"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY `id`"

        for row in db.query(sql, tuple(params)):
            out = {"model_id": model_id, "config_table": table}
            out.update(row)
            result.append(out)

    return result


def choose_precision_by_rate(
    db: BrainDB,
    model_id: int,
    signal_id: int,
    rate_table: str,
    timeframe: str,
    date_from: datetime,
    date_to: datetime,
    precision_writer: GzipCsvWriter,
) -> Dict[int, Dict[str, Any]]:
    """
    Export all precision records in the requested date/rate range.
    Return latest (highest id) precision row for each rate_id.
    """
    table = f"brain_precision{model_id}_{signal_id}"
    if not db.table_exists(table):
        return {}

    cols = db.columns(table)
    required = {"rate_id"}
    if not required.issubset(cols):
        return {}

    # Filter by precision.date when present; otherwise join rates by rate_id.
    if "date" in cols:
        sql = (
            f"SELECT p.* FROM {safe_ident(table)} p "
            "WHERE p.`date` >= %s AND p.`date` <= %s "
            "ORDER BY p.`rate_id`, p.`id`"
        )
        params = (dt_to_sql(date_from), dt_to_sql(date_to))
    else:
        sql = (
            f"SELECT p.* FROM {safe_ident(table)} p "
            f"JOIN {safe_ident(rate_table)} r ON r.`id` = p.`rate_id` "
            "WHERE r.`date` >= %s AND r.`date` <= %s "
            "ORDER BY p.`rate_id`, p.`id`"
        )
        params = (dt_to_sql(date_from), dt_to_sql(date_to))

    latest: Dict[int, Dict[str, Any]] = {}
    for row in db.iter_query(sql, params):
        rid = int(row["rate_id"])
        out = {
            "timeframe": timeframe,
            "model_id": model_id,
            "signal_id": signal_id,
            "precision_table": table,
            "precision_id": row.get("id"),
            "rate_id": rid,
            "stage": row.get("stage"),
            "date": row.get("date"),
            "precision_value": row.get("value"),
            "precision_fact": row.get("fact"),
            "precision_result": row.get("result"),
            "precision_neurons": row.get("neurons"),
            "precision_added": row.get("added"),
        }
        precision_writer.writerow(out)
        latest[rid] = out

    return latest


def iter_signal_history(
    db: BrainDB,
    model_id: int,
    signal_id: int,
    rate_table: str,
    shared_history_table: str,
    date_from: datetime,
    date_to: datetime,
) -> Iterator[Tuple[Dict[str, Any], str]]:
    """
    Yield final historical signal values for one model/signal.

    Preference:
      1) brain_signal{model}_{signal}
      2) shared brain_signal{model}_history_{pair} for missing rate_id
    """
    seen_rate_ids: set[int] = set()

    dedicated = f"brain_signal{model_id}_{signal_id}"
    if db.table_exists(dedicated):
        cols = db.columns(dedicated)
        if {"rate_id", "value"}.issubset(cols):
            sql = (
                f"SELECT s.`rate_id`, s.`value`, r.`date` "
                f"FROM {safe_ident(dedicated)} s "
                f"JOIN {safe_ident(rate_table)} r ON r.`id` = s.`rate_id` "
                "WHERE r.`date` >= %s AND r.`date` <= %s "
                "ORDER BY r.`date`, s.`rate_id`"
            )
            for row in db.iter_query(
                sql,
                (dt_to_sql(date_from), dt_to_sql(date_to)),
            ):
                rid = int(row["rate_id"])
                seen_rate_ids.add(rid)
                yield row, dedicated

    if db.table_exists(shared_history_table):
        cols = db.columns(shared_history_table)
        if {"signal_id", "rate_id", "value"}.issubset(cols):
            sql = (
                f"SELECT s.`rate_id`, s.`value`, r.`date` "
                f"FROM {safe_ident(shared_history_table)} s "
                f"JOIN {safe_ident(rate_table)} r ON r.`id` = s.`rate_id` "
                "WHERE s.`signal_id` = %s "
                "AND r.`date` >= %s AND r.`date` <= %s "
                "ORDER BY r.`date`, s.`rate_id`"
            )
            for row in db.iter_query(
                sql,
                (signal_id, dt_to_sql(date_from), dt_to_sql(date_to)),
            ):
                rid = int(row["rate_id"])
                if rid in seen_rate_ids:
                    continue
                yield row, shared_history_table


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        result = float(value)
        if math.isfinite(result):
            return result
    except Exception:
        pass
    return None


def build_strength_proxy(
    value: Optional[float],
    buy_threshold: Optional[float],
    sell_threshold: Optional[float],
) -> Optional[float]:
    """
    Current-threshold-normalized magnitude.
    WARNING: historical thresholds may have changed over time, so this is only
    a diagnostic proxy using the CURRENT signal configuration.
    """
    if value is None or value == 0:
        return 0.0 if value == 0 else None

    if value > 0 and buy_threshold is not None and buy_threshold > 0:
        return value / buy_threshold

    if value < 0 and sell_threshold is not None and sell_threshold < 0:
        return abs(value / sell_threshold)

    return None


def aggregate_add(
    agg: Dict[Tuple[str, str], Dict[str, Any]],
    timeframe: str,
    date_val: Any,
    value: Optional[float],
    precision_value: Optional[float],
) -> None:
    date_str = serialize(date_val)
    key = (timeframe, str(date_str))
    a = agg[key]

    a["timeframe"] = timeframe
    a["date"] = date_str
    a["signals_total"] += 1

    if value is None:
        a["signals_null"] += 1
        return

    a["raw_sum"] += value
    a["raw_abs_sum"] += abs(value)

    if value > 0:
        a["positive_votes"] += 1
        a["positive_raw_sum"] += value
        a["nonzero_signals"] += 1
    elif value < 0:
        a["negative_votes"] += 1
        a["negative_raw_abs_sum"] += abs(value)
        a["nonzero_signals"] += 1
    else:
        a["zero_votes"] += 1

    p = precision_value
    if p is not None and p >= 0:
        a["precision_rows"] += 1
        if value > 0:
            a["precision_positive_weight"] += p
        elif value < 0:
            a["precision_negative_weight"] += p


def finalize_aggregate(
    agg: Dict[Tuple[str, str], Dict[str, Any]]
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for _, a in sorted(agg.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        directional_votes = a["positive_votes"] + a["negative_votes"]
        if directional_votes:
            a["vote_score"] = (
                a["positive_votes"] - a["negative_votes"]
            ) / directional_votes
        else:
            a["vote_score"] = 0.0

        raw_directional = a["positive_raw_sum"] + a["negative_raw_abs_sum"]
        if raw_directional:
            a["raw_direction_score"] = (
                a["positive_raw_sum"] - a["negative_raw_abs_sum"]
            ) / raw_directional
        else:
            a["raw_direction_score"] = 0.0

        pw = a["precision_positive_weight"]
        nw = a["precision_negative_weight"]
        if pw + nw:
            a["precision_vote_score"] = (pw - nw) / (pw + nw)
        else:
            a["precision_vote_score"] = None

        rows.append(dict(a))
    return rows


def make_agg_record() -> Dict[str, Any]:
    return {
        "timeframe": "",
        "date": "",
        "signals_total": 0,
        "signals_null": 0,
        "nonzero_signals": 0,
        "positive_votes": 0,
        "negative_votes": 0,
        "zero_votes": 0,
        "raw_sum": 0.0,
        "raw_abs_sum": 0.0,
        "positive_raw_sum": 0.0,
        "negative_raw_abs_sum": 0.0,
        "precision_rows": 0,
        "precision_positive_weight": 0.0,
        "precision_negative_weight": 0.0,
        "vote_score": None,
        "raw_direction_score": None,
        "precision_vote_score": None,
    }


def zip_dir(source_dir: Path) -> Path:
    zip_path = source_dir.with_suffix(".zip")
    if zip_path.exists():
        zip_path.unlink()
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for p in sorted(source_dir.rglob("*")):
            if p.is_file():
                z.write(p, p.relative_to(source_dir))
    return zip_path


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="READ-ONLY Brain Server historical signal exporter for Risk Manager"
    )

    p.add_argument("--env-file", help="Explicit .env path")
    p.add_argument("--host")
    p.add_argument("--port", type=int)
    p.add_argument("--user")
    p.add_argument("--password")
    p.add_argument("--database")
    p.add_argument("--connect-timeout", type=int, default=20)

    p.add_argument(
        "--pair",
        type=int,
        choices=sorted(PAIR_MAP),
        default=1,
        help="1=EURUSD, 3=BTCUSD, 4=ETHUSD. Default: 1",
    )
    p.add_argument(
        "--timeframe",
        choices=["hour", "day", "both"],
        default="hour",
        help="Default: hour",
    )
    p.add_argument(
        "--from-date",
        type=parse_dt,
        default=parse_dt("2026-03-18 00:00:00"),
        help='Default: "2026-03-18 00:00:00"',
    )
    p.add_argument(
        "--to-date",
        type=parse_dt,
        default=None,
        help="Default: current local/server time",
    )
    p.add_argument(
        "--models",
        help="Optional comma-separated model IDs, e.g. 33,50,58,61,71",
    )
    p.add_argument(
        "--active-only",
        action="store_true",
        help=(
            "Export only signals whose CURRENT config active=1. "
            "Usually leave OFF for historical research."
        ),
    )
    p.add_argument(
        "--output",
        default="brain_risk_history_export",
        help="Output directory",
    )
    p.add_argument(
        "--no-zip",
        action="store_true",
        help="Do not create ZIP at the end",
    )
    return p


def main() -> int:
    args = build_parser().parse_args()
    if args.to_date is None:
        args.to_date = datetime.now()

    if args.from_date >= args.to_date:
        raise SystemExit("--from-date must be earlier than --to-date")

    loaded_env = load_env_files(args.env_file)
    cfg = choose_db_config(args)

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)

    safe_cfg = dict(cfg)
    safe_cfg["password"] = "***"

    summary: Dict[str, Any] = {
        "started_at": datetime.now(),
        "read_only": True,
        "db": safe_cfg,
        "loaded_env_files": loaded_env,
        "pair": args.pair,
        "pair_postfix": PAIR_MAP[args.pair],
        "timeframe": args.timeframe,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "warnings": [
            (
                "brain_signal{model}.buy/sell are current threshold settings, "
                "not the historical front-api buyMultiplier/sellMultiplier."
            ),
            (
                "current_threshold_strength_proxy uses CURRENT thresholds and "
                "must not be treated as historically exact if thresholds changed."
            ),
            (
                "signal_aggregate files are research proxies. They do not claim "
                "bit-for-bit parity with historical api.php aggregation."
            ),
            (
                "Brain DB datetimes are exported exactly as stored. "
                "No automatic UTC/Moscow shift is applied."
            ),
        ],
        "timeframes": {},
        "missing_tables": [],
        "errors": [],
    }

    db: Optional[BrainDB] = None
    try:
        log("=" * 80)
        log("Brain Server historical Risk Manager exporter — READ ONLY")
        log("=" * 80)
        log(
            f"DB: {cfg['user']}@{cfg['host']}:{cfg['port']}/{cfg['database']} "
            f"(password hidden)"
        )
        log(
            f"Range: {dt_to_sql(args.from_date)} -> {dt_to_sql(args.to_date)}, "
            f"pair={args.pair} ({PAIR_MAP[args.pair]}), timeframe={args.timeframe}"
        )

        db = BrainDB(**cfg)
        info = db.query(
            """
            SELECT
                DATABASE() AS db_name,
                VERSION() AS mysql_version,
                NOW() AS db_now,
                @@session.time_zone AS session_time_zone,
                @@system_time_zone AS system_time_zone
            """
        )
        summary["server_info"] = info[0] if info else {}

        actual_db = str(summary["server_info"].get("db_name") or "")
        if actual_db.lower() != "brain":
            log(
                f"WARNING: connected database is {actual_db!r}, expected 'brain'. "
                "Use --database brain or verify MASTER_NAME."
            )
            summary["warnings"].append(
                f"Connected database is {actual_db!r}; expected 'brain'."
            )

        discovered = discover_model_ids(db)
        requested = parse_model_filter(args.models)
        if requested is not None:
            model_ids = [x for x in discovered if x in requested]
            missing_requested = sorted(requested - set(discovered))
            if missing_requested:
                summary["warnings"].append(
                    f"Requested model IDs not discovered: {missing_requested}"
                )
        else:
            model_ids = discovered

        log(f"Models discovered/selected: {len(model_ids)}")

        if db.table_exists("brain_models"):
            model_rows = db.query("SELECT * FROM brain_models ORDER BY id")
            if requested is not None:
                model_rows = [
                    r for r in model_rows
                    if int(r.get("id", -1)) in requested
                ]
            write_csv(out_dir / "brain_models.csv", model_rows)
        else:
            model_rows = []
            summary["missing_tables"].append("brain_models")

        timeframes = (
            ["hour", "day"] if args.timeframe == "both" else [args.timeframe]
        )

        aggregate: Dict[Tuple[str, str], Dict[str, Any]] = defaultdict(make_agg_record)

        signal_fields = [
            "timeframe",
            "pair",
            "date",
            "rate_id",
            "model_id",
            "signal_id",
            "signal_value",
            "signal_direction",
            "history_source_table",
            "config_active_current",
            "config_ml_stage_current",
            "config_precision_current",
            "config_result_current",
            "config_value_current",
            "buy_threshold_current",
            "sell_threshold_current",
            "current_threshold_strength_proxy",
            "precision_stage",
            "precision_value",
            "precision_fact",
            "precision_result",
            "precision_neurons",
        ]

        precision_fields = [
            "timeframe",
            "model_id",
            "signal_id",
            "precision_table",
            "precision_id",
            "rate_id",
            "stage",
            "date",
            "precision_value",
            "precision_fact",
            "precision_result",
            "precision_neurons",
            "precision_added",
        ]

        for timeframe in timeframes:
            log("\n" + "-" * 80)
            log(f"TIMEFRAME: {timeframe}")
            log("-" * 80)

            rate_table = get_rate_table(args.pair, timeframe)
            tf_summary: Dict[str, Any] = {
                "rate_table": rate_table,
                "signal_configs": 0,
                "signal_history_rows": 0,
                "precision_rows": 0,
                "signals_with_history": 0,
                "signals_without_history": 0,
            }
            summary["timeframes"][timeframe] = tf_summary

            if not db.table_exists(rate_table):
                log(f"ERROR: rate table missing: {rate_table}")
                summary["missing_tables"].append(rate_table)
                continue

            rate_count = export_table_range(
                db,
                rate_table,
                "date",
                args.from_date,
                args.to_date,
                out_dir / f"rates_{PAIR_MAP[args.pair]}_{timeframe}.csv.gz",
            )
            tf_summary["rates_rows"] = rate_count
            log(f"Rates exported: {rate_count:,}")

            configs = fetch_signal_configs(
                db=db,
                model_ids=model_ids,
                pair=args.pair,
                timeframe=timeframe,
            )

            if args.active_only:
                configs = [
                    c for c in configs
                    if str(c.get("active", "0")) in ("1", "True", "true")
                ]

            tf_summary["signal_configs"] = len(configs)
            write_csv(
                out_dir / f"signal_config_{PAIR_MAP[args.pair]}_{timeframe}.csv",
                configs,
            )
            log(f"Signal configs selected: {len(configs):,}")

            signal_path = (
                out_dir
                / f"signal_history_{PAIR_MAP[args.pair]}_{timeframe}.csv.gz"
            )
            precision_path = (
                out_dir
                / f"precision_history_{PAIR_MAP[args.pair]}_{timeframe}.csv.gz"
            )

            with GzipCsvWriter(signal_path, signal_fields) as signal_writer, \
                 GzipCsvWriter(precision_path, precision_fields) as precision_writer:

                for idx, cfg_row in enumerate(configs, start=1):
                    model_id = int(cfg_row["model_id"])
                    signal_id = int(cfg_row["id"])

                    if idx == 1 or idx % 50 == 0 or idx == len(configs):
                        log(
                            f"[{idx}/{len(configs)}] "
                            f"model={model_id}, signal={signal_id}"
                        )

                    shared = get_shared_history_table(
                        model_id,
                        args.pair,
                        timeframe,
                    )

                    # Precision history, latest row per rate_id for contextual weighting.
                    try:
                        precision_map = choose_precision_by_rate(
                            db=db,
                            model_id=model_id,
                            signal_id=signal_id,
                            rate_table=rate_table,
                            timeframe=timeframe,
                            date_from=args.from_date,
                            date_to=args.to_date,
                            precision_writer=precision_writer,
                        )
                    except Exception as exc:
                        precision_map = {}
                        summary["errors"].append(
                            {
                                "timeframe": timeframe,
                                "model_id": model_id,
                                "signal_id": signal_id,
                                "phase": "precision",
                                "error": repr(exc),
                            }
                        )

                    rows_this_signal = 0
                    buy_thr = as_float(cfg_row.get("buy"))
                    sell_thr = as_float(cfg_row.get("sell"))

                    try:
                        for hist, source_table in iter_signal_history(
                            db=db,
                            model_id=model_id,
                            signal_id=signal_id,
                            rate_table=rate_table,
                            shared_history_table=shared,
                            date_from=args.from_date,
                            date_to=args.to_date,
                        ):
                            rid = int(hist["rate_id"])
                            value = as_float(hist.get("value"))
                            p = precision_map.get(rid, {})
                            p_value = as_float(p.get("precision_value"))

                            direction = (
                                "positive" if value is not None and value > 0
                                else "negative" if value is not None and value < 0
                                else "zero" if value == 0
                                else "null"
                            )

                            out = {
                                "timeframe": timeframe,
                                "pair": args.pair,
                                "date": hist.get("date"),
                                "rate_id": rid,
                                "model_id": model_id,
                                "signal_id": signal_id,
                                "signal_value": value,
                                "signal_direction": direction,
                                "history_source_table": source_table,
                                "config_active_current": cfg_row.get("active"),
                                "config_ml_stage_current": cfg_row.get("ml_stage"),
                                "config_precision_current": cfg_row.get("precision"),
                                "config_result_current": cfg_row.get("result"),
                                "config_value_current": cfg_row.get("value"),
                                "buy_threshold_current": buy_thr,
                                "sell_threshold_current": sell_thr,
                                "current_threshold_strength_proxy": build_strength_proxy(
                                    value, buy_thr, sell_thr
                                ),
                                "precision_stage": p.get("stage"),
                                "precision_value": p_value,
                                "precision_fact": p.get("precision_fact"),
                                "precision_result": p.get("precision_result"),
                                "precision_neurons": p.get("precision_neurons"),
                            }
                            signal_writer.writerow(out)
                            rows_this_signal += 1

                            aggregate_add(
                                aggregate,
                                timeframe,
                                hist.get("date"),
                                value,
                                p_value,
                            )

                    except Exception as exc:
                        summary["errors"].append(
                            {
                                "timeframe": timeframe,
                                "model_id": model_id,
                                "signal_id": signal_id,
                                "phase": "signal_history",
                                "error": repr(exc),
                            }
                        )

                    if rows_this_signal:
                        tf_summary["signals_with_history"] += 1
                    else:
                        tf_summary["signals_without_history"] += 1

                tf_summary["signal_history_rows"] = signal_writer.count
                tf_summary["precision_rows"] = precision_writer.count

            log(
                f"Signal history rows: {tf_summary['signal_history_rows']:,}; "
                f"precision rows: {tf_summary['precision_rows']:,}"
            )

        aggregate_rows = finalize_aggregate(aggregate)
        aggregate_by_tf: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in aggregate_rows:
            aggregate_by_tf[str(row["timeframe"])].append(row)

        aggregate_fields = [
            "timeframe",
            "date",
            "signals_total",
            "signals_null",
            "nonzero_signals",
            "positive_votes",
            "negative_votes",
            "zero_votes",
            "vote_score",
            "raw_sum",
            "raw_abs_sum",
            "positive_raw_sum",
            "negative_raw_abs_sum",
            "raw_direction_score",
            "precision_rows",
            "precision_positive_weight",
            "precision_negative_weight",
            "precision_vote_score",
        ]

        for timeframe, rows in aggregate_by_tf.items():
            path = (
                out_dir
                / f"signal_aggregate_{PAIR_MAP[args.pair]}_{timeframe}.csv.gz"
            )
            with GzipCsvWriter(path, aggregate_fields) as w:
                for row in rows:
                    w.writerow(row)
            summary["timeframes"][timeframe]["aggregate_rows"] = len(rows)

        summary["finished_at"] = datetime.now()
        summary["models_selected"] = model_ids
        summary["models_selected_count"] = len(model_ids)
        write_json(out_dir / "export_summary.json", summary)

        readme = f"""Brain Risk History Export
=========================

READ ONLY export.

Pair:
    {args.pair} = {PAIR_MAP[args.pair]}

Period:
    {dt_to_sql(args.from_date)} -> {dt_to_sql(args.to_date)}

Important files:
    brain_models.csv
    signal_config_*.csv
    rates_*.csv.gz
    signal_history_*.csv.gz
    precision_history_*.csv.gz
    signal_aggregate_*.csv.gz
    export_summary.json

signal_history columns:
    date / rate_id
    model_id / signal_id
    signal_value
    signal_direction
    current config metadata
    current thresholds
    historical precision (when available)

signal_aggregate proxy metrics:
    vote_score:
        -1 = all non-zero models negative
         0 = balanced
        +1 = all non-zero models positive

    raw_direction_score:
        normalized balance of positive vs negative raw signal magnitudes.

    precision_vote_score:
        same directional vote idea, weighted by historical precision row
        when that row exists for the rate_id.

WARNING:
    These aggregate columns are research proxies. They are not claimed to be
    the exact historical four-field response of api.php.

    In brain_signal{{model_id}}, buy/sell are model threshold settings.
    They are NOT the same thing as api.php buyMultiplier/sellMultiplier.

    Datetimes are written exactly as stored by Brain MySQL. No timezone shift
    is applied automatically.
"""
        (out_dir / "README.txt").write_text(readme, encoding="utf-8")

        zip_path = None
        if not args.no_zip:
            zip_path = zip_dir(out_dir)
            log(f"\nZIP: {zip_path.resolve()}")

        log("\n" + "=" * 80)
        log("EXPORT COMPLETE")
        log("=" * 80)
        log(f"Output: {out_dir.resolve()}")
        if zip_path:
            log(f"Archive: {zip_path.resolve()}")
        log("No INSERT/UPDATE/DELETE/TRUNCATE statements were executed.")
        return 0

    except KeyboardInterrupt:
        log("\nInterrupted by user.")
        return 130
    except Exception as exc:
        summary["fatal_error"] = repr(exc)
        summary["finished_at"] = datetime.now()
        try:
            write_json(out_dir / "export_summary.json", summary)
        except Exception:
            pass
        log(f"\nFATAL: {exc}")
        raise
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    raise SystemExit(main())