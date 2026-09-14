"""
vision_codes.py — индекс CV-паттернов из таблиц brain_index{0-6}_*.

Лайт-версия (модель 97) кодировала свечку одним битом направления. Здесь паттерн
уже посчитан монолитом: на каждую свечку — матрица 1x1…12x12 (около 124 ячеек
по 4 разряда). Ячейка 1x1 — машинное зрение текущей свечки относительно
предыдущей, 2x1 — предыдущей относительно ещё более ранней, и так далее.

Вложенные подмножества — как в 97, только не по битам назад во времени, а по
глубине окна: 1x1, затем блок 2x2, …, блок 12x12. На каждом уровне ищем в
истории такие же блоки и суммируем T1 свечек, которые за ними следовали.

Прямой скан стоил бы O(N) на уровень на каждый вызов /values. Поэтому история
индексируется один раз на таблицу индексов: окна сортируются по хешу блока,
суммы T1 сворачиваются в префиксные, значение — два бинарных поиска.
"""

from __future__ import annotations

import re
import threading
from datetime import date as date_cls, datetime
from urllib.parse import quote_plus

import numpy as np
from sqlalchemy import create_engine, text

MAX_PATTERN_DEPTH = 12
MAX_ALGORITHM = 6

_HOUR_SECONDS = 3600
_DAY_SECONDS = 86400
_CELL_RE = re.compile(r"^(\d+)\s*[xхX×]\s*(\d+)$")
_POSITION_DTYPE = np.int32
_HASH_DTYPE = np.uint64

# FNV-1a 64. На numpy uint64 переполнение оборачивается — это и нужно.
_FNV_OFFSET = np.uint64(14695981039346656037)
_FNV_PRIME = np.uint64(1099511628211)

# До 7 алгоритмов на текущий инструмент; чужие пары вытесняются первыми.
_CACHE_LIMIT = 8
_CACHE: dict[tuple, "VisionHistory"] = {}
_CACHE_LOCK = threading.Lock()
_ENGINE = None
_ENGINE_LOCK = threading.Lock()
_TABLES: set[str] | None = None
_TABLES_LOCK = threading.Lock()


class _Level:
    """Окна одной глубины, отсортированные по хешу блока и позиции."""

    __slots__ = ("codes", "anchors", "cumsum")

    def __init__(self, codes: np.ndarray, anchors: np.ndarray, cumsum: np.ndarray):
        self.codes = codes
        self.anchors = anchors
        self.cumsum = cumsum

    def sum_after(self, code: int, last_anchor: int) -> float:
        target = _HASH_DTYPE(code)
        lo = int(self.codes.searchsorted(target, "left"))
        hi = int(self.codes.searchsorted(target, "right"))
        if lo >= hi:
            return 0.0
        found = int(self.anchors[lo:hi].searchsorted(
            _POSITION_DTYPE(last_anchor), "right"
        ))
        if found == 0:
            return 0.0
        return float(self.cumsum[lo + found] - self.cumsum[lo])


class VisionHistory:
    """Индекс одной таблицы brain_indexX_* , выровненный на даты np_rates."""

    __slots__ = ("dates_ns", "cells", "valid", "col_pairs", "levels", "depth_columns")

    def __init__(
        self,
        dates_ns: np.ndarray,
        cells: np.ndarray,
        valid: np.ndarray,
        col_pairs: list[tuple[int, int]],
        levels: list[_Level],
        depth_columns: list[np.ndarray],
    ):
        self.dates_ns = dates_ns
        self.cells = cells
        self.valid = valid
        self.col_pairs = col_pairs
        self.levels = levels
        self.depth_columns = depth_columns

    def last_closed_index(self, date: datetime, is_daily: bool) -> int:
        """Индекс последней свечки, полностью закрытой к моменту date.

        Тот же cutoff, что в модели 97 и во фреймворке
        ``_previous_completed_candle_direction``.
        """
        unit = _DAY_SECONDS if is_daily else _HOUR_SECONDS
        cutoff = int(date.timestamp()) - unit
        return int(self.dates_ns.searchsorted(cutoff, "right")) - 1

    def codes_at(
        self,
        date: datetime,
        *,
        is_daily: bool,
        max_depth: int = MAX_PATTERN_DEPTH,
    ) -> list[tuple[str, float]]:
        """Пары код-значение для квадратных подмножеств 1x1 … DxD."""
        anchor = self.last_closed_index(date, is_daily)
        if anchor < 0 or not bool(self.valid[anchor]):
            return []

        limit = min(max_depth, len(self.levels))
        pairs: list[tuple[str, float]] = []
        row = self.cells[anchor]
        for depth in range(1, limit + 1):
            columns = self.depth_columns[depth - 1]
            if columns.size == 0:
                continue
            key = _format_key(row, columns)
            code = int(_hash_row(row, columns))
            pairs.append((key, self.levels[depth - 1].sum_after(code, anchor - 1)))
        return pairs


def _format_key(row: np.ndarray, columns: np.ndarray) -> str:
    return "".join(f"{int(row[int(c)]):04d}" for c in columns)


def _hash_row(row: np.ndarray, columns: np.ndarray) -> np.uint64:
    return _hash_columns(np.asarray(row, dtype=np.uint16).reshape(1, -1), columns)[0]


def _hash_columns(cells: np.ndarray, columns: np.ndarray) -> np.ndarray:
    hashed = np.full(cells.shape[0], _FNV_OFFSET, dtype=_HASH_DTYPE)
    with np.errstate(over="ignore"):
        for column in columns:
            hashed ^= cells[:, int(column)].astype(_HASH_DTYPE, copy=False)
            hashed *= _FNV_PRIME
    return hashed


def _parse_cell_name(name: str) -> tuple[int, int] | None:
    matched = _CELL_RE.match(str(name).strip())
    if not matched:
        return None
    left, right = int(matched.group(1)), int(matched.group(2))
    if left < 1 or right < 1 or left > MAX_PATTERN_DEPTH or right > MAX_PATTERN_DEPTH:
        return None
    return left, right


def _cell_int(value) -> int:
    if value is None:
        return 0
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("ascii", "ignore")
    if isinstance(value, (int, np.integer)):
        return int(value) & 0xFFFF
    if isinstance(value, float):
        if value != value:  # NaN
            return 0
        return int(value) & 0xFFFF
    text = str(value).strip()
    if not text:
        return 0
    try:
        return int(float(text)) & 0xFFFF
    except ValueError:
        return 0


def _to_ts(value) -> int | None:
    if isinstance(value, datetime):
        return int(value.timestamp())
    if isinstance(value, date_cls):
        return int(datetime.combine(value, datetime.min.time()).timestamp())
    if value is None:
        return None
    text = str(value).strip().replace("T", " ")
    if not text:
        return None
    try:
        return int(datetime.fromisoformat(text[:19]).timestamp())
    except ValueError:
        return None


def _env(*names: str, default: str | None = None) -> str | None:
    import os
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def _model_section() -> dict:
    try:
        from brain_framework import get_service_config
        return dict((get_service_config() or {}).get("model") or {})
    except Exception:
        return {}


def _index_creds() -> dict:
    """Креды мастер-базы, где лежат brain_index*. Fallback — локальная DB_*."""
    which = str(
        _env("BRAIN_INDEX_ENGINE")
        or _model_section().get("index_engine")
        or "brain"
    ).lower()
    if which in ("vlad", "local", "sasha"):
        return {
            "user": _env("DB_USER"),
            "password": _env("DB_PASSWORD") or "",
            "host": _env("DB_HOST", default="127.0.0.1"),
            "port": _env("DB_PORT", default="3306"),
            "name": _env("DB_NAME"),
        }
    return {
        "user": _env("MASTER_USER", "DB_USER"),
        "password": _env("MASTER_PASSWORD", "DB_PASSWORD") or "",
        "host": _env("MASTER_HOST", "DB_HOST", default="127.0.0.1"),
        "port": _env("MASTER_PORT", "DB_PORT", default="3306"),
        "name": _env("MASTER_NAME", "DB_NAME", default="brain"),
    }


def engine():
    """Синхронный движок: model() вызывается из event loop, async engine не подходит."""
    global _ENGINE
    with _ENGINE_LOCK:
        if _ENGINE is None:
            creds = _index_creds()
            url = (
                f"mysql+pymysql://{creds['user']}:"
                f"{quote_plus(creds['password'])}@"
                f"{creds['host']}:{creds['port']}/"
                f"{creds['name']}?charset=utf8mb4"
            )
            _ENGINE = create_engine(
                url, pool_pre_ping=True, pool_recycle=1800, pool_size=2, max_overflow=3,
            )
        return _ENGINE


def invalidate() -> None:
    global _TABLES
    with _CACHE_LOCK:
        _CACHE.clear()
    with _TABLES_LOCK:
        _TABLES = None


def discover_tables() -> list[str]:
    """Имена brain_index* в мастер-базе. Нужно /rebuild_index для диагностики."""
    return sorted(_list_index_tables())


def _list_index_tables() -> set[str]:
    global _TABLES
    with _TABLES_LOCK:
        if _TABLES is not None:
            return _TABLES
    prefix = str(_model_section().get("index_prefix") or "brain_index")
    try:
        with engine().connect() as conn:
            rows = conn.execute(text(f"SHOW TABLES LIKE '{prefix}%'")).fetchall()
        found = {row[0] for row in rows}
    except Exception:
        found = set()
    with _TABLES_LOCK:
        _TABLES = found
    return found


def rates_suffix(rates_table: str) -> str:
    name = str(rates_table or "")
    prefix = "brain_rates_"
    if name.startswith(prefix):
        return name[len(prefix):]
    return name


def index_table_name(algorithm: int, rates_table: str) -> str | None:
    """Первая существующая таблица для алгоритма и инструмента."""
    if algorithm < 0 or algorithm > MAX_ALGORITHM:
        return None
    suffix = rates_suffix(rates_table)
    if not suffix:
        return None
    existing = _list_index_tables()
    candidates = [
        f"brain_index{algorithm}_{suffix}",
        f"brain_index_{algorithm}_{suffix}",
    ]
    if existing:
        for name in candidates:
            if name in existing:
                return name
        return None
    return candidates[0]


def _describe(conn, table: str) -> list[str]:
    rows = conn.execute(text(f"DESCRIBE `{table}`")).fetchall()
    return [str(row[0]) for row in rows]


def _load_cells(conn, index_table: str, rates_table: str) -> tuple[np.ndarray, np.ndarray, list[tuple[int, int]]]:
    columns = _describe(conn, index_table)
    cell_cols: list[tuple[str, int, int]] = []
    for name in columns:
        parsed = _parse_cell_name(name)
        if parsed is None:
            continue
        cell_cols.append((name, parsed[0], parsed[1]))
    if not cell_cols:
        return (
            np.empty(0, dtype=np.int64),
            np.empty((0, 0), dtype=np.uint16),
            [],
        )
    cell_cols.sort(key=lambda item: (max(item[1], item[2]), item[1], item[2]))
    pairs = [(item[1], item[2]) for item in cell_cols]
    bare_cells = ", ".join(f"`{item[0]}`" for item in cell_cols)
    aliased_cells = ", ".join(f"i.`{item[0]}`" for item in cell_cols)

    lower = {name.lower(): name for name in columns}
    date_col = next((lower[name] for name in ("date", "rate_date", "datetime") if name in lower), None)
    rate_id_col = next((lower[name] for name in ("rate_id", "rates_id", "id") if name in lower), None)

    if date_col:
        sql = (
            f"SELECT `{date_col}`, {bare_cells} "
            f"FROM `{index_table}` ORDER BY `{date_col}`"
        )
    elif rate_id_col and rates_table:
        sql = (
            f"SELECT r.`date`, {aliased_cells} "
            f"FROM `{index_table}` i "
            f"INNER JOIN `{rates_table}` r ON r.`id` = i.`{rate_id_col}` "
            f"ORDER BY r.`date`"
        )
    else:
        return (
            np.empty(0, dtype=np.int64),
            np.empty((0, 0), dtype=np.uint16),
            [],
        )

    rows = conn.execute(text(sql)).fetchall()
    size = len(rows)
    width = len(cell_cols)
    dates_ns = np.empty(size, dtype=np.int64)
    cells = np.zeros((size, width), dtype=np.uint16)
    keep = 0
    for row in rows:
        stamp = _to_ts(row[0])
        if stamp is None:
            continue
        dates_ns[keep] = stamp
        for column in range(width):
            cells[keep, column] = _cell_int(row[column + 1])
        keep += 1
    if keep != size:
        dates_ns = dates_ns[:keep]
        cells = cells[:keep]
    return dates_ns, cells, pairs


def _fingerprint(conn, table: str) -> tuple[int, int]:
    columns = {str(row[0]).lower() for row in conn.execute(text(f"DESCRIBE `{table}`")).fetchall()}
    if "date" in columns:
        row = conn.execute(
            text(f"SELECT COUNT(*), UNIX_TIMESTAMP(MAX(`date`)) FROM `{table}`")
        ).fetchone()
    elif "rate_id" in columns:
        row = conn.execute(text(f"SELECT COUNT(*), MAX(`rate_id`) FROM `{table}`")).fetchone()
    else:
        row = conn.execute(text(f"SELECT COUNT(*), 0 FROM `{table}`")).fetchone()
    return int(row[0] or 0), int(row[1] or 0)


def _depth_column_index(pairs: list[tuple[int, int]], max_depth: int) -> list[np.ndarray]:
    out: list[np.ndarray] = []
    for depth in range(1, max_depth + 1):
        columns = [idx for idx, (left, right) in enumerate(pairs) if left <= depth and right <= depth]
        out.append(np.asarray(columns, dtype=np.int32))
    return out


def build_from_arrays(
    dates_ns: np.ndarray,
    cells: np.ndarray,
    t1: np.ndarray,
    col_pairs: list[tuple[int, int]],
    *,
    max_depth: int = MAX_PATTERN_DEPTH,
    valid: np.ndarray | None = None,
) -> VisionHistory:
    """Строит индекс по уже выровненным массивам. Нужен и рантайму, и тестам."""
    dates_ns = np.ascontiguousarray(dates_ns, dtype=np.int64)
    cells = np.ascontiguousarray(cells, dtype=np.uint16)
    stored_t1 = np.nan_to_num(
        np.asarray(t1, dtype=np.float64),
        nan=0.0, posinf=0.0, neginf=0.0,
    )
    size = dates_ns.size
    if valid is None:
        valid = np.ones(size, dtype=bool)
    else:
        valid = np.asarray(valid, dtype=bool)

    following = np.zeros(size, dtype=np.float64)
    if size >= 2:
        following[:-1] = stored_t1[1:]

    max_depth = max(1, min(MAX_PATTERN_DEPTH, int(max_depth)))
    depth_columns = _depth_column_index(col_pairs, max_depth)
    usable = np.flatnonzero(valid)
    if usable.size:
        usable = usable[usable < max(size - 1, 0)]

    levels: list[_Level] = []
    for columns in depth_columns:
        if columns.size == 0 or usable.size == 0:
            levels.append(_Level(
                np.empty(0, dtype=_HASH_DTYPE),
                np.empty(0, dtype=_POSITION_DTYPE),
                np.zeros(1, dtype=np.float64),
            ))
            continue
        codes = _hash_columns(cells, columns)[usable]
        order = np.argsort(codes, kind="stable")
        ordered_anchors = usable[order].astype(_POSITION_DTYPE, copy=False)
        cumsum = np.empty(order.size + 1, dtype=np.float64)
        cumsum[0] = 0.0
        np.cumsum(following[ordered_anchors], out=cumsum[1:])
        levels.append(_Level(
            codes[order].astype(_HASH_DTYPE, copy=False),
            ordered_anchors,
            cumsum,
        ))

    return VisionHistory(dates_ns, cells, valid, col_pairs, levels, depth_columns)


def _align_to_rates(
    np_rates: dict,
    idx_dates: np.ndarray,
    idx_cells: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    dates_ns = np.ascontiguousarray(np_rates["dates_ns"], dtype=np.int64)
    width = idx_cells.shape[1] if idx_cells.size else 0
    cells = np.zeros((dates_ns.size, width), dtype=np.uint16)
    valid = np.zeros(dates_ns.size, dtype=bool)
    if idx_dates.size == 0 or width == 0:
        return dates_ns, cells, valid
    positions = np.searchsorted(idx_dates, dates_ns, side="left")
    in_range = positions < idx_dates.size
    matched = np.zeros(dates_ns.size, dtype=bool)
    matched[in_range] = idx_dates[positions[in_range]] == dates_ns[in_range]
    cells[matched] = idx_cells[positions[matched]]
    valid[matched] = True
    return dates_ns, cells, valid


def _load_history(index_table: str, rates_table: str, np_rates: dict, max_depth: int) -> VisionHistory | None:
    try:
        with engine().connect() as conn:
            idx_dates, idx_cells, pairs = _load_cells(conn, index_table, rates_table)
    except Exception:
        return None
    if not pairs:
        return None
    dates_ns, cells, valid = _align_to_rates(np_rates, idx_dates, idx_cells)
    return build_from_arrays(
        dates_ns,
        cells,
        np_rates["t1"],
        pairs,
        max_depth=max_depth,
        valid=valid,
    )


def history_for(
    np_rates: dict | None,
    *,
    algorithm: int,
    rates_table: str = "",
    max_depth: int = MAX_PATTERN_DEPTH,
) -> VisionHistory | None:
    """Индекс CV-таблицы с кешем между вызовами model()."""
    if not np_rates:
        return None
    dates_ns = np_rates.get("dates_ns")
    if dates_ns is None or len(dates_ns) < 2:
        return None
    table = index_table_name(int(algorithm), rates_table)
    if not table:
        return None

    fingerprint = (0, 0)
    try:
        with engine().connect() as conn:
            fingerprint = _fingerprint(conn, table)
    except Exception:
        return None
    if fingerprint[0] <= 0:
        return None

    key = (
        str(table),
        int(algorithm),
        int(max_depth),
        int(len(dates_ns)),
        int(dates_ns[0]),
        int(dates_ns[-1]),
        int(fingerprint[0]),
        int(fingerprint[1]),
    )
    cached = _CACHE.get(key)
    if cached is not None:
        return cached

    with _CACHE_LOCK:
        cached = _CACHE.get(key)
        if cached is None:
            cached = _load_history(table, rates_table, np_rates, max_depth)
            if cached is None:
                return None
            suffix = rates_suffix(rates_table)
            for item in [k for k in _CACHE if k[0] == table]:
                del _CACHE[item]
            if len(_CACHE) >= _CACHE_LIMIT:
                for item in [k for k in _CACHE if suffix not in str(k[0])]:
                    del _CACHE[item]
                    if len(_CACHE) < _CACHE_LIMIT:
                        break
            while len(_CACHE) >= _CACHE_LIMIT:
                _CACHE.pop(next(iter(_CACHE)))
            _CACHE[key] = cached
    return cached
