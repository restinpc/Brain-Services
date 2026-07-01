"""
extremum_weights.py — framework builder for brain_extremum_weights.

Аналог weights.py, но для экстремум-интервальных паттернов.

Фреймворк вызывает:
    await build_weights(engine_vlad)

Формат weight_code:
    EXT_{table_code}_{var_idx}_{ext_type_code}_{bucket_int}_{mode}
    EXT_{table_code}_{var_idx}_{ext_type_code}_{bucket_int}_{mode}_{shift}

Где:
    table_code   = короткое имя таблицы (btcusd, ethusd, eurusd + _d для дневных)
    var_idx      = 0/1/2 — индекс порога в THRESHOLDS_MAP (v0, v1, v2)
    ext_type_code = A (all) / X (max) / N (min)
    bucket_int   = int(entry_x * 100)  — 4.75 → 475
    mode         = 0 (T1-сигнал) / 1 (экстремум-сигнал)
    shift        = смещение в барах (опционально, только для recurring)
"""

from __future__ import annotations

import os

from sqlalchemy import text


CTX_TABLE = os.getenv("EXTREMUM_CTX_TABLE", "brain_extremum_ctx")
OUT_TABLE = os.getenv("EXTREMUM_WEIGHTS_TABLE", "brain_extremum_weights")

SHIFT_MIN = int(os.getenv("EXTREMUM_SHIFT_MIN", "-6"))
SHIFT_MAX = int(os.getenv("EXTREMUM_SHIFT_MAX", "6"))

_MODES_RAW = os.getenv("EXTREMUM_WEIGHT_MODES", "0,1")
MODES: tuple[int, ...] = tuple(
    int(x.strip())
    for x in _MODES_RAW.split(",")
    if x.strip()
)

TRUNCATE_OUT = os.getenv("EXTREMUM_TRUNCATE_OUT", "1") == "1"

# Пороги для каждой таблицы (в том же порядке что и в extremum_ctx.py).
# Нужны чтобы посчитать var_idx.
_THRESHOLDS_RAW = os.getenv(
    "EXTREMUM_THRESHOLDS",
    "brain_rates_btc_usd:8.0,5.0,3.0"
    "|brain_rates_eth_usd:8.0,5.0,3.0"
    "|brain_rates_eur_usd:0.8,0.5,0.3"
    "|brain_rates_btc_usd_day:8.0,5.0,3.0"
    "|brain_rates_eth_usd_day:8.0,5.0,3.0"
    "|brain_rates_eur_usd_day:0.8,0.5,0.3",
)

def _parse_thresholds(raw: str) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    for part in raw.split("|"):
        if ":" not in part:
            continue
        table, vals = part.split(":", 1)
        out[table.strip()] = [float(v) for v in vals.split(",") if v.strip()]
    return out

THRESHOLDS_MAP: dict[str, list[float]] = _parse_thresholds(_THRESHOLDS_RAW)

# Короткие коды таблиц
TABLE_CODE_MAP: dict[str, str] = {
    "brain_rates_btc_usd":      "btcusd",
    "brain_rates_eth_usd":      "ethusd",
    "brain_rates_eur_usd":      "eurusd",
    "brain_rates_btc_usd_day":  "btcusd_d",
    "brain_rates_eth_usd_day":  "ethusd_d",
    "brain_rates_eur_usd_day":  "eurusd_d",
}

EXT_TYPE_CODE_MAP: dict[str, str] = {
    "all": "A",
    "max": "X",
    "min": "N",
}


DDL = f"""
CREATE TABLE IF NOT EXISTS `{OUT_TABLE}` (
  `weight_code`      VARCHAR(64)   NOT NULL,
  `event_key`        VARCHAR(64)   NOT NULL,
  `table_name`       VARCHAR(64)   NOT NULL,
  `threshold_pct`    DOUBLE        NOT NULL,
  `var_idx`          TINYINT       NOT NULL,
  `ext_type`         VARCHAR(8)    NOT NULL,
  `entry_x`          DOUBLE        NOT NULL,
  `exit_x`           DOUBLE        NOT NULL,
  `mode_val`         TINYINT       NOT NULL,
  `hour_shift`       SMALLINT      NULL,
  `occurrence_count` INT           NULL,
  `created_at`       TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`weight_code`),
  KEY `idx_ew_table`   (`table_name`),
  KEY `idx_ew_thr`     (`threshold_pct`),
  KEY `idx_ew_exttype` (`ext_type`),
  KEY `idx_ew_entry`   (`entry_x`),
  KEY `idx_ew_mode`    (`mode_val`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def make_weight_code(
    table_name: str,
    var_idx: int,
    ext_type: str,
    entry_x: float,
    mode: int,
    shift: int | None = None,
) -> str:
    """
    Генерирует weight_code.

    Пример: EXT_btcusd_v1_N_475_0_3
    (BTC/USD hourly, var=1 (5%), min extrema, entry_x=4.75, mode=0, shift=+3)
    """
    tc   = TABLE_CODE_MAP.get(table_name, table_name.replace("brain_rates_", ""))
    etc  = EXT_TYPE_CODE_MAP.get(ext_type, ext_type[0].upper())
    bint = int(round(entry_x * 100))
    base = f"EXT_{tc}_v{var_idx}_{etc}_{bint}_{mode}"
    return base if shift is None else f"{base}_{shift}"


def decode_weight_code(code: str) -> dict:
    """Обратное декодирование weight_code → поля."""
    tc_map_rev = {v: k for k, v in TABLE_CODE_MAP.items()}
    etc_map_rev = {v: k for k, v in EXT_TYPE_CODE_MAP.items()}

    parts = code.split("_")
    # EXT_{tc}[_d?]_v{idx}_{etc}_{bint}_{mode}[_{shift}]
    # Минимум: EXT + tc + v{idx} + etc + bint + mode = 6 частей (больше если tc = btcusd_d)
    if len(parts) < 6 or parts[0] != "EXT":
        return {}

    try:
        # Собираем table_code: может быть btcusd или btcusd_d
        # var_idx часть начинается с 'v'
        var_part_idx = next(i for i in range(1, len(parts)) if parts[i].startswith("v"))
        tc = "_".join(parts[1:var_part_idx])
        var_idx = int(parts[var_part_idx][1:])
        etc_code = parts[var_part_idx + 1]
        bint = int(parts[var_part_idx + 2])
        mode = int(parts[var_part_idx + 3])
        shift = int(parts[var_part_idx + 4]) if len(parts) > var_part_idx + 4 else None
    except Exception:
        return {}

    return {
        "table_name":  tc_map_rev.get(tc, tc),
        "var_idx":     var_idx,
        "ext_type":    etc_map_rev.get(etc_code, etc_code),
        "entry_x":     bint / 100.0,
        "mode_val":    mode,
        "hour_shift":  shift,
    }


async def _ensure_table(engine_vlad) -> None:
    async with engine_vlad.begin() as conn:
        await conn.execute(text(DDL))


def _generate_rows(ctx_row: dict) -> list[dict]:
    """Генерирует weight_code строки для одного ctx паттерна."""
    table_name    = str(ctx_row["table_name"])
    threshold_pct = float(ctx_row["threshold_pct"])
    ext_type      = str(ctx_row["ext_type"])
    entry_x       = float(ctx_row["entry_x"])
    exit_x        = float(ctx_row["exit_x"])
    event_key     = str(ctx_row["event_key"])
    occ           = int(ctx_row.get("occurrence_count") or 0)

    # Определяем var_idx — позиция порога в списке порогов для этой таблицы
    thr_list = THRESHOLDS_MAP.get(table_name, [])
    try:
        var_idx = thr_list.index(threshold_pct)
    except ValueError:
        # Если точного совпадения нет — ищем ближайший
        if thr_list:
            var_idx = min(range(len(thr_list)), key=lambda i: abs(thr_list[i] - threshold_pct))
        else:
            var_idx = 0

    is_recurring = occ > 1
    rows: list[dict] = []

    # Базовые строки (без shift)
    for mode in MODES:
        rows.append({
            "weight_code":      make_weight_code(table_name, var_idx, ext_type, entry_x, mode, None),
            "event_key":        event_key,
            "table_name":       table_name,
            "threshold_pct":    threshold_pct,
            "var_idx":          var_idx,
            "ext_type":         ext_type,
            "entry_x":          entry_x,
            "exit_x":           exit_x,
            "mode_val":         mode,
            "hour_shift":       None,
            "occurrence_count": occ,
        })

    # Shift-строки — только для recurring паттернов
    if is_recurring:
        for shift in range(SHIFT_MIN, SHIFT_MAX + 1):
            for mode in MODES:
                rows.append({
                    "weight_code":      make_weight_code(table_name, var_idx, ext_type, entry_x, mode, shift),
                    "event_key":        event_key,
                    "table_name":       table_name,
                    "threshold_pct":    threshold_pct,
                    "var_idx":          var_idx,
                    "ext_type":         ext_type,
                    "entry_x":          entry_x,
                    "exit_x":           exit_x,
                    "mode_val":         mode,
                    "hour_shift":       shift,
                    "occurrence_count": occ,
                })

    return rows


async def build_weights(engine_vlad) -> dict:
    """
    Основная функция — вызывается фреймворком.
    """
    await _ensure_table(engine_vlad)

    # Загружаем паттерны из ctx
    async with engine_vlad.connect() as conn:
        res = await conn.execute(text(f"""
            SELECT
                event_key, table_name, threshold_pct,
                ext_type, entry_x, exit_x, occurrence_count
            FROM `{CTX_TABLE}`
            ORDER BY table_name, threshold_pct, ext_type, entry_x
        """))
        ctx_rows = [dict(r) for r in res.mappings().all()]

    all_rows: list[dict] = []
    recurring_count = 0
    non_recurring_count = 0

    for ctx in ctx_rows:
        occ = int(ctx.get("occurrence_count") or 0)
        if occ > 1:
            recurring_count += 1
        else:
            non_recurring_count += 1
        all_rows.extend(_generate_rows(ctx))

    # Записываем
    async with engine_vlad.begin() as conn:
        if TRUNCATE_OUT:
            await conn.execute(text(f"TRUNCATE TABLE `{OUT_TABLE}`"))

        if all_rows:
            BATCH = 500
            for i in range(0, len(all_rows), BATCH):
                batch = all_rows[i:i + BATCH]
                await conn.execute(text(f"""
                    INSERT INTO `{OUT_TABLE}` (
                        weight_code, event_key, table_name, threshold_pct,
                        var_idx, ext_type, entry_x, exit_x,
                        mode_val, hour_shift, occurrence_count
                    )
                    VALUES (
                        :weight_code, :event_key, :table_name, :threshold_pct,
                        :var_idx, :ext_type, :entry_x, :exit_x,
                        :mode_val, :hour_shift, :occurrence_count
                    )
                    ON DUPLICATE KEY UPDATE
                        occurrence_count = VALUES(occurrence_count)
                """), batch)

    return {
        "table":          OUT_TABLE,
        "ctx_table":      CTX_TABLE,
        "patterns":       len(ctx_rows),
        "recurring":      recurring_count,
        "non_recurring":  non_recurring_count,
        "weights":        len(all_rows),
        "modes":          list(MODES),
        "shift_min":      SHIFT_MIN,
        "shift_max":      SHIFT_MAX,
        "truncate":       TRUNCATE_OUT,
    }