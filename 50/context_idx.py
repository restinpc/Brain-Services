async def build_index(engine_sasha, engine_brain):
    """
    Читаем `sasha_fred_cbbtcusd`, строим контексты по силе и направлению
    изменения BTC-цены и пишем индекс в sasha.
    """
    from datetime import datetime, time
    import traceback
 
    from sqlalchemy import text
 
    def _log(stage: str, message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[context_idx][{ts}][{stage}] {message}")
 
    SOURCE_TABLE = "sasha_fred_cbbtcusd_ml"
    CTX_TABLE = "sasha_fred_cbbtcusd_context_idx_ml"
    # ──────────────────────────────────────────────────────────────────────
    # ОБОГАЩЁННАЯ КЛАССИФИКАЦИЯ КОНТЕКСТА (фикс ML-режима).
    # Старый словарь = 5 крупных бакетов, которые горят и на вершинах, и на
    # днах -> обратное обучение не разводит знаки -> мусорный универсум.
    # Новый код = "{move}.{regime}":
    #   move   — знак+величина дневного %-изменения (7 уровней, НЕСЁТ знак);
    #   regime — режим тренда (value vs SMA(_REGIME_WINDOW)): ru/rd/rf.
    # Кардинальность 7×3 = 21 базовый тип, коды дискриминативны по направлению
    # -> RL сходится и ОБОБЩАЕТ так же, как на календарных моделях 44/49.
    # ВАЖНО: этот блок ДОЛЖЕН быть бит-идентичен такому же в model.py.
    # ──────────────────────────────────────────────────────────────────────
    _REGIME_WINDOW = 20      # баров для SMA-режима
    _REGIME_BAND = 0.01      # ±1% мёртвая зона вокруг SMA -> flat
 
    def _move_bucket(pct: float) -> str:
        if pct >= 2.0:
            return "p3"
        if pct >= 0.7:
            return "p2"
        if pct >= 0.15:
            return "p1"
        if pct <= -2.0:
            return "n3"
        if pct <= -0.7:
            return "n2"
        if pct <= -0.15:
            return "n1"
        return "z0"
 
    def _classify_events(series):
        """series: list[(datetime, value)] по возрастанию даты.
        -> list[(datetime, value, pct_change, event_type)].
        Режим считается КАУЗАЛЬНО (только прошлые значения)."""
        out = []
        prev = None
        win = []
        wsum = 0.0
        for dt, value in series:
            try:
                v = float(value)
            except (TypeError, ValueError):
                continue
            if win:
                sma = wsum / len(win)
                if sma and v > sma * (1.0 + _REGIME_BAND):
                    regime = "ru"
                elif sma and v < sma * (1.0 - _REGIME_BAND):
                    regime = "rd"
                else:
                    regime = "rf"
            else:
                regime = "rf"
            if prev is not None and prev != 0:
                pct = (v - prev) / prev * 100.0
                out.append((dt, v, pct, f"{_move_bucket(pct)}.{regime}"))
            win.append(v)
            wsum += v
            if len(win) > _REGIME_WINDOW:
                wsum -= win.pop(0)
            prev = v
        return out
 
    _log("START", "build_index started")
    try:
        _log("DDL", f"Ensuring table `{CTX_TABLE}` exists")
        async with engine_sasha.begin() as conn:
            await conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
                    id                   INT          NOT NULL AUTO_INCREMENT,
                    event_type           VARCHAR(64)  NOT NULL,
                    occurrence_count     INT          NOT NULL DEFAULT 0,
                    avg_value            DOUBLE       NULL,
                    min_value            DOUBLE       NULL,
                    max_value            DOUBLE       NULL,
                    avg_pct_change       DOUBLE       NULL,
                    avg_abs_pct_change   DOUBLE       NULL,
                    first_dt             DATETIME     NULL,
                    last_dt              DATETIME     NULL,
                    PRIMARY KEY (id),
                    UNIQUE KEY uk_ctx (event_type)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
 
        _log("READ", f"Reading source dataset from `{SOURCE_TABLE}`")
        async with engine_brain.connect() as conn:
            res = await conn.execute(text(f"""
                SELECT date_iso, value
                FROM `{SOURCE_TABLE}`
                WHERE value IS NOT NULL AND date_iso IS NOT NULL
                ORDER BY date_iso
            """))
            rows = res.fetchall()
        _log("READ", f"Rows fetched: {len(rows)}")
 
        # Упорядоченный ряд (datetime, value) для общей классификации.
        series = []
        for date_iso, value in rows:
            try:
                fv = float(value)
            except (TypeError, ValueError):
                continue
            series.append((datetime.combine(date_iso, time.min), fv))
 
        # stats строится ДИНАМИЧЕСКИ по обогащённым типам (а не по 5 фиксированным).
        stats = {}
        for dt, v, pct_change, event_type in _classify_events(series):
            item = stats.get(event_type)
            if item is None:
                item = stats[event_type] = {
                    "occurrence_count": 0,
                    "value_sum": 0.0,
                    "value_min": None,
                    "value_max": None,
                    "pct_change_sum": 0.0,
                    "abs_pct_change_sum": 0.0,
                    "first_dt": None,
                    "last_dt": None,
                }
            item["occurrence_count"] += 1
            item["value_sum"] += v
            item["value_min"] = v if item["value_min"] is None else min(item["value_min"], v)
            item["value_max"] = v if item["value_max"] is None else max(item["value_max"], v)
            item["pct_change_sum"] += pct_change
            item["abs_pct_change_sum"] += abs(pct_change)
            if item["first_dt"] is None:
                item["first_dt"] = dt
            item["last_dt"] = dt
 
        _log("WRITE", f"Truncating `{CTX_TABLE}` and inserting context rows")
        async with engine_sasha.begin() as conn:
            await conn.execute(text(f"TRUNCATE TABLE `{CTX_TABLE}`"))
            for event_type, item in stats.items():
                occ = int(item["occurrence_count"])
                avg_value = (item["value_sum"] / occ) if occ > 0 else None
                avg_pct_change = (item["pct_change_sum"] / occ) if occ > 0 else None
                avg_abs_pct_change = (item["abs_pct_change_sum"] / occ) if occ > 0 else None
 
                await conn.execute(text(f"""
                    INSERT INTO `{CTX_TABLE}`
                        (event_type, occurrence_count, avg_value, min_value, max_value,
                         avg_pct_change, avg_abs_pct_change, first_dt, last_dt)
                    VALUES
                        (:event_type, :cnt, :avg_value, :min_value, :max_value,
                         :avg_pct_change, :avg_abs_pct_change, :first_dt, :last_dt)
                """), {
                    "event_type": event_type,
                    "cnt": occ,
                    "avg_value": avg_value,
                    "min_value": item["value_min"],
                    "max_value": item["value_max"],
                    "avg_pct_change": avg_pct_change,
                    "avg_abs_pct_change": avg_abs_pct_change,
                    "first_dt": item["first_dt"],
                    "last_dt": item["last_dt"],
                })
 
        _log("DONE", "build_index finished successfully")
        total_events = sum(int(item["occurrence_count"]) for item in stats.values())
        return {
            "contexts_total": len(stats),
            "rows_total": len(rows),
            "events_total": total_events,
        }
    except Exception as exc:
        _log("ERROR", f"{type(exc).__name__}: {exc}")
        _log("TRACE", traceback.format_exc())
        raise
 
 
if __name__ == "__main__":
    import asyncio
    import os
    import sys
    import traceback
    from datetime import datetime
    from pathlib import Path
 
    from dotenv import load_dotenv
 
    def _main_log(message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[context_idx][{ts}][MAIN] {message}")
 
    base_dir = Path(__file__).resolve().parent
    loaded_env_paths = []
    for env_path in (base_dir / ".env", base_dir.parent / ".env"):
        if env_path.exists():
            load_dotenv(env_path, override=False)
            loaded_env_paths.append(str(env_path))
    if loaded_env_paths:
        _main_log(f"Loaded env files: {', '.join(loaded_env_paths)}")
    else:
        _main_log("No .env file found in service or project root")
 
    shared_dir = base_dir.parent / "shared"
    sys.path.insert(1, str(shared_dir))
 
    from common import build_engines  # pylint: disable=import-error
 
    def _fallback_env(target_prefix: str, source_prefix: str) -> None:
        keys = ("HOST", "PORT", "USER", "PASSWORD", "NAME")
        applied = []
        for key in keys:
            target = f"{target_prefix}_{key}"
            source = f"{source_prefix}_{key}"
            if not os.getenv(target) and os.getenv(source):
                os.environ[target] = os.getenv(source, "")
                applied.append(f"{target}<-{source}")
        if applied:
            _main_log("Applied fallback env: " + ", ".join(applied))
 
    _fallback_env("MASTER", "DB")
    _fallback_env("SUPER", "DB")
 
    required_vars = [
        "DB_HOST", "DB_USER", "DB_NAME",
        "MASTER_HOST", "MASTER_USER", "MASTER_NAME",
    ]
    missing = [name for name in required_vars if not os.getenv(name)]
    if missing:
        _main_log(
            "Missing required environment variables: "
            + ", ".join(missing)
            + ". Check .env values before running."
        )
        raise SystemExit(2)
 
    async def _main():
        _main_log("Creating database engines")
        engine_sasha, engine_brain, engine_super = build_engines()
        try:
            _main_log("Starting build_index")
            stats = await build_index(engine_sasha, engine_brain)
            _main_log(f"Result: {stats}")
        except Exception as exc:
            _main_log(f"Failed: {type(exc).__name__}: {exc}")
            _main_log(traceback.format_exc())
            raise
        finally:
            _main_log("Disposing database engines")
            await engine_sasha.dispose()
            await engine_brain.dispose()
            await engine_super.dispose()
            _main_log("Shutdown complete")
 
    asyncio.run(_main())
