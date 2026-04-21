async def build_index(engine_sasha, engine_brain):
    """
    Читаем сырой датасет, группируем в типы событий, пишем ctx_index в sasha.
    Возвращаем dict со статистикой (фреймворк логирует его).
    """
    from datetime import datetime
    import traceback

    from sqlalchemy import text

    def _log(stage: str, message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[context_idx][{ts}][{stage}] {message}")

    CTX_TABLE = "sasha_eq_context_idx"

    _log("START", "build_index started")
    try:
        _log("DDL", f"Ensuring table `{CTX_TABLE}` exists")
        async with engine_sasha.begin() as conn:
            await conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{CTX_TABLE}` (
                    id               INT         NOT NULL AUTO_INCREMENT,
                    mag_class        VARCHAR(10) NOT NULL,
                    tsunami          TINYINT     NOT NULL DEFAULT 0,
                    alert_level      VARCHAR(10) NOT NULL DEFAULT 'none',
                    occurrence_count INT         NOT NULL DEFAULT 0,
                    avg_significance DOUBLE      NULL,
                    avg_depth_km     DOUBLE      NULL,
                    felt_ratio       DOUBLE      NULL,
                    first_dt         DATETIME    NULL,
                    last_dt          DATETIME    NULL,
                    PRIMARY KEY (id),
                    UNIQUE KEY uk_ctx (mag_class, tsunami, alert_level)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))

        _log("READ", "Reading and grouping source dataset from `vlad_usgs_earthquakes`")
        async with engine_brain.connect() as conn:
            res = await conn.execute(text("""
                SELECT
                    CASE
                        WHEN magnitude < 5.5 THEN 'small'
                        WHEN magnitude < 6.5 THEN 'medium'
                        ELSE 'large'
                    END                                              AS mag_class,
                    COALESCE(tsunami, 0)                            AS tsunami,
                    COALESCE(alert_level, 'none')                   AS alert_level,
                    COUNT(*)                                        AS cnt,
                    AVG(COALESCE(significance, 0))                  AS avg_sig,
                    AVG(COALESCE(depth_km, 50))                     AS avg_depth,
                    SUM(COALESCE(felt_reports, 0) > 10) / COUNT(*) AS felt_ratio,
                    MIN(event_time)                                 AS first_dt,
                    MAX(event_time)                                 AS last_dt
                FROM vlad_usgs_earthquakes
                WHERE event_time IS NOT NULL
                GROUP BY mag_class, tsunami, alert_level
            """))
            rows = res.fetchall()
        _log("READ", f"Grouped contexts fetched: {len(rows)}")

        _log("WRITE", f"Truncating `{CTX_TABLE}` and inserting fresh rows")
        async with engine_sasha.begin() as conn:
            await conn.execute(text(f"TRUNCATE TABLE `{CTX_TABLE}`"))
            for idx, (mag_class, ts, al, cnt, sig, dep, felt, fd, ld) in enumerate(rows, start=1):
                await conn.execute(text(f"""
                    INSERT INTO `{CTX_TABLE}`
                        (mag_class, tsunami, alert_level,
                         occurrence_count, avg_significance,
                         avg_depth_km, felt_ratio, first_dt, last_dt)
                    VALUES (:mc, :ts, :al, :cnt, :sig, :dep, :felt, :fd, :ld)
                """), {
                    "mc":   mag_class,
                    "ts":   int(ts),
                    "al":   al,
                    "cnt":  cnt,
                    "sig":  sig,
                    "dep":  dep,
                    "felt": felt,
                    "fd":   fd,
                    "ld":   ld,
                })
                if idx % 50 == 0:
                    _log("WRITE", f"Inserted {idx}/{len(rows)} rows")

        _log("DONE", f"build_index finished successfully, contexts_total={len(rows)}")
        return {"contexts_total": len(rows)}
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