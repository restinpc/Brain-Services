async def build_weights(engine_sasha):
    """
    Читаем ctx_index, генерируем все коды весов формата {ctx_id}_{mode}_{shift}.
    Возвращаем dict со статистикой.
    """
    from datetime import datetime
    import traceback

    from sqlalchemy import text

    def _log(stage: str, message: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[weights][{ts}][{stage}] {message}")

    CTX_TABLE      = "sasha_eq_context_idx"
    WEIGHTS_TABLE  = "sasha_eq_weights"
    SHIFT_WINDOW   = 24      
    MIN_RECURRING  = 2       # порог: occurrence - рутинное

    _log("START", "build_weights started")
    try:
        _log("DDL", f"Ensuring table `{WEIGHTS_TABLE}` exists and truncating it")
        async with engine_sasha.begin() as conn:
            await conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS `{WEIGHTS_TABLE}` (
                    id          INT         NOT NULL AUTO_INCREMENT,
                    weight_code VARCHAR(40) NOT NULL,
                    ctx_id      INT         NOT NULL,
                    mode        TINYINT     NOT NULL,   -- 0=T1, 1=Extremum
                    shift       SMALLINT    NOT NULL DEFAULT 0,
                    PRIMARY KEY (id),
                    UNIQUE KEY uk_wc (weight_code)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
            await conn.execute(text(f"TRUNCATE TABLE `{WEIGHTS_TABLE}`"))

        _log("READ", f"Reading context rows from `{CTX_TABLE}`")
        async with engine_sasha.connect() as conn:
            res = await conn.execute(text(
                f"SELECT id, occurrence_count FROM `{CTX_TABLE}`"
            ))
            ctx_rows = res.fetchall()
        _log("READ", f"Contexts fetched: {len(ctx_rows)}")

        _log("WRITE", "Generating and inserting weight codes")
        total = 0
        async with engine_sasha.begin() as conn:
            for idx, (ctx_id, occ) in enumerate(ctx_rows, start=1):
                max_shift = SHIFT_WINDOW if occ >= MIN_RECURRING else 0

                for mode in (0, 1):
                    for shift in range(0, max_shift + 1):
                        wc = f"{ctx_id}_{mode}_{shift}"
                        await conn.execute(text(f"""
                            INSERT IGNORE INTO `{WEIGHTS_TABLE}`
                                (weight_code, ctx_id, mode, shift)
                            VALUES (:wc, :cid, :mode, :shift)
                        """), {
                            "wc":    wc,
                            "cid":   ctx_id,
                            "mode":  mode,
                            "shift": shift,
                        })
                        total += 1

                if idx % 50 == 0:
                    _log("WRITE", f"Processed contexts: {idx}/{len(ctx_rows)}")

        _log("DONE", f"build_weights finished successfully, weights_generated={total}")
        return {"weights_generated": total}
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
        print(f"[weights][{ts}][MAIN] {message}")

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
            _main_log("Starting build_weights")
            stats = await build_weights(engine_sasha)
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