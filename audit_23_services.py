#!/usr/bin/env python3
from __future__ import annotations

import argparse
import ast
import json
import os
import re
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path("/brain/Brain-Services")
SERVICES = [35, 42, 44, 46, 47, 49, 50, 53, 56, 57, 58, 62, 63, 65, 66, 67, 68, 70, 72, 73, 74, 75, 76]
PAIRS = [1, 3, 4]
DAYS = [0, 1]

OUT_DIR = Path("/brain/Brain-Server/logs")
OUT_JSON = OUT_DIR / "framework_audit_23.json"
OUT_TXT = OUT_DIR / "framework_audit_23.txt"
PROGRESS_LOG = OUT_DIR / "framework_audit_23_progress.log"

PRINT_LOCK = threading.Lock()
WRITE_LOCK = threading.Lock()
STOP_EVENT = threading.Event()

DEFAULT_PORT_OVERRIDES = {
    # Добавляй сюда только подтверждённые исключения.
    # 35: 8898,
}


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(message: str) -> None:
    line = f"{now()} {message}"
    with PRINT_LOCK:
        print(line, flush=True)
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        with PROGRESS_LOG.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()


def handle_signal(signum, frame) -> None:
    STOP_EVENT.set()
    log(f"[STOP] Получен сигнал {signum}. Завершаем после текущих запросов.")


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def request_json(url: str, timeout: int, label: str) -> tuple[int | None, Any, str, float]:
    started = time.monotonic()
    log(f"[REQ] {label} BEGIN timeout={timeout}s {url}")

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Brain-23-Audit-Fast/2.0",
            "Connection": "close",
            "Accept": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            code = int(response.getcode())
    except urllib.error.HTTPError as exc:
        code = int(exc.code)
        body = exc.read().decode("utf-8", errors="replace")
    except Exception as exc:
        elapsed = time.monotonic() - started
        log(f"[REQ] {label} ERROR elapsed={elapsed:.2f}s {type(exc).__name__}: {exc}")
        return None, None, str(exc), elapsed

    elapsed = time.monotonic() - started
    try:
        payload = json.loads(body)
        log(f"[REQ] {label} END http={code} elapsed={elapsed:.2f}s bytes={len(body)}")
        return code, payload, body, elapsed
    except Exception:
        log(f"[REQ] {label} BAD_JSON http={code} elapsed={elapsed:.2f}s bytes={len(body)}")
        return code, None, body, elapsed


def normalize_range(value: Any) -> list[int]:
    if isinstance(value, list):
        result = []
        for item in value:
            try:
                result.append(int(item))
            except Exception:
                pass
        return result
    if isinstance(value, int):
        return list(range(max(0, value)))
    if isinstance(value, str):
        return [int(x) for x in re.findall(r"-?\d+", value)]
    return []


def find_recursive(obj: Any, names: tuple[str, ...]) -> Any:
    if isinstance(obj, dict):
        for name in names:
            if name in obj:
                return obj[name]
        for value in obj.values():
            found = find_recursive(value, names)
            if found is not None:
                return found
    return None


def parse_python_range(raw: str) -> list[int]:
    try:
        if raw.startswith("range("):
            nums = [int(x) for x in re.findall(r"-?\d+", raw)]
            return list(range(*nums))
        return normalize_range(ast.literal_eval(raw))
    except Exception:
        return []


def read_ranges(service_id: int) -> tuple[list[int], list[int]]:
    service_dir = ROOT / str(service_id)
    types: list[int] = []
    vars_: list[int] = []

    config_path = service_dir / "config.toml"
    if config_path.exists():
        try:
            try:
                import tomllib
            except ImportError:
                import tomli as tomllib

            with config_path.open("rb") as f:
                data = tomllib.load(f)

            types = normalize_range(
                find_recursive(data, ("types_range", "type_range", "types", "TYPES_RANGE", "TYPE_RANGE"))
            )
            vars_ = normalize_range(
                find_recursive(data, ("var_range", "vars_range", "vars", "VAR_RANGE", "VARS_RANGE"))
            )
        except Exception as exc:
            log(f"[SERVICE {service_id}] config.toml parse warning: {exc}")

    for filename in ("config.py", "server.py", "model.py"):
        path = service_dir / filename
        if not path.exists():
            continue

        text = path.read_text(encoding="utf-8", errors="replace")

        if not types:
            for key in ("TYPES_RANGE", "TYPE_RANGE", "types_range", "type_range"):
                match = re.search(
                    rf"(?m)^\s*{re.escape(key)}\s*=\s*(\[[^\n]+\]|\([^\n]+\)|range\([^\n]+\))",
                    text,
                )
                if match:
                    types = parse_python_range(match.group(1))
                    if types:
                        break

        if not vars_:
            for key in ("VAR_RANGE", "VARS_RANGE", "var_range", "vars_range"):
                match = re.search(
                    rf"(?m)^\s*{re.escape(key)}\s*=\s*(\[[^\n]+\]|\([^\n]+\)|range\([^\n]+\))",
                    text,
                )
                if match:
                    vars_ = parse_python_range(match.group(1))
                    if vars_:
                        break

    overrides = {
        58: ([0], [2]),
        67: ([0, 1, 2], [0, 1, 2, 3]),
    }
    if service_id in overrides:
        forced_types, forced_vars = overrides[service_id]
        if not types or types == [0]:
            types = forced_types
        if not vars_ or vars_ == [0]:
            vars_ = forced_vars

    return sorted(set(types or [0]))[:64], sorted(set(vars_ or [0]))[:128]


def discover_port(service_id: int, explicit_overrides: dict[int, int]) -> int:
    if service_id in explicit_overrides:
        return explicit_overrides[service_id]

    service_dir = ROOT / str(service_id)
    candidates = [
        service_dir / ".env",
        service_dir / "config.toml",
        service_dir / "config.py",
        service_dir / "server.py",
        service_dir / "main.py",
        service_dir / "run.sh",
    ]

    patterns = [
        r"(?im)^\s*(?:PORT|SERVICE_PORT|UVICORN_PORT)\s*=\s*[\"']?(\d{4,5})",
        r"(?im)^\s*port\s*=\s*[\"']?(\d{4,5})",
        r"(?im)--port\s+(\d{4,5})",
        r"(?im)localhost:(\d{4,5})",
        r"(?im)127\.0\.0\.1:(\d{4,5})",
    ]

    for path in candidates:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                port = int(match.group(1))
                if 1024 <= port <= 65535:
                    return port

    return 8862 + service_id


def endpoint_limit(openapi: dict, path: str, default: int) -> int:
    try:
        for param in openapi["paths"][path]["get"].get("parameters", []):
            if param.get("name") == "samples":
                maximum = param.get("schema", {}).get("maximum")
                if maximum is not None:
                    return max(1, min(default, int(maximum)))
    except Exception:
        pass
    return default


def frame_stats(payload: Any, frame: str) -> dict[str, Any]:
    try:
        checks = payload["payLoad"]["frames"][frame]["checks"]
    except Exception:
        return {"valid": False, "nonempty": False, "checks": 0, "keys": 0, "nonzero": 0}

    keys = sum(int(item.get("keys", 0) or 0) for item in checks)
    nonzero = sum(int(item.get("nonzero", 0) or 0) for item in checks)

    return {
        "valid": True,
        "nonempty": keys > 0 or nonzero > 0,
        "checks": len(checks),
        "keys": keys,
        "nonzero": nonzero,
    }


def causal_status(payload: Any) -> str:
    if not isinstance(payload, dict) or payload.get("status") != "ok":
        return "INVALID"

    text = json.dumps(payload.get("payLoad", {}), ensure_ascii=False).upper()
    if '"STATUS": "FAIL"' in text:
        return "FAIL"
    if '"STATUS": "PASS"' in text or ('"PASS"' in text and '"FAIL"' not in text):
        return "PASS"
    return "INCONCLUSIVE"


def save_partial(report: dict, text_lines: list[str]) -> None:
    with WRITE_LOCK:
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        tmp_json = OUT_JSON.with_suffix(".json.tmp")
        tmp_txt = OUT_TXT.with_suffix(".txt.tmp")

        tmp_json.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp_txt.write_text("\n".join(text_lines) + "\n", encoding="utf-8")

        tmp_json.replace(OUT_JSON)
        tmp_txt.replace(OUT_TXT)


def values_one(
    service_id: int,
    port: int,
    pair: int,
    day: int,
    typ: int,
    var: int,
    dt: datetime,
    timeout: int,
) -> dict[str, Any]:
    date_text = dt.strftime("%Y-%m-%d 00:00:00")
    query = urllib.parse.urlencode(
        {
            "pair": pair,
            "day": day,
            "date": date_text,
            "type": typ,
            "var": var,
            "param": "",
        }
    )
    label = f"S{service_id} VALUES p={pair} d={day} t={typ} v={var} date={dt:%F}"
    code, payload, _, elapsed = request_json(
        f"http://127.0.0.1:{port}/values?{query}",
        timeout,
        label,
    )

    count = 0
    details = None
    if isinstance(payload, dict):
        if isinstance(payload.get("payLoad"), dict):
            count = len(payload["payLoad"])
        details = payload.get("details")

    log(f"[RESULT] {label} http={code} count={count} elapsed={elapsed:.2f}s")
    return {
        "date": dt.strftime("%Y-%m-%d"),
        "http": code,
        "count": count,
        "details": details,
        "elapsed": round(elapsed, 3),
    }


def audit_service(
    service_id: int,
    args: argparse.Namespace,
    port_overrides: dict[int, int],
) -> dict[str, Any]:
    started = time.monotonic()
    port = discover_port(service_id, port_overrides)

    rec: dict[str, Any] = {
        "id": service_id,
        "port": port,
        "metadata": {},
        "ranges": {},
        "direct": [],
        "values": [],
        "causal": [],
        "classification": [],
        "elapsed": 0.0,
    }

    log(f"[SERVICE {service_id}] START port={port}")

    code, root, _, _ = request_json(
        f"http://127.0.0.1:{port}/",
        args.http_timeout,
        f"S{service_id} ROOT",
    )
    if code != 200 or not isinstance(root, dict):
        rec["classification"].append("HTTP_FAILED")
        rec["elapsed"] = round(time.monotonic() - started, 3)
        log(f"[SERVICE {service_id}] FAILED HTTP code={code}")
        return rec

    metadata = root.get("metadata", {})
    rec["metadata"] = {
        key: root.get("name") if key == "name" else metadata.get(key)
        for key in (
            "name",
            "dataset",
            "ctx_index",
            "weight_codes",
            "simple_rates",
            "enriched_table",
            "cache_role",
            "cache_table",
            "last_reload",
        )
    }
    log(f"[SERVICE {service_id}] META {json.dumps(rec['metadata'], ensure_ascii=False)}")

    types, vars_ = read_ranges(service_id)
    rec["ranges"] = {"types": types, "vars": vars_}
    log(f"[SERVICE {service_id}] RANGES types={types} vars={vars_}")

    code, openapi, _, _ = request_json(
        f"http://127.0.0.1:{port}/openapi.json",
        args.http_timeout,
        f"S{service_id} OPENAPI",
    )
    paths = openapi.get("paths", {}) if isinstance(openapi, dict) else {}
    has_timeframes = "/diagnostics/timeframes" in paths
    has_causal = "/diagnostics/future_leak" in paths

    if not has_timeframes:
        rec["classification"].append("DIAGNOSTICS_MISSING")
        rec["elapsed"] = round(time.monotonic() - started, 3)
        log(f"[SERVICE {service_id}] FAILED diagnostics/timeframes missing")
        return rec

    samples = endpoint_limit(openapi, "/diagnostics/timeframes", args.samples)
    causal_samples = endpoint_limit(openapi, "/diagnostics/future_leak", args.causal_samples)

    direct_jobs: list[tuple[int, int, int]] = [
        (pair, typ, var)
        for pair in PAIRS
        for typ in types
        for var in vars_
    ]

    any_direct = False
    first_working: dict[int, tuple[int, int]] = {}

    # Внутри одного сервиса параллелизм ограничен, чтобы не задушить один FastAPI-процесс.
    with ThreadPoolExecutor(max_workers=args.per_service_workers) as executor:
        future_map = {}

        for pair, typ, var in direct_jobs:
            query = urllib.parse.urlencode(
                {"pair": pair, "type": typ, "var": var, "samples": samples}
            )
            label = f"S{service_id} DIRECT p={pair} t={typ} v={var}"
            future = executor.submit(
                request_json,
                f"http://127.0.0.1:{port}/diagnostics/timeframes?{query}",
                args.direct_timeout,
                label,
            )
            future_map[future] = (pair, typ, var, label)

        for future in as_completed(future_map):
            if STOP_EVENT.is_set():
                break

            pair, typ, var, label = future_map[future]
            try:
                code, payload, _, elapsed = future.result()
            except Exception as exc:
                code, payload, elapsed = None, None, 0.0
                log(f"[RESULT] {label} FUTURE_ERROR {exc}")

            hour = frame_stats(payload, "hour")
            day = frame_stats(payload, "day")
            ok = (
                code == 200
                and isinstance(payload, dict)
                and payload.get("status") == "ok"
            )

            rec["direct"].append(
                {
                    "pair": pair,
                    "type": typ,
                    "var": var,
                    "http": code,
                    "ok": ok,
                    "hour": hour,
                    "day": day,
                    "elapsed": round(elapsed, 3),
                }
            )

            if hour["nonempty"] or day["nonempty"]:
                any_direct = True
                first_working.setdefault(pair, (typ, var))

            log(
                f"[RESULT] {label} http={code} "
                f"H={int(hour['nonempty'])} D={int(day['nonempty'])} "
                f"keysH={hour['keys']} keysD={day['keys']} elapsed={elapsed:.2f}s"
            )

    rec["direct"].sort(key=lambda x: (x["pair"], x["type"], x["var"]))

    if not any_direct:
        rec["classification"].append("DIRECT_MODEL_ALL_EMPTY")

    # /values: все последние даты запускаются параллельно.
    value_jobs = []
    for pair in PAIRS:
        typ, var = first_working.get(pair, (types[0], vars_[0]))
        for day in DAYS:
            for offset in range(args.lookback_days - 1, -1, -1):
                dt = args.target_end - timedelta(days=offset)
                value_jobs.append((pair, day, typ, var, dt))

    grouped_values: dict[tuple[int, int, int, int], list[dict[str, Any]]] = {}
    any_values = False

    with ThreadPoolExecutor(max_workers=args.values_workers) as executor:
        future_map = {
            executor.submit(
                values_one,
                service_id,
                port,
                pair,
                day,
                typ,
                var,
                dt,
                args.values_timeout,
            ): (pair, day, typ, var)
            for pair, day, typ, var, dt in value_jobs
        }

        for future in as_completed(future_map):
            if STOP_EVENT.is_set():
                break

            key = future_map[future]
            try:
                row = future.result()
            except Exception as exc:
                log(f"[SERVICE {service_id}] VALUES FUTURE_ERROR key={key}: {exc}")
                row = {
                    "date": None,
                    "http": None,
                    "count": 0,
                    "details": [str(exc)],
                    "elapsed": 0.0,
                }

            grouped_values.setdefault(key, []).append(row)
            any_values = any_values or row["count"] > 0

    for key, rows in sorted(grouped_values.items()):
        pair, day, typ, var = key
        rows.sort(key=lambda x: x["date"] or "")
        nonempty = sum(1 for row in rows if row["count"] > 0)

        rec["values"].append(
            {
                "pair": pair,
                "day": day,
                "type": typ,
                "var": var,
                "nonempty_dates": nonempty,
                "rows": rows,
            }
        )
        log(
            f"[SERVICE {service_id}] VALUES SUMMARY "
            f"p={pair} d={day} t={typ} v={var} "
            f"nonempty={nonempty}/{len(rows)}"
        )

    if not any_values:
        rec["classification"].append("VALUES_LAST10_ALL_EMPTY")

    if has_causal and not args.skip_causal and first_working:
        service_pass = False

        with ThreadPoolExecutor(max_workers=min(3, len(first_working))) as executor:
            future_map = {}

            for pair, (typ, var) in first_working.items():
                query = urllib.parse.urlencode(
                    {
                        "pair": pair,
                        "type": typ,
                        "var": var,
                        "samples": causal_samples,
                    }
                )
                label = f"S{service_id} CAUSAL p={pair} t={typ} v={var}"
                future = executor.submit(
                    request_json,
                    f"http://127.0.0.1:{port}/diagnostics/future_leak?{query}",
                    args.causal_timeout,
                    label,
                )
                future_map[future] = (pair, typ, var, label)

            for future in as_completed(future_map):
                pair, typ, var, label = future_map[future]
                try:
                    code, payload, _, elapsed = future.result()
                except Exception as exc:
                    code, payload, elapsed = None, None, 0.0
                    log(f"[RESULT] {label} FUTURE_ERROR {exc}")

                status = causal_status(payload)
                rec["causal"].append(
                    {
                        "pair": pair,
                        "type": typ,
                        "var": var,
                        "http": code,
                        "status": status,
                        "elapsed": round(elapsed, 3),
                    }
                )
                service_pass = service_pass or status == "PASS"
                log(
                    f"[RESULT] {label} http={code} status={status} elapsed={elapsed:.2f}s"
                )

        rec["causal"].sort(key=lambda x: x["pair"])

        if not service_pass:
            rec["classification"].append("CAUSAL_NO_PASS")

    if any_direct and not any_values:
        rec["classification"].append("DIRECT_NONEMPTY_VALUES_EMPTY")
    elif not any_direct and not any_values:
        rec["classification"].append("MODEL_OR_INPUT_DATA_EMPTY")
    elif any_direct and any_values:
        rec["classification"].append("BASIC_PATH_OK")

    rec["elapsed"] = round(time.monotonic() - started, 3)
    log(
        f"[SERVICE {service_id}] DONE elapsed={rec['elapsed']:.2f}s "
        f"classification={','.join(rec['classification']) or 'NONE'}"
    )
    return rec


def parse_port_overrides(raw: str) -> dict[int, int]:
    result = dict(DEFAULT_PORT_OVERRIDES)
    if not raw:
        return result

    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        service_text, port_text = item.split(":", 1)
        result[int(service_text)] = int(port_text)

    return result


def build_text_report(report: dict[str, Any]) -> list[str]:
    lines = [
        f"created_at={report['created_at']}",
        "",
    ]

    for rec in sorted(report["services"], key=lambda item: item["id"]):
        lines.append(f"===== SERVICE {rec['id']} port={rec['port']} =====")
        lines.append("META " + json.dumps(rec.get("metadata", {}), ensure_ascii=False))
        lines.append(
            "RANGES "
            + json.dumps(rec.get("ranges", {}), ensure_ascii=False)
        )

        for row in rec.get("direct", []):
            lines.append(
                f"DIRECT pair={row['pair']} type={row['type']} var={row['var']} "
                f"http={row['http']} H={int(row['hour']['nonempty'])} "
                f"D={int(row['day']['nonempty'])} elapsed={row.get('elapsed', 0)}"
            )

        for row in rec.get("values", []):
            lines.append(
                f"VALUES pair={row['pair']} day={row['day']} "
                f"type={row['type']} var={row['var']} "
                f"nonempty={row['nonempty_dates']}/{len(row['rows'])}"
            )

        for row in rec.get("causal", []):
            lines.append(
                f"CAUSAL pair={row['pair']} type={row['type']} var={row['var']} "
                f"http={row['http']} status={row['status']}"
            )

        lines.append(
            "CLASSIFICATION "
            + ",".join(rec.get("classification", []))
        )
        lines.append(f"ELAPSED {rec.get('elapsed', 0)}")
        lines.append("")

    lines.append("===== SUMMARY =====")
    for key, value in report.get("summary", {}).items():
        lines.append(f"{key}={value}")

    return lines


def calculate_summary(services: list[dict[str, Any]]) -> dict[str, int]:
    summary = {
        "services": len(SERVICES),
        "completed": len(services),
        "http_ok": 0,
        "diagnostics_ok": 0,
        "direct_all_empty": 0,
        "values_all_empty": 0,
        "causal_pass": 0,
        "failed": 0,
        "basic_path_ok": 0,
    }

    for rec in services:
        classes = set(rec.get("classification", []))

        if "HTTP_FAILED" not in classes:
            summary["http_ok"] += 1
        if "HTTP_FAILED" not in classes and "DIAGNOSTICS_MISSING" not in classes:
            summary["diagnostics_ok"] += 1
        if "DIRECT_MODEL_ALL_EMPTY" in classes:
            summary["direct_all_empty"] += 1
        if "VALUES_LAST10_ALL_EMPTY" in classes:
            summary["values_all_empty"] += 1
        if any(item.get("status") == "PASS" for item in rec.get("causal", [])):
            summary["causal_pass"] += 1
        if "BASIC_PATH_OK" in classes:
            summary["basic_path_ok"] += 1
        if "HTTP_FAILED" in classes or "DIAGNOSTICS_MISSING" in classes:
            summary["failed"] += 1

    return summary


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Быстрый параллельный аудит 23 Brain Framework сервисов"
    )
    parser.add_argument("--workers", type=int, default=12, help="Параллельно проверяемых сервисов")
    parser.add_argument("--per-service-workers", type=int, default=2, help="Параллельные DIRECT-запросы к одному сервису")
    parser.add_argument("--values-workers", type=int, default=8, help="Параллельные /values к одному сервису")
    parser.add_argument("--samples", type=int, default=4, help="Количество дат в diagnostics/timeframes")
    parser.add_argument("--causal-samples", type=int, default=2)
    parser.add_argument("--lookback-days", type=int, default=10)
    parser.add_argument("--target-end", default="2026-08-04")
    parser.add_argument("--http-timeout", type=int, default=8)
    parser.add_argument("--direct-timeout", type=int, default=75)
    parser.add_argument("--values-timeout", type=int, default=25)
    parser.add_argument("--causal-timeout", type=int, default=120)
    parser.add_argument("--skip-causal", action="store_true", help="Максимально быстрый запуск без future_leak")
    parser.add_argument(
        "--port-overrides",
        default=os.environ.get("AUDIT_PORT_OVERRIDES", ""),
        help='Исключения портов, пример: "35:8898,42:8904"',
    )
    args = parser.parse_args()
    args.target_end = datetime.strptime(args.target_end, "%Y-%m-%d")

    port_overrides = parse_port_overrides(args.port_overrides)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PROGRESS_LOG.write_text("", encoding="utf-8")

    report: dict[str, Any] = {
        "created_at": datetime.now().isoformat(),
        "settings": vars(args) | {"target_end": args.target_end.isoformat()},
        "services": [],
        "summary": {},
    }

    log(
        "[AUDIT] START "
        f"services={len(SERVICES)} workers={args.workers} "
        f"per_service_workers={args.per_service_workers} "
        f"values_workers={args.values_workers} samples={args.samples} "
        f"skip_causal={args.skip_causal}"
    )

    started = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(audit_service, service_id, args, port_overrides): service_id
            for service_id in SERVICES
        }

        for future in as_completed(future_map):
            service_id = future_map[future]

            try:
                rec = future.result()
            except Exception as exc:
                log(f"[SERVICE {service_id}] UNHANDLED ERROR {type(exc).__name__}: {exc}")
                rec = {
                    "id": service_id,
                    "port": discover_port(service_id, port_overrides),
                    "metadata": {},
                    "ranges": {},
                    "direct": [],
                    "values": [],
                    "causal": [],
                    "classification": ["UNHANDLED_ERROR"],
                    "error": f"{type(exc).__name__}: {exc}",
                    "elapsed": 0.0,
                }

            report["services"].append(rec)
            report["services"].sort(key=lambda item: item["id"])
            report["summary"] = calculate_summary(report["services"])
            text_lines = build_text_report(report)
            save_partial(report, text_lines)

            log(
                f"[AUDIT] PROGRESS completed={len(report['services'])}/{len(SERVICES)} "
                f"service={service_id}"
            )

            if STOP_EVENT.is_set():
                break

    report["finished_at"] = datetime.now().isoformat()
    report["elapsed"] = round(time.monotonic() - started, 3)
    report["summary"] = calculate_summary(report["services"])

    text_lines = build_text_report(report)
    save_partial(report, text_lines)

    log(
        f"[AUDIT] DONE elapsed={report['elapsed']:.2f}s "
        f"completed={len(report['services'])}/{len(SERVICES)} "
        f"summary={json.dumps(report['summary'], ensure_ascii=False)}"
    )
    log(f"[AUDIT] JSON={OUT_JSON}")
    log(f"[AUDIT] TEXT={OUT_TXT}")
    log(f"[AUDIT] PROGRESS_LOG={PROGRESS_LOG}")

    return 0 if len(report["services"]) == len(SERVICES) else 2


if __name__ == "__main__":
    sys.exit(main())
