"""GDELT 2.0 Events/Mentions/GKG → MySQL, архив с 2015-02-19.

python parsers/GDELT.py sasha_gdelt_news --start 2019-01-01 --end 2019-01-02
Даты отбора — время архивного 15-минутного пакета, а не публикации.
Самостоятельный файл: служебный код и схема БД включены в этот скрипт.
Зависимости: pip install requests python-dotenv mysql-connector-python
.env: рядом со скриптом, иначе на один каталог выше.
"""
from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import json
import math
import os
import re
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit


def utc_now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def timestamp(value):
    """UTC без tzinfo для MySQL DATETIME; неизвестное время остаётся NULL."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        result = value
    else:
        value = str(value).strip()
        if re.fullmatch(r"\d{14}", value):
            result = datetime.strptime(value, "%Y%m%d%H%M%S")
        elif re.fullmatch(r"\d{8}", value):
            result = datetime.strptime(value, "%Y%m%d")
        else:
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is not None:
        result = result.astimezone(timezone.utc).replace(tzinfo=None)
    return result


def json_text(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, allow_nan=False)


def digest(value):
    return hashlib.sha256(json_text(value).encode("utf-8")).hexdigest()


def record(provider, kind, raw, **fields):
    row = dict.fromkeys(DB_COLUMNS)
    row.update(provider=provider, record_type=kind, raw_payload=raw,
               entities=[], themes=[], relationships=[])
    row.update(fields)
    # Сохраняем разные версии/аналитические строки, удаляем только точные повторы.
    row["record_key"] = digest([provider, kind, raw])
    return row


def argument_parser(description):
    from dotenv import load_dotenv
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    # Standalone: .env next to this file; repository layout: .env one level above.
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.is_file():
        env_path = env_path.parent.parent / ".env"
    load_dotenv(env_path)
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("table_name", help="Целевая таблица MySQL")
    for name, env, default in [("host", "DB_HOST", None), ("port", "DB_PORT", "3306"),
                               ("user", "DB_USER", None), ("password", "DB_PASSWORD", None),
                               ("database", "DB_NAME", None)]:
        parser.add_argument(name, nargs="?", default=os.getenv(env, default))
    parser.add_argument("--start", default="2019-01-01", help="UTC, включительно; по умолчанию 2019-01-01")
    parser.add_argument("--end", default=utc_now().date().isoformat(), help="UTC, исключительно; по умолчанию начало сегодня")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-units", type=int, default=0, help="Максимум новых файлов/интервалов; 0 = все")
    parser.add_argument("--jsonl", type=Path, help="Писать JSONL вместо MySQL (без checkpoint, режим append)")
    parser.add_argument("--replay", action="store_true", help="Повторно обработать завершённые блоки; строки дедуплицируются")
    return parser


def validate_args(args):
    # Оставляем место для суффикса _state (MySQL: максимум 64 символа).
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,57}", args.table_name):
        raise ValueError("Имя таблицы: 1–58 латинских букв, цифр или _, первая не цифра")
    args.start = timestamp(args.start)
    args.end = timestamp(args.end)
    if args.start >= args.end:
        raise ValueError("--start должен быть меньше --end")
    if args.batch_size < 1 or args.max_units < 0:
        raise ValueError("--batch-size > 0, --max-units >= 0")


def http_session():
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET"], respect_retry_after_header=True)
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "BrainHistoricalNews/1.0"
    return session


def download(session, url, target):
    """Скачиваем в файл, без хранения всего архива в RAM."""
    from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
    # Retry в HTTPAdapter не повторяет ошибки, возникшие ПОСЛЕ получения headers.
    for attempt in range(3):
        try:
            with session.get(url, stream=True, timeout=(20, 60)) as response:
                response.raise_for_status()
                with open(target, "wb") as out:
                    for chunk in response.iter_content(1024 * 1024):
                        out.write(chunk)
            return
        except (ChunkedEncodingError, ConnectionError, Timeout):
            if attempt == 2:
                raise
            print(f"Сбой при скачивании, повтор {attempt + 2}/3", flush=True)
            time.sleep(2 ** attempt)


GKG_FEATURE_COLUMNS = ("v2_themes", "v2_locations", "v2_persons", "v2_organizations",
                       "v2_tone", "gcam", "amounts", "dates", "all_names", "feature_parse_errors")
EXTENDED_COLUMNS = {
    "available_at": "DATETIME(6)",
    "observation_id": "CHAR(64) CHARACTER SET ascii COLLATE ascii_bin",
    **{name: "JSON" for name in GKG_FEATURE_COLUMNS},
}
DB_COLUMNS = ("record_key", "provider", "record_type", "publication_id", "event_id", "story_id",
           "headline", "full_text", "source", "source_url", "published_at", "observed_at",
           "updated_at", "event_date", "date_iso", "entities", "themes", "relationships", "raw_payload",
           *EXTENDED_COLUMNS)
JSON_COLUMNS = {"entities", "themes", "relationships", "raw_payload", *GKG_FEATURE_COLUMNS}


class NewsSink:
    """Коммит по пакетам; checkpoint только после успешного чтения всего блока.

    После сбоя блок читается заново: уникальный hash делает повтор безопасным.
    В JSONL намеренно нет checkpoint: повторный запуск добавляет строки ещё раз.
    """
    def __init__(self, args):
        self.args = args
        self.conn = None
        self.out = None
        self.table = args.table_name
        self.state = self.table + "_state"

    def __enter__(self):
        if self.args.jsonl:
            self.args.jsonl.parent.mkdir(parents=True, exist_ok=True)
            self.out = self.args.jsonl.open("a", encoding="utf-8")
        else:
            if not all([self.args.host, self.args.user, self.args.database]) or self.args.password is None:
                raise ValueError("Задайте DB_HOST/DB_USER/DB_PASSWORD/DB_NAME или --jsonl")
            import mysql.connector
            self.conn = mysql.connector.connect(host=self.args.host, port=int(self.args.port),
                user=self.args.user, password=self.args.password, database=self.args.database,
                charset="utf8mb4", time_zone="+00:00", use_pure=True, autocommit=False,
                connection_timeout=20)
            try:
                self.ensure_table()
            except BaseException:
                self.conn.close()
                raise
        return self

    def __exit__(self, *_):
        if self.conn:
            self.conn.close()
        if self.out:
            self.out.close()

    def ensure_table(self):
        with self.conn.cursor() as cur:
            cur.execute(f"""CREATE TABLE IF NOT EXISTS `{self.table}` (
                id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                record_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
                provider VARCHAR(24) NOT NULL, record_type VARCHAR(32) NOT NULL,
                publication_id VARCHAR(512), event_id VARCHAR(512), story_id VARCHAR(512),
                headline LONGTEXT, full_text LONGTEXT, source TEXT, source_url LONGTEXT,
                published_at DATETIME(6), observed_at DATETIME(6), updated_at DATETIME(6),
                event_date DATE, date_iso DATE,
                entities JSON NOT NULL, themes JSON NOT NULL, relationships JSON NOT NULL,
                raw_payload JSON NOT NULL,
                loaded_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                UNIQUE KEY uq_record (record_key), INDEX idx_date (date_iso),
                INDEX idx_published (published_at), INDEX idx_observed (observed_at),
                INDEX idx_event (event_id(128)), INDEX idx_story (story_id(128)),
                INDEX idx_publication (publication_id(128))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
            self.ensure_extended_columns(cur)
            cur.execute(f"""CREATE TABLE IF NOT EXISTS `{self.state}` (
                unit_key CHAR(64) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
                unit_name TEXT NOT NULL, rows_read BIGINT NOT NULL,
                completed_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""")
        self.conn.commit()

    def ensure_extended_columns(self, cur):
        """Additive migration for tables created by earlier parser versions."""
        cur.execute("SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s", (self.table,))
        existing = {item[0] for item in cur.fetchall()}
        additions = [f"ADD COLUMN `{name}` {definition}" for name, definition in EXTENDED_COLUMNS.items()
                     if name not in existing]
        cur.execute("SELECT DISTINCT INDEX_NAME FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s", (self.table,))
        indexes = {item[0] for item in cur.fetchall()}
        for name, columns in {
            "idx_available": "available_at",
            "idx_observation": "observation_id",
            "idx_publication_available": "publication_id(128), available_at",
        }.items():
            if name not in indexes:
                additions.append(f"ADD INDEX `{name}` ({columns})")
        if additions:
            cur.execute(f"ALTER TABLE `{self.table}` " + ", ".join(additions))

    def completed(self, unit):
        if not self.conn or self.args.replay:
            return False
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT 1 FROM `{self.state}` WHERE unit_key=%s", (digest(unit),))
            return cur.fetchone() is not None

    def save_rows(self, rows):
        if self.out:
            for row in rows:
                self.out.write(json_text(dict(row, loaded_at=utc_now())) + "\n")
            self.out.flush()
            return
        sql = (f"INSERT INTO `{self.table}` ({','.join('`'+c+'`' for c in DB_COLUMNS)}) "
               f"VALUES ({','.join(['%s'] * len(DB_COLUMNS))}) "
               "ON DUPLICATE KEY UPDATE " + ",".join(
                   f"`{c}`=VALUES(`{c}`)" for c in DB_COLUMNS if c not in {"record_key", "raw_payload"}))
        # Replays refresh derived fields; original loaded_at and raw identity are unchanged.
        values = [tuple(json_text(row[c]) if c in JSON_COLUMNS and row.get(c) is not None
                        else row.get(c) for c in DB_COLUMNS)
                  for row in rows]
        with self.conn.cursor() as cur:
            cur.executemany(sql, values)
        self.conn.commit()

    def consume(self, unit, rows):
        count, batch = 0, []
        for row in rows:
            if row["provider"] == "gdelt":
                # Never backdate GDELT training buckets to a publication/event date.
                dt = row.get("available_at")
                if dt is None:
                    raise ValueError("GDELT: available_at обязателен для записи наблюдения")
                row["date_iso"] = dt.date()
            else:
                dt = row.get("published_at") or row.get("observed_at")
                row["date_iso"] = dt.date() if dt else row.get("event_date")
            batch.append(row)
            count += 1
            if len(batch) >= self.args.batch_size:
                self.save_rows(batch)
                batch = []
        if batch:
            self.save_rows(batch)
        if self.conn:
            with self.conn.cursor() as cur:
                cur.execute(f"INSERT INTO `{self.state}` (unit_key,unit_name,rows_read) VALUES (%s,%s,%s) "
                            "ON DUPLICATE KEY UPDATE rows_read=VALUES(rows_read), completed_at=UTC_TIMESTAMP(6)",
                            (digest(unit), json_text(unit), count))
            self.conn.commit()
        print(f"Завершён блок: прочитано {count} строк (повторы в MySQL обновлены без дублирования)")
        return count


def run(main):
    try:
        main()
    except KeyboardInterrupt:
        print("Прервано. Незавершённый блок будет прочитан повторно.", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:
        message = str(exc)
        for name, value in os.environ.items():
            if value and any(part in name.upper() for part in ("PASSWORD", "SECRET", "TOKEN", "API_KEY")):
                message = message.replace(value, "[REDACTED]")
        print(f"Ошибка {type(exc).__name__}: {message}", file=sys.stderr)
        sys.exit(1)

MANIFEST = "https://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
EVENT_COLUMNS = """GLOBALEVENTID SQLDATE MonthYear Year FractionDate
Actor1Code Actor1Name Actor1CountryCode Actor1KnownGroupCode Actor1EthnicCode Actor1Religion1Code Actor1Religion2Code Actor1Type1Code Actor1Type2Code Actor1Type3Code
Actor2Code Actor2Name Actor2CountryCode Actor2KnownGroupCode Actor2EthnicCode Actor2Religion1Code Actor2Religion2Code Actor2Type1Code Actor2Type2Code Actor2Type3Code
IsRootEvent EventCode EventBaseCode EventRootCode QuadClass GoldsteinScale NumMentions NumSources NumArticles AvgTone
Actor1Geo_Type Actor1Geo_FullName Actor1Geo_CountryCode Actor1Geo_ADM1Code Actor1Geo_ADM2Code Actor1Geo_Lat Actor1Geo_Long Actor1Geo_FeatureID
Actor2Geo_Type Actor2Geo_FullName Actor2Geo_CountryCode Actor2Geo_ADM1Code Actor2Geo_ADM2Code Actor2Geo_Lat Actor2Geo_Long Actor2Geo_FeatureID
ActionGeo_Type ActionGeo_FullName ActionGeo_CountryCode ActionGeo_ADM1Code ActionGeo_ADM2Code ActionGeo_Lat ActionGeo_Long ActionGeo_FeatureID DATEADDED SOURCEURL""".split()
MENTION_COLUMNS = """GLOBALEVENTID EventTimeDate MentionTimeDate MentionType MentionSourceName MentionIdentifier SentenceID Actor1CharOffset Actor2CharOffset ActionCharOffset InRawText Confidence MentionDocLen MentionDocTone MentionDocTranslationInfo Extras""".split()
GKG_COLUMNS = """GKGRECORDID DATE SourceCollectionIdentifier SourceCommonName DocumentIdentifier Counts V2Counts Themes V2Themes Locations V2Locations Persons V2Persons Organizations V2Organizations V2Tone Dates GCAM SharingImage RelatedImages SocialImageEmbeds SocialVideoEmbeds Quotations AllNames Amounts TranslationInfo Extras""".split()
COLUMNS = {"export": EVENT_COLUMNS, "mentions": MENTION_COLUMNS, "gkg": GKG_COLUMNS}
CHECKPOINT_VERSION = "gdelt-v2"
TONE_FIELDS = ("tone", "positive_score", "negative_score", "polarity",
               "activity_reference_density", "self_group_reference_density", "word_count")


def publication_id(url):
    return "url:" + digest(url) if url else None


def observation_id(url, available_at):
    # Structured input avoids ambiguous concatenation; publication identity stays unchanged.
    return digest([url, available_at.isoformat(timespec="seconds")]) if url and available_at else None


def availability(value):
    if not re.fullmatch(r"\d{14}", value or ""):
        raise ValueError("GDELT: отсутствует корректное время доступности (YYYYMMDDHHMMSS)")
    return timestamp(value)


def gkg_batch_time(record_id):
    match = re.fullmatch(r"(\d{14})-T?\d+", record_id)
    if not match:
        raise ValueError("GDELT: некорректный GKGRECORDID; нельзя определить available_at")
    return availability(match.group(1))


def number(value):
    if value == "":
        return None
    if re.fullmatch(r"[+-]?\d+", value):
        return int(value)
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("число должно быть конечным")
    return result


def name_offset(value, key="name"):
    name, offset = value.rsplit(",", 1)
    return {key: name, "offset": int(offset)}


def location(value):
    kind, name, country, adm1, adm2, lat, lon, feature_id, offset = value.split("#")
    return {"type": int(kind), "name": name, "country_code": country,
            "adm1_code": adm1, "adm2_code": adm2, "latitude": number(lat),
            "longitude": number(lon), "feature_id": feature_id, "offset": int(offset)}


def amount(value):
    numeric, rest = value.split(",", 1)
    obj, offset = rest.rsplit(",", 1)
    return {"amount": number(numeric), "object": obj, "offset": int(offset)}


def date_reference(value):
    # Live archives use '#'; the 2.1 codebook also describes a comma form.
    resolution, month, day, year, offset = value.split("#" if "#" in value else ",")
    # A zero denotes an unknown date component; never infer a year from the batch.
    return dict(zip(("resolution", "month", "day", "year", "offset"),
                    map(int, (resolution, month, day, year, offset))))


def gkg_features(raw):
    """Typed GKG 2.1 features, retaining repeats and character offsets."""
    errors = []

    def parse_items(column, parse, separator=";"):
        result = []
        for index, value in enumerate(raw[column].split(separator)):
            if not value:
                continue
            try:
                result.append(parse(value))
            except (ValueError, OverflowError) as exc:
                errors.append({"column": column, "index": index, "value": value, "error": str(exc)})
        return result

    features = {
        "v2_themes": parse_items("V2Themes", lambda value: name_offset(value, "theme")),
        "v2_locations": parse_items("V2Locations", location),
        "v2_persons": parse_items("V2Persons", name_offset),
        "v2_organizations": parse_items("V2Organizations", name_offset),
        "amounts": parse_items("Amounts", amount),
        "dates": parse_items("Dates", date_reference),
        "all_names": parse_items("AllNames", name_offset),
    }

    def tone(value):
        values = value.split(",")
        if len(values) != len(TONE_FIELDS):
            raise ValueError("V2Tone: ожидается 7 чисел")
        return dict(zip(TONE_FIELDS, [number(v) for v in values[:6]] + [int(values[6])]))

    features["v2_tone"] = None
    if raw["V2Tone"]:
        try:
            features["v2_tone"] = tone(raw["V2Tone"])
        except (ValueError, OverflowError) as exc:
            errors.append({"column": "V2Tone", "index": 0, "value": raw["V2Tone"], "error": str(exc)})
    gcam = {}

    def dimension(value):
        key, score = value.split(":", 1)
        if not key or key in gcam:
            raise ValueError("GCAM: пустой или повторный ключ")
        gcam[key] = number(score)

    parse_items("GCAM", dimension, ",")
    features["gcam"] = gcam
    features["feature_parse_errors"] = errors
    return features


def normalize(kind, fields, encoding_errors=None):
    columns = COLUMNS[kind]
    if len(fields) != len(columns):
        raise ValueError(f"GDELT {kind}: ожидается {len(columns)} столбцов, получено {len(fields)}")
    raw = dict(zip(columns, fields))
    if encoding_errors:
        raw["_encoding_errors"] = encoding_errors
    if kind == "export":
        url = raw["SOURCEURL"] or None
        available_at = availability(raw["DATEADDED"])
        entities = [{"role": f"actor{n}", "name": raw[f"Actor{n}Name"],
                     "code": raw[f"Actor{n}Code"], "country": raw[f"Actor{n}CountryCode"]}
                    for n in (1, 2) if raw[f"Actor{n}Name"] or raw[f"Actor{n}Code"]]
        return record("gdelt", "event", raw, event_id=raw["GLOBALEVENTID"],
            publication_id=publication_id(url), source_url=url,
            source=urlsplit(url).hostname if url else None,
            available_at=available_at, observed_at=available_at,
            observation_id=observation_id(url, available_at), event_date=timestamp(raw["SQLDATE"]).date(),
            entities=entities, themes=[{"taxonomy": "CAMEO", "code": raw["EventCode"]}])
    if kind == "mentions":
        url = raw["MentionIdentifier"]
        available_at = availability(raw["MentionTimeDate"])
        return record("gdelt", "mention", raw, event_id=raw["GLOBALEVENTID"],
            publication_id=publication_id(url), source=raw["MentionSourceName"],
            source_url=url if url.startswith(("https://", "http://")) else None,
            available_at=available_at, observed_at=available_at,
            observation_id=observation_id(url, available_at),
            relationships=[{"type": "mentions_event", "event_id": raw["GLOBALEVENTID"]}])
    url = raw["DocumentIdentifier"]
    available_at = gkg_batch_time(raw["GKGRECORDID"])
    entities = [{"type": kind_, "name": name} for column, kind_ in
                [("Persons", "person"), ("Organizations", "organization")]
                for name in raw[column].split(";") if name]
    return record("gdelt", "article_metadata", raw, publication_id=publication_id(url),
        source=raw["SourceCommonName"], source_url=url if url.startswith(("https://", "http://")) else None,
        published_at=timestamp(raw["DATE"]) if raw["DATE"] not in ("", "0") else None,
        available_at=available_at, observed_at=available_at,
        observation_id=observation_id(url, available_at), entities=entities,
        themes=[t for t in raw["Themes"].split(";") if t], **gkg_features(raw))


def archive_rows(path, kind):
    # GDELT CSV — TSV без quoting; кавычки внутри полей являются обычным текстом.
    csv.field_size_limit(16 * 1024 * 1024)
    with zipfile.ZipFile(path) as archive:
        members = [item for item in archive.infolist() if not item.is_dir()]
        if len(members) != 1:
            raise ValueError("GDELT: ZIP должен содержать ровно один TSV")
        with archive.open(members[0]) as data:
            damaged_rows, feature_errors = 0, 0
            with io.TextIOWrapper(data, encoding="utf-8-sig", errors="surrogateescape", newline="") as text:
                for fields in csv.reader(text, delimiter="\t", quoting=csv.QUOTE_NONE):
                    if fields:
                        encoding_errors = {}
                        for index, value in enumerate(fields):
                            if re.search(r"[\udc80-\udcff]", value):
                                original = value.encode("utf-8", errors="surrogateescape")
                                encoding_errors[str(index)] = base64.b64encode(original).decode("ascii")
                                fields[index] = original.decode("utf-8", errors="replace")
                        row = normalize(kind, fields, encoding_errors)
                        damaged_rows += bool(encoding_errors)
                        feature_errors += len(row.get("feature_parse_errors") or [])
                        yield row
            if damaged_rows or feature_errors:
                print(f"GDELT: строк с повреждённым UTF-8: {damaged_rows}; "
                      f"ошибок формата признаков: {feature_errors}; исходные значения сохранены", flush=True)


def manifest_entries(lines, start, end, streams):
    placeholders = 0
    for line in lines:
        parts = line.strip().split()
        if not parts:
            continue
        if parts in (["http://data.gdeltproject.org/gdeltv2/"], ["https://data.gdeltproject.org/gdeltv2/"]):
            # В реальном masterfilelist встречаются пустые записи без имени файла.
            placeholders += 1
            if placeholders == 1:
                print("GDELT: в индексе есть пустые записи поставщика; пропускаем только строки без имени файла", flush=True)
            continue
        if len(parts) != 3:
            raise ValueError("Некорректная строка GDELT masterfilelist")
        size, checksum, url = parts
        match = re.fullmatch(r"https?://data\.gdeltproject\.org/gdeltv2/(\d{14})\.(export|mentions|gkg)\.(?:CSV|csv)\.zip", url)
        if not match:
            raise ValueError("Неожиданное имя файла в GDELT masterfilelist")
        stamp, kind = match.groups()
        if kind in streams and start <= timestamp(stamp) < end:
            yield {"size": int(size), "md5": checksum, "url": url.replace("http://", "https://", 1),
                   "stamp": stamp, "kind": kind}
    if placeholders:
        print(f"GDELT: пустых записей в индексе {placeholders}", flush=True)


def verify_file(path, entry):
    md5 = hashlib.md5()  # Контрольная сумма поставщика, не криптографическая подпись.
    with path.open("rb") as data:
        for block in iter(lambda: data.read(1024 * 1024), b""):
            md5.update(block)
    if path.stat().st_size != entry["size"] or md5.hexdigest() != entry["md5"]:
        raise ValueError("GDELT: размер или MD5 скачанного архива не совпадает с манифестом")


def process(args, sink):
    selected = 0
    processed = 0
    with http_session() as session, tempfile.TemporaryDirectory(prefix="brain_gdelt_") as temp:
        # Храним индекс на диске: HTTP-соединение не остаётся открытым во время долгого backfill.
        index = args.manifest_file
        if index is None:
            index = Path(temp) / "masterfilelist.txt"
            print("GDELT: скачивание индекса архивов masterfilelist.txt", flush=True)
            download(session, MANIFEST, index)
            print(f"GDELT: индекс скачан ({index.stat().st_size:,} байт)", flush=True)
        with index.open(encoding="utf-8") as lines:
            for entry in manifest_entries(lines, args.start, args.end, args.streams.split(",")):
                selected += 1
                # Re-read v1 checkpoints once to backfill the new derived fields in place.
                unit = [CHECKPOINT_VERSION, entry["url"], entry["md5"]]
                if sink.completed(unit):
                    continue
                print(f"GDELT {entry['stamp']} {entry['kind']}")
                path = Path(temp) / "archive.zip"
                if args.archive_dir:
                    path = args.archive_dir / entry["url"].rsplit("/", 1)[1]
                else:
                    download(session, entry["url"], path)
                verify_file(path, entry)
                sink.consume(unit, archive_rows(path, entry["kind"]))
                processed += 1
                if args.max_units and processed >= args.max_units:
                    print("Достигнут --max-units; оставшиеся блоки доступны при следующем запуске")
                    break
    if not selected:
        raise ValueError("В манифесте нет архивов для выбранного периода")
    print(f"GDELT: обработано блоков {processed}")


def main():
    parser = argument_parser("GDELT 2.0 архив → MySQL")
    parser.add_argument("--streams", default="export,mentions,gkg", help="export,mentions,gkg или их подмножество")
    parser.add_argument("--manifest-file", type=Path, help="Локальный masterfilelist вместо загрузки индекса")
    parser.add_argument("--archive-dir", type=Path, help="Локальные ZIP вместо HTTP (для повторного импорта)")
    args = parser.parse_args()
    validate_args(args)
    if args.start < timestamp("2015-02-19"):
        parser.error("GDELT 2.0 поддерживается с 2015-02-19")
    if not set(args.streams.split(",")) <= COLUMNS.keys():
        parser.error("Допустимые streams: export,mentions,gkg")
    with NewsSink(args) as sink:
        print(f"GDELT: таблица {args.table_name}; период {args.start} — {args.end} UTC", flush=True)
        process(args, sink)


if __name__ == "__main__":
    run(main)
