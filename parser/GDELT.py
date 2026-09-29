"""GDELT 2.0 Events/Mentions/GKG → MySQL, архив с 2015-02-19.

python parsers/GDELT.py sasha_gdelt_news --start 2019-01-01 --end 2019-01-02
Даты отбора — время архивного 15-минутного пакета, а не публикации.
"""
from __future__ import annotations

import csv
import hashlib
import io
import re
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlsplit

try:
    from .news_common import argument_parser, digest, download, http_session, NewsSink, record, run, timestamp, validate_args
except ImportError:
    from news_common import argument_parser, digest, download, http_session, NewsSink, record, run, timestamp, validate_args

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


def publication_id(url):
    return "url:" + digest(url) if url else None


def normalize(kind, fields):
    columns = COLUMNS[kind]
    if len(fields) != len(columns):
        raise ValueError(f"GDELT {kind}: ожидается {len(columns)} столбцов, получено {len(fields)}")
    raw = dict(zip(columns, fields))
    if kind == "export":
        url = raw["SOURCEURL"] or None
        entities = [{"role": f"actor{n}", "name": raw[f"Actor{n}Name"],
                     "code": raw[f"Actor{n}Code"], "country": raw[f"Actor{n}CountryCode"]}
                    for n in (1, 2) if raw[f"Actor{n}Name"] or raw[f"Actor{n}Code"]]
        return record("gdelt", "event", raw, event_id=raw["GLOBALEVENTID"],
            publication_id=publication_id(url), source_url=url,
            source=urlsplit(url).hostname if url else None,
            observed_at=timestamp(raw["DATEADDED"]), event_date=timestamp(raw["SQLDATE"]).date(),
            entities=entities, themes=[{"taxonomy": "CAMEO", "code": raw["EventCode"]}])
    if kind == "mentions":
        url = raw["MentionIdentifier"]
        return record("gdelt", "mention", raw, event_id=raw["GLOBALEVENTID"],
            publication_id=publication_id(url), source=raw["MentionSourceName"],
            source_url=url if url.startswith(("https://", "http://")) else None,
            observed_at=timestamp(raw["MentionTimeDate"]),
            relationships=[{"type": "mentions_event", "event_id": raw["GLOBALEVENTID"]}])
    url = raw["DocumentIdentifier"]
    entities = [{"type": kind_, "name": name} for column, kind_ in
                [("Persons", "person"), ("Organizations", "organization")]
                for name in raw[column].split(";") if name]
    return record("gdelt", "article_metadata", raw, publication_id=publication_id(url),
        source=raw["SourceCommonName"], source_url=url if url.startswith(("https://", "http://")) else None,
        observed_at=timestamp(raw["DATE"]), entities=entities,
        themes=[t for t in raw["Themes"].split(";") if t])


def archive_rows(path, kind):
    # GDELT CSV — TSV без quoting; кавычки внутри полей являются обычным текстом.
    csv.field_size_limit(16 * 1024 * 1024)
    with zipfile.ZipFile(path) as archive:
        members = [item for item in archive.infolist() if not item.is_dir()]
        if len(members) != 1:
            raise ValueError("GDELT: ZIP должен содержать ровно один TSV")
        with archive.open(members[0]) as data:
            with io.TextIOWrapper(data, encoding="utf-8-sig", newline="") as text:
                for fields in csv.reader(text, delimiter="\t", quoting=csv.QUOTE_NONE):
                    if fields:
                        yield normalize(kind, fields)


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
                unit = ["gdelt-v1", entry["url"], entry["md5"]]
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
