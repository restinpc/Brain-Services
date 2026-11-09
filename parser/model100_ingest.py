"""Local, source-neutral Model100 market-data normalization helpers.

This module intentionally has no network client or database connection. It parses
files already obtained under the applicable provider terms; the Brain deployment
wrapper can be added only after source access and table replication are approved.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import lzma
import math
import statistics
import struct
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence

UTC = timezone.utc
SCHEMA_VERSION = "model100-market-v1"
PARSER_VERSION = "0.1.0"


def utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        raise ValueError("timestamp must have an explicit timezone")
    return dt.astimezone(UTC)


def iso(dt: datetime | None) -> str | None:
    return utc(dt).isoformat(timespec="microseconds").replace("+00:00", "Z") if dt else None


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class QuoteTick:
    event_at: datetime
    bid: float
    ask: float
    bid_quoted_volume: float | None = None
    ask_quoted_volume: float | None = None
    brain_first_received_at: datetime | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "event_at", utc(self.event_at))
        if self.brain_first_received_at is not None:
            object.__setattr__(self, "brain_first_received_at", utc(self.brain_first_received_at))
        for name, value in (("bid", self.bid), ("ask", self.ask)):
            if not math.isfinite(value) or not 0.5 <= value <= 2.0:
                raise ValueError(f"EUR/USD {name} outside sanity range")
        if self.bid > self.ask:
            raise ValueError("SPOT_BID must be <= SPOT_ASK")
        for name in ("bid_quoted_volume", "ask_quoted_volume"):
            value = getattr(self, name)
            if value is not None and (not math.isfinite(value) or value < 0):
                raise ValueError(f"{name} must be finite and nonnegative")


def _decode_lzma(payload: bytes) -> bytes:
    """Decode XZ/Alone or Dukascopy's 5-byte-properties raw LZMA1 container."""
    if not payload:
        return b""
    try:
        return lzma.decompress(payload)
    except lzma.LZMAError:
        if len(payload) < 6:
            raise
        props = payload[0]
        lc = props % 9
        remainder = props // 9
        lp = remainder % 5
        pb = remainder // 5
        if pb > 4:
            raise ValueError("invalid LZMA properties in BI5 header")
        dict_size = struct.unpack("<I", payload[1:5])[0]
        return lzma.decompress(payload[5:], format=lzma.FORMAT_RAW,
            filters=[{"id": lzma.FILTER_LZMA1, "dict_size": dict_size,
                      "lc": lc, "lp": lp, "pb": pb}])


def decode_dukascopy_bi5(payload: bytes, period_start: datetime, *,
                         point_value: int = 100_000, period: str = "day") -> list[QuoteTick]:
    """Decode a BI5 file; period_start is UTC midnight or the legacy hour start."""
    start = utc(period_start)
    if period not in {"day", "hour"}:
        raise ValueError("period must be 'day' or 'hour'")
    if point_value <= 0:
        raise ValueError("point_value must be positive")
    raw = _decode_lzma(payload)
    if len(raw) % 20:
        raise ValueError("BI5 payload length is not a multiple of 20 bytes")
    ticks: list[QuoteTick] = []
    for offset in range(0, len(raw), 20):
        ms, ask_i, bid_i, ask_size, bid_size = struct.unpack_from(">IIIff", raw, offset)
        event_at = start + timedelta(milliseconds=ms)
        tick = QuoteTick(event_at, bid_i / point_value, ask_i / point_value,
                         bid_size, ask_size)
        ticks.append(tick)
    return ticks


def aggregate_quote_ticks(ticks: Iterable[QuoteTick], *, provider: str,
                          instrument: str = "EUR/USD", batch_sha256: str | None = None,
                          source_archive_available_at: datetime | None = None) -> list[dict]:
    """Build completed UTC H1 rows; quoted snapshot sizes are averaged/last, never summed."""
    grouped: dict[datetime, list[QuoteTick]] = defaultdict(list)
    for tick in ticks:
        start = tick.event_at.replace(minute=0, second=0, microsecond=0)
        grouped[start].append(tick)
    out = []
    archive_at = utc(source_archive_available_at) if source_archive_available_at else None
    for start in sorted(grouped):
        rows = sorted(grouped[start], key=lambda row: row.event_at)
        unique, seen = [], set()
        for row in rows:
            key = (row.event_at, row.bid, row.ask, row.bid_quoted_volume, row.ask_quoted_volume)
            if key not in seen:
                seen.add(key)
                unique.append(row)
        rows = unique
        if not rows:
            continue
        end = start + timedelta(hours=1)
        spreads = [r.ask - r.bid for r in rows]
        bid_changes = ask_changes = quote_changes = 0
        for previous, current in zip(rows, rows[1:]):
            bid_changed = (current.bid, current.bid_quoted_volume) != (previous.bid, previous.bid_quoted_volume)
            ask_changed = (current.ask, current.ask_quoted_volume) != (previous.ask, previous.ask_quoted_volume)
            bid_changes += int(bid_changed)
            ask_changes += int(ask_changed)
            quote_changes += int(bid_changed or ask_changed)
        mids = [(r.bid + r.ask) / 2 for r in rows]
        rv = math.sqrt(sum(math.log(b / a) ** 2 for a, b in zip(mids, mids[1:]) if a > 0 and b > 0))
        bvol = [r.bid_quoted_volume for r in rows if r.bid_quoted_volume is not None]
        avol = [r.ask_quoted_volume for r in rows if r.ask_quoted_volume is not None]
        imbalances = [(b - a) / (b + a) for b, a in zip(
            [r.bid_quoted_volume for r in rows], [r.ask_quoted_volume for r in rows])
            if b is not None and a is not None and b + a > 0]
        receipts = [r.brain_first_received_at for r in rows]
        received_at = max(receipts) if receipts and all(t is not None for t in receipts) else None
        feature_available = max([end, received_at, archive_at] if received_at or archive_at else [end])
        if received_at:
            asof = "BRAIN_RECEIVED"
        elif archive_at:
            asof = "SOURCE_ARCHIVE_AVAILABLE"
        else:
            asof = "HISTORICAL_RECONSTRUCTED"
        out.append({
            "provider": provider, "instrument": instrument, "market_type": "SPOT_FX_QUOTE",
            "window_start_at": iso(start), "window_end_at": iso(end),
            "feature_available_at": iso(feature_available),
            "source_archive_available_at": iso(archive_at),
            "brain_first_received_at": iso(received_at),
            "SPOT_BID": rows[-1].bid, "SPOT_ASK": rows[-1].ask,
            "SPOT_SPREAD": spreads[-1],
            "SPOT_SPREAD_MEAN": statistics.fmean(spreads),
            "SPOT_SPREAD_MEDIAN": statistics.median(spreads),
            "SPOT_SPREAD_MAX": max(spreads),
            "SPOT_SPREAD_STDDEV": statistics.pstdev(spreads) if len(spreads) > 1 else 0.0,
            "QUOTE_TICK_COUNT": len(rows), "QUOTE_CHANGE_COUNT": quote_changes,
            "BID_UPDATE_COUNT": bid_changes, "ASK_UPDATE_COUNT": ask_changes,
            "BEST_BID_QUOTED_VOLUME_MEAN": statistics.fmean(bvol) if bvol else None,
            "BEST_BID_QUOTED_VOLUME_LAST": rows[-1].bid_quoted_volume,
            "BEST_ASK_QUOTED_VOLUME_MEAN": statistics.fmean(avol) if avol else None,
            "BEST_ASK_QUOTED_VOLUME_LAST": rows[-1].ask_quoted_volume,
            "TOP_OF_BOOK_QUOTE_IMBALANCE": statistics.fmean(imbalances) if imbalances else None,
            "INTRAHOUR_REALIZED_VOLATILITY": rv,
            "volume_type": "BEST_QUOTE_SIZE_SNAPSHOT",
            "ASOF_STATUS": asof, "quality_status": "VALIDATED",
            "parser_version": PARSER_VERSION, "schema_version": SCHEMA_VERSION,
            "source_batch_sha256": batch_sha256,
        })
    return out


def parse_fxcm_h1_csv(text: str, *, instrument: str = "EUR/USD") -> list[dict]:
    """Parse FXCM public H1 Bid/Ask candles (timestamps are UTC per FXCM/MarketData)."""
    reader = csv.DictReader(io.StringIO(text))
    expected = {"DateTime", "BidOpen", "BidHigh", "BidLow", "BidClose",
                "AskOpen", "AskHigh", "AskLow", "AskClose"}
    if not reader.fieldnames or not expected.issubset(reader.fieldnames):
        raise ValueError("FXCM H1 file is missing required Bid/Ask OHLC columns")
    out = []
    seen = set()
    for row in reader:
        event = datetime.strptime(row["DateTime"], "%m/%d/%Y %H:%M:%S.%f").replace(tzinfo=UTC)
        values = {k: float(row[k]) for k in expected if k != "DateTime"}
        if not all(math.isfinite(v) and 0.5 <= v <= 2.0 for v in values.values()):
            raise ValueError("FXCM EUR/USD price outside sanity range")
        for suffix in ("Open", "High", "Low", "Close"):
            if values["Bid" + suffix] > values["Ask" + suffix]:
                raise ValueError(f"FXCM Bid > Ask at {suffix}")
        key = (event, tuple(sorted(values.items())))
        if key in seen:
            continue
        seen.add(key)
        end = event + timedelta(hours=1)
        out.append({
            "provider": "FXCM", "instrument": instrument, "market_type": "SPOT_FX_BID_ASK_H1",
            "source_event_at": iso(event), "window_end_at": iso(end),
            "BID_OPEN": values["BidOpen"], "BID_HIGH": values["BidHigh"],
            "BID_LOW": values["BidLow"], "BID_CLOSE": values["BidClose"],
            "ASK_OPEN": values["AskOpen"], "ASK_HIGH": values["AskHigh"],
            "ASK_LOW": values["AskLow"], "ASK_CLOSE": values["AskClose"],
            "SPOT_SPREAD_OPEN": values["AskOpen"] - values["BidOpen"],
            "SPOT_SPREAD_CLOSE": values["AskClose"] - values["BidClose"],
            "QUOTE_TICK_COUNT": None, "BEST_BID_QUOTED_VOLUME": None,
            "BEST_ASK_QUOTED_VOLUME": None, "volume_type": "NONE",
            "SOURCE_ARCHIVE_AVAILABLE_AT": None, "BRAIN_FIRST_RECEIVED_AT": None,
            "ASOF_STATUS": "HISTORICAL_SOURCE_RECORD",
            "quality_status": "VALIDATED", "parser_version": PARSER_VERSION,
            "schema_version": SCHEMA_VERSION,
        })
    return out


def parse_fxcm_client_orderflow_csv(text: str, *, source_timezone=UTC,
                                    instrument: str | None = None) -> list[dict]:
    """Parse FXCM client transactions. Signed quantity is long/short client flow, not aggressor side."""
    reader = csv.DictReader(io.StringIO(text))
    required = {"DATETIME", "SYMBOL", "QUANTITY", "RATE"}
    if not reader.fieldnames or not required.issubset({x.upper() for x in reader.fieldnames}):
        raise ValueError("FXCM order-flow file must have DATETIME,SYMBOL,QUANTITY,RATE")
    out = []
    for raw in reader:
        row = {k.upper(): v for k, v in raw.items()}
        symbol = row["SYMBOL"].replace("/", "").upper()
        if instrument and symbol != instrument.replace("/", "").upper():
            continue
        stamp = datetime.strptime(row["DATETIME"], "%m/%d/%Y %H:%M:%S.%f").replace(tzinfo=source_timezone)
        quantity, price = float(row["QUANTITY"]), float(row["RATE"])
        if not math.isfinite(quantity) or not math.isfinite(price) or price <= 0:
            raise ValueError("FXCM order-flow record has invalid numeric values")
        direction = "LONG" if quantity > 0 else "SHORT" if quantity < 0 else "ZERO_OR_UNKNOWN"
        out.append({
            "provider": "FXCM", "provider_id": "FXCM_RETAIL_CLIENTS",
            "instrument": symbol, "market_type": "FXCM_CLIENT_TRANSACTION",
            "event_at": iso(stamp), "quantity_signed": quantity,
            "client_direction": direction, "rate": price,
            "volume_type": "FXCM_CLIENT_TRANSACTION_QUANTITY",
            "aggressor_side": None, "ASOF_STATUS": "HISTORICAL_SOURCE_RECORD",
            "quality_status": "VALIDATED", "parser_version": PARSER_VERSION,
            "schema_version": SCHEMA_VERSION,
        })
    return out


def parse_fxcm_volume_csv(text: str, *, source_timezone=UTC,
                          instrument: str | None = None) -> list[dict]:
    """Preserve FXCM's named volume metrics without assuming their product-sheet meanings."""
    reader = csv.DictReader(io.StringIO(text))
    required = {"DATETIME", "SYMBOL", "NAME", "VALUE"}
    if not reader.fieldnames or not required.issubset({x.upper() for x in reader.fieldnames}):
        raise ValueError("FXCM volume file must have DateTime,Symbol,Name,Value")
    out = []
    for raw in reader:
        row = {k.upper(): v for k, v in raw.items()}
        symbol = row["SYMBOL"].replace("/", "").upper()
        if instrument and symbol != instrument.replace("/", "").upper():
            continue
        stamp = datetime.strptime(row["DATETIME"], "%m/%d/%Y %H:%M:%S").replace(tzinfo=source_timezone)
        value = float(row["VALUE"])
        if not math.isfinite(value):
            raise ValueError("FXCM volume metric must be finite")
        out.append({"provider": "FXCM", "provider_id": "FXCM_RETAIL_CLIENTS",
            "instrument": symbol, "event_at": iso(stamp), "metric_name": row["NAME"],
            "metric_value": value, "volume_type": "FXCM_PROVIDER_METRIC_UNINTERPRETED",
            "executed_volume": None, "aggressor_side": None,
            "ASOF_STATUS": "HISTORICAL_SOURCE_RECORD", "quality_status": "VALIDATED",
            "parser_version": PARSER_VERSION, "schema_version": SCHEMA_VERSION})
    return out


def aggregate_fxcm_client_flow(rows: Iterable[Mapping]) -> list[dict]:
    grouped: dict[datetime, list[Mapping]] = defaultdict(list)
    for row in rows:
        event = datetime.fromisoformat(str(row["event_at"]).replace("Z", "+00:00")).astimezone(UTC)
        grouped[event.replace(minute=0, second=0, microsecond=0)].append(row)
    out = []
    for start, items in sorted(grouped.items()):
        long_vol = sum(float(r["quantity_signed"]) for r in items if float(r["quantity_signed"]) > 0)
        short_vol = sum(abs(float(r["quantity_signed"])) for r in items if float(r["quantity_signed"]) < 0)
        total = long_vol + short_vol
        out.append({
            "provider": "FXCM", "provider_id": "FXCM_RETAIL_CLIENTS",
            "instrument": items[0]["instrument"], "market_type": "FXCM_CLIENT_TRANSACTION",
            "window_start_at": iso(start), "window_end_at": iso(start + timedelta(hours=1)),
            "FXCM_CLIENT_LONG_VOLUME": long_vol, "FXCM_CLIENT_SHORT_VOLUME": short_vol,
            "FXCM_CLIENT_SIGNED_FLOW": long_vol - short_vol,
            "FXCM_CLIENT_TOTAL_VOLUME": total, "FXCM_CLIENT_TRANSACTION_COUNT": len(items),
            "FXCM_CLIENT_VOLUME_IMBALANCE": ((long_vol - short_vol) / total) if total else None,
            "BUY_AGGRESSOR_VOLUME": None, "SELL_AGGRESSOR_VOLUME": None,
            "SIGNED_TRADE_DELTA": None,
            "volume_type": "FXCM_CLIENT_POSITION_DIRECTION_NOT_GLOBAL_AGGRESSOR",
            "ASOF_STATUS": "HISTORICAL_RECONSTRUCTED", "quality_status": "VALIDATED",
            "parser_version": PARSER_VERSION, "schema_version": SCHEMA_VERSION,
        })
    return out


def aggregate_databento_trades(rows: Iterable[Mapping], *, contract: str,
                               venue: str = "CME") -> list[dict]:
    """Aggregate Databento Trades rows; side B/A denotes aggressor, absent side stays separate."""
    grouped: dict[datetime, list[Mapping]] = defaultdict(list)
    for row in rows:
        stamp = row.get("ts_event", row.get("timestamp"))
        if isinstance(stamp, (int, float)):
            event = datetime.fromtimestamp(stamp / (1_000_000_000 if stamp > 10**12 else 1), UTC)
        elif isinstance(stamp, datetime):
            event = utc(stamp)
        else:
            event = datetime.fromisoformat(str(stamp).replace("Z", "+00:00")).astimezone(UTC)
        grouped[event.replace(minute=0, second=0, microsecond=0)].append(row)
    out = []
    for start, items in sorted(grouped.items()):
        buy = sell = unknown = 0.0
        count = 0
        for row in items:
            size = float(row.get("size", row.get("quantity", 0)))
            if not math.isfinite(size) or size < 0:
                raise ValueError("Databento trade size must be finite and nonnegative")
            side = str(row.get("side", "N")).upper()
            if side in {"B", "BUY", "1"}:
                buy += size
            elif side in {"A", "S", "SELL", "2"}:
                sell += size
            else:
                unknown += size
            count += 1
        known = buy + sell
        out.append({
            "provider": "Databento", "venue": venue, "instrument": "6E",
            "contract": contract, "market_type": "CME_EURO_FX_FUTURES",
            "window_start_at": iso(start), "window_end_at": iso(start + timedelta(hours=1)),
            "BUY_AGGRESSOR_VOLUME": buy, "SELL_AGGRESSOR_VOLUME": sell,
            "UNKNOWN_AGGRESSOR_VOLUME": unknown,
            "SIGNED_TRADE_DELTA": buy - sell,
            "TOTAL_EXECUTED_VOLUME": buy + sell + unknown,
            "TRADE_COUNT": count,
            "TRADE_VOLUME_IMBALANCE": ((buy - sell) / known) if known else None,
            "volume_type": "CME_EXECUTED_CONTRACTS",
            "ASOF_STATUS": "HISTORICAL_RECONSTRUCTED", "quality_status": "VALIDATED",
            "parser_version": PARSER_VERSION, "schema_version": SCHEMA_VERSION,
        })
    return out


def inspect_gdelt_export(path: str | Path, *, archive_stamp: datetime | None = None) -> dict:
    """Read the first GDELT Events row and preserve DATEADDED separately from event date.

    Production parsing remains in Brain's existing GDELT.py. The explicit archive stamp
    comes from its signed/indexed file name; DATEADDED is that source batch's availability.
    """
    with zipfile.ZipFile(path) as archive:
        members = [m for m in archive.namelist() if not m.endswith("/")]
        if len(members) != 1:
            raise ValueError("GDELT ZIP must contain exactly one member")
        with archive.open(members[0]) as stream:
            line = stream.readline().decode("utf-8-sig").rstrip("\r\n")
    fields = line.split("\t")
    if len(fields) < 3:
        raise ValueError("GDELT export row is malformed")
    archive_at = datetime.strptime(fields[-2], "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    if archive_stamp is not None and archive_at != utc(archive_stamp):
        raise ValueError("GDELT DATEADDED and archive stamp disagree")
    event_day = datetime.strptime(fields[1], "%Y%m%d").replace(tzinfo=UTC)
    return {"record_type": "GDELT_EVENT", "event_date": iso(event_day),
            "source_archive_available_at": iso(archive_at),
            "brain_first_received_at": None,
            "ASOF_STATUS": "SOURCE_ARCHIVE_RECONSTRUCTED",
            "source_archive_name": members[0]}
