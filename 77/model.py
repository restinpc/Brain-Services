"""
Shared brain_llm ensemble consensus/disagreement model for services 77/78.

This model intentionally does NOT reproduce the legacy PHP neuronet11 logic.
The old model aggregates squared individual LLM scores over a time window.
Services 77/78 instead treat every news item as an ensemble event and learns the
historical market reaction to a *type of committee decision*:

    pair × press × relative direction × agreement × dissenting-model role
    × disagreement regime

For every source news item the three configured LLMs are converted to a
relative base/quote score:

    EUR/USD = EUR score - USD score
    BTC/USD = BTC score - USD score
    ETH/USD = ETH score - USD score

The event keeps robust ensemble statistics (median/mean edge, dispersion,
coherence, agreement and confidence).  brain_framework then learns T1/extremum
reaction by context and lag.  All input news rows are causally clipped at the
target date before historical event selection, so a future news item can never
participate in an H1/D1 calculation.
"""
from __future__ import annotations

import math
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable

from brain_framework import get_service_config, run_standard_model


# ---------------------------------------------------------------------------
# Stable source contract
# ---------------------------------------------------------------------------

LLM_MODELS: tuple[str, ...] = (
    "gpt-oss:20b",
    "gemma3:12b",
    "deepseek-r1:8b",
)
CURRENCIES: tuple[str, ...] = ("usd", "eur", "btc", "eth")
PRESSES: tuple[str, ...] = ("cnn", "nyt", "twp", "tgd", "wsj")
PAIRS: dict[int, tuple[str, str, str]] = {
    1: ("eur", "usd", "eurusd"),
    3: ("btc", "usd", "btcusd"),
    4: ("eth", "usd", "ethusd"),
}
RATES_TO_PAIR: dict[str, int] = {
    "brain_rates_eur_usd": 1,
    "brain_rates_eur_usd_day": 1,
    "brain_rates_btc_usd": 3,
    "brain_rates_btc_usd_day": 3,
    "brain_rates_eth_usd": 4,
    "brain_rates_eth_usd_day": 4,
}

# Bump when the enrichment/classification semantics change.  The rebuild code
# will automatically discard incompatible enriched rows.
ENRICH_SCHEMA_VERSION = "3"

# Prompt asks for [-10, 10], while real brain_llm contains a small number of
# values outside that range.  Clamp only those malformed outputs.
LLM_SCORE_MIN = -10.0
LLM_SCORE_MAX = 10.0

# Disagreement thresholds follow the empirical scale of three relative scores.
DISPERSION_LOW_MAX = 1.5
DISPERSION_MID_MAX = 4.0
COHERENCE_FILTER_MIN = 0.60
CONFLICT_DISPERSION_MIN = 4.0

# Standard framework constants retained for old-style config readers/tools.
RATES_TABLE = "brain_rates_eur_usd"
FILTER_DATASET_BY_DATE = True
MODEL_USES_RATE_HISTORY = True
SHIFT_WINDOW = 12
VAR_RANGE = [0, 1, 2, 3, 4, 5]
TYPES_RANGE = [0, 1, 2, 3, 4]
USE_ML_VALUES = False


# ---------------------------------------------------------------------------
# Enrichment helpers
# ---------------------------------------------------------------------------

def _safe_score(value: Any) -> float:
    x = float(value)
    if not math.isfinite(x):
        return 0.0
    return max(LLM_SCORE_MIN, min(LLM_SCORE_MAX, x))


def _direction(edges: list[float]) -> str:
    med = float(statistics.median(edges))
    if med > 0:
        return "up"
    if med < 0:
        return "down"

    # If the robust center is exactly zero, require an actual two-model vote;
    # one extreme dissenting model is not allowed to manufacture direction.
    pos = sum(v > 0 for v in edges)
    neg = sum(v < 0 for v in edges)
    if pos >= 2:
        return "up"
    if neg >= 2:
        return "down"
    return "flat"


def _agreement(edges: list[float]) -> tuple[str, int, float]:
    pos = sum(v > 0 for v in edges)
    neg = sum(v < 0 for v in edges)
    vote = max(pos, neg)
    if vote == len(edges) and vote > 0:
        cls = "unanimous"
    elif vote >= 2:
        cls = "majority"
    else:
        cls = "mixed"
    return cls, vote, vote / float(len(edges) or 1)


def _dispersion_class(value: float) -> str:
    if value < DISPERSION_LOW_MAX:
        return "low"
    if value < DISPERSION_MID_MAX:
        return "mid"
    return "high"


def _vote_role(edges: list[float], agreement_class: str) -> str:
    """Preserve *which* LLM dissents without exploding to all 27 sign patterns.

    The real dump shows weak pairwise agreement between committee members, so
    the identity of the dissenting model is potentially informative.  For a
    2/3 majority we keep that identity; unanimous and fully mixed cases stay
    pooled.  On the supplied dump this raises context cardinality only from
    ~225 to ~405, while retaining healthy support per context.
    """
    if agreement_class == "unanimous":
        return "all"
    if agreement_class != "majority":
        return "mixed"

    pos = sum(v > 0 for v in edges)
    majority_sign = 1 if pos >= 2 else -1
    short_names = ("gpt", "gemma", "deepseek")
    for idx, value in enumerate(edges):
        sign = 1 if value > 0 else (-1 if value < 0 else 0)
        if sign != majority_sign:
            return f"dissent-{short_names[idx]}"
    return "majority"


def _event_row(
    *,
    press: str,
    news_id: int,
    date_dt: datetime,
    source_max_llm_id: int,
    pair_id: int,
    scores: dict[str, dict[str, float]],
) -> dict[str, Any]:
    base, quote, pair_code = PAIRS[pair_id]
    edges = [scores[m][base] - scores[m][quote] for m in LLM_MODELS]

    mean_edge = float(statistics.fmean(edges))
    median_edge = float(statistics.median(edges))
    dispersion = float(statistics.pstdev(edges))
    mean_abs = float(statistics.fmean(abs(v) for v in edges))
    coherence = (abs(mean_edge) / mean_abs) if mean_abs > 1e-12 else 0.0
    agreement_class, agreement_count, agreement_ratio = _agreement(edges)
    vote_role = _vote_role(edges, agreement_class)
    direction = _direction(edges)
    dispersion_class = _dispersion_class(dispersion)

    # Confidence is deliberately internal to the LLM committee.  It does not
    # use market prices, so enrichment cannot leak future market outcomes.
    strength_norm = min(abs(median_edge) / 10.0, 2.0)
    confidence = strength_norm * agreement_ratio * (0.5 + 0.5 * coherence)

    # Keep the context reasonably dense: magnitude remains continuous and is
    # tested through var=3 instead of exploding event_type cardinality.
    event_type = (
        f"{pair_code}|{press}|{direction}|{agreement_class}|"
        f"{vote_role}|{dispersion_class}"
    )

    return {
        "date_dt": date_dt,
        "press": press,
        "news_id": int(news_id),
        "pair_id": int(pair_id),
        "pair_code": pair_code,
        "event_type": event_type,
        # brain_framework's standard event parser calls this pct_change.  Here
        # it is a signed relative LLM edge, not a literal percentage.
        "pct_change": median_edge,
        "mean_edge": mean_edge,
        "median_edge": median_edge,
        "dispersion": dispersion,
        "coherence": coherence,
        "agreement_class": agreement_class,
        "agreement_count": agreement_count,
        "agreement_ratio": agreement_ratio,
        "confidence": confidence,
        "edge_gpt_oss": edges[0],
        "edge_gemma3": edges[1],
        "edge_deepseek": edges[2],
        "source_max_llm_id": int(source_max_llm_id),
    }


def _aggregate_select_sql(press: str, id_placeholders: str = "") -> str:
    if press not in PRESSES:
        raise ValueError(f"Unsupported press: {press}")

    score_columns: list[str] = []
    for mi, _model in enumerate(LLM_MODELS):
        for currency in CURRENCIES:
            score_columns.append(
                "MAX(CASE WHEN bl.model = :m{mi} AND bl.currency = '{currency}' "
                "THEN GREATEST(-10, LEAST(10, bl.result)) END) AS m{mi}_{currency}".format(
                    mi=mi, currency=currency
                )
            )

    id_filter = f" AND n.id IN ({id_placeholders})" if id_placeholders else ""
    return f"""
        SELECT
            n.id AS news_id,
            n.date AS date_dt,
            MAX(bl.id) AS source_max_llm_id,
            {', '.join(score_columns)}
        FROM `brain_{press}_news` n
        JOIN `brain_llm` bl
          ON bl.press = :press
         AND bl.news_id = n.id
        WHERE n.date IS NOT NULL
          AND bl.model IN (:m0, :m1, :m2)
          {id_filter}
        GROUP BY n.id, n.date
        ORDER BY n.date, n.id
    """


def _score_matrix(row: dict[str, Any]) -> dict[str, dict[str, float]] | None:
    scores: dict[str, dict[str, float]] = {}
    for mi, model_name in enumerate(LLM_MODELS):
        per_currency: dict[str, float] = {}
        for currency in CURRENCIES:
            value = row.get(f"m{mi}_{currency}")
            if value is None:
                return None
            per_currency[currency] = _safe_score(value)
        scores[model_name] = per_currency
    return scores


async def _ensure_enriched_tables(engine_vlad, enriched_table: str, meta_table: str) -> None:
    from sqlalchemy import text

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`                BIGINT       NOT NULL AUTO_INCREMENT,
                `date_dt`           DATETIME     NOT NULL,
                `press`             VARCHAR(3)   NOT NULL,
                `news_id`           INT          NOT NULL,
                `pair_id`           TINYINT      NOT NULL,
                `pair_code`         VARCHAR(8)   NOT NULL,
                `event_type`        VARCHAR(96)  NOT NULL,
                `pct_change`        DOUBLE       NOT NULL DEFAULT 0,
                `mean_edge`         DOUBLE       NOT NULL DEFAULT 0,
                `median_edge`       DOUBLE       NOT NULL DEFAULT 0,
                `dispersion`        DOUBLE       NOT NULL DEFAULT 0,
                `coherence`         DOUBLE       NOT NULL DEFAULT 0,
                `agreement_class`   VARCHAR(12)  NOT NULL,
                `agreement_count`   TINYINT      NOT NULL DEFAULT 0,
                `agreement_ratio`   DOUBLE       NOT NULL DEFAULT 0,
                `confidence`        DOUBLE       NOT NULL DEFAULT 0,
                `edge_gpt_oss`      DOUBLE       NOT NULL DEFAULT 0,
                `edge_gemma3`       DOUBLE       NOT NULL DEFAULT 0,
                `edge_deepseek`     DOUBLE       NOT NULL DEFAULT 0,
                `source_max_llm_id` BIGINT       NOT NULL DEFAULT 0,
                `updated_at`        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                                ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_news_pair` (`press`, `news_id`, `pair_id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`),
                INDEX `idx_pair_date` (`pair_id`, `date_dt`),
                INDEX `idx_source_llm_id` (`source_max_llm_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='brain_llm relative ensemble events (services 77/78)'
        """))
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{meta_table}` (
                `meta_key`   VARCHAR(64)  NOT NULL,
                `meta_value` VARCHAR(255) NOT NULL,
                `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                           ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`meta_key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))


async def _meta_get(engine_vlad, meta_table: str, key: str) -> str | None:
    from sqlalchemy import text

    async with engine_vlad.connect() as conn:
        row = (await conn.execute(
            text(f"SELECT meta_value FROM `{meta_table}` WHERE meta_key=:k"),
            {"k": key},
        )).fetchone()
    return str(row[0]) if row else None


async def _meta_set(engine_vlad, meta_table: str, key: str, value: Any) -> None:
    from sqlalchemy import text

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{meta_table}` (`meta_key`, `meta_value`)
            VALUES (:k, :v)
            ON DUPLICATE KEY UPDATE meta_value=VALUES(meta_value)
        """), {"k": key, "v": str(value)})


async def _source_max_id(engine_brain) -> int:
    from sqlalchemy import text

    params = {f"m{i}": name for i, name in enumerate(LLM_MODELS)}
    async with engine_brain.connect() as conn:
        value = (await conn.execute(text("""
            SELECT COALESCE(MAX(id), 0)
            FROM brain_llm
            WHERE model IN (:m0, :m1, :m2)
        """), params)).scalar()
    return int(value or 0)


async def _changed_keys(engine_brain, last_llm_id: int) -> dict[str, list[int]]:
    from sqlalchemy import text

    params = {
        "last_id": int(last_llm_id),
        **{f"m{i}": name for i, name in enumerate(LLM_MODELS)},
    }
    async with engine_brain.connect() as conn:
        res = await conn.execute(text("""
            SELECT DISTINCT press, news_id
            FROM brain_llm
            WHERE id > :last_id
              AND model IN (:m0, :m1, :m2)
            ORDER BY press, news_id
        """), params)
        rows = res.fetchall()

    result: dict[str, list[int]] = defaultdict(list)
    for press, news_id in rows:
        press = str(press or "").lower()
        if press in PRESSES and news_id is not None:
            result[press].append(int(news_id))
    return dict(result)


async def _fetch_news_aggregates(
    engine_brain,
    press: str,
    news_ids: Iterable[int] | None = None,
) -> list[dict[str, Any]]:
    from sqlalchemy import text

    base_params = {
        "press": press,
        **{f"m{i}": name for i, name in enumerate(LLM_MODELS)},
    }

    if news_ids is None:
        async with engine_brain.connect() as conn:
            res = await conn.execute(text(_aggregate_select_sql(press)), base_params)
            return [dict(r) for r in res.mappings().all()]

    ids = sorted({int(v) for v in news_ids})
    result: list[dict[str, Any]] = []
    for start in range(0, len(ids), 800):
        batch = ids[start:start + 800]
        placeholders = ", ".join(f":id{i}" for i in range(len(batch)))
        params = dict(base_params)
        params.update({f"id{i}": value for i, value in enumerate(batch)})
        async with engine_brain.connect() as conn:
            res = await conn.execute(
                text(_aggregate_select_sql(press, placeholders)), params
            )
            result.extend(dict(r) for r in res.mappings().all())
    return result


async def _upsert_events(engine_vlad, enriched_table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    from sqlalchemy import text

    sql = text(f"""
        INSERT INTO `{enriched_table}` (
            date_dt, press, news_id, pair_id, pair_code, event_type,
            pct_change, mean_edge, median_edge, dispersion, coherence,
            agreement_class, agreement_count, agreement_ratio, confidence,
            edge_gpt_oss, edge_gemma3, edge_deepseek, source_max_llm_id
        ) VALUES (
            :date_dt, :press, :news_id, :pair_id, :pair_code, :event_type,
            :pct_change, :mean_edge, :median_edge, :dispersion, :coherence,
            :agreement_class, :agreement_count, :agreement_ratio, :confidence,
            :edge_gpt_oss, :edge_gemma3, :edge_deepseek, :source_max_llm_id
        )
        ON DUPLICATE KEY UPDATE
            date_dt=VALUES(date_dt),
            event_type=VALUES(event_type),
            pct_change=VALUES(pct_change),
            mean_edge=VALUES(mean_edge),
            median_edge=VALUES(median_edge),
            dispersion=VALUES(dispersion),
            coherence=VALUES(coherence),
            agreement_class=VALUES(agreement_class),
            agreement_count=VALUES(agreement_count),
            agreement_ratio=VALUES(agreement_ratio),
            confidence=VALUES(confidence),
            edge_gpt_oss=VALUES(edge_gpt_oss),
            edge_gemma3=VALUES(edge_gemma3),
            edge_deepseek=VALUES(edge_deepseek),
            source_max_llm_id=VALUES(source_max_llm_id)
    """)

    written = 0
    async with engine_vlad.begin() as conn:
        for start in range(0, len(rows), 500):
            batch = rows[start:start + 500]
            await conn.execute(sql, batch)
            written += len(batch)
    return written


async def _enrich_dataset_unlocked(engine_vlad, engine_brain) -> dict[str, Any]:
    """Incrementally materialize brain_llm + press news dates into vlad.

    Initial build reads all five press tables.  Later rebuilds inspect only
    brain_llm rows with id greater than the last successfully processed id and
    recompute the affected news items.  This catches delayed LLM completions for
    old news without rescanning the ~800k source rows every two hours.
    """
    cfg = get_service_config() or {}
    dcfg = cfg.get("dataset") or {}
    enriched_table = str(dcfg.get("enriched_table") or "vlad_brain_llm_ensemble")
    meta_table = f"{enriched_table}_meta"

    await _ensure_enriched_tables(engine_vlad, enriched_table, meta_table)

    current_version = await _meta_get(engine_vlad, meta_table, "schema_version")
    full_rebuild = current_version != ENRICH_SCHEMA_VERSION
    if full_rebuild:
        from sqlalchemy import text
        async with engine_vlad.begin() as conn:
            await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))
        last_llm_id = 0
    else:
        try:
            last_llm_id = int(await _meta_get(engine_vlad, meta_table, "last_llm_id") or 0)
        except (TypeError, ValueError):
            last_llm_id = 0

    source_max = await _source_max_id(engine_brain)
    if not full_rebuild and source_max <= last_llm_id:
        return {
            "mode": "noop",
            "source_max_llm_id": source_max,
            "last_llm_id": last_llm_id,
            "events_written": 0,
        }

    changed: dict[str, list[int]] | None = None
    if not full_rebuild and last_llm_id > 0:
        changed = await _changed_keys(engine_brain, last_llm_id)

    source_news = 0
    complete_news = 0
    incomplete_news = 0
    events_written = 0
    per_press: dict[str, dict[str, int]] = {}

    for press in PRESSES:
        if changed is not None:
            ids = changed.get(press, [])
            if not ids:
                continue
            # If a backlog is large, a single grouped scan is cheaper than a
            # giant sequence of IN batches.
            aggregates = await _fetch_news_aggregates(
                engine_brain, press, None if len(ids) > 20_000 else ids
            )
        else:
            aggregates = await _fetch_news_aggregates(engine_brain, press, None)

        source_news += len(aggregates)
        out: list[dict[str, Any]] = []
        complete_for_press = 0
        incomplete_for_press = 0

        for raw in aggregates:
            scores = _score_matrix(raw)
            if scores is None:
                incomplete_news += 1
                incomplete_for_press += 1
                continue

            date_dt = raw.get("date_dt")
            if date_dt is None:
                continue
            if not isinstance(date_dt, datetime):
                # SQLAlchemy normally returns datetime, keep a defensive path.
                try:
                    date_dt = datetime.fromisoformat(str(date_dt))
                except ValueError:
                    continue

            complete_news += 1
            complete_for_press += 1
            for pair_id in PAIRS:
                out.append(_event_row(
                    press=press,
                    news_id=int(raw["news_id"]),
                    date_dt=date_dt,
                    source_max_llm_id=int(raw.get("source_max_llm_id") or 0),
                    pair_id=pair_id,
                    scores=scores,
                ))

        written = await _upsert_events(engine_vlad, enriched_table, out)
        events_written += written
        per_press[press] = {
            "source_news": len(aggregates),
            "complete_news": complete_for_press,
            "incomplete_news": incomplete_for_press,
            "events_written": written,
        }

    # Advance the watermark only after every affected press has been committed.
    await _meta_set(engine_vlad, meta_table, "schema_version", ENRICH_SCHEMA_VERSION)
    await _meta_set(engine_vlad, meta_table, "last_llm_id", source_max)

    return {
        "mode": "full" if full_rebuild else "incremental",
        "source_max_llm_id": source_max,
        "previous_last_llm_id": last_llm_id,
        "source_news": source_news,
        "complete_news": complete_news,
        "incomplete_news": incomplete_news,
        "events_written": events_written,
        "per_press": per_press,
    }


async def enrich_dataset(engine_vlad, engine_brain) -> dict[str, Any]:
    """Build/update the single shared brain_llm ensemble dataset.

    Services 77 (baseline) and 78 (ML) intentionally point to the same
    materialized table.  A MySQL advisory lock serializes rebuilds across both
    service processes: the first service performs the work, while the second
    re-checks the shared watermark afterwards and normally returns ``noop``.
    This keeps the A/B input identical and avoids duplicate source scans.
    """
    from sqlalchemy import text

    cfg = get_service_config() or {}
    dcfg = cfg.get("dataset") or {}
    enriched_table = str(dcfg.get("enriched_table") or "vlad_brain_llm_ensemble")
    lock_name = f"brain_llm_enrich:{enriched_table}"[:64]

    async with engine_vlad.connect() as lock_conn:
        acquired = (await lock_conn.execute(
            text("SELECT GET_LOCK(:name, 600)"), {"name": lock_name}
        )).scalar()
        if int(acquired or 0) != 1:
            raise RuntimeError(
                f"Could not acquire shared brain_llm enrichment lock: {lock_name}"
            )
        try:
            # Important: execute the normal watermark check only *after* the
            # lock is held.  If service 77 has just updated the dataset, 78
            # will observe the new last_llm_id and skip duplicate work.
            return await _enrich_dataset_unlocked(engine_vlad, engine_brain)
        finally:
            try:
                await lock_conn.execute(
                    text("SELECT RELEASE_LOCK(:name)"), {"name": lock_name}
                )
            except Exception:
                # MySQL also releases named locks automatically if this
                # connection is closed, so cleanup failure must not mask the
                # enrichment result/error.
                pass


# ---------------------------------------------------------------------------
# Runtime model
# ---------------------------------------------------------------------------

def _pair_id(dataset_index: dict | None) -> int:
    table = str((dataset_index or {}).get("rates_table") or RATES_TABLE)
    return int(RATES_TO_PAIR.get(table, 1))


def _row_allowed_for_var(row: dict[str, Any], var: int) -> bool:
    agreement = str(row.get("agreement_class") or "mixed")
    coherence = float(row.get("coherence") or 0.0)
    dispersion = float(row.get("dispersion") or 0.0)

    if var == 0:  # full committee context
        return True
    if var == 1:  # at least 2/3 directional agreement
        return agreement in ("majority", "unanimous")
    if var == 2:  # strict 3/3 agreement
        return agreement == "unanimous"
    if var == 3:  # magnitude-weighted, no prefilter
        return True
    if var == 4:  # coherent mean: committee is not cancelling itself
        return coherence >= COHERENCE_FILTER_MIN
    if var == 5:  # explicit conflict regime
        return dispersion >= CONFLICT_DISPERSION_MIN and agreement != "unanimous"
    return False


def _event_parser(pair_id: int, var: int):
    def parse(row: dict[str, Any]):
        try:
            if int(row.get("pair_id") or 0) != pair_id:
                return None
        except (TypeError, ValueError):
            return None
        if not _row_allowed_for_var(row, var):
            return None

        event_time = row.get("date_dt") or row.get("date")
        if event_time is None:
            return None
        if isinstance(event_time, str):
            try:
                event_time = datetime.fromisoformat(event_time[:19])
            except ValueError:
                return None

        # pct keeps the signed robust edge.  var=3 consumes its magnitude in
        # _apply_var; other variants use it only as event metadata/direction.
        return event_time, float(row.get("median_edge") or 0.0), str(row["event_type"])

    return parse


def _apply_var(stored_t1: float, analog_edge: float, var: int, ctx_info: dict) -> float:
    if var == 3:
        # Historical market outcome is weighted by how strong the *historical*
        # LLM relative edge was.  Clamp prevents one malformed/extreme news item
        # from dominating a context.
        scale = min(max(abs(float(analog_edge)) / 3.0, 0.25), 3.0)
        return float(stored_t1) * scale
    return float(stored_t1)


def _ml_signal_fn(
    calc_type: int,
    ctx_id: int,
    shift: int,
    mode0_value: float,
    has_extremum_hits: bool,
    current_edge: float,
    outcomes: int,
    direction: float,
    ctx_info: dict,
):
    """Expose the same active code universe for reverse-learning type 3/4.

    For normal calculation types 0/1/2 returning None delegates completely to
    brain_framework's standard contribution logic.  Type 3/4 only need stable
    active keys for reverse_learning.active_codes_at(); values themselves are
    not used as the trained weight.
    """
    if calc_type not in (3, 4):
        return None

    result: dict[str, float] = {}
    if mode0_value != 0.0:
        result[f"{ctx_id}_0_{shift}"] = round(float(mode0_value), 6)
    if has_extremum_hits:
        result[f"{ctx_id}_1_{shift}"] = 1.0 if direction >= 0 else -1.0
    return result


def model(
    rates: list[dict],
    dataset: list[dict],
    date: datetime,
    *,
    type: int = 0,
    var: int = 0,
    param: str = "",
    dataset_index: dict | None = None,
) -> dict[str, float]:
    if not dataset or not date:
        return {}

    pair_id = _pair_id(dataset_index)

    # Critical causal guard.  brain_framework exposes full_dataset as an
    # accelerator, but for a news model the selector must never see rows later
    # than target date.  FILTER_DATASET_BY_DATE makes `dataset` a causal prefix;
    # replace full_dataset with exactly that prefix before calling the standard
    # event engine.
    local_index = dict(dataset_index or {})
    local_index["full_dataset"] = dataset

    return run_standard_model(
        rates,
        dataset,
        date,
        type=int(type),
        var=int(var),
        dataset_index=local_index,
        shift_window=SHIFT_WINDOW,
        apply_var_fn=_apply_var,
        min_occurrence=2,
        get_event_fn=_event_parser(pair_id, int(var)),
        signal_fn=_ml_signal_fn,
        # News reactions may remain relevant through the H1 12-hour window.
        # Future events are impossible here because full_dataset was replaced
        # by the framework-provided causal dataset prefix above.
        rare_event_fn=lambda _ctx: True,
        rare_occurrence_max=0,
    )


# ============================================================================
# FAST BATCH CACHE v1 — services 77/78
#
# Behavior-preserving acceleration for Brain Framework fill_cache.
#
# Important:
#   - model() above is NOT changed;
#   - live calculation continues to use the original run_standard_model();
#   - batch calculation uses the SAME brain_framework helpers;
#   - mode0/mode1, T1, extrema, shifts, causality and _apply_var are unchanged;
#   - type=3 and type=4 keep the original _ml_signal_fn behaviour.
#
# Main optimization:
#   1. Build parsed event index once for a whole candle chunk, not once/candle.
#   2. Calculate historical analogs once per current event.
#   3. Produce all type 0..4 results from that single aggregation.
#   4. Cache the batch across framework calls for different type values.
# ============================================================================

import threading as _fast_threading
from collections import OrderedDict as _FastOrderedDict

from brain_framework import (
    _dataset_event_index as _fast_dataset_event_index,
    _select_current_events as _fast_select_current_events,
    _aggregate_event_history as _fast_aggregate_event_history,
    _execution_is_daily as _fast_execution_is_daily,
    _standard_contribution as _fast_standard_contribution,
)


_FAST_BATCH_LOCK = _fast_threading.RLock()

# One H1 timeframe normally consists of ~6-7 chunks × 6 vars.
# 64 entries are enough to retain all var/chunk results until framework starts
# requesting the next calc_type, without allowing unlimited RAM growth.
_FAST_BATCH_CACHE_MAX = 64
_FAST_BATCH_CACHE: _FastOrderedDict = _FastOrderedDict()


def _fast_mode_from_code(code: str) -> int | None:
    """
    Brain weight code:
        <ctx_id>_<mode>_<shift>

    ctx_id and shift may vary; mode is always the middle component from the end.
    """
    try:
        return int(str(code).rsplit("_", 2)[1])
    except (ValueError, IndexError):
        return None


def _fast_batch_cache_key(
    dataset: list[dict],
    dates: list[datetime],
    var: int,
    dataset_index: dict | None,
) -> tuple:
    di = dataset_index or {}

    pair_id = _pair_id(di)
    is_daily = bool(di.get("is_daily"))

    np_rates = di.get("np_rates") or {}
    dates_ns = np_rates.get("dates_ns")

    if dates:
        first_ts = int(dates[0].timestamp())
        last_ts = int(dates[-1].timestamp())
    else:
        first_ts = 0
        last_ts = 0

    return (
        int(pair_id),
        int(is_daily),
        int(var),

        # New dataset / reload => another cache generation automatically.
        id(dataset),
        len(dataset),

        # New rates arrays / reload => another cache generation.
        id(dates_ns),

        len(dates),
        first_ts,
        last_ts,
    )


def _fast_cache_get(key):
    with _FAST_BATCH_LOCK:
        item = _FAST_BATCH_CACHE.get(key)
        if item is None:
            return None

        # LRU
        _FAST_BATCH_CACHE.move_to_end(key)
        return item


def _fast_cache_put(key, value):
    with _FAST_BATCH_LOCK:
        _FAST_BATCH_CACHE[key] = value
        _FAST_BATCH_CACHE.move_to_end(key)

        while len(_FAST_BATCH_CACHE) > _FAST_BATCH_CACHE_MAX:
            _FAST_BATCH_CACHE.popitem(last=False)


def _fast_build_batch_base(
    rates: list[dict],
    dataset: list[dict],
    dates: list[datetime],
    *,
    var: int,
    dataset_index: dict | None,
) -> dict[datetime, tuple[dict[str, float], dict[str, float]]]:
    """
    Calculate one var for all dates.

    Returned value per date:
        (
            type0_result,   # contains mode0 + mode1
            type3_result,   # active-code representation for ML types 3/4
        )

    From those two dictionaries we can reproduce all five calc_type values
    exactly:

        type 0 = type0_result
        type 1 = only mode0 codes from type0_result
        type 2 = only mode1 codes from type0_result
        type 3 = type3_result
        type 4 = type3_result

    This follows the original framework contract.
    """

    empty = {d: ({}, {}) for d in dates}

    if not dataset or not dates:
        return empty

    di = dict(dataset_index or {})

    ctx_index = di.get("ctx_index") or {}
    np_rates = di.get("np_rates")

    if not ctx_index or np_rates is None:
        return empty

    pair_id = _pair_id(di)
    calc_var = int(var)

    # Same parser as original model().
    parser = _event_parser(pair_id, calc_var)

    # ------------------------------------------------------------------
    # Critical optimization #1
    #
    # Original model() -> run_standard_model() builds this index again
    # for every target candle.
    #
    # Here it is built ONCE for the whole batch.
    # ------------------------------------------------------------------

    local_index = dict(di)

    # We deliberately allow the index to contain the complete loaded dataset.
    # Causality is enforced below by:
    #
    #   event_time <= target_date
    #
    # and _aggregate_event_history itself only accepts analogs strictly before
    # target_date and only outcomes whose candle is already closed.
    local_index["full_dataset"] = dataset

    events_by_type = _fast_dataset_event_index(
        dataset,
        local_index,
        parser,
    )

    if not events_by_type:
        return empty

    reverse: dict[str, tuple[int, dict]] = {
        str(info.get("event_type") or "").strip().lower(): (
            int(info["id"]),
            info,
        )
        for info in ctx_index.values()
        if info.get("id") and info.get("event_type")
    }

    if not reverse:
        return empty

    is_daily = _fast_execution_is_daily(rates, di)

    result: dict[
        datetime,
        tuple[dict[str, float], dict[str, float]]
    ] = {}

    for target_date in dates:

        # Same event selection as _run_standard_model_core().
        current_events = _fast_select_current_events(
            events_by_type,
            target_date,
            is_daily=is_daily,
            shift_window=SHIFT_WINDOW,
            reverse=reverse,

            # Same values as the original model().
            rare_occurrence_max=0,
            rare_event_fn=lambda _ctx: True,
        )

        out_type0: dict[str, float] = {}
        out_type3: dict[str, float] = {}

        for (
            current_event_time,
            current_pct,
            event_type,
            ctx_id,
            ctx_info,
            shift,
        ) in current_events:

            # ----------------------------------------------------------
            # CRITICAL CAUSAL GUARD
            #
            # Original model() gives run_standard_model() a causal dataset
            # prefix. A batch receives a larger dataset, therefore future
            # news selected by the ±12h selector must explicitly be rejected.
            # ----------------------------------------------------------
            if current_event_time > target_date:
                continue

            # ----------------------------------------------------------
            # Critical optimization #2
            #
            # Calculate historical outcomes ONCE.
            #
            # Previously type 0,1,2,3,4 caused the same aggregation to be
            # repeated five times.
            # ----------------------------------------------------------
            mode0, mode1, outcomes, hits = _fast_aggregate_event_history(
                current_event_time=current_event_time,
                current_pct=current_pct,
                event_type=event_type,
                shift=shift,
                ctx_info=ctx_info,
                events_by_type=events_by_type,
                np_rates=np_rates,
                target_date=target_date,
                is_daily=is_daily,
                var=calc_var,
                apply_var_fn=_apply_var,
                min_occurrence=2,
            )

            # ----------------------------------------------------------
            # TYPE 0
            #
            # Framework definition:
            # type=0 -> mode0 + mode1
            # ----------------------------------------------------------
            contribution0 = _fast_standard_contribution(
                calc_type=0,
                ctx_id=ctx_id,
                shift=shift,
                mode0_value=mode0,
                mode1_value=mode1,
            )

            for code, value in contribution0.items():
                value = float(value)
                if value != 0.0:
                    out_type0[code] = out_type0.get(code, 0.0) + value

            # ----------------------------------------------------------
            # TYPE 3 / TYPE 4
            #
            # Use EXACTLY the original model's _ml_signal_fn().
            #
            # In services 77/78 _ml_signal_fn returns the same active-code
            # representation for calc_type 3 and 4.
            # ----------------------------------------------------------
            direction = 1.0 if current_pct > 0 else -1.0

            contribution3 = _ml_signal_fn(
                3,
                ctx_id,
                shift,
                mode0,
                bool(hits),
                current_pct,
                outcomes,
                direction,
                ctx_info,
            )

            if contribution3:
                for code, value in contribution3.items():
                    value = float(value)
                    if value != 0.0:
                        out_type3[code] = out_type3.get(code, 0.0) + value

        # Same final zero filtering as brain_framework core.
        out_type0 = {
            k: v
            for k, v in out_type0.items()
            if v != 0.0
        }

        out_type3 = {
            k: v
            for k, v in out_type3.items()
            if v != 0.0
        }

        result[target_date] = (
            out_type0,
            out_type3,
        )

    return result


def _fast_extract_type(
    base: dict[datetime, tuple[dict[str, float], dict[str, float]]],
    calc_type: int,
) -> dict[datetime, dict[str, float]]:

    result: dict[datetime, dict[str, float]] = {}

    for dt, (type0, type3) in base.items():

        if calc_type == 0:
            result[dt] = dict(type0)

        elif calc_type == 1:
            # mode0 only
            result[dt] = {
                code: value
                for code, value in type0.items()
                if _fast_mode_from_code(code) == 0
            }

        elif calc_type == 2:
            # mode1 only
            result[dt] = {
                code: value
                for code, value in type0.items()
                if _fast_mode_from_code(code) == 1
            }

        elif calc_type in (3, 4):
            # Existing _ml_signal_fn has identical output for 3 and 4.
            result[dt] = dict(type3)

        else:
            result[dt] = {}

    return result


def batch_model(
    rates: list[dict],
    dataset: list[dict],
    dates: list[datetime],
    *,
    type: int = 0,
    var: int = 0,
    param: str = "",
    dataset_index: dict | None = None,
) -> dict[datetime, dict[str, float]]:
    """
    Optimized batch implementation for fill_cache / ML prewarm.

    It is intentionally separate from model(), so real-time calculation keeps
    using the original implementation.
    """

    if not dates:
        return {}

    if not dataset:
        return {d: {} for d in dates}

    calc_type = int(type)
    calc_var = int(var)

    if calc_type not in TYPES_RANGE:
        return {d: {} for d in dates}

    if calc_var not in VAR_RANGE:
        return {d: {} for d in dates}

    key = _fast_batch_cache_key(
        dataset,
        dates,
        calc_var,
        dataset_index,
    )

    base = _fast_cache_get(key)

    if base is None:
        base = _fast_build_batch_base(
            rates,
            dataset,
            dates,
            var=calc_var,
            dataset_index=dataset_index,
        )
        _fast_cache_put(key, base)

    return _fast_extract_type(base, calc_type)