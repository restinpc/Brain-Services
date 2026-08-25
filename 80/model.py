"""
Algorithmic news topic/event model for services 80.

Pipeline (NO LLM semantic labeling):
    raw CNN/NYT/TWP/WSJ/TGD news
      -> boilerplate cleanup + TF-IDF word/bigram representation
      -> MiniBatchNMF broad latent topics
      -> TruncatedSVD semantic subspace
      -> topic-local MiniBatchKMeans event clusters
      -> causal novelty + cross-source confirmation as separate features
      -> one shared enriched table
      -> Brain Framework learns historical T1/extremum reaction.

Service 79 is the baseline (Brain ML disabled).
Service 80 uses exactly the same enriched data and event algorithm with
Brain Framework reverse-learning enabled.

Important anti-leak rule:
The NLP representation is fitted only on news strictly earlier than the fixed
training cutoff (default 2025-01-15).  It is then frozen.  Later news are only
transformed/predicted.  Market prices/T1 are never used by TF-IDF/NMF/KMeans.
"""
from __future__ import annotations

import hashlib
import html
import io
import math
import re
import bisect
import threading
from collections import OrderedDict, defaultdict, deque
from datetime import datetime, timedelta
from typing import Any, Iterable

import numpy as np

from brain_framework import get_service_config

# ---------------------------------------------------------------------------
# Framework contract
# ---------------------------------------------------------------------------

PRETEST_ALLOW_EMPTY = True
RATES_TABLE = "brain_rates_eur_usd"
FILTER_DATASET_BY_DATE = True
MODEL_USES_RATE_HISTORY = True
SHIFT_WINDOW = 12
VAR_RANGE = list(range(8))
TYPES_RANGE = [0, 1, 2, 3, 4]
USE_ML_VALUES = True

# Shared by 79/80.  Bump these when feature semantics change.
ENRICH_SCHEMA_VERSION = "3"
ALGO_VERSION = "tfidf-nmf-svd-hierkmeans-strict-causal-v3"

SOURCES: dict[str, str] = {
    "cnn": "brain_cnn_news",
    "nyt": "brain_nyt_news",
    "twp": "brain_twp_news",
    "wsj": "brain_wsj_news",
    "tgd": "brain_tgd_news",
}

# Defaults can be overridden in [nlp] config.toml.  Both services intentionally
# carry identical values so they remain a clean A/B test of Brain ML only.
DEFAULT_TRAIN_CUTOFF = "2025-01-15 00:00:00"
DEFAULT_SAMPLE_PER_SOURCE = 20_000
DEFAULT_MAX_FEATURES = 20_000
DEFAULT_NMF_TOPICS = 40
DEFAULT_SVD_COMPONENTS = 96
# Local event clustering is allocated independently inside every dominant NMF
# topic.  Roughly one event cluster per TARGET_EVENT_DOCS training documents.
DEFAULT_TARGET_EVENT_DOCS = 300
DEFAULT_MIN_EVENT_SUPPORT = 60
DEFAULT_MAX_LOCAL_CLUSTERS = 32
DEFAULT_TEXT_CHARS = 1_800
DEFAULT_PERIOD_DAYS = 14
DEFAULT_NOVELTY_DAYS = 7
DEFAULT_CONFIRM_HOURS = 12
DEFAULT_CONFIRM_SIM = 0.78

# Runtime var gates.  These are intentionally NOT embedded in event_type: the
# historical event identity stays dense as topic+local-event while vars filter
# current and historical analog rows by their causal feature columns.
TOPIC_FOCUS_MIN = 0.36
CLUSTER_SIM_MIN = 0.70
NOVELTY_MIN = 0.35

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.I)
_WS_RE = re.compile(r"\s+")
# Publisher/navigation boilerplate that otherwise becomes fake latent topics.
_BOILERPLATE_RE = re.compile(
    r"(?:\bcontinue reading\b|\bread (?:the )?full (?:article|story)\b|"
    r"\bread article\b|\bclick here\b|\bsign up for (?:our|the) newsletter\b|"
    r"\bsubscribe (?:now|today)?\b|\bworld briefing\b|\bimage of the day\b|"
    r"\bwonkblog\b|\bthe wall street journal\b|\bwall street journal\b|"
    r"\bnew york times\b|\bthe guardian\b|\bcnn(?:\.com)?\b|"
    r"\bwashington post\b|\bwsj(?:\.com)?\b)",
    re.I,
)


def _nlp_cfg() -> dict[str, Any]:
    cfg = get_service_config() or {}
    raw = cfg.get("nlp") or {}
    cutoff_s = str(raw.get("train_cutoff") or DEFAULT_TRAIN_CUTOFF)
    try:
        cutoff = datetime.fromisoformat(cutoff_s[:19])
    except ValueError:
        cutoff = datetime.fromisoformat(DEFAULT_TRAIN_CUTOFF)
    return {
        "train_cutoff": cutoff,
        "sample_per_source": max(2_000, int(raw.get("sample_per_source", DEFAULT_SAMPLE_PER_SOURCE))),
        "max_features": max(5_000, int(raw.get("max_features", DEFAULT_MAX_FEATURES))),
        "nmf_topics": max(8, int(raw.get("nmf_topics", DEFAULT_NMF_TOPICS))),
        "svd_components": max(24, int(raw.get("svd_components", DEFAULT_SVD_COMPONENTS))),
        "target_event_docs": max(100, int(raw.get("target_event_docs", DEFAULT_TARGET_EVENT_DOCS))),
        "min_event_support": max(20, int(raw.get("min_event_support", DEFAULT_MIN_EVENT_SUPPORT))),
        "max_local_clusters": max(2, int(raw.get("max_local_clusters", DEFAULT_MAX_LOCAL_CLUSTERS))),
        "text_chars": max(300, int(raw.get("text_chars", DEFAULT_TEXT_CHARS))),
        "period_days": max(1, int(raw.get("period_days", DEFAULT_PERIOD_DAYS))),
        "novelty_days": max(1, int(raw.get("novelty_days", DEFAULT_NOVELTY_DAYS))),
        "confirm_hours": max(1, int(raw.get("confirmation_hours", DEFAULT_CONFIRM_HOURS))),
        "confirm_similarity": float(raw.get("confirmation_similarity", DEFAULT_CONFIRM_SIM)),
    }


def _signature(cfg: dict[str, Any]) -> str:
    payload = "|".join([
        ALGO_VERSION,
        cfg["train_cutoff"].isoformat(sep=" "),
        str(cfg["sample_per_source"]),
        str(cfg["max_features"]),
        str(cfg["nmf_topics"]),
        str(cfg["svd_components"]),
        str(cfg["target_event_docs"]),
        str(cfg["min_event_support"]),
        str(cfg["max_local_clusters"]),
        str(cfg["text_chars"]),
        str(cfg["novelty_days"]),
        str(cfg["confirm_hours"]),
        f"{cfg['confirm_similarity']:.6f}",
    ])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _clean_news_text(value: str) -> str:
    value = html.unescape(str(value or ""))
    value = _URL_RE.sub(" ", value)
    value = _BOILERPLATE_RE.sub(" ", value)
    return _WS_RE.sub(" ", value).strip()


def _document_text(row: dict[str, Any], text_chars: int) -> str:
    title = _clean_news_text(str(row.get("title") or ""))
    body = _clean_news_text(str(row.get("text") or "")[:text_chars])
    # Title is intentionally duplicated: it is the cleanest event summary, but
    # source/navigation boilerplate is removed before TF-IDF fitting.
    return _WS_RE.sub(" ", f"{title} {title} {body}").strip()


def _require_nlp_stack():
    try:
        import joblib  # noqa: F401
        from sklearn.cluster import MiniBatchKMeans  # noqa: F401
        from sklearn.decomposition import MiniBatchNMF, TruncatedSVD  # noqa: F401
        from sklearn.feature_extraction.text import TfidfVectorizer  # noqa: F401
    except Exception as exc:  # pragma: no cover - production dependency check
        raise RuntimeError(
            "Services 79/80 enrichment requires scikit-learn + joblib. "
            "Install: pip install 'scikit-learn>=1.4' joblib"
        ) from exc


# ---------------------------------------------------------------------------
# Database schema / metadata / shared artifact
# ---------------------------------------------------------------------------

async def _ensure_tables(engine_vlad, enriched_table: str, meta_table: str, artifact_table: str) -> None:
    from sqlalchemy import text

    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{enriched_table}` (
                `id`                   BIGINT       NOT NULL AUTO_INCREMENT,
                `date_dt`              DATETIME     NOT NULL,
                `press`                VARCHAR(3)   NOT NULL,
                `source_id`            INT          NOT NULL,
                `feed`                 VARCHAR(64)  NOT NULL DEFAULT '',
                `event_type`           VARCHAR(64)  NOT NULL,
                `pct_change`           DOUBLE       NOT NULL DEFAULT 0,
                `cluster_id`           SMALLINT     NOT NULL,
                `topic_id`             SMALLINT     NOT NULL,
                `topic_focus`          DOUBLE       NOT NULL DEFAULT 0,
                `cluster_similarity`   DOUBLE       NOT NULL DEFAULT 0,
                `novelty`              DOUBLE       NOT NULL DEFAULT 0,
                `confirmation_count`   TINYINT      NOT NULL DEFAULT 1,
                `quality_score`        DOUBLE       NOT NULL DEFAULT 0,
                `updated_at`           TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                                                     ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`id`),
                UNIQUE KEY `uk_source` (`press`, `source_id`),
                INDEX `idx_date_dt` (`date_dt`),
                INDEX `idx_event_type` (`event_type`),
                INDEX `idx_cluster_date` (`cluster_id`, `date_dt`),
                INDEX `idx_topic_date` (`topic_id`, `date_dt`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='algorithmic TFIDF/NMF/SVD hierarchical news events shared by services 79/80'
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
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{artifact_table}` (
                `artifact_key` VARCHAR(32) NOT NULL,
                `signature`    CHAR(64)    NOT NULL,
                `sha256`       CHAR(64)    NOT NULL,
                `blob_data`    LONGBLOB    NOT NULL,
                `updated_at`   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
                                           ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`artifact_key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            COMMENT='frozen sklearn artifact for services 79/80'
        """))


async def _meta_get(engine_vlad, table: str, key: str) -> str | None:
    from sqlalchemy import text
    async with engine_vlad.connect() as conn:
        row = (await conn.execute(
            text(f"SELECT meta_value FROM `{table}` WHERE meta_key=:k"), {"k": key}
        )).fetchone()
    return str(row[0]) if row else None


async def _meta_set_many(engine_vlad, table: str, values: dict[str, Any]) -> None:
    if not values:
        return
    from sqlalchemy import text
    rows = [{"k": str(k), "v": str(v)} for k, v in values.items()]
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{table}` (`meta_key`, `meta_value`)
            VALUES (:k, :v)
            ON DUPLICATE KEY UPDATE meta_value=VALUES(meta_value)
        """), rows)


async def _artifact_load(engine_vlad, table: str, signature: str):
    _require_nlp_stack()
    import joblib
    from sqlalchemy import text

    async with engine_vlad.connect() as conn:
        row = (await conn.execute(text(f"""
            SELECT signature, sha256, blob_data
            FROM `{table}`
            WHERE artifact_key='main'
        """))).fetchone()
    if not row or str(row[0]) != signature:
        return None
    blob = bytes(row[2])
    if hashlib.sha256(blob).hexdigest() != str(row[1]):
        raise RuntimeError("Shared news NLP artifact checksum mismatch")
    return joblib.load(io.BytesIO(blob))


async def _artifact_store(engine_vlad, table: str, signature: str, artifact: dict[str, Any]) -> int:
    _require_nlp_stack()
    import joblib
    from sqlalchemy import text

    bio = io.BytesIO()
    joblib.dump(artifact, bio, compress=3)
    blob = bio.getvalue()
    digest = hashlib.sha256(blob).hexdigest()
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            INSERT INTO `{table}` (`artifact_key`, `signature`, `sha256`, `blob_data`)
            VALUES ('main', :sig, :sha, :blob)
            ON DUPLICATE KEY UPDATE
                signature=VALUES(signature), sha256=VALUES(sha256), blob_data=VALUES(blob_data)
        """), {"sig": signature, "sha": digest, "blob": blob})
    return len(blob)


# ---------------------------------------------------------------------------
# Source queries
# ---------------------------------------------------------------------------

async def _source_count_before(engine_brain, table: str, cutoff: datetime) -> int:
    from sqlalchemy import text
    async with engine_brain.connect() as conn:
        value = (await conn.execute(
            text(f"SELECT COUNT(*) FROM `{table}` WHERE date IS NOT NULL AND date < :cutoff"),
            {"cutoff": cutoff},
        )).scalar()
    return int(value or 0)


async def _training_sample(engine_brain, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    from sqlalchemy import text

    out: list[dict[str, Any]] = []
    limit = int(cfg["sample_per_source"])
    cutoff = cfg["train_cutoff"]
    for press, table in SOURCES.items():
        count = await _source_count_before(engine_brain, table, cutoff)
        if count <= 0:
            continue
        stride = max(1, count // limit)
        sql = text(f"""
            SELECT id AS source_id, title, text, date AS date_dt, feed
            FROM `{table}`
            WHERE date IS NOT NULL
              AND date < :cutoff
              AND MOD(id, :stride) = 0
            ORDER BY id
            LIMIT {limit}
        """)
        async with engine_brain.connect() as conn:
            rows = (await conn.execute(sql, {"cutoff": cutoff, "stride": stride})).mappings().all()
        for row in rows:
            item = dict(row)
            item["press"] = press
            out.append(item)
    out.sort(key=lambda r: (r.get("date_dt") or datetime.min, r["press"], int(r["source_id"])))
    return out


async def _source_bounds(engine_brain) -> tuple[datetime | None, datetime | None]:
    from sqlalchemy import text
    parts = [f"SELECT MIN(date) mn, MAX(date) mx FROM `{table}` WHERE date IS NOT NULL" for table in SOURCES.values()]
    sql = text("SELECT MIN(mn), MAX(mx) FROM (" + " UNION ALL ".join(parts) + ") x")
    async with engine_brain.connect() as conn:
        row = (await conn.execute(sql)).fetchone()
    if not row:
        return None, None
    return row[0], row[1]


async def _source_max_ids(engine_brain) -> dict[str, int]:
    from sqlalchemy import text
    result: dict[str, int] = {}
    async with engine_brain.connect() as conn:
        for press, table in SOURCES.items():
            value = (await conn.execute(text(f"SELECT COALESCE(MAX(id),0) FROM `{table}`"))).scalar()
            result[press] = int(value or 0)
    return result


async def _changed_dates(engine_brain, last_ids: dict[str, int]) -> list[datetime]:
    from sqlalchemy import text
    dates: list[datetime] = []
    async with engine_brain.connect() as conn:
        for press, table in SOURCES.items():
            last_id = int(last_ids.get(press, 0))
            res = await conn.execute(text(f"""
                SELECT date FROM `{table}`
                WHERE id > :last_id AND date IS NOT NULL
                ORDER BY id
            """), {"last_id": last_id})
            dates.extend(row[0] for row in res.fetchall() if isinstance(row[0], datetime))
    return sorted(dates)


async def _fetch_period(engine_brain, start: datetime, end: datetime) -> list[dict[str, Any]]:
    from sqlalchemy import text
    selects = []
    for press, table in SOURCES.items():
        selects.append(f"""
            SELECT '{press}' AS press, id AS source_id, title, text,
                   date AS date_dt, COALESCE(feed,'') AS feed
            FROM `{table}`
            WHERE date >= :start AND date < :end
        """)
    sql = text(" UNION ALL ".join(selects) + " ORDER BY date_dt, press, source_id")
    async with engine_brain.connect() as conn:
        rows = (await conn.execute(sql, {"start": start, "end": end})).mappings().all()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# NLP fit / transform
# ---------------------------------------------------------------------------


def _fit_artifact(sample_rows: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    _require_nlp_stack()
    from sklearn.cluster import MiniBatchKMeans
    from sklearn.decomposition import MiniBatchNMF, TruncatedSVD
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.preprocessing import normalize

    texts = [_document_text(r, cfg["text_chars"]) for r in sample_rows]
    texts = [t for t in texts if len(t) >= 12]
    if len(texts) < 500:
        raise RuntimeError(f"Too few pre-cutoff news documents for NLP fit: {len(texts)}")

    vectorizer = TfidfVectorizer(
        lowercase=True,
        strip_accents="unicode",
        stop_words="english",
        ngram_range=(1, 2),
        min_df=5,
        max_df=0.97,
        max_features=int(cfg["max_features"]),
        sublinear_tf=True,
        norm="l2",
        dtype=np.float32,
    )
    X = vectorizer.fit_transform(texts)

    # NMF decides the broad semantic family.  It is NOT used as the only event
    # geometry anymore: that was the source of overly broad global clusters.
    n_topics = min(int(cfg["nmf_topics"]), max(8, X.shape[1] // 100))
    nmf = MiniBatchNMF(
        n_components=n_topics,
        init="nndsvda",
        random_state=79,
        batch_size=2048,
        max_iter=80,
        max_no_improvement=12,
        tol=1e-4,
    )
    W = nmf.fit_transform(X).astype(np.float32, copy=False)
    dominant_topics = np.argmax(W, axis=1).astype(np.int32, copy=False)

    # SVD keeps much more lexical/semantic detail than the 40-dimensional NMF
    # mixture and is therefore used for local event clustering + similarity.
    svd_components = min(int(cfg["svd_components"]), max(24, X.shape[1] - 1))
    svd = TruncatedSVD(n_components=svd_components, n_iter=7, random_state=79)
    Z = svd.fit_transform(X).astype(np.float32, copy=False)
    Zn = normalize(Z, norm="l2", copy=False)

    topic_models: dict[int, dict[str, Any]] = {}
    total_events = 0
    target_docs = int(cfg["target_event_docs"])
    min_support = int(cfg["min_event_support"])
    max_local = int(cfg["max_local_clusters"])

    for topic_id in range(n_topics):
        idx = np.flatnonzero(dominant_topics == topic_id)
        n = int(len(idx))
        if n == 0:
            continue

        # Allocate event count by topic population, not globally.  This makes an
        # event cluster semantically nested inside one broad NMF topic.
        raw_k = max(1, int(round(n / float(target_docs))))
        raw_k = min(raw_k, max(1, n // max(1, min_support)), max_local)

        if raw_k == 1:
            raw_labels = np.zeros(n, dtype=np.int32)
            raw_centers = normalize(Zn[idx].mean(axis=0, keepdims=True), norm="l2").astype(np.float32)
            kmeans = None
        else:
            kmeans = MiniBatchKMeans(
                n_clusters=raw_k,
                random_state=7900 + topic_id,
                batch_size=min(2048, max(256, n)),
                n_init=3,
                max_iter=120,
                max_no_improvement=15,
                reassignment_ratio=0.005,
            )
            raw_labels = kmeans.fit_predict(Zn[idx]).astype(np.int32, copy=False)
            raw_centers = normalize(kmeans.cluster_centers_.astype(np.float32), norm="l2")

        # KMeans can create a few tiny clusters despite a sensible target size.
        # Merge every undersupported raw cluster into the nearest healthy one.
        counts = np.bincount(raw_labels, minlength=raw_k)
        healthy = np.flatnonzero(counts >= min_support)
        if len(healthy) == 0:
            healthy = np.asarray([int(np.argmax(counts))], dtype=np.int32)
        label_map = np.full(raw_k, -1, dtype=np.int32)
        for final_id, raw_id in enumerate(healthy):
            label_map[int(raw_id)] = int(final_id)
        for raw_id in range(raw_k):
            if label_map[raw_id] >= 0:
                continue
            nearest_healthy = int(healthy[np.argmax(raw_centers[healthy] @ raw_centers[raw_id])])
            label_map[raw_id] = label_map[nearest_healthy]

        final_labels = label_map[raw_labels]
        final_k = int(final_labels.max()) + 1
        final_centers = np.zeros((final_k, Zn.shape[1]), dtype=np.float32)
        for event_id in range(final_k):
            members = idx[final_labels == event_id]
            final_centers[event_id] = normalize(
                Zn[members].mean(axis=0, keepdims=True), norm="l2"
            )[0]

        topic_models[int(topic_id)] = {
            "kmeans": kmeans,
            "label_map": label_map,
            "centers_norm": final_centers,
            "n_events": final_k,
            "training_documents": n,
        }
        total_events += final_k

    return {
        "algo_version": ALGO_VERSION,
        "train_cutoff": cfg["train_cutoff"].isoformat(sep=" "),
        "vectorizer": vectorizer,
        "nmf": nmf,
        "svd": svd,
        "topic_models": topic_models,
        "n_topics": int(n_topics),
        "n_events": int(total_events),
        # Compatibility alias used by enrichment status/older monitoring.
        "n_clusters": int(total_events),
        "training_documents": int(len(texts)),
        "vocabulary_size": int(X.shape[1]),
    }



def _transform_rows(rows: list[dict[str, Any]], artifact: dict[str, Any], cfg: dict[str, Any]):
    if not rows:
        return (
            np.zeros((0, 1), dtype=np.float32),
            np.zeros(0, dtype=np.int32),
            np.zeros(0, dtype=np.float32),
            np.zeros(0, dtype=np.int32),
            np.zeros(0, dtype=np.float32),
        )
    from sklearn.preprocessing import normalize

    texts = [_document_text(r, cfg["text_chars"]) for r in rows]
    X = artifact["vectorizer"].transform(texts)
    # scikit-learn's default positive tolerance stops NMF transform when the
    # aggregate batch criterion converges.  That can make one row differ very
    # slightly when unrelated later rows are appended to the same period batch.
    # A zero tolerance forces the fixed max_iter path: every row still solves
    # independently, and its result is exactly prefix-invariant.
    nmf = artifact["nmf"]
    original_tol = float(getattr(nmf, "tol", 0.0))
    try:
        nmf.tol = 0.0
        W = nmf.transform(X).astype(np.float32, copy=False)
    finally:
        nmf.tol = original_tol
    sums = W.sum(axis=1)
    top_ids = np.argmax(W, axis=1).astype(np.int32, copy=False)
    top_vals = W[np.arange(W.shape[0]), top_ids]
    topic_focus = np.divide(top_vals, sums, out=np.zeros_like(top_vals), where=sums > 1e-12)

    Z = artifact["svd"].transform(X).astype(np.float32, copy=False)
    Zn = normalize(Z, norm="l2", copy=False)
    labels = np.zeros(len(rows), dtype=np.int32)
    cluster_sim = np.zeros(len(rows), dtype=np.float32)

    for topic_id in np.unique(top_ids):
        idx = np.flatnonzero(top_ids == topic_id)
        model_info = artifact["topic_models"].get(int(topic_id))
        if model_info is None:
            continue
        kmeans = model_info["kmeans"]
        if kmeans is None:
            raw = np.zeros(len(idx), dtype=np.int32)
        else:
            raw = kmeans.predict(Zn[idx]).astype(np.int32, copy=False)
        final = np.asarray(model_info["label_map"], dtype=np.int32)[raw]
        labels[idx] = final
        centers = np.asarray(model_info["centers_norm"], dtype=np.float32)
        cluster_sim[idx] = np.einsum("ij,ij->i", Zn[idx], centers[final]).astype(np.float32, copy=False)

    # Return normalized SVD vectors: novelty/confirmation now use the richer
    # semantic subspace instead of coarse NMF topic-mixture vectors.
    return Zn, labels, topic_focus.astype(np.float32), top_ids, cluster_sim


# ---------------------------------------------------------------------------
# Causal novelty + cross-source confirmation
# ---------------------------------------------------------------------------


def _quality(topic_focus: float, cluster_sim: float, novelty: float, confirmation_count: int) -> float:
    confirmation_boost = 1.0 + 0.20 * math.log2(max(1, int(confirmation_count)))
    q = (
        (0.40 + 0.60 * max(0.0, min(1.0, topic_focus)))
        * (0.40 + 0.60 * max(0.0, min(1.0, cluster_sim)))
        * (0.55 + 0.45 * max(0.0, min(1.0, novelty)))
        * confirmation_boost
    )
    return float(max(0.05, min(3.0, q)))



def _enrich_rows(
    rows: list[dict[str, Any]],
    artifact: dict[str, Any],
    cfg: dict[str, Any],
    history: dict[int, deque],
    *,
    write_from: datetime | None = None,
    write_to: datetime | None = None,
) -> list[dict[str, Any]]:
    if not rows:
        return []

    Wn, labels, topic_focus, top_ids, cluster_sim = _transform_rows(rows, artifact, cfg)
    novelty_horizon = timedelta(days=int(cfg["novelty_days"]))
    confirm_horizon = timedelta(hours=int(cfg["confirm_hours"]))
    confirm_threshold = float(cfg["confirm_similarity"])

    out: list[dict[str, Any]] = []
    for i, row in enumerate(rows):
        dt = row.get("date_dt")
        if not isinstance(dt, datetime):
            continue
        cluster_id = int(labels[i])
        topic_id = int(top_ids[i])
        vector = np.asarray(Wn[i], dtype=np.float32)
        # Similar-story lookup is gated by dominant NMF topic.  Within that
        # broad semantic family, normalized SVD vectors detect neighboring/local
        # stories without requiring an identical event-cluster assignment.
        bucket = history[topic_id]
        min_dt = dt - novelty_horizon
        while bucket and bucket[0][0] < min_dt:
            bucket.popleft()

        max_similarity = 0.0
        confirmed_sources = {str(row.get("press") or "")}
        for prev_dt, prev_press, prev_vec in bucket:
            sim = float(np.dot(vector, prev_vec))
            if sim > max_similarity:
                max_similarity = sim
            if dt - prev_dt <= confirm_horizon and sim >= confirm_threshold:
                confirmed_sources.add(prev_press)

        novelty = 1.0 if not bucket else max(0.0, min(1.0, 1.0 - max_similarity))
        confirmation_count = max(1, len(confirmed_sources))
        focus = float(topic_focus[i])
        c_sim = max(0.0, min(1.0, float(cluster_sim[i])))
        press = str(row.get("press") or "")[:3]
        # Dense historical identity only.  Source/focus/fit/novelty/
        # confirmation stay as separate causal columns and are selected by var.
        # This avoids multiplying one semantic event into thousands of tiny
        # historical contexts.
        event_type = f"t{topic_id:02d}|e{cluster_id:02d}"
        quality = _quality(focus, c_sim, novelty, confirmation_count)

        should_write = (write_from is None or dt >= write_from) and (write_to is None or dt < write_to)
        if should_write:
            out.append({
                "date_dt": dt,
                "press": press,
                "source_id": int(row.get("source_id") or 0),
                "feed": str(row.get("feed") or "")[:64],
                "event_type": event_type,
                # Standard Brain event engine calls this pct_change.  Here it is
                # deliberately NOT sentiment: it is a positive event-quality
                # scalar used only by var=7 weighting / ML active-code exposure.
                "pct_change": quality,
                "cluster_id": cluster_id,
                "topic_id": topic_id,
                "topic_focus": focus,
                "cluster_similarity": c_sim,
                "novelty": novelty,
                "confirmation_count": confirmation_count,
                "quality_score": quality,
            })

        # Append only after computing current features: every similarity feature
        # can see current/past documents, never future documents.
        bucket.append((dt, press, vector.copy()))

    return out


async def _upsert(engine_vlad, table: str, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    from sqlalchemy import text
    sql = text(f"""
        INSERT INTO `{table}` (
            date_dt, press, source_id, feed, event_type, pct_change,
            cluster_id, topic_id, topic_focus, cluster_similarity,
            novelty, confirmation_count, quality_score
        ) VALUES (
            :date_dt, :press, :source_id, :feed, :event_type, :pct_change,
            :cluster_id, :topic_id, :topic_focus, :cluster_similarity,
            :novelty, :confirmation_count, :quality_score
        )
        ON DUPLICATE KEY UPDATE
            date_dt=VALUES(date_dt), feed=VALUES(feed), event_type=VALUES(event_type),
            pct_change=VALUES(pct_change), cluster_id=VALUES(cluster_id),
            topic_id=VALUES(topic_id), topic_focus=VALUES(topic_focus),
            cluster_similarity=VALUES(cluster_similarity), novelty=VALUES(novelty),
            confirmation_count=VALUES(confirmation_count), quality_score=VALUES(quality_score)
    """)
    written = 0
    async with engine_vlad.begin() as conn:
        for start in range(0, len(rows), 1000):
            batch = rows[start:start + 1000]
            await conn.execute(sql, batch)
            written += len(batch)
    return written


def _periods(start: datetime, end: datetime, days: int):
    cur = start
    step = timedelta(days=max(1, days))
    while cur < end:
        nxt = min(end, cur + step)
        yield cur, nxt
        cur = nxt


def _affected_windows(changed_dates: list[datetime], cfg: dict[str, Any]):
    if not changed_dates:
        return []
    horizon = timedelta(days=int(cfg["novelty_days"]))
    raw = [(dt - horizon, dt, dt + horizon + timedelta(seconds=1)) for dt in changed_dates]
    raw.sort(key=lambda x: x[0])
    merged: list[list[datetime]] = []
    for context_start, write_start, end in raw:
        if not merged or context_start > merged[-1][2]:
            merged.append([context_start, write_start, end])
        else:
            merged[-1][0] = min(merged[-1][0], context_start)
            merged[-1][1] = min(merged[-1][1], write_start)
            merged[-1][2] = max(merged[-1][2], end)
    return [tuple(x) for x in merged]


# ---------------------------------------------------------------------------
# Shared Brain context indexes / weights
# ---------------------------------------------------------------------------

async def _table_exists(engine_vlad, table_name: str) -> bool:
    from sqlalchemy import text
    async with engine_vlad.connect() as conn:
        value = (await conn.execute(text("""
            SELECT COUNT(*)
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :tbl
        """), {"tbl": table_name})).scalar()
    return bool(int(value or 0))


async def _table_has_rows(engine_vlad, table_name: str) -> bool:
    from sqlalchemy import text
    try:
        async with engine_vlad.connect() as conn:
            row = (await conn.execute(text(f"SELECT 1 FROM `{table_name}` LIMIT 1"))).fetchone()
        return row is not None
    except Exception:
        return False


async def _ensure_shared_weights(
    engine_vlad,
    enriched_table: str,
    *,
    reset: bool = False,
) -> int:
    """Materialize standard ctx/mode/shift codes once for both services."""
    from sqlalchemy import text

    indexes_table = f"{enriched_table}_indexes"
    weights_table = f"{enriched_table}_weights"
    async with engine_vlad.begin() as conn:
        await conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{weights_table}` (
                id          INT         NOT NULL AUTO_INCREMENT,
                weight_code VARCHAR(40) NOT NULL,
                ctx_id      BIGINT      NOT NULL,
                mode        TINYINT     NOT NULL,
                shift       SMALLINT    NOT NULL DEFAULT 0,
                PRIMARY KEY (id),
                UNIQUE KEY uk_wc (weight_code),
                INDEX idx_ctx_id (ctx_id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """))
        if reset:
            await conn.execute(text(f"TRUNCATE TABLE `{weights_table}`"))

    async with engine_vlad.connect() as conn:
        rows = (await conn.execute(text(f"""
            SELECT idx.id, COALESCE(agg.cnt, 0) AS occ
            FROM `{indexes_table}` idx
            LEFT JOIN (
                SELECT event_type, COUNT(*) AS cnt
                FROM `{enriched_table}`
                GROUP BY event_type
            ) agg ON agg.event_type = idx.event_type
            WHERE idx.mask_id = 1
            ORDER BY idx.id
        """))).fetchall()

    payload = [
        {
            "wc": f"{int(ctx_id)}_{mode}_{shift}",
            "ctx_id": int(ctx_id),
            "mode": mode,
            "shift": shift,
        }
        for ctx_id, occ in rows
        for mode in (0, 1)
        for shift in range(0, (SHIFT_WINDOW if int(occ or 0) >= 2 else 0) + 1)
    ]
    if payload:
        async with engine_vlad.begin() as conn:
            sql = text(f"""
                INSERT IGNORE INTO `{weights_table}`
                    (weight_code, ctx_id, mode, shift)
                VALUES (:wc, :ctx_id, :mode, :shift)
            """)
            for start in range(0, len(payload), 1000):
                await conn.execute(sql, payload[start:start + 1000])
    return len(payload)


async def _sync_shared_brain_indexes(
    engine_vlad,
    enriched_table: str,
    *,
    mode: str,
    index_from: datetime | None,
) -> dict[str, Any]:
    """Keep shared event_type indexes stable and avoid 79/80 duplicate rebuilds.

    A full NLP rebuild creates a fresh index universe. Incremental enrichment only
    appends newly observed event_type combinations from the affected date onward;
    old unused contexts may remain, which intentionally preserves ctx ids and
    therefore cache/ML code identity.
    """
    from dataset_indexer import build_indexes, parse_indexes

    mask_table = f"{enriched_table}_mask"
    indexes_table = f"{enriched_table}_indexes"
    weights_table = f"{enriched_table}_weights"
    mask_ok = (await _table_exists(engine_vlad, mask_table)) and (await _table_has_rows(engine_vlad, mask_table))
    indexes_ok = (await _table_exists(engine_vlad, indexes_table)) and (await _table_has_rows(engine_vlad, indexes_table))
    weights_ok = (await _table_exists(engine_vlad, weights_table)) and (await _table_has_rows(engine_vlad, weights_table))

    full_index = mode == "full" or not (mask_ok and indexes_ok)
    indexed = False
    if full_index:
        await build_indexes(engine_vlad, enriched_table, ["event_type"])
        ok = await parse_indexes(engine_vlad, enriched_table, "date_dt", date=None)
        if not ok:
            raise RuntimeError(f"Could not build shared indexes for {enriched_table}")
        indexed = True
    elif mode == "incremental" and index_from is not None:
        ok = await parse_indexes(engine_vlad, enriched_table, "date_dt", date=index_from)
        if not ok:
            raise RuntimeError(f"Could not increment shared indexes for {enriched_table}")
        indexed = True

    # If an old deployment left indexes but no weights, repair automatically.
    weight_rows = 0
    if indexed or not weights_ok:
        weight_rows = await _ensure_shared_weights(
            engine_vlad, enriched_table, reset=full_index,
        )

    return {
        "mode": "full" if full_index else ("incremental" if indexed else "noop"),
        "mask_table": mask_table,
        "indexes_table": indexes_table,
        "weights_table": weights_table,
        "weight_codes_seen": weight_rows,
    }


# ---------------------------------------------------------------------------
# Shared enrichment entry point
# ---------------------------------------------------------------------------

async def _enrich_unlocked(engine_vlad, engine_brain) -> dict[str, Any]:
    from sqlalchemy import text

    cfg = _nlp_cfg()
    service_cfg = get_service_config() or {}
    dcfg = service_cfg.get("dataset") or {}
    enriched_table = str(dcfg.get("enriched_table") or "vlad_news_algo_events")
    meta_table = f"{enriched_table}_meta"
    artifact_table = f"{enriched_table}_artifact"
    signature = _signature(cfg)

    await _ensure_tables(engine_vlad, enriched_table, meta_table, artifact_table)

    stored_schema = await _meta_get(engine_vlad, meta_table, "schema_version")
    stored_sig = await _meta_get(engine_vlad, meta_table, "algo_signature")
    full_rebuild = stored_schema != ENRICH_SCHEMA_VERSION or stored_sig != signature

    artifact = await _artifact_load(engine_vlad, artifact_table, signature)
    fitted = False
    artifact_bytes = 0
    if artifact is None:
        sample = await _training_sample(engine_brain, cfg)
        artifact = _fit_artifact(sample, cfg)
        artifact_bytes = await _artifact_store(engine_vlad, artifact_table, signature, artifact)
        fitted = True
        full_rebuild = True

    source_max = await _source_max_ids(engine_brain)
    last_ids = {
        press: int(await _meta_get(engine_vlad, meta_table, f"last_id_{press}") or 0)
        for press in SOURCES
    }

    if not full_rebuild and all(source_max[p] <= last_ids[p] for p in SOURCES):
        return {
            "mode": "noop",
            "artifact_fitted": fitted,
            "artifact_bytes": artifact_bytes,
            "training_documents": int(artifact.get("training_documents", 0)),
            "vocabulary_size": int(artifact.get("vocabulary_size", 0)),
            "topics": int(artifact.get("n_topics", 0)),
            "clusters": int(artifact.get("n_clusters", 0)),
            "events_written": 0,
            "source_max_ids": source_max,
            "index_from": None,
        }

    events_written = 0
    source_rows_seen = 0

    if full_rebuild:
        # Do not truncate until a valid frozen artifact exists.
        async with engine_vlad.begin() as conn:
            await conn.execute(text(f"TRUNCATE TABLE `{enriched_table}`"))

        mn, mx = await _source_bounds(engine_brain)
        if mn is not None and mx is not None:
            history: dict[int, deque] = defaultdict(deque)
            rebuild_end = mx + timedelta(seconds=1)
            for start, end in _periods(mn, rebuild_end, int(cfg["period_days"])):
                rows = await _fetch_period(engine_brain, start, end)
                source_rows_seen += len(rows)
                enriched = _enrich_rows(rows, artifact, cfg, history)
                events_written += await _upsert(engine_vlad, enriched_table, enriched)

        await _meta_set_many(engine_vlad, meta_table, {
            "schema_version": ENRICH_SCHEMA_VERSION,
            "algo_signature": signature,
            **{f"last_id_{p}": source_max[p] for p in SOURCES},
        })
        return {
            "mode": "full",
            "artifact_fitted": fitted,
            "artifact_bytes": artifact_bytes,
            "training_documents": int(artifact.get("training_documents", 0)),
            "vocabulary_size": int(artifact.get("vocabulary_size", 0)),
            "topics": int(artifact.get("n_topics", 0)),
            "clusters": int(artifact.get("n_clusters", 0)),
            "source_rows_seen": source_rows_seen,
            "events_written": events_written,
            "source_max_ids": source_max,
            "index_from": None,
        }

    # Incremental: delayed/old inserts can change novelty/confirmation for the
    # following seven days. Recompute only those causally affected windows.
    changed_dates = await _changed_dates(engine_brain, last_ids)
    windows = _affected_windows(changed_dates, cfg)
    for context_start, write_start, end in windows:
        rows = await _fetch_period(engine_brain, context_start, end)
        source_rows_seen += len(rows)
        history: dict[int, deque] = defaultdict(deque)
        enriched = _enrich_rows(
            rows, artifact, cfg, history,
            write_from=write_start,
            write_to=end,
        )
        events_written += await _upsert(engine_vlad, enriched_table, enriched)

    # Advance watermarks only after every affected window committed.
    await _meta_set_many(engine_vlad, meta_table, {
        "schema_version": ENRICH_SCHEMA_VERSION,
        "algo_signature": signature,
        **{f"last_id_{p}": source_max[p] for p in SOURCES},
    })
    return {
        "mode": "incremental",
        "artifact_fitted": fitted,
        "training_documents": int(artifact.get("training_documents", 0)),
        "vocabulary_size": int(artifact.get("vocabulary_size", 0)),
        "topics": int(artifact.get("n_topics", 0)),
        "clusters": int(artifact.get("n_clusters", 0)),
        "changed_news": len(changed_dates),
        "recomputed_windows": len(windows),
        "source_rows_seen": source_rows_seen,
        "events_written": events_written,
        "source_max_ids": source_max,
        "index_from": min(changed_dates).isoformat(sep=" ") if changed_dates else None,
    }


async def enrich_dataset(engine_vlad, engine_brain) -> dict[str, Any]:
    """Build/update the ONE shared algorithmic-news dataset for 79 and 80."""
    from sqlalchemy import text

    cfg = get_service_config() or {}
    dcfg = cfg.get("dataset") or {}
    enriched_table = str(dcfg.get("enriched_table") or "vlad_news_algo_events")
    lock_name = f"news_algo_enrich:{enriched_table}"[:64]

    async with engine_vlad.connect() as lock_conn:
        acquired = (await lock_conn.execute(
            text("SELECT GET_LOCK(:name, 1800)"), {"name": lock_name}
        )).scalar()
        if int(acquired or 0) != 1:
            raise RuntimeError(f"Could not acquire shared news enrichment lock: {lock_name}")
        try:
            result = await _enrich_unlocked(engine_vlad, engine_brain)
            index_from_raw = result.get("index_from") if isinstance(result, dict) else None
            index_from = None
            if index_from_raw:
                try:
                    index_from = datetime.fromisoformat(str(index_from_raw)[:19])
                except ValueError:
                    index_from = None
            index_stats = await _sync_shared_brain_indexes(
                engine_vlad, enriched_table,
                mode=str((result or {}).get("mode") or "noop"),
                index_from=index_from,
            )
            result["shared_index"] = index_stats
            return result
        finally:
            try:
                await lock_conn.execute(text("SELECT RELEASE_LOCK(:name)"), {"name": lock_name})
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Runtime Brain model: indexed O(current-events × analogs), not O(all-news)
# ---------------------------------------------------------------------------

def _var_allows_row(row: dict[str, Any], var: int) -> bool:
    """Apply vars to causal row features, not to event identity tokens."""
    try:
        focus = float(row.get("topic_focus") or 0.0)
        fit = float(row.get("cluster_similarity") or 0.0)
        novelty = float(row.get("novelty") or 0.0)
        confirm = int(row.get("confirmation_count") or 1)
    except (TypeError, ValueError):
        return var in (0, 7)
    if var == 0:  # all semantic events
        return True
    if var == 1:  # clearly dominated by one NMF topic
        return focus >= TOPIC_FOCUS_MIN
    if var == 2:  # close to its topic-local event centroid
        return fit >= CLUSTER_SIM_MIN
    if var == 3:  # novel vs prior seven days
        return novelty >= NOVELTY_MIN
    if var == 4:  # confirmed by >=2 sources causally
        return confirm >= 2
    if var == 5:  # confirmed by >=3 sources causally
        return confirm >= 3
    if var == 6:  # novel + cross-source confirmation
        return novelty >= NOVELTY_MIN and confirm >= 2
    if var == 7:  # quality-weighted all events
        return True
    return False


def _frame_date(dt: datetime, is_daily: bool) -> datetime:
    if is_daily:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.replace(minute=0, second=0, microsecond=0)


def _ctx_reverse(ctx_index: dict) -> dict[str, tuple[int, dict]]:
    return {
        str(info.get("event_type") or "").strip().lower(): (int(info["id"]), info)
        for info in (ctx_index or {}).values()
        if info.get("id") and info.get("event_type")
    }


def _dataset_token(source, dataset_index: dict) -> tuple:
    if not source:
        return (0, 0, 0)
    last = source[-1].get("date") or source[-1].get("date_dt")
    return (id(source), len(source), int(last.timestamp()) if isinstance(last, datetime) else 0)


def _current_rows(source, target_date: datetime, dataset_index: dict, is_daily: bool):
    if not source:
        return []
    horizon = timedelta(days=1) if is_daily else timedelta(hours=SHIFT_WINDOW)
    left_ts = int((target_date - horizon).timestamp())
    right_ts = int(target_date.timestamp())
    ts_arr = dataset_index.get("dataset_timestamps")
    if ts_arr is not None and len(ts_arr) == len(source):
        lo = int(np.searchsorted(ts_arr, left_ts, side="left"))
        hi = int(np.searchsorted(ts_arr, right_ts, side="right"))
        return source[lo:hi]

    dates = dataset_index.get("dates") or []
    if dates and len(dates) == len(source):
        lo = bisect.bisect_left(dates, target_date - horizon)
        hi = bisect.bisect_right(dates, target_date)
        return source[lo:hi]

    # Defensive fallback; expected only in standalone unit tests.
    return [
        r for r in source
        if isinstance((r.get("date") or r.get("date_dt")), datetime)
        and target_date - horizon <= (r.get("date") or r.get("date_dt")) <= target_date
    ]


def _previous_direction(np_rates: dict | None, target_date: datetime) -> tuple[bool, str]:
    if not np_rates:
        return False, "ext_min"
    dates_ns = np_rates.get("dates_ns")
    opens = np_rates.get("open")
    closes = np_rates.get("close")
    if dates_ns is None or opens is None or closes is None or len(dates_ns) == 0:
        return False, "ext_min"
    cut = int(np.searchsorted(dates_ns, int(target_date.timestamp()), side="left"))
    if cut <= 0:
        return False, "ext_min"
    idx = cut - 1
    predict_max = float(closes[idx]) > float(opens[idx])
    return predict_max, "ext_max" if predict_max else "ext_min"


def _aggregate_analogs_by_var(
    analog_rows: list[dict[str, Any]],
    analog_dates: list[datetime],
    target_date: datetime,
    current_event_time: datetime,
    shift: int,
    *,
    is_daily: bool,
    np_rates: dict,
    ext_name: str,
) -> dict[int, tuple[float, float, int, float, float, float]]:
    """Aggregate historical outcomes once while maintaining all var subsets."""
    end = bisect.bisect_left(analog_dates, target_date)
    stats = {v: [0.0, 0.0, 0, 0.0, 0.0, 0.0] for v in VAR_RANGE}
    if end <= 0:
        return {v: tuple(x) for v, x in stats.items()}

    dates_ns = np_rates.get("dates_ns") if np_rates else None
    t1_arr = np_rates.get("t1") if np_rates else None
    ext_arr = np_rates.get(ext_name) if np_rates else None
    if dates_ns is None or t1_arr is None or ext_arr is None:
        return {v: tuple(x) for v, x in stats.items()}

    unit = timedelta(days=1) if is_daily else timedelta(hours=1)
    for j in range(end):
        analog_dt = analog_dates[j]
        if analog_dt == current_event_time:
            continue
        outcome_time = analog_dt + unit * int(shift)
        frame = _frame_date(outcome_time, is_daily)
        if frame + unit > target_date:
            continue
        ts = int(frame.timestamp())
        idx = int(np.searchsorted(dates_ns, ts, side="left"))
        if idx >= len(dates_ns) or int(dates_ns[idx]) != ts:
            continue

        analog = analog_rows[j]
        stored_t1 = float(t1_arr[idx])
        hit = 1.0 if bool(ext_arr[idx]) else 0.0
        quality = float(analog.get("quality_score") or 1.0)
        q_weight = min(max(quality / 0.65, 0.25), 2.5)

        for var in VAR_RANGE:
            if not _var_allows_row(analog, var):
                continue
            st = stats[var]
            st[0] += stored_t1
            st[1] += hit
            st[2] += 1
            st[3] += stored_t1 * q_weight
            st[4] += hit * q_weight
            st[5] += q_weight

    return {v: tuple(x) for v, x in stats.items()}


def _mode1_score(hits: float, total: float, predict_max: bool) -> float:
    if total <= 0:
        return 0.0
    score = ((hits / total) * 2.0) - 1.0
    return -score if predict_max else score


def _add_code(target: dict[str, float], code: str, value: float) -> None:
    value = float(value)
    if value != 0.0:
        target[code] = target.get(code, 0.0) + value


def _compute_all_slots_for_date(
    dataset: list[dict],
    target_date: datetime,
    dataset_index: dict,
) -> dict[tuple[int, int], dict[str, float]]:
    outputs = {(t, v): {} for t in TYPES_RANGE for v in VAR_RANGE}
    if target_date < _nlp_cfg()["train_cutoff"]:
        return outputs

    di = dataset_index or {}
    source = di.get("full_dataset") if di.get("full_dataset") is not None else dataset
    if not source:
        return outputs
    by_key = di.get("by_key") or {}
    key_dates = di.get("key_dates") or {}
    ctx_reverse = _ctx_reverse(di.get("ctx_index") or {})
    np_rates = di.get("np_rates")
    if not by_key or not key_dates or not ctx_reverse or not np_rates:
        return outputs

    is_daily = bool(di.get("is_daily"))
    unit_seconds = 86400 if is_daily else 3600
    predict_max, ext_name = _previous_direction(np_rates, target_date)
    current = _current_rows(source, target_date, di, is_daily)

    for row in current:
        event_dt = row.get("date") or row.get("date_dt")
        if not isinstance(event_dt, datetime) or event_dt > target_date:
            continue
        event_type = str(row.get("event_type") or "").strip().lower()
        ctx = ctx_reverse.get(event_type)
        if ctx is None:
            continue
        ctx_id, _ctx_info = ctx

        seconds = max(0.0, (target_date - event_dt).total_seconds())
        shift = int(seconds // unit_seconds)
        if not is_daily and shift > SHIFT_WINDOW:
            continue
        if is_daily and shift > 1:
            continue

        analog_rows = by_key.get(event_type) or []
        analog_dates = key_dates.get(event_type) or []
        if len(analog_rows) < 3:
            continue

        by_var = _aggregate_analogs_by_var(
            analog_rows, analog_dates, target_date, event_dt, shift,
            is_daily=is_daily, np_rates=np_rates, ext_name=ext_name,
        )

        for var in VAR_RANGE:
            if not _var_allows_row(row, var):
                continue
            raw_t1, raw_hits, n, w_t1, w_hits, w_total = by_var[var]
            if n < 3:
                continue
            raw_mode1 = _mode1_score(raw_hits, float(n), predict_max)
            weighted_mode1 = _mode1_score(w_hits, w_total, predict_max)
            mode0 = w_t1 if var == 7 else raw_t1
            mode1 = weighted_mode1 if var == 7 else raw_mode1
            code0 = f"{ctx_id}_0_{shift}"
            code1 = f"{ctx_id}_1_{shift}"

            if 0 in TYPES_RANGE:
                _add_code(outputs[(0, var)], code0, round(mode0, 6))
                _add_code(outputs[(0, var)], code1, round(mode1, 6))
            if 1 in TYPES_RANGE:
                _add_code(outputs[(1, var)], code0, round(mode0, 6))
            if 2 in TYPES_RANGE:
                _add_code(outputs[(2, var)], code1, round(mode1, 6))
            # Type 3/4: expose the same active-code universe used by the existing
            # reverse-learning services.  In ML mode only keys matter; weights are
            # learned by Brain's ReverseStore.
            for calc_type in (3, 4):
                if calc_type not in TYPES_RANGE:
                    continue
                if mode0 != 0.0:
                    _add_code(outputs[(calc_type, var)], code0, round(mode0, 6))
                if raw_hits > 0.0:
                    _add_code(outputs[(calc_type, var)], code1, 1.0)

    return outputs


def _runtime_cache_token(dataset: list[dict], dataset_index: dict, target_dates: tuple[int, ...] | None = None) -> tuple:
    source = dataset_index.get("full_dataset") if dataset_index.get("full_dataset") is not None else dataset
    np_rates = dataset_index.get("np_rates") or {}
    dates_ns = np_rates.get("dates_ns")
    rates_tail = int(dates_ns[-1]) if dates_ns is not None and len(dates_ns) else 0
    base = (
        _dataset_token(source, dataset_index),
        str(dataset_index.get("rates_table") or ""),
        bool(dataset_index.get("is_daily")),
        rates_tail,
    )
    return base + ((target_dates,) if target_dates is not None else ())


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
    if not dataset or not date or dataset_index is None:
        return {}
    calc_type, calc_var = int(type), int(var)
    if calc_type not in TYPES_RANGE or calc_var not in VAR_RANGE:
        return {}

    di = dict(dataset_index)
    # Use the framework's full immutable dataset only with our own explicit
    # <= target_date bounds. This avoids per-call prefix construction and cannot
    # introduce future news into current/analog selection.
    token = _runtime_cache_token(dataset, di) + (int(date.timestamp()),)
    with _CACHE_LOCK:
        cached = _SINGLE_CACHE.get(token)
        if cached is not None:
            _SINGLE_CACHE.move_to_end(token)
            return dict(cached.get((calc_type, calc_var), {}))

    all_slots = _compute_all_slots_for_date(dataset, date, di)
    with _CACHE_LOCK:
        _SINGLE_CACHE[token] = all_slots
        _SINGLE_CACHE.move_to_end(token)
        while len(_SINGLE_CACHE) > _SINGLE_CACHE_MAX:
            _SINGLE_CACHE.popitem(last=False)
    return dict(all_slots.get((calc_type, calc_var), {}))


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
    """Compute all type/var slots in one chronological pass and reuse them.

    Brain Framework invokes batch_model once per slot.  The first invocation for
    a date batch computes the whole 5×8 slot cube; the remaining slot calls are
    cache lookups.  This is critical for ~872k news rows.
    """
    if not dates or not dataset or dataset_index is None:
        return {d: {} for d in dates}
    calc_type, calc_var = int(type), int(var)
    if calc_type not in TYPES_RANGE or calc_var not in VAR_RANGE:
        return {d: {} for d in dates}

    di = dict(dataset_index)
    date_token = tuple(int(d.timestamp()) for d in dates)
    key = _runtime_cache_token(dataset, di, date_token)

    with _CACHE_LOCK:
        cached = _BATCH_CACHE.get(key)
        if cached is not None:
            _BATCH_CACHE.move_to_end(key)
            selected = cached.get((calc_type, calc_var), {})
            return {d: dict(selected.get(d, {})) for d in dates}

    cube: dict[tuple[int, int], dict[datetime, dict[str, float]]] = {
        (t, v): {} for t in TYPES_RANGE for v in VAR_RANGE
    }
    for dt in dates:
        slots = _compute_all_slots_for_date(dataset, dt, di)
        for slot, value in slots.items():
            cube[slot][dt] = value

    with _CACHE_LOCK:
        _BATCH_CACHE[key] = cube
        _BATCH_CACHE.move_to_end(key)
        while len(_BATCH_CACHE) > _BATCH_CACHE_MAX:
            _BATCH_CACHE.popitem(last=False)

    selected = cube.get((calc_type, calc_var), {})
    return {d: dict(selected.get(d, {})) for d in dates}
