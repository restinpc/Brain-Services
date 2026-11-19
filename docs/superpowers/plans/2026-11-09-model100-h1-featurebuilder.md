# Unified H1 FeatureBuilder implementation plan

Scope: one read-only source loader and deterministic offline/live H1 feature library; no model/service/training or existing data writes.
1. Reuse vetted 11-source conservative macro contract and source policy, preserving first-estimate/amendment/expiry rules. Lock schema before replay.
2. Pure shared H1 engine: Yahoo closed bars, macro first versions, existing Service79/80 NLP events, Binance proxy, GDELT aggregates; CFTC optional and disabled by default.
3. SQLAlchemy async streaming loader takes existing Brain engines; SELECT-only transactions, explicit UTC, B1 identity, narrow projections without t1 or text. Frozen input snapshot and per-row provenance.
4. STRICT_ASOF uses receipt/text/NLP clocks. RESEARCH separately labels legacy date assumptions; no historical availability invented.
5. Append-only resumable file dataset with schema/hash/config checks and input invalidation checks. Same build_at code for batches/live; bounded lookback and binary-search event windows.
6. Synthetic tests: future news/macro/revisions/open bars; deterministic repeats, incremental/full equality, missing groups, dataset recovery. Read-only B1 2025-26 benchmark for both modes and actual stats.
7. Commit code/tests/docs only to feature branch; runtime stays outside Git. No production checkout changes or service restart.
