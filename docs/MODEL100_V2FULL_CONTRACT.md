# Model100 V2-Full input contract

Dataset: `model100-h1-v2full-20261109`. No neural network, targets, weights or trading signals.

## Storage and immutability

One existing table: `brain.vlad_model100_h1_features`. Key remains `(dataset_version, mode, cutoff_utc)`.
V1 is neither updated nor deleted. Add nullable `rich_context JSON`, `rich_sha256 CHAR(64)` and
`context_sha256 CHAR(64)`, with explicit `ALGORITHM=INSTANT`; unsupported instant DDL fails without
falling back to a rebuild. Root migration holds the existing registered writer lease and first streams
every old V1 field, including formation time, to a compressed backup and manifest outside Git.
Rollback means retaining additive columns and immutable rows while disabling the V2 extension;
never deleting V1, reverting source data or recreating the table.

The fixed 489 values/masks reuse the V1 implementation and IEEE754 hex codec. The V2 schema embeds
the rich contract. The registry schema hash therefore differs intentionally from V1 even though fixed
feature names/order remain identical. `row_hash` verifies the reconstructed fixed row.
V2 alone uses `math.fsum` over admitted news/GDELT windows. This removes tiny rounding differences
caused by subtracting long versus short historical prefix sums. The policy is part of the V2 schema;
V1 implementation and stored float bits are unchanged. Admission/provenance rules are unchanged.

Rich JSON is canonical (sorted keys, UTF-8, compact separators, no NaN). Float values are tagged
`{"$f64":"0x1...p..."}`; SQL DECIMAL fields remain exact decimal strings. Native integers/nulls
remain integers/nulls. Reserved `$` keys are forbidden in input. `rich_sha256` hashes the full encoded
context; each group has a separate SHA. `context_sha256` hashes the fixed row hash, fixed payload SHA
and rich SHA together. Any insert is a complete atomic row. Existing different content is an error,
never an UPDATE. Payload limit: 512 KiB of canonical rich JSON.

## Groups

- **news:** latest 64 admissible article events in the preceding 24 hours, selected and sorted by
  availability and stable provider/article id. Actual `event_type`/`cluster_id` attach only when the NLP
  row version clock matches the frozen snapshot. Topic, novelty, quality, confirmations, similarity,
  publication/receipt/text/NLP clocks and existing artifact SHA are retained. Article identity does not
  assert cross-provider identity. No embedding is invented: the inspected event table has no persisted
  per-event embedding/NMF/SVD vector. Window statistics use 1/3/6/12/24 hours, include topic activity
  changes, total/omitted counts and omitted-topic distribution. No full article/HTML is copied.
- **macro:** current admissible indicator states, six preceding versions per series, up to 64 release
  events in seven days, and 32 separately retained known revisions. First values are never replaced
  with later revisions. Consensus/surprises remain null with a reason unless a genuine pre-release
  matched value exists. Independent nowcasts keep their role and are not called consensus. Existing
  forecast expiry policy is reused; no future release calendar extends expired forecasts. Historical
  backfill receipt after cutoff is masked and explicitly labelled, not called original Brain receipt.
- **price:** one Yahoo EUR/USD series from `brain.sasha_rates_eur_usd`; last 72 calendar hours, no
  synthetic candles, explicit gaps and 24/48/72-hour summaries. OHLC, shadows, true range, close
  position, log return, prior volatility, momentum, ATR, range position, acceleration and directional
  streak use only admitted closed candles. Session labels are fixed UTC activity buckets, explicitly
  not exchange/DST session clocks. Weekend gaps are not trading-hour substitutions.
- **gdelt:** geo country/category counts over all source rows; bounded GKG samples with actual themes,
  locations/country codes, tone, provider and source ids. Sampling retains hourly total counts and is
  labelled; sample distributions are separate from full geo distributions. No inferred causal links.
- **binance:** up to 72 admitted EURUSDT crypto proxy bars, native volumes/trades/buy/sell/delta and
  3/6/12/24-hour activity. Additional columns require matching original archive SHA. Historical data
  predating feature-ready/receipt do not become admissible merely because V2 has more fields.
- **cftc:** published report state/history only with actual public release and receipt clocks. Expected
  Friday dates alone are insufficient. Missing actual clocks produce null plus reason.
- **fxcm:** archive bid/ask/spread state, at most 24 admitted recent bars, actual receipt/feature-ready
  clocks required. Clearly `ARCHIVE_ONLY`; never presented as live global Forex spread or volume.

## Time modes

`STRICT_ASOF` preserves V1 admission: price row-version clocks, verified news Brain receipt/text/NLP
version, and source-specific macro availability. `RESEARCH` explicitly retains legacy-news-date and
historical-price assumptions. Neither admits open candles, future macro numeric values, future GDELT
availability or future proxy receipts. Backfilled macro documented public replay does not certify
contemporaneous Brain receipt. `DAY_ONLY` keeps next-day 12:00 UTC conservative policy, never a claimed
source publication hour. Later article text is not newly admitted to strict history.

## Scheduler and reads

Same registered parser ID168 and PHP `parser_run_registered_row()` / `BrainParserWriterFence`.
V1 commits first. V2 failures have separate status and cannot undo successful V1 work. At most 256
missing market cutoffs per mode per call; subsequent calls resume from missing keys. Historical
captures are pinned/hash checked; new hours use bounded source reads and retained snapshots.
No second cron, no writers or admission changes on B2–B4.

Python async interfaces, using the existing Brain engine:

```python
from shared.model100_rich_store import get_context, get_context_range
row = await get_context(engine, T, "STRICT_ASOF")
async for row in get_context_range(engine, start, end, "RESEARCH", groups=("news", "macro")):
    ...
```

Full reads verify full SHA; selected-group reads verify each selected group SHA. Fixed-only reads
select explicit old columns, avoiding rich JSON transfer. Stream batches are 16 rows. No snapshot
files are required by the read API. Unknown versions/groups or checksum mismatches fail explicitly.

## Source tables

`brain.sasha_rates_eur_usd`, `brain.vlad_model100_primary_source_records`,
`vlad.vlad_news_algo_events`, `vlad.vlad_news_algo_events_artifact`,
`brain.brain_news_publication_time`, the existing five `brain_*_news` tables for strict text proofs,
`brain.sasha_gdelt_news`, `brain.vlad_gdelt_geo_events`,
`brain.vlad_model100_binance_eurusdt_h1`, `brain.vlad_model100_cftc_tff_euro_fx`,
`brain.vlad_model100_fxcm_h1`. Existing immutable evidence files are consulted for strict text proof;
full texts, credentials and runtime snapshots are never committed to Git.

## Validation status

Runtime counts, benchmarks, V1 backup equality and native cron evidence are reported separately.
This contract is not a claim that pending deployment/history/cron checks have passed.
