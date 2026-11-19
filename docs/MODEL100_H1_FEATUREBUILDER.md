# Unified H1 inputs, version 1

This is a data preparation library, not a model or scheduled service. It never
trains, calls a model, changes source records, or installs a production hook.
Its source adapter reuses `common.build_engines()` / existing Framework engines
and the Framework server-side SQL stream/partition pattern. No new credentials.

## Entry points

* `shared.model100_h1_features.FeatureBuilder(snapshot, Config(), schema=None)`
* `build_at(T)` returns one immutable-by-copy feature row at a UTC hourly cutoff.
* `cutoffs(start, end)` returns the unique closing hours present in Yahoo input.
* `vectorize(row)` returns schema-ordered values (null -> zero), then null masks.
* `shared.model100_feature_sources.prepare_from_framework(state,start,end,config)`
  reads using existing `state.engine_brain` / `state.engine_vlad`.
* `FeatureBuilder.from_framework_state` accepts an explicitly loaded
  `global_rates['sasha_rates_eur_usd']`, with version clocks in strict mode.
  It refuses substitution of the legacy Finam RAM series.

`T` is a decision cutoff, not a candle opening timestamp. A bar with open 14:00
is never eligible before 15:00. `build_at` also accepts a weekend/missing hour:
it carries prior available observations and reports their age/staleness. Batch
`cutoffs` does not manufacture market candles for missing hours or holidays.

## Sources and clocks

| Group | Existing inputs | Rule |
|---|---|---|
| price | `brain.sasha_rates_eur_usd` | Yahoo EURUSD, date = UTC open, no `t1` |
| macro | `vlad_model100_primary_source_records`, existing compile contract | 11-source policy, first estimates, conservative DAY_ONLY |
| news | `vlad.vlad_news_algo_events`, publication sidecar and immutable text evidence | reuse persisted Service79/80 NLP, no fit or second text pipeline |
| proxy | `vlad_model100_binance_eurusdt_h1` | separate crypto proxy; close, feature-ready and receipt bounds |
| gdelt | `sasha_gdelt_news`, `vlad_gdelt_geo_events` | tone/structured hourly aggregates, not article NLP |
| cftc | `vlad_model100_cftc_tff_euro_fx` | opt-in; requires actual public release and receipt |

Macro allowlist: FED_FOMC, ECB_HIST, BLS_RELEASES, BEA_RELEASES,
EUROSTAT_FLASH, IFO_VINTAGE, GDP_NOW_ATL, ALFRED, ZEW, CLEVELAND_NOW, NYFED_NOW.
The reused policy excludes later revisions, unsuitable HTTP responses and
non-first GDP editions. Regions/horizons have independent columns. ADP is
released private-payroll context, not an NFP consensus. DAY_ONLY is no earlier
than next day 12:00 UTC. Forecasts expire at conservative target period end
(or an earlier documented bound), without learning a future release calendar.
This deliberately drops some forecasts made after the target period ended.
Actual series carry forward with age/stale/missing masks.

### STRICT_ASOF

* Yahoo current row is eligible at max(bar close, `loaded_at` UTC). `loaded_at`
  is an ON UPDATE version clock, not first receipt. Rows last updated before
  close remain partial and are excluded. Late historical updates cannot appear
  in the past. Cached unchanged complete OHLC preserves its earlier observed
  version clock despite later target-only updates. This yields sparse strict
  historical price features; no claim of historical tick vintages is made.
* News require receipt, an existing immutable text snapshot matching current
  title/text and SHA, and an NLP row version no earlier than text observation.
  Eligibility is the maximum of those clocks and verified publication (or
  conservative DAY_ONLY bound). Unknown publication does not prevent using a
  demonstrably received text; uncertainty is labelled. Backfill time is never
  invented as historical first receipt.
* Frozen NLP artifact signature, fit cutoff and source-code SHA are checked;
  rows before the fit cutoff are excluded. No artifact is unpickled or fitted.
* Macro is documented/conservative public replay from the existing contract,
  not a claim that Brain received an archive document at publication time.
* GDELT uses max(public clock, stored ingestion clock), rounded up to H1.

### RESEARCH

Yahoo complete historical bars are assumed publicly available at close. News
use the existing legacy `date_dt` as Service80 does, explicitly labelled
uncertain; current text/NLP may include later editorial revisions. GDELT uses
public timestamps. This mode is not certified historical Brain availability.
Proxy feature-ready evidence is still required. Modes cannot share a dataset.

## Output and provenance

Each row contains timestamp/date_dt, mode, schema_hash, features, a null mask for
every feature, group provenance and row_hash. Prices include their provider,
table and an input-history SHA; macro includes original URL/SHA and release
identity; news includes frozen artifact and range provenance digests; GDELT
uses a **derived DB aggregate digest**, not an original-document SHA. Immutable
input snapshots plus schema/manifest provide reproducible detailed lineage.
Missing group flags distinguish empty windows from known observations; no
future filling or interpolation is performed. No targets or labels are output.

Schema binds configuration, ordering, macro keys/policy and NLP artifact/code.
New macro keys or changed contracts require a new explicit schema/dataset.
Feature groups are configurable; CFTC is disabled by default. A future caller
must reject stale groups according to its own decision policy, using masks.

## CLI and incremental operation

From the repository root (with its existing Python environment):

```bash
python3 -m shared.model100_feature_cli --start 2025-01-01T00:00:00Z \
  --end 2026-11-09T09:00:00Z --mode RESEARCH --output /isolated/research
python3 -m shared.model100_feature_cli --start 2025-01-01T00:00:00Z \
  --end 2026-11-09T10:00:00Z --mode RESEARCH --output /isolated/research --refresh
```

First capture is SELECT-only B1. Cached snapshot filenames are content hashes.
Refresh reads a bounded trailing range (plus warmup); it does not reread the
whole news/price/GDELT history. The small macro registry is reread for new
releases; proxy archive is currently small. Existing engines/credentials are
used without logging configuration. No scheduler or model registration added.

Output shards are deterministic gzip JSONL, with SHA256 in manifest. Each chunk
is fsynced then admitted atomically; manifest is checkpointed. A per-output
lock prevents concurrent writers. Orphan chunks are ignored; corrupted admitted
shards fail. Resume verifies schema and the committed input prefix. Changed
historical inputs raise `INPUT_PREFIX_CHANGED_REBUILD_REQUIRED`, rather than
silently rewriting features. Explicitly rebuild to a new dataset after review.
This guard is essential when mutable source rows lack retained old versions.

Read a dataset with `read_dataset(directory)`. No MySQL feature table is created.
Integration into a future model caller is separate; existing Framework and
production models remain unchanged.

## Verification

```bash
PYTHONPATH=shared:. python3 -m unittest discover -s tests -p 'test_model100*.py'
python3 -m shared.model100_feature_validate --snapshot /isolated/research/inputs-HASH.json.gz \
  --full /isolated/research --incremental /isolated/research-incremental
```

Validator checks every admitted hour against fresh build_at calls, an
independent macro selection oracle, cutoff/open-bar bounds, all feature-vector
shapes, shard hashes and two-stage incremental equality. Synthetic tests cover
late news, revision changes, DST/offset conversion, partial price versions,
missing groups, schema locking, changed historical prefixes and recovery.
Production source clocks and retained-version limitations are reported
separately; passing synthetic tests does not create missing source evidence.
