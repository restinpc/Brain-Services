# H1 FeatureBuilder: verified B1 results, 2026-11-09

Source DB reads only. No model, training, source mutation or service restart.

## History and features

Cutoffs: 2025-01-01 06:00 UTC through 2026-11-09 09:00 UTC.
Each mode: 11,443 unique Yahoo closing hours, 489 nullable fields and 489 null masks (978 vector dimensions).
CFTC reserves 7 fields but is disabled by default; all other groups are enabled.

| Group | Fields | STRICT hours with data | RESEARCH hours with data |
|---|---:|---:|---:|
| cftc | 7 | 0 | 0 |
| gdelt | 32 | 3862 | 3862 |
| macro | 272 | 11401 | 11401 |
| news | 147 | 9 | 10853 |
| price | 21 | 953 | 11443 |
| proxy | 10 | 17 | 17 |

## Verification

69 FeatureBuilder/accepted macro tests + 13 production compatibility tests passed.
Python compilation passed for 16 new/promoted modules and test files.
Both modes: 11,443/11,443 full/incremental/live-function/repeat rows equal; independent macro oracle and cutoff checks: zero errors.
All admitted shard SHA256 values verified while reading; modes have different schema hashes.
Actual bounded Yahoo refresh: 57 -> 58 rows; rerun stays 58. Append build 0.0094 seconds, unchanged build 0.0034 seconds.

## Performance

| Mode | SELECT capture seconds | Feature shard build seconds | Null cells % |
|---|---:|---:|---:|
| strict-release | 54.97 | 127.97 | 24.514 |
| research-release | 58.17 | 164.70 | 12.121 |

Capture/build timings are separate; schema/index initialization and final summarization are additional.
Cached research initialization: 31.82 seconds under concurrent audit load.
100 previously unbuilt hours: median 10.72 ms, max 21.31 ms per build_at.
Null percentages cover all 489 nullable fields, including reserved disabled fields and flags; they are not the share of unusable H1 rows.

## Evidence hashes

### strict-release

- Schema: `4c78a561d7677ce3f7a16a1ec57ab58c01c1e55e0757d0503c0220dce57ecd06`
- All-hour row hash chain: `26e4bf2124979a31557f585d32408676da38fdb6f04aa8ad52ebddb84099aef1`
- Input cache: `inputs-e7c2f319384e29efef6ce8b11bf859a20b9c914584dacc0a2b45a34bbe8b9d2d.json.gz`

### research-release

- Schema: `81473fc1ccc4dde876b377d7bb9a1e1b461d934a5b24772c6f77666ce70f533b`
- All-hour row hash chain: `2f181b786e8adf494f48d514a9c2f367216e734badf824906f3ad8c0f594fbd2`
- Input cache: `inputs-29a672b54a2080ea0824365d83bf1cdc69f8de7457f75aa4717974661918ab03.json.gz`

## Remaining source limitations

- STRICT historical prices: only 953 cutoffs have an observed eligible current row version. Yahoo loaded_at changes on update, including target-only updates; it is not historical first receipt. Research assumes complete public bars at close.
- STRICT news: 133 verified current NLP/text rows, present in 9 hourly windows. Research uses 175,997 persisted NLP rows before fit-cutoff filtering, and covers 10,853 H1 windows; old publication dates and later edits remain uncertain. No new text pipeline was fitted.
- Macro uses documented public/conservative availability, not certified historical Brain receipt. 368 revision rows retained in inputs are excluded from first-estimate features. Conservative period-end expiry excludes 1,714 forecast rows rather than learning future release calendars.
- Binance is explicitly EURUSDT crypto proxy. Historical feature-ready/receipt clocks yield only 17 eligible H1 windows; its old volumes are not backdated.
- GDELT coverage: 3,862 H1 windows; provenance hashes describe stored aggregates, not original document bytes.
- CFTC remains disabled because an expected report calendar is not proof of actual publication. FXCM, Finam and NY Fed SME are not V1 inputs.
- Changed historical inputs or contracts fail with an explicit rebuild-required error. Lost prior source versions cannot be reconstructed by this builder. Input snapshots preserve the versions actually used.
- Nothing was attached to a running model or a new scheduler. Framework engine and explicit Yahoo RAM adapters are ready for a future caller.

See MODEL100_H1_FEATUREBUILDER.md for API, per-group clocks, incremental commands and recovery.
