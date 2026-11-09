# MySQL prepared H1 inputs — B1 verification, 2026-11-09

## Results

| Check | Result |
|---|---|
| Main table | brain.vlad_model100_h1_features |
| Dataset | model100-h1-v1hex-20261109 |
| Initial historical rows | 22,886 (11,443 per mode) |
| After natural cron | 22,888 (11,444 per mode) |
| Cutoffs UTC | 2025-01-01 06:00 through 2026-11-09 10:00 |
| Shape | 489 original fields + 489 null masks |
| Duplicate keys / future rows | 0 / 0 |
| Original history DB/file equality | all 22,886 rows, values/masks/provenance/original SHA identical |
| New live rows | both modes rebuilt from retained input captures, identical SHA and no future provenance |
| Native schedule | ID168, existing parser.php hourly queue, WriterFence + DB named lock |
| Natural cron observed | 11:47:02 UTC start; 11:47:38 UTC completed; +1 row per mode |
| Controlled repeat | 11:51:13 UTC SUCCESS_UNCHANGED, no new rows |
| Tests | 80 builder/storage tests + 13 existing production compatibility tests |
| Runtime boundaries | B1 only; no models, training, source updates or service restarts |

The natural cron occurred before the first controlled normal test. It is not a manual run labelled automatic. Later controlled runs are separate checks; another future natural hourly cycle is not claimed observed here.

## Streaming reader performance

Existing async engine, warmed MySQL buffers, checksums and lossless decoding included:

| Mode | One hour | 100 hours | All 11,444 hours |
|---|---:|---:|---:|
| RESEARCH | 11.08 ms | 0.255 s | 43.75 s |
| STRICT_ASOF | 6.42 ms | 0.252 s | 41.30 s |

Peak reader RSS: 64.28 MiB. No all-history list accumulation.
Full DB/source comparison (also reads original shards): research 56.53 s, strict 52.83 s.

## Integrity, clocks and recovery

- Fixed-order float hex JSON avoids the demonstrated native JSON numeric ULP change; signed zero, nulls and integers are preserved. The numeric trial is retained separately as an explicit rejected prototype, not active input.
- Per-version/mode/hour PK, mode/schema composite FK, array length and hour-boundary checks, payload/original-row SHA validation, and immutable UPDATE/DELETE triggers are active.
- A native lease owned by an ancestor PHP process is mandatory. Direct unscheduled writer was rejected. Child node fixture is rejected before DB access.
- Actual DB keys are the checkpoint; transaction batches are 64 rows. A maximum 168-hour recovery wave and seven-day missing-key overlap handle missed ticks and late arrivals without rescanning years. Fixtures cover gaps, unclosed bars, idempotence and conflicting old contents.
- Raw masks/nulls and both availability modes are unchanged from the original tested builder. No target t1 is read/stored. Existing full/incremental original builder validation remains applicable.
- Bounded source overlap corrections are recorded by before/after digests. Existing feature rows are never updated. A new reviewed dataset version is required to change old features.
- Installed native lock provision helper and tmpfiles rule were exercised: root:apache 0660, same live inode. An actual production reboot was deliberately not performed.
- B2 and B3 had no feature table, as expected for declared NONE ownership. B4 MySQL was unavailable; no repair or filter changes attempted.

## Source limitations retained

Yahoo had no newer stored bar than 09:00 UTC at inspection, so the last prepared cutoff is 10:00 UTC. Provider delay is not hidden by fabricated rows. A stored version last fetched before bar close is not used as a complete candle.
STRICT_ASOF historical sparsity, RESEARCH legacy-news/editorial assumptions, sparse proxy availability, and disabled experimental CFTC are retained exactly. This store does not manufacture missing source evidence.

## Read contract and deployment

See MODEL100_MYSQL_INPUT_CONTRACT.md and migrations/model100_h1_store.sql. Reader uses only MySQL, including the schema registry; snapshots are for audit/recovery.
The production source installation is a safe fast-forward of new data modules only; existing model/framework files are excluded by deployment allowlist. Model94 local config hash is checked unchanged. No process restart is needed.

## Representative row (metadata only)

```json
{
  "dataset_version": "model100-h1-v1hex-20261109",
  "mode": "STRICT_ASOF",
  "cutoff_utc": "2026-11-09 10:00:00",
  "value_encoding": "IEEE754_HEX_V1",
  "schema_hash": "4c78a561d7677ce3f7a16a1ec57ab58c01c1e55e0757d0503c0220dce57ecd06",
  "row_hash": "1d6871a4536a988df2d84984833803123a387d1b702d9cc1f8f37ac94d3071b5",
  "payload_sha256": "8b6785c75461f6769dbc1580f270c468400e68c2c5cad5c12783c464251dc014",
  "formed_at_utc": "2026-11-09 11:47:38.520027",
  "feature_count": 489,
  "mask_count": 489
}
```

## Actual SHOW CREATE TABLE

```sql
CREATE TABLE `vlad_model100_h1_features` (
  `dataset_version` varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `mode` enum('RESEARCH','STRICT_ASOF') NOT NULL,
  `cutoff_utc` datetime NOT NULL,
  `schema_hash` char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `feature_values` json NOT NULL,
  `missing_masks` json NOT NULL,
  `provenance` json NOT NULL,
  `row_hash` char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `payload_sha256` char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  `builder_version` varchar(64) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  `value_encoding` enum('IEEE754_HEX_V1') NOT NULL,
  `formed_at_utc` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`dataset_version`,`mode`,`cutoff_utc`),
  KEY `time_mode` (`cutoff_utc`,`mode`),
  KEY `model100_schema_hex_fk` (`dataset_version`,`mode`,`schema_hash`),
  CONSTRAINT `model100_schema_hex_fk` FOREIGN KEY (`dataset_version`, `mode`, `schema_hash`) REFERENCES `vlad_model100_h1_feature_sets` (`dataset_version`, `mode`, `schema_hash`),
  CONSTRAINT `model100_hex_cutoff_hour` CHECK (((minute(`cutoff_utc`) = 0) and (second(`cutoff_utc`) = 0))),
  CONSTRAINT `model100_hex_masks_shape` CHECK (((json_type(`missing_masks`) = _utf8mb3'ARRAY') and (json_length(`missing_masks`) = 489))),
  CONSTRAINT `model100_hex_values_shape` CHECK (((json_type(`feature_values`) = _utf8mb3'ARRAY') and (json_length(`feature_values`) = 489)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;
```
