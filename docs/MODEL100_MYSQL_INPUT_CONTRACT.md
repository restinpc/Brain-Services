# Model100 MySQL input contract

No model, training, target labels or trading code are included.

## Storage

Canonical feature table: `brain.vlad_model100_h1_features`. The existing
`vlad_model100_*` namespace is retained. One `(dataset_version, mode, cutoff_utc)`
is one immutable row. Dataset V1: `model100-h1-v1hex-20261109`.

`vlad_model100_h1_feature_sets` is a small schema/state registry (two rows), not
a second feature dataset. It contains the original schema, feature ordering,
hash, original builder code fingerprint and current polling status. The
composite FK prevents mixing modes/schemas. Both tables use InnoDB.

Main fields:

| Field | Meaning |
|---|---|
| cutoff_utc | UTC decision cutoff; DATETIME, hour boundary |
| mode | RESEARCH / STRICT_ASOF |
| dataset_version | explicit immutable dataset identity |
| schema_hash | original FeatureBuilder schema hash |
| feature_values | 489 ordered values; nulls retained |
| missing_masks | 489 corresponding 0/1 null masks |
| value_encoding | IEEE754_HEX_V1 |
| provenance | original source/availability/uncertainty evidence |
| row_hash | original FeatureBuilder row SHA256 |
| payload_sha256 | checksum of ordered stored arrays + provenance |
| builder_version | model100-h1-features-v1 |
| formed_at_utc | actual SQL insertion clock, not historical publication |

### Exact float representation

MySQL 8.4.8 JSON decimal-number conversion changed the least significant bit
in a real fixture: `1.0351966619491577` became `1.0351966619491575` after SELECT.
Thus native JSON numeric doubles are unsuitable for exact original row hashes.

`feature_values` stores floats as `float.hex()` ASCII strings, integers as JSON
integers, and missing values as JSON null. `float.fromhex()` reconstructs the
original bits, including negative zero. The reader returns normal numeric
features; future models should use it rather than CAST hex strings in SQL.
Stored masks always remain aligned. No source values are rounded or imputed.
For direct SQL display of the TIMESTAMP insertion clock, use a UTC session;
cutoff_utc itself is a UTC DATETIME and is not session-zone converted.
The rejected numeric prototype is preserved in explicitly named `_json_trial`
tables outside the active contract; it is not training input.

Example structure (not all 978 elements):

```json
{
  "dataset_version": "model100-h1-v1hex-20261109",
  "mode": "STRICT_ASOF",
  "cutoff_utc": "2026-11-09 10:00:00",
  "value_encoding": "IEEE754_HEX_V1",
  "feature_values": "array[489]: int / IEEE754 hex string / null",
  "missing_masks": "array[489]: 0 / 1",
  "provenance": "original per-source evidence",
  "row_hash": "64-character original builder SHA256"
}
```

## SQL and Framework reader

```sql
SELECT f.cutoff_utc, f.mode, f.schema_hash, f.value_encoding,
       f.feature_values, f.missing_masks, f.provenance, f.row_hash,
       JSON_EXTRACT(s.schema_json, '$.feature_order') AS feature_order
FROM brain.vlad_model100_h1_features AS f
JOIN brain.vlad_model100_h1_feature_sets AS s
  USING (dataset_version, mode, schema_hash)
WHERE f.dataset_version = 'model100-h1-v1hex-20261109'
  AND f.mode = 'STRICT_ASOF'
  AND f.cutoff_utc BETWEEN '2026-11-09 00:00:00' AND '2026-11-09 10:00:00'
ORDER BY f.cutoff_utc;
```

```python
from shared.model100_feature_store import read_schema, read_features, feature_vector

version = 'model100-h1-v1hex-20261109'
schema = await read_schema(state.engine_brain, version, 'STRICT_ASOF')
async for row in read_features(state.engine_brain, version, 'STRICT_ASOF',
                               '2026-11-09T10:00:00Z'):
    vector = feature_vector(row, schema)  # existing builder method; 978 values
```

Use a B1 Brain engine. The asynchronous reader streams 256 rows per partition,
checks schema/payload/original row hashes and does not alter the shared engine
session timezone. `end=None` selects exactly one cutoff; supplying end selects
an inclusive range. Optional limit is a validated positive integer. SQL and
schema are sufficient: the reader never opens snapshots, NLP artifacts or
original source files. Raw nulls are preserved in the returned row; explicit
vectorization uses the existing tested builder method and appends null masks.

## Native update and recovery

Registered parser ID168, script `model100_feature_store_parser.py`, ordinary
hourly Brain interval. Existing `parser.php` calls `parser_run_registered_row`
and `BrainParserWriterFence`; no new cron or scheduler is installed. The Python
entry requires the trusted lease to be held by an ancestor PHP process and also
takes a MySQL named lock. B1 hostname is verified before any table write.

Each mode runs independently. Pending Yahoo bar closes are selected with an
indexed NOT EXISTS lookup, including a seven-day overlap for late source
arrivals. Only already closed hours are eligible. A recovery wave is capped at
168 hours, with four days context plus the existing loader's warmup. All macro
source semantics, first estimates, revisions policy, frozen NLP and masks are
reused unchanged. No full news/price history rebuild or downloads occur.

Rows commit in batches of 64. Actual DB keys/high watermark are authoritative;
restarting after a crash resumes missing rows, without a file cursor being
required. Repeating an identical key is a no-op; changed content raises
IMMUTABLE_HISTORY_CONFLICT. UPDATE/DELETE triggers prohibit normal historical
mutation. A partial invalid-market wave is explicitly PARTIAL_COVERAGE, not
SUCCESS_UNCHANGED. Failures produce nonzero native health evidence and retry
on the next native due tick. Retained source capture hashes and JSONL audit runs
support reproducibility. Source comparisons flag changed overlapping identities
without treating a bounded query's omitted old rows as deletions.

No newer provider bar -> SUCCESS_UNCHANGED; no synthetic market row is inserted.
Upstream delay is recorded separately and cannot be repaired by fabricating
OHLC. Original STRICT_ASOF sparsity and RESEARCH assumptions remain unchanged.

### Late corrections / new schema

Old feature rows are never recalculated in place. Overlap changes are recorded
with before/after digests; the current new cutoff may use an actually available
new version under the chosen mode. For corrected historical features, create
an explicit reviewed dataset version via the root migration path. Keep old
versions. New schemas/contracts fail closed until approved. V1 SQL shape checks
require 489 fields; a future different shape needs an explicit migration.

## Root migration and deployment

Code first, then existing verified snapshot preparation:

```bash
sudo python3 tools/model100_feature_store_prepare.py --apply
sudo php migrations/model100_h1_store.php dry-run
sudo php migrations/model100_h1_store.php apply
sudo php migrations/model100_h1_store.php import
sudo php migrations/model100_h1_store.php activate
```

These reuse Brain's existing PHP bootstrap/engine, registry and source-policy
guards. Preparation verifies original 8e08f59 builder/helper bytes, manifests,
shard/input hashes and previous whole-history validation. Apply saves before
state, creates only the new tables/triggers/registration, calls the installed
native lock provision helper and installs a root-owned tmpfiles rule to restore
the trusted writer lock on boot. No DB account or grant is created.

Controlled `normal` calls are available through the same root PHP helper for
immediate checks; they are labelled separately from natural cron evidence.
An isolated trial deployment can be fast-forwarded to a reviewed commit using
the helper's `deploy COMMIT` action: native lease, preserved entrypoint backup,
explicit path allowlist, no reset/clean/force, and unchanged Model94 config hash.
No production process is restarted.

## Replication boundary

Both objects are explicitly registered CANONICAL / brain1 / NONE with empty
required_children. This is intentional B1-only prepared input storage. No B2-B4
filters/admission settings are changed and no child writer is installed. A
future distributed model needs an explicit native admission/seed decision;
do not claim the table is already replicated. B4 MySQL was unavailable during
read-only verification, independently of this task.

## Reproduce checks

```bash
PYTHONPATH=shared:. python3 -m unittest discover -s tests -p 'test_model100*.py'
python3 tools/model100_feature_store_audit.py
python3 tools/model100_feature_store_audit.py --benchmark-only
```

Audit compares every historical DB row to its SHA-checked source shard after
lossless decoding. Benchmark uses the MySQL-only reader and verifies hashes
while streaming. Original full/incremental FeatureBuilder parity remains
documented in MODEL100_H1_FEATUREBUILDER_VALIDATION_20261109.md.
