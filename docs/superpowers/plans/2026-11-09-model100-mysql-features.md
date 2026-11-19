# Model100 MySQL input store

Goal: immutable per-version/mode/hour input rows; no model.
Architecture: reuse FeatureBuilder; fixed-order JSON arrays plus schema registry; native PHP registered runner/WriterFence; B1-only ownership NONE (no replication filter changes).

- [ ] Codec + SQL reader: fail-first fixtures, original row hash preserved.
- [ ] Root-only PHP migration via existing engine bootstrap, backup/dry-run, InnoDB checks, immutable triggers, native parser registry and trusted lock.
- [ ] Import both SHA-verified historical manifests in bounded idempotent transactions; compare every DB row to source.
- [ ] Incremental registered parser: bounded SELECT snapshot, both modes, closed hours only, append-only, recover DB high watermark, logs/audit snapshots.
- [ ] Native cron evidence, repeated runs, gap recovery, malformed/late/partial fixtures, read performance.
- [ ] Fresh compatibility tests, Git commit/push/remote SHA, SQL/read contract and final checkpoint.
