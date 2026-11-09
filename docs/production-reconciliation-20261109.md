# B1–B4 production reconciliation, 2026-11-09

Base: origin/main ba7e52b74d4a48234380c37585c256c5604c4e27.
All original tracked/untracked changes, binary patches and SHA inventories were backed up independently outside each worktree at /brain/git-sync-20261109. Backups include private runtime and must never be published.

Already upstream: identical B1–B4 brain_framework.py; B2/B3/B4 Model92, reverse_learning.py and brain_strict_reduction.py. These are preserved, not overwritten from B1's older versions.
Model94: B4 supersets B1 source-engine split, supplies missing condition_ids/bindparam imports and mandatory canonical-source guard; chosen after offline compatibility tests. Upstream undefined price_source is resolved.
B1 unique changes: GDELT launcher argv/cache support; EURUSD bounded gap recovery; currently deployed source adapters added to version control without credentials/config/runtime.
GDELT recent-hours schedule remains in existing Brain registry, not a hardcoded date in code; no DB mutation performed by this reconciliation.

Host-specific config.toml overrides remain local and byte-preserved (B1/B4 94; B2/B3 80; B3 92). They are intentionally not copied between hosts or silently committed.
Previously tracked Python bytecode and Telegram session credentials are removed from index only. Deployment must preserve those host-local files, restoring the freshly backed-up bytes immediately after fast-forward. No git history rewrite is performed.
No model execution, database mutation, prediction rebuild, training, weight change or service restart is part of this integration.

Verification: compile entire tracked Python source tree; offline integration tests for framework cutoff/append, reverse precision/error propagation, strict reduction, Model92 dependency, Model94 source guards/SQL bindings and GDELT registry argv. Existing source-adapter tests run offline. All imports use test-only framework stub where needed; no live DB is accessed.
