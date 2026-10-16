# GMX tick release and intake implementation

Review handoff, 2026-10-16. Base `3d623680dd7f1014c46a1ea1139f91fc5bc0ab1a`;
branch `feat/tick-release-intake`, isolated worktree `/home/avik/gmx-ticks-intake`.
The exact committed head is recorded in the epic's `gmx-ticks-implementation`
artifact. No source checkout on master, release, cron or `/srv` data was changed.

Implemented deterministic dedicated archives and coverage/checksum manifests;
immutable additive publication; preserving same-date full/light assets; safe
hash/schema checked downloads; locked staged union and rollback recovery;
canonical status; a cron entry point and the deployment runbook at
[`docs/tick-intake.md`](docs/tick-intake.md). Optional impact schema drift is
handled without synthesizing zero for absent fields. No subagents used.

Intake records source scan metadata separately in `checkpoints/tick_intake.json`.
It deliberately never advances a local collector checkpoint, rewrites OHLCV,
or certifies completeness. Volume is rebuilt using the existing helper from
union tapes. If a larger local aggregate has no supporting union tape, intake
fails for reconciliation rather than discarding it or adding overlapping sums.
Recovery covers process interruption with rollback on the next locked command;
unlocked readers can observe an in-progress file set. Power-loss atomicity is
not promised. Finality, decode/unmapped gaps and full-history coverage remain
unknown; no reorg rollback or unsupported Avalanche expansion was added.

Validation uses the existing collector submodule Python environment and this
worktree's source via `PYTHONPATH=src`:

- Focused release/decoder/collector/volume/workflow tests: **94 passed**.
- Full CI unit selection: **711 passed, 1 skipped, 1 failed**. The failure is
  `test_recent_external_variant_is_not_called_stale` in
  `test_freqtrade_exporter_funding_orphans.py`; its hardcoded 2026-09-12 fixture
  is stale as of 2026-10-16. Identical failure reproduced at the clean base.
  Funding code/tests were left untouched. Full selection used all CI ignores
  for the seven integration/live files listed in `.github/workflows/test.yml`.
- Whole repo Ruff check/format, `poetry check --lock`, collector smoke imports
  (8 targets), shell syntax and cron snippet AST parse passed. Poetry emitted
  existing metadata deprecation warnings.
- Tests cover traversal/absolute paths/links/devices/duplicate members; hashes,
  schema and manifest mismatch; union/dedup/rerun/longer history; preserved
  candles and collector checkpoints; partial commit failure and real process
  `os._exit` recovery; immutable asset reuse/revisions; latest release selection,
  REST asset digest, cached seed reuse and legacy fallback.

Read-only GitHub verification: `gh release view --json assets` omits digest on
this host; REST `gh api .../releases/tags/data-2026-10-16` exposes it. Intake uses
REST. Existing full archive: 1,186,061,866 bytes, SHA256
`1d2524200b1392e2bce377872b6607a73a68c20cd383ac3bd6a2c779a5095bc6`.
No archive was downloaded; real seed/publication and measured production tape
coverage/size remain acceptance work after independent review.

The runbook provides exact build/publish/staged seed/canonical/status commands
and a 03:15 UTC marked cron update. Its crontab read/backup/merge/write/read-back
verification runs under `/home/avik/.local/state/data-collector-crontab.lock`,
preserving Apex, GRVT and unrelated markers. No deployment command was executed.
