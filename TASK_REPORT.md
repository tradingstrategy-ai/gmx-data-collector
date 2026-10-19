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

## Fix round 1: atomic journal publication

Review finding P2 addressed on top of `2e56f6ba975ef1a0fee0f84c5fb4a411d20d8a4e`.
The exact focused fix commit is recorded in the epic implementation artifact.
The shared JSON writer now stages a complete sibling file, rejects short
writes, flushes/fsyncs it, atomically replaces the destination, and fsyncs its
parent directory before returning. Canonical replacement only begins after
journal publication returns. Metadata remains group-readable (0640).

Real subprocess exits now test interruption during a partial journal write and
immediately after its atomic rename. Both permit repeated status and intake
recovery; pre-rename orphan temporaries are ignored. Existing midcommit crash
and exception tests still exercise the second canonical replacement, excluding
the new metadata renames from their counters. New tests cover write exceptions,
short write counts, file-sync/rename/directory-sync ordering and malformed
legacy journals. An invalid legacy active journal and transaction backups are
preserved with an explicit manual-recovery error; automatic deletion or
quarantine cannot safely establish whether canonical replacement already began.

Exact red evidence (current new regression tests against the committed old
module, exported without changing any checkout):

```bash
mkdir -p /tmp/gmx-ticks-fixround1-base
git archive 2e56f6ba975ef1a0fee0f84c5fb4a411d20d8a4e src | tar -x -C /tmp/gmx-ticks-fixround1-base
PYTHONPATH=/tmp/gmx-ticks-fixround1-base/src /home/avik/freqtrade-multistrategy-orchestrator/gmx-data-collector/.venv/bin/python -m pytest tests/test_tick_release.py -k 'journal_publication or legacy_partial_journal' -q -p no:cacheprovider
```

Result: **3 failed**, as expected. Partial-write child exited 91, leaving an
active malformed journal; the pre-rename invariant failed. The old writer had
no journal rename boundary, and its legacy error lacked recovery guidance.

Exact green/check commands from `/home/avik/gmx-ticks-intake`:

```bash
PYTHONPATH=src /home/avik/freqtrade-multistrategy-orchestrator/gmx-data-collector/.venv/bin/python -m pytest tests/test_tick_release.py tests/test_gmx_trade_ticks.py tests/test_trade_tick_collector.py tests/test_candle_volume.py tests/test_release_workflow.py -q -p no:cacheprovider
/home/avik/freqtrade-multistrategy-orchestrator/gmx-data-collector/.venv/bin/ruff check .
/home/avik/freqtrade-multistrategy-orchestrator/gmx-data-collector/.venv/bin/ruff format --check .
git diff --check
```

Result: **100 passed**, one existing dependency deprecation warning; lint,
format (166 files) and diff checks passed. No unrelated date-fixture changes,
external actions or subagents. Reviewer-owned `.review/` remains untracked and
untouched. Production acceptance still awaits the fresh review verdict.

## Live deployment acceptance completed

After independent approval of 9efa8af, the authorized deployment completed on
2026-10-16 at 20:59 UTC. See [the measured deployment report](docs/tick-intake-deployment.md)
for exact asset IDs, hashes, sizes, chain/date/row coverage, existing-data
preservation and the installed 03:15 UTC job. Both monitored phases exited zero.
The 1.186 GB legacy seed was downloaded once and reused. Independent public
dedicated download/schema/key/hash roundtrip passed; running the identical
cron wrapper preserved all 333,257 rows and every canonical tick/volume hash.
All 5,056 prior OHLCV/funding/OI/pool/futures/checkpoint hashes stayed unchanged,
as did the old release ID/body/assets and public visibility. The cron update
used the shared crontab lock, fresh backup and exact read-back verification,
preserving unrelated markers. Runtime source remains the reviewed 9efa8af;
subsequent commits only document deployment. Upstream workflow changes are
still pending merge; the installed host job composes reviewed intake/build/
publish and works with legacy full releases immediately. History/finality/
decode limitations stay explicit. No extra backfill or source-code changes.
