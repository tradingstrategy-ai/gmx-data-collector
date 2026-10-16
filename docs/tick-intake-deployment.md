# GMX tick deployment on vitalik8

Deployed 2026-10-16 from independently approved collector source
`9efa8afee0796576c2607e383ae94676806feff6` in
`/home/avik/gmx-ticks-intake`. Runtime Python is
`/home/avik/freqtrade-multistrategy-orchestrator/gmx-data-collector/.venv/bin/python`.
No shared-master changes or workflow dispatch were used. The repository remains
public; the upstream workflow changes are pending merge in this branch.

## Public release and measured canonical tape

Existing release: <https://github.com/tradingstrategy-ai/gmx-data-collector/releases/tag/data-2026-10-16>,
release ID `414471646`. Added only:

| Asset | ID | Bytes | SHA256 |
|---|---:|---:|---|
| gmx-ticks.tar.gz | 648597096 | 29,738,530 | `19905052499be36c06aacd0b3d079b137f6c74cc34bf557585b5c5951af34f9a` |
| gmx-ticks.manifest.json | 648597100 | 106,984 | `010e938e98bf1a3fe1045c6c94d63202d4f88f914c9d0cbaf452237a6bd6137a` |

A single 1,186,061,866-byte full legacy seed download was retained and reused;
its REST asset digest is
`1d2524200b1392e2bce377872b6607a73a68c20cd383ac3bd6a2c779a5095bc6`.
Scratch intake passed before canonical intake. Published ticks were built from
actual canonical tape, not from estimates.

| Measurement | Observed |
|---|---|
| Chain | Arbitrum, chain ID 42161 |
| Rows | 333,257: 125,838 perp executions + 207,419 swap-side rows |
| Partitions / symbols | 37 UTC days / 110 symbols across both kinds |
| Dates | 2026-09-10 18:51:37 – 2026-10-16 02:23:20 UTC |
| Blocks | 503,800,058 – 515,243,851 |
| Duplicate existing identity keys | 0 |
| Canonical tape / sidecar bytes | 27,909,521 / 7,073,604 |

The source legacy scan checkpoint was 515,244,135, ahead of the latest fill.
It was retained as source metadata, never installed as a local collector
checkpoint. The canonical-built public manifest has `last_scanned_block: null`
because no canonical collector scan checkpoint exists; max tape block is
measured. Neither value certifies complete block traversal or complete fills.
History is explicitly incomplete, decoder/unmapped gaps and finality unknown,
reorg handling absent. Swap-side oracle valuations are not CLOB prices, and
both legs must not be summed as independent protocol trades. Legacy optional
impact zeros can mean missing values; no retroactive accuracy claim is made.

## Preservation and replay evidence

All 5,056 pre-existing OHLCV/candles, funding, OI, pool, futures and checkpoint
files, totaling 8,589,534,150 bytes, were hashed before the first intake and
verified unchanged afterward. Full/light/report asset IDs, sizes, digests,
release ID and exact body stayed unchanged. The additive publisher never ran
the old same-date deletion workflow.

Deployment evidence is retained under
`/home/avik/.local/state/gmx-ticks-deployment`, including per-file baseline,
release before/after metadata, original source coverage, independent public
asset roundtrip and actual host-job rerun proof. Download cache is
`/home/avik/.cache/gmx-tick-release-intake` and the original legacy file was
reused for scratch and canonical seed. These are local evidence files; the epic
`gmx-deployment` artifact records their important measurements and hashes.

## Active host job

The exact runtime wrapper is `/home/avik/.local/bin/gmx-tick-release-intake`,
SHA256 `ff1490ca9443d8748382ac087d3a63a35d56b67e718b6f3666c505237af71ec8`.
It composes the reviewed CLIs with `set -e` and a shared host-job flock, so a
failed intake/build cannot proceed to publish. The CLI also holds its canonical
intake lock. It resolves the exact latest daily release, intakes the dedicated
asset when present, otherwise downloads/hash-checks legacy full once for that
release, builds from the union and publishes an immutable dedicated pair.
Thus the host works before the new upstream packaging workflow merges. Until
that merge, each new daily release may require a full legacy transfer; do not
claim the future network transfer is already tick-only. Cached full seeds are
retained for audit/reuse and can be retired after dedicated upstream packaging
is active and verified.

```bash
#!/bin/bash
# Composes the independently reviewed 9efa8af intake/build/publish commands.
set -euo pipefail
umask 0007
export PATH=/home/avik/.local/bin:/usr/local/bin:/usr/bin:/bin
export PYTHONPATH=/home/avik/gmx-ticks-intake/src
PYTHON=/home/avik/freqtrade-multistrategy-orchestrator/gmx-data-collector/.venv/bin/python
CACHE=/home/avik/.cache/gmx-tick-release-intake
exec 9>/home/avik/.local/state/gmx-tick-release-intake.lock
flock -n 9
cd /home/avik/gmx-ticks-intake
TAG=$(gh api repos/tradingstrategy-ai/gmx-data-collector/releases/latest --jq .tag_name)
OUTPUT=$(mktemp -d "$CACHE/build.XXXXXXXX")
trap 'rm -rf -- "$OUTPUT"' EXIT
printf 'GMX tick intake start: %s release=%s reviewed-source=9efa8af\n' "$(date -u +%FT%TZ)" "$TAG"
"$PYTHON" -m gmx_historical_data.tick_release intake --root /srv/gmx --release "$TAG" --download-dir "$CACHE"
"$PYTHON" -m gmx_historical_data.tick_release build --root /srv/gmx --output "$OUTPUT"
"$PYTHON" -m gmx_historical_data.tick_release publish --release "$TAG" --archive "$OUTPUT/gmx-ticks.tar.gz" --manifest "$OUTPUT/gmx-ticks.manifest.json"
printf 'GMX tick intake success: %s release=%s\n' "$(date -u +%FT%TZ)" "$TAG"
```

The wrapper uses the existing avik GitHub login, no collector secret file.
New canonical tick directories are `avik:data`, mode 2770; tapes are 0660,
intake metadata 0640. No recursive permissions change was performed.

Installed cron line (UTC host, active cron service):

```cron
15 3 * * * /home/avik/.local/bin/gmx-tick-release-intake >> /home/avik/logs/gmx-tick-intake.log 2>&1
```

It is managed only inside `# BEGIN GMX TICK INTAKE` / `# END GMX TICK INTAKE`.
Cron read/backup/merge/write/read-back runs under
`/home/avik/.local/state/data-collector-crontab.lock`, preserving Apex, GRVT
and unrelated blocks. The exact wrapper is manually exercised before installing
the schedule; the first future 03:15 invocation is still a future event.

Rollback removes only this cron block under the same shared lock; retain
canonical ticks and immutable published assets. Do not restore an old whole
crontab over subsequent unrelated edits, delete existing datasets, or remove
this runtime worktree while the job references it. A later move to merged
master requires updating the wrapper path and verifying the reviewed source.

## Completed live acceptance

At 20:59 UTC, an independent download of the new public archive and manifest
passed GitHub digest checks, manifest/file checks, exact schema/key validation
and scratch roundtrip intake; the manifest files matched retained canonical
files. The exact host wrapper then completed successfully through latest-tag
resolution, dedicated intake, build and byte-identical asset reuse. Rerun row
counts, identities, ranges and every tape/volume file SHA256 remained unchanged;
no duplicate/revision asset was needed. Existing local collector checkpoints
were unchanged with the other 5,056 baseline files.

The 03:15 UTC line was installed once and read back under the shared cron lock.
Backup: `/home/avik/.local/state/gmx-ticks-deployment/crontab-before-20261016T205918582865Z.txt`.
Installed crontab SHA256:
`84e8a01cbcea99d7ef6a460277d6fc93e2f6fcda82f174ff838ec77cd397137e`.
Pre-existing Apex, GRVT and other cron text was preserved byte-for-byte.
The first future scheduled invocation has not occurred; the identical command
has been executed successfully during acceptance.

Preservation baseline JSON SHA256:
`ec4258bcea5580c7a700de6afc36b5b2f0a635d33cd907451068bf8d714bdb37`.
Unchanged release body SHA256:
`b9020b125f7759d6c9ee483103e9857bfd6aa824d90f3e73c39d719a925cd46e`.
Original preserved assets: full ID 645876241, light ID 645876247,
report ID 645876243. Their REST sizes/digests match the saved pre-deployment
metadata. Repository visibility remains PUBLIC.

Both monitored deployment commands exited zero. The independently approved
source's 100 focused tests remain the code validation; this deployment added
only documentation, host composition and acceptance evidence. No source code
changed after review. Workflow integration remains pending normal review/merge;
there was no force push, master merge, CI dispatch or giant historical backfill.
