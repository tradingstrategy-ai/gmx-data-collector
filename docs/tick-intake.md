# Arbitrum execution tick intake

Ticks already run in the 02:00 UTC daily snapshot. This adds a small dedicated
release asset and a separate consumer; it does not enable another collector.
No native matched L2 book is provided. Perps use execution prices; swap rows
represent token sides valued with oracle prices. Do not sum both sides as
independent trades. Existing float schemas, transaction/log identities and
market addresses are retained. Chain 42161 is explicit in the manifest.

The manifest records actual per-file hashes, row counts, schemas, symbols,
block/date ranges, and the source scan checkpoint. History completeness,
decoder/unmapped gaps and finality remain unknown. No reorg rollback is
implemented. A scan checkpoint beyond the latest fill is explicitly flagged;
it can also represent legitimately quiet blocks, never proof of completeness.
The optional impact column uses `pendingPriceImpactUsd` on newer increases,
otherwise `priceImpactUsd`; absent impact is NaN, and legacy zero values
may mean missing. It is not a verified realized-impact series.

## Local commands

Use the project's installed Python with collector dependencies. No secrets or
`.env` are needed by intake; GitHub downloads use `gh` and its existing login.
These examples assume the changes have been merged into the main checkout.

```bash
cd /home/avik/gmx-data-collector
GMX_PYTHON=/home/avik/freqtrade-multistrategy-orchestrator/gmx-data-collector/.venv/bin/python
PYTHONPATH=src "$GMX_PYTHON" -m gmx_historical_data.tick_release build --root user_data/data/gmx --output /tmp/gmx-tick-assets
PYTHONPATH=src "$GMX_PYTHON" -m gmx_historical_data.tick_release intake --root /srv/gmx
PYTHONPATH=src "$GMX_PYTHON" -m gmx_historical_data.tick_release status --root /srv/gmx
```

Intake chooses the newest complete dedicated archive/manifest revision pair
from the requested release (`--release latest` by default). It verifies the
archive SHA256 and every extracted file against the manifest before merging.
Legacy releases fall back to `gmx-full.tar.gz`, downloading that full asset
once per invocation until dedicated assets exist; SHA256 must be available
from GitHub's asset digest. An interrupted archive/manifest pair is ignored
in favor of the last complete pair (or legacy full). For an audited offline
archive, use `intake --root <root> --archive <file> --sha256 <expected-sha>`;
add `--manifest <file>` for dedicated archives. Never substitute a digest
computed from an untrusted download for an independently expected digest.

Safe extraction rejects links, devices, traversal and duplicate paths, and
extracts only ticks, sidecars and the source tick checkpoint. Parquet schemas,
partitions, event kinds and keys are checked. Union uses the existing key
`transaction_hash, log_index, symbol, kind` (including its known same-symbol
swap-side limitation). Volume is rebuilt from the union tape; existing volume
keys absent from incoming data are preserved. A local aggregate exceeding its
available union tape is rejected rather than overwritten or summed twice.

Intake takes an exclusive lock, stages all outputs, saves backups and a
rollback journal on the canonical filesystem, then replaces files atomically.
JSON metadata is published through a sibling temporary file, flush/file fsync,
atomic rename and directory fsync before any canonical replacement. Interrupted
temporary journal files are ignored; failed writes remove their temporary file.
The intake checkpoint commits last. Exceptions roll back; process interruption
is recovered under the lock by the next intake or status command. A malformed
legacy active journal is preserved with its transaction backups and an explicit
manual-recovery error: automatic discard cannot prove the canonical commit state.
This is
recoverable across process termination, not a multi-file atomic snapshot for
unlocked readers or a power-loss durability guarantee. Failed validation does
not change existing tape. Do not run other independent writers against these
same tick directories. Existing OHLCV, funding, OI, pools, futures and collector
checkpoints are never written. The source scan checkpoint is retained in
`checkpoints/tick_intake.json`, never installed as the local collector's
`trade_ticks.json`. `status` inventories actual canonical files and always
reports history incomplete. No historical backfill is implied.

## Deployment after independent review

No commands in this section were executed during implementation. `/srv/gmx`
was observed as `avik:data`, mode `2770`; its existing checkpoints directory
is `avik:avik`, mode `775`. Run intake as avik with `umask 0007`; newly created
children inherit the root's data group. Do not recursively chmod/chown the
shared dataset. Read status and verify permissions after the first intake.
The wrapper sets a restrictive group-write umask. No credentials are copied.

1. Merge the reviewed commit. Do **not** dispatch the old destructive workflow.
   A normal next-day release includes the dedicated pair. To append ticks to
   an existing release from already audited local data, build as above, then:

   ```bash
   PYTHONPATH=src "$GMX_PYTHON" -m gmx_historical_data.tick_release publish \
       --release data-2026-10-16 --archive /tmp/gmx-tick-assets/gmx-ticks.tar.gz \
       --manifest /tmp/gmx-tick-assets/gmx-ticks.manifest.json
   ```

   Publish uses the fixed pair if absent, reuses byte-identical pairs, otherwise
   appends numbered immutable pairs. It never clobbers or deletes assets.
   Same-date workflow reruns preserve full/light/report even if tick revisions
   are newer. Normal new-date full/light packaging stays unchanged. Existing
   14-day release retention is unchanged; it is not a tape retention policy.

2. First seed may download the existing ~1.186 GB full archive if no dedicated
   pair exists. Stage/probe first under a scratch root, then intake canonical:

   ```bash
   umask 0007
   PYTHONPATH=src "$GMX_PYTHON" -m gmx_historical_data.tick_release intake --root /tmp/gmx-reviewed-intake --release data-2026-10-16 --download-dir /tmp/gmx-reviewed-download
   PYTHONPATH=src "$GMX_PYTHON" -m gmx_historical_data.tick_release status --root /tmp/gmx-reviewed-intake
   # Reuses the same retained archive/manifest, rechecking SHA256 and schema.
   PYTHONPATH=src "$GMX_PYTHON" -m gmx_historical_data.tick_release intake --root /srv/gmx --release data-2026-10-16 --download-dir /tmp/gmx-reviewed-download
   PYTHONPATH=src "$GMX_PYTHON" -m gmx_historical_data.tick_release status --root /srv/gmx
   ```

   Measure bytes, dates, kinds and row counts from status. Verify a rerun leaves
   row counts stable and pre-existing OHLCV files unchanged. No massive backfill.

3. Install the 03:15 UTC intake only after that verification. This host is UTC;
   the observed upstream release completed at 02:28. The following exact code
   takes the shared data-collector crontab lock, backs up the current crontab and replaces only its own marked block. It
   refuses malformed/duplicate markers. Other cron lines remain byte-for-byte.

   ```bash
   mkdir -p /home/avik/logs
   python3 - <<'PY'
   import datetime
   import fcntl
   import pathlib
   import subprocess

   begin = '# BEGIN GMX TICK INTAKE'
   end = '# END GMX TICK INTAKE'
   lock_path = pathlib.Path('/home/avik/.local/state/data-collector-crontab.lock')
   lock_path.parent.mkdir(parents=True, exist_ok=True)
   with lock_path.open('a') as lock:
       fcntl.flock(lock, fcntl.LOCK_EX)
       prior = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
       if prior.returncode and 'no crontab' not in prior.stderr.lower():
           raise SystemExit(prior.stderr)
       old = prior.stdout
       if old.count(begin) != old.count(end) or old.count(begin) > 1:
           raise SystemExit('Malformed or duplicate GMX cron markers')
       if begin in old and old.index(end) < old.index(begin):
           raise SystemExit('Reversed GMX cron markers')
       stamp = datetime.datetime.now(datetime.UTC).strftime('%Y%m%dT%H%M%S%fZ')
       backup = pathlib.Path('/home/avik/logs') / f'crontab-before-gmx-ticks-{stamp}.txt'
       backup.write_text(old)
       line = '15 3 * * * PATH=/home/avik/.local/bin:/usr/local/bin:/usr/bin:/bin /bin/bash /home/avik/gmx-data-collector/scripts/intake_gmx_ticks.sh /home/avik/freqtrade-multistrategy-orchestrator/gmx-data-collector/.venv/bin/python /srv/gmx >> /home/avik/logs/gmx-tick-intake.log 2>&1'
       block = begin + '\n' + line + '\n' + end + '\n'
       if begin in old:
           start = old.index(begin)
           stop = old.index(end) + len(end)
           if old[stop:stop + 1] == '\n':
               stop += 1
           updated = old[:start] + block + old[stop:]
       else:
           updated = old + ('' if not old or old.endswith('\n') else '\n') + block
       subprocess.run(['crontab', '-'], input=updated, text=True, check=True)
       print(backup)
       verified = subprocess.run(['crontab', '-l'], capture_output=True, text=True, check=True).stdout
       if verified != updated:
           raise SystemExit('Crontab read-back did not match installed content')
   PY
   crontab -l
   ```

   Retain the backup path. Rollback removes only the marked block (or restores
   the backup after checking for subsequent unrelated cron edits). The existing
   04:30 collector refresh and 06:45 shared sync remain untouched; neither writes
   these new tape directories in the audited configuration. Check the first
   scheduled log/status; a late or stale upstream release requires follow-up,
   not a claim that canonical data advanced.
