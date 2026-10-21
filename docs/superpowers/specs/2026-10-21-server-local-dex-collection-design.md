# Server-local DEX Collection — Design Spec

**Date:** 2026-10-21
**Scope:** `gmx-data-collector` and `Apex-Historical-Data`
**Target host:** `vitalik8`

## Goal

Move DEX data collection off GitHub Actions and keep collected data on `vitalik8`, under a separate `/srv` directory for each DEX. Stop using GitHub Releases as the data store. Keep normal test and lint CI enabled; it does not collect market data.

## Current state

| Repo | CI data work | Other collection behavior |
|---|---|---|
| GMX | `release-data.yml` runs daily at 02:00 UTC, restores the previous release asset, collects, then publishes a new release. The old branch workflows are manually dispatchable. `collect-full-history.yml` is also manually dispatchable. | An older Docker daemon exists, but it collects a narrower API dataset and is not a replacement for the daily snapshot. |
| ApeX | `release-data.yml` runs the sharded collector daily and publishes assets. `collect-shards.yml` is reusable by scheduled and backfill workflows. `backfill-history.yml` is manually dispatchable. | Docker Compose has persistent trade and depth streams and a release publisher. |

## Proposed architecture

Use `/srv/gmx` and `/srv/apex` as independent deployment roots on `vitalik8`. Keep each checkout, runtime configuration, logs, and data inside its DEX root. The local data roots should preserve each collector's existing layout:

| DEX | Runtime data root | Collection |
|---|---|---|
| GMX | `/srv/gmx/user_data` (contains `data/gmx/...`) | A server-side daily timer runs `scripts/collect_daily_snapshot.py` against the persistent tree. A separate OHLCV-only timer runs more often to stay inside GMX's short rolling candle window and check/repair recent continuity. Neither restores from nor publishes GitHub Releases. |
| ApeX | `/srv/apex` (contains `data/raw`, `data/normalized`, `data/state`, etc.) | A server-side daily timer runs the resumable daily update. Persistent services run the trade stream, depth stream, and trade poller. They write directly to the same local root. |

Use host timers for bounded collection and supervised persistent services (Docker Compose or systemd) for ApeX's continuous streams. The GMX OHLCV timer starts at a two-hour interval, then is tuned from observed API lookback, request rate-limit headroom, and end-to-end job runtime on vitalik8. It uses a dedicated OHLCV-only path that intentionally re-fetches the recent API window even when the latest stored candle is current; repeated full snapshots are not the gap-prevention mechanism because their coverage gate can skip current candles. Share an exclusive lock between GMX daily and OHLCV jobs. Preserve local checkpoints and atomic writes; a restarted or failed job must resume from local state.

The continuity target is **no collector-caused gaps while the host and source APIs are available**. Each GMX OHLCV run checks recent candles for missing intervals and repairs gaps still inside the API's available window. Daily GMX datasets and ApeX incremental datasets must resume from their watermarks after a missed run; ApeX trades/depth remain continuous services because those histories cannot be backfilled. If a server/API outage outlasts a source's available window, a stream disconnect loses data, or an endpoint returns incomplete data that can no longer be recovered, record the affected dataset, symbols/timeframes, and date range and raise an operator-visible alert; never silently mark the data complete. Historical gaps outside the available window remain identified as unrecoverable unless another source can fill them.

No data-collection job should execute in GitHub Actions, including scheduled, manual, or reusable collection entry points. Remove every data workflow schedule; make any retained manual or reusable job a constant no-op that allocates no runner and cannot fetch or publish. Retain test/lint workflows. This prevents CI from spending hours on daily runs or manual backfills.

## Data continuity and cutover

Before the first server run, seed each DEX's local data root with its existing accumulated data. Prefer any complete copy already on `vitalik8`; if none exists, transfer the latest complete dataset once during cutover. This one-time seed is separate from ongoing collection. Do not make server runs fetch prior state from releases. For GMX, inventory and report existing candle gaps before enabling the frequent OHLCV timer; repair what is still within GMX's API window and report older gaps as unrecoverable.

Install and validate each server-side schedule/service before disabling the matching CI data path. Disable all collection entry points together at cutover so there is no overlapping writer. Verify the server can resume from the seeded local state, writes under the correct `/srv/<dex>` root, and records enough logs for failure diagnosis. Thereafter, data remains local to `vitalik8`; there is no GitHub Release upload or CI-based data fetch.

The server's actual existing data, disk capacity, credentials, and deployment access have not yet been inspected. Confirm those during implementation before selecting the seed source and enabling schedules.

## Options considered

1. **GitHub Actions self-hosted runner on vitalik8:** data would be local, but collection would still consume GitHub Actions workflow minutes and depend on CI orchestration. Rejected because the stated goal is to stop CI collection hours.
2. **Server-side schedules and persistent services:** daily tasks run on the host and continuous ApeX streams stay supervised. Recommended; it removes CI from collection and keeps state beside the collectors.
3. **Keep CI schedules and only change release destination:** leaves collection hours in CI and does not meet the goal.

## Risks and mitigations

| Risk | Mitigation |
|---|---|
| Server disk fills or data is lost | Check capacity before seeding; monitor free space; preserve append/resume state and arrange backup policy during implementation. |
| Service configuration drifts from the repo | Keep deployment manifests and timer definitions in version control and deploy from the checked-out code under each `/srv` root. |
| Cutover starts from incomplete data | Seed from a verified complete dataset and compare file inventory/coverage before scheduling; explicitly identify GMX candle gaps that the API can no longer repair. |
| GMX jobs run less often than the API's rolling candle window | Start the OHLCV-only timer at two hours; tune from measured API window, job runtime, and rate-limit headroom; alert on any gap that becomes unrecoverable. |
| A scheduled job overlaps a persistent writer | Use one writer per dataset or a shared lock; avoid running GMX's legacy daemon alongside the daily snapshot unless its distinct data role is explicitly needed. |
| Host reboot interrupts capture | Enable restart-on-boot supervision and verify services recover automatically. |

## Out of scope

- Serving or replicating data to other hosts or consumers.
- Deleting old GitHub Releases, frozen data branches, or repository history.
- Replacing normal test/lint CI.
- Adding a public download service.

## Success criteria

- GMX and ApeX collection run on `vitalik8` and write beneath `/srv/gmx` and `/srv/apex`, respectively.
- ApeX's continuous capture services resume after a process or host restart.
- GitHub Actions no longer runs any market-data collection or publishing path and no daily schedule allocates a runner; test/lint CI remains active.
- No new data is uploaded as GitHub Releases.
- GMX's frequent OHLCV job repairs gaps within the API window, never silently passes an unresolved recent gap, and alerts on gaps no longer recoverable from GMX.
- A missed scheduled run or interrupted ApeX stream leaves a visible recovery/incident record; incremental datasets resume from state and data that cannot be backfilled is reported.
- Existing accumulated data is present locally before the first scheduled server run, with the collector able to resume from it.
