# Server-local DEX Collection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run GMX and ApeX data collection on `vitalik8`, persist each DEX under `/srv/<dex>`, and stop GitHub Actions from collecting or publishing market data.

**Architecture:** Keep the GMX broad daily snapshot and ApeX daily updater as bounded host-timer jobs. Run a separate GMX OHLCV-only job more often than once daily so it refreshes the API's recent candle window; start at two-hour intervals, then use the measured API retention, rate-limit headroom, and runtime to select the final cadence; current host measurements support a 12-hour schedule. Run ApeX trade capture, trade polling, and depth capture as restart-on-failure services. Retain each collector's local checkpoint and storage formats; seed them from a verified existing dataset before enabling the server schedule. Remove data-collection triggers from GitHub Actions while retaining ordinary test/lint workflows.

**Tech Stack:** Python 3.11/3.12, Poetry/uv as used by each repo, systemd timers, Docker Compose for ApeX's long-running processes, Parquet/SQLite local state.

**Spec:** `docs/superpowers/specs/2026-10-21-server-local-dex-collection-design.md`

## Global Constraints

- Use `/srv/gmx` and `/srv/apex` as independent deployment roots on `vitalik8`, preserving the existing server datasets and ownership.
- GMX collector output is rooted at `/srv/gmx/user_data`; map its expected `data/gmx/{futures,snapshots,tickers,apy,volumes,ticks,tick_volume,checkpoints}` directories to the existing top-level folders under `/srv/gmx` without moving the files.
- ApeX daily and new continuous collector output uses `/srv/apex/upstream` (which contains `data/...`); keep the existing `/srv/apex/tick-capture` tree intact until its replacement capture is verified.
- No GitHub Actions workflow may collect or publish DEX market data; retain test/lint CI.
- Do not publish new data as GitHub Releases or fetch prior data from releases during server runs.
- Seed existing data before the first scheduled run; prefer a complete server-side copy and use a one-time verified transfer only if none exists.
- Daily collection uses host timers; ApeX continuous capture uses supervised services.
- GMX OHLCV runs twice daily after live API-window, request-rate, and runtime measurements: 6d22h minimum observed 1m history, two successful runs at 259.3s and 237.4s, and 0 recent gaps.
- The continuity target is no collector-caused gap while the host and source API are available; unrecoverable gaps are recorded and alerted, never silently passed.
- Missed GMX daily snapshots and ApeX incremental runs resume where source history permits; ApeX stream disconnects that lose non-backfillable trades/depth produce incident records.
- Do not delete old release assets or data branches.
- Follow the user-approved `$tdd` request: write a failing test at approved public seams before each behavior change, then verify the relevant suite.

## Review Focus

- The destination path must not add or drop a `data/` layer; verify actual GMX and ApeX output paths before first collection.
- ApeX daily collection and its continuous writers must share one data root and cannot start from an empty container-local filesystem.
- The trade poller, depth stream, and trade stream must restart after host reboot and must not be started twice.
- CI test/lint workflows must remain configured after all data collection entry points are removed.
- Data seeding must preserve the local watermarks, manifests, and SQLite state required for incremental continuation.
- GMX's recent-window audit must run even when the latest stored candle is current; an outage beyond API history must leave a visible incident.
- A missed daily schedule or ApeX stream disconnect must be visible in the journal and a durable incident record; watermark-based datasets must catch up on restart.

---

## File Structure

### GMX repository: `tradingstrategy-ai/gmx-data-collector`

- Create `deploy/systemd/gmx-daily-collection.service` and `.timer` for the existing `scripts/collect_daily_snapshot.py` entry point.
- Create `deploy/systemd/gmx-ohlcv-collection.service` and `.timer` for an OHLCV-only recent-window repair job, initially scheduled every two hours; final host timer is twice daily (00:00 and 12:00 UTC).
- Create `deploy/server/gmx-daily-collection.sh` and `gmx-ohlcv-collection.sh` to resolve the installed checkout, activate the repo's pinned runtime, take a shared GMX lock, and write/report under `/srv/gmx`.
- Create `scripts/collect_gmx_ohlcv.py` as the scheduled OHLCV-only entry point; it reuses current fetch/merge code but refreshes the recent API window even when the latest candle is current.
- Create `deploy/server/README.md` with the GMX install, environment, seed, timer, logs, and rollback instructions.
- Modify `.github/workflows/release-data.yml`, `collect-gmx-data.yml`, `collect-volume.yml`, and `collect-full-history.yml`: remove every schedule and prevent every collection job from running; retain only no-op manual triggers if needed for history. Leave `.github/workflows/test.yml` and lint CI active.
- Modify active README/CLI comments that describe GitHub Releases or CI as the daily publisher.

### ApeX repository: `tradingstrategy-ai/Apex-Historical-Data`

- Create `deploy/systemd/apex-daily-update.service` and `.timer` for `apex-data update daily`.
- Modify `docker/docker-compose.daemon.yml` to use `/srv/apex` as the persistent data mount, remove `apex-publish`, and supervise trade stream, continuous trade poll, and depth stream services.
- Create `deploy/server/README.md` with install, environment, seed, service/timer, logs, and rollback instructions.
- Modify `.github/workflows/release-data.yml`, `backfill-history.yml`, and `collect-shards.yml`: remove every schedule and prevent every collection/release job from running, including reusable workflow jobs; retain only no-op manual/reusable triggers if needed for history. Retain `.github/workflows/test.yml`.
- Modify `README.md` to describe server-local data and deployment, and remove release publishing instructions from the active operating guide.

### Host cutover on `vitalik8`

- Create `/srv/gmx` and `/srv/apex` deployment roots with restricted ownership/permissions appropriate to the service account.
- Install both repo revisions, runtime secrets, systemd units, and the ApeX Compose project.
- Seed and verify data before enabling timers/services; store logs under each DEX root or the system journal with unit names that identify the DEX.

## Task 1: Confirm vitalik8 readiness and existing data

**Files:** No repository changes.

**Interfaces:** Produces the verified host facts used by Tasks 2–5: reachable deployment path, available disk, runtime versions, environment variables, current `/srv` contents, measured GMX candle API window/runtime/rate-limit behavior, and the complete seed source for each DEX.

- [ ] Inspect `vitalik8` connectivity and the current `/srv/gmx` and `/srv/apex` contents using read-only commands.
- [ ] Check disk capacity, Docker Compose/systemd availability, service account ownership, and required RPC/HyperSync environment values without printing secret values.
- [ ] Probe GMX's actual candle-window floor across representative listed symbols/timeframes; record request limit, returned rows/earliest timestamp, run duration, and rate-limit responses without exceeding provider limits.
- [ ] Use the API-window probe and current daily collector timings to propose a safe initial interval; defer one-hour versus two-hour job comparison until the dedicated CLI exists in Task 2.
- [ ] Confirm whether the maximum 10,000-row request and current coverage floor are useful for repair; do not equate the requested limit with the API's actual available history.
- [ ] Record whether each existing dataset is complete enough to resume; if not, identify the one-time source to transfer before cutover.
- [ ] Confirm systemd persistent-timer behavior and what alert route is available on vitalik8; use unit failure/journal plus a durable incident file if no external notifier is already configured.
- [ ] Stop before enabling jobs if the host, disk, credentials, or seed source is unavailable; report the specific blocker and keep CI collection workflows enabled until the server is ready.

## Task 2: Add GMX host-side daily collection

**Files:**

- Create: `deploy/server/gmx-daily-collection.sh`
- Create: `deploy/server/gmx-ohlcv-collection.sh`
- Create: `deploy/systemd/gmx-daily-collection.service`
- Create: `deploy/systemd/gmx-daily-collection.timer`
- Create: `deploy/systemd/gmx-ohlcv-collection.service`
- Create: `deploy/systemd/gmx-ohlcv-collection.timer`
- Create: `scripts/collect_gmx_ohlcv.py`
- Create: `deploy/server/README.md`
- Modify: `README.md`
- Modify: `scripts/collect_daily_snapshot.py`
- Create: `src/gmx_historical_data/recent_candle_audit.py` with `audit_recent_candles(futures_dir: Path, window_start: datetime, window_end: datetime) -> list[CadenceBreak]` to check recent candles independently of a previous manifest.

**Interfaces:** The daily service invokes the existing snapshot CLI with output root `/srv/gmx/user_data`. The OHLCV service invokes `scripts/collect_gmx_ohlcv.py --output-dir /srv/gmx/user_data`; the script calls existing `collect_and_save_ohlcv(..., repair=True)` to fetch/merge the latest API-returnable candles for each active symbol/timeframe even when disk already reaches the current bar, then calls `audit_recent_candles(...)`. Both services share `/srv/gmx/collection.lock`. A recent gap remaining after retries fails the run and writes `/srv/gmx/incidents/<UTC timestamp>-gmx-candle-gap.json`; an older gap beyond API history is recorded as unrecoverable. systemd reports nonzero failures in the journal and exposes a failed unit for operator inspection.

- [ ] Add the host runner with strict shell error handling, explicit `/srv/gmx` paths, one-writer locking, and environment-file loading that never logs secret values.
- [ ] Add the daily snapshot systemd service and timer, using a persistent, restart-safe working directory and a deterministic UTC execution time.
- [ ] Implement the OHLCV-only CLI by reusing existing market discovery, retry, merge, volume-preservation, and API-limit handling; bypass only the current-candle short-circuit so each run refreshes the available recent window.
- [ ] Implement `audit_recent_candles(futures_dir: Path, window_start: datetime, window_end: datetime) -> list[CadenceBreak]` in `src/gmx_historical_data/recent_candle_audit.py` using existing cadence-break detection and the maximum observed API window.
- [ ] Repair gaps still covered by the endpoint, retry transient partial responses, and exit nonzero with an incident record when recent gaps remain or the window has already expired.
- [ ] Configure the daily timer to catch up after a missed activation where possible; preserve per-run dates in reports and record any daily snapshot that the source API can no longer reconstruct.
- [ ] Add the OHLCV service and initial two-hour UTC timer with `Persistent=true`; apply Task 1 measurements and the Task 5 runtime comparison to choose the final safe interval (currently 12 hours). Require a >=2x margin within the measured minimum API window and job completion below half of the selected interval; prevent overlap with the daily snapshot using the shared lock.
- [ ] Confirm with a bounded live command that paths resolve to `/srv/gmx/user_data/data/gmx`, request count/runtime stay within provider limits, and a second run refreshes overlapping rows without losing dense candles or volume.
- [ ] Document installation, first-run seed requirements, `systemctl` operations, journal inspection, and disable/rollback commands.
- [ ] Update active GMX documentation and stale collector comments so local server storage is the operating path and release assets are historical only.

## Task 3: Add ApeX host-side daily and continuous collection

**Files:**

- Create: `deploy/systemd/apex-daily-update.service`
- Create: `deploy/systemd/apex-daily-update.timer`
- Modify: `docker/docker-compose.daemon.yml`
- Create: `deploy/server/README.md`
- Modify: `README.md`

**Interfaces:** The daily unit invokes `apex-data update daily --output-dir /srv/apex/upstream`. Compose mounts `/srv/apex/upstream` at the CLI data root for all services. Add `apex-data trades poll --all-symbols --market-types perp,rwa --continuous --output-dir /data`; keep trades and depth streams on the same root.

- [ ] Add the daily timer with no release restore/publish behavior and an exclusive lock against any other daily writer.
- [ ] Rework the Compose volume so all services persist to `/srv/apex/upstream`, and remove `apex-publish` and its GitHub credentials.
- [ ] Add the continuous tiered trade poller service alongside stream and depth; set restart policy and common environment/data paths for all three.
- [ ] Confirm daily klines/funding catch up from persisted watermarks after an outage; configure unit failure reporting for ticker snapshots and write a durable incident when a trade/depth stream disconnect creates an unrecoverable interval.
- [ ] Confirm Compose renders each process with `/srv/apex` as the host-backed root and that daily output uses the same `Settings.data_dir`.
- [ ] Document installation, first-run seed requirements, environment setup, start/stop/restart, journal/container logs, and rollback commands.
- [ ] Update active ApeX docs to make local storage the operating path and mark release assets as historical.

## Task 4: Remove GitHub Actions data-collection entry points

**Files:**

- Modify: GMX `.github/workflows/release-data.yml`, `collect-gmx-data.yml`, `collect-volume.yml`, `collect-full-history.yml`
- Modify: ApeX `.github/workflows/release-data.yml`, `backfill-history.yml`, `collect-shards.yml`

**Interfaces:** Test and lint workflows remain available. No schedule, manual dispatch, or reusable workflow caller may execute market-data collection or release publication.

- [ ] Remove the `schedule` event from every GMX data workflow and add a constant false job condition to each remaining manual collection job; keep any manual trigger as a no-op so it allocates no runner and cannot fetch or publish.
- [ ] Remove the `schedule` event from ApeX release collection and guard all jobs in the manual backfill and reusable workflow with constant false conditions; no reusable caller may perform collection or publishing.
- [ ] Keep `.github/workflows/test.yml` and other code-quality workflows unchanged and active.
- [ ] Parse/render every changed workflow and inspect its triggers and job conditions; confirm only the test/lint workflow can allocate a runner for repository code and no data workflow can execute a collector or `gh release` command.

## Task 5: Seed, deploy, and cut over on vitalik8

**Files:** No additional code files; use the deployment instructions created in Tasks 2 and 3.

**Interfaces:** Consumes the outputs from Tasks 1–4. Produces active systemd units and ApeX services writing verified local data under `/srv/gmx` and `/srv/apex`, with GitHub Actions data paths disabled.

- [ ] Install the pinned code revisions and service definitions under each `/srv/<dex>` root; configure secrets through protected host environment files.
- [ ] Seed each local data root from its verified server-side copy, or transfer the selected complete source once; verify manifests, watermarks, database state, and representative dataset files before starting collection.
- [ ] For GMX, inventory existing cadence breaks before cutover, repair those inside the API window, and record older/out-of-window breaks as unrecoverable with symbol/timeframe/date detail.
- [ ] Run one bounded/manual server collection for each DEX and confirm its report, state advancement, and newly written files remain under the expected root.
- [ ] For GMX, compare one-hour and two-hour OHLCV runs through multiple cycles, including an interruption/restart; choose the largest interval that has a >=2x margin within the measured minimum API window, completes below half its interval, and stays within rate limits. Confirm overlapping recovery leaves no new cadence break and an expired API window generates an incident rather than false success.
- [ ] Interrupt one ApeX incremental run and one continuous stream; confirm incremental data resumes from stored state and any non-backfillable trade/depth interval is reported with the affected time range.
- [ ] Start and enable the ApeX stream, poller, and depth services; verify all three survive a service restart and a host reboot/restart window.
- [ ] Enable the daily timers only after local writes and resume state are confirmed; inspect the next scheduled run and logs.
- [ ] Disable GitHub Actions data workflows only once corresponding server jobs are healthy; verify test/lint workflows remain enabled and no new release is produced by collection.

## Spec coverage and self-review

- Server-local GMX schedule and storage: Task 2.
- Frequent GMX candle-window refresh, gap repair, and unrecoverable-gap alerting: Tasks 1, 2, and 5.
- Missed-run recovery and gap reporting for daily data and ApeX streams: Tasks 1, 3, and 5.
- Server-local ApeX schedule and persistent capture: Task 3.
- No CI market-data collection or publication, with test/lint preserved: Task 4.
- Local state continuity, cutover ordering, path verification, and restart behavior: Tasks 1 and 5.
- Data access remains local; release deletion and new public serving are out of scope.
