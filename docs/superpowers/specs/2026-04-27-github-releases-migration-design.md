# GitHub Releases Migration — Design Spec

**Date:** 2026-04-27
**Issue:** [tradingstrategy-ai/gmx-data-collector#11](https://github.com/tradingstrategy-ai/gmx-data-collector/issues/11)
**Reference:** [freqtrade-strategies#269](https://github.com/tradingstrategy-ai/freqtrade-strategies/issues/269) (Hyperliquid migration that motivated this)

## Goal

Replace the `data/daily-collection` git-tree storage with GitHub Releases. The repo's working tree stays code-only forever; daily data ships as release assets.

## Why

The repo is already at 8.7 GB and growing. Each day rewrites every `futures/*.feather` (≈380 markets × 6 timeframes), so pack files balloon. Hyperliquid's mirror hit 32 GB and started failing `git push` with HTTP 500. The same wall is on this trajectory — migrating now is cheaper than migrating after a push fails.

## Background

Two daily workflows write to `data/daily-collection`:

| Workflow | Cron | Writes |
|---|---|---|
| `collect-gmx-data.yml` | 02:00 UTC | `user_data/data/gmx/{apy,futures,snapshots,tickers}/`, `data_report.txt` |
| `collect-volume.yml` | 02:30 UTC | `user_data/data/gmx/volumes/` |

Branch contents (latest tree):
- `user_data/data/gmx/apy/{YYYY-MM-DD}.parquet` — small, append-only
- `user_data/data/gmx/snapshots/{YYYY-MM-DD}.parquet` — small, append-only
- `user_data/data/gmx/tickers/{YYYY-MM-DD}.parquet` — small, append-only
- `user_data/data/gmx/volumes/{YYYY-MM-DD}.parquet` — small, append-only
- `user_data/data/gmx/futures/{SYMBOL}_{TF}-futures.feather` — cumulative, rewritten daily (the bulky bit)
- `data_report.txt` — text summary
- `README.md` — manual, retained on the branch only

`data/full-history` (one-time historical bootstrap with funding/OI/pool-liquidity) is **out of scope** for this migration — leave as-is.

Consumers today:
- `src/gmx_historical_data/quickstart.py` — `git clone --depth 1 --branch data/daily-collection`, merge-copy `user_data/`.
- Used by `scripts/collect_daily_snapshot.py` and `gmx_historical_data collect --quickstart`.
- `Oracle-server-setup` references the branch in `config/repos.yaml` — out of scope for this PR (separate coordination later).

## Architecture

### Single combined daily workflow

Fold both existing daily workflows into one `release-data.yml` triggered at 02:00 UTC. Volume collection runs in the same job after the main snapshot. Rationale: volume data is < 1 MB; a separate scheduled job exists only because they originally had to coordinate two pushes to the same branch — releases eliminate that constraint, so the second job becomes pure complexity.

### Pipeline (single job)

```
1. Restore prior state
   gh release download latest --pattern "gmx-full.tar.gz" → extract into work dir
   (falls back to "no previous release — starting fresh" on first run)

2. Run collectors (incremental, extends existing feathers + writes today's parquets)
   - scripts/collect_daily_snapshot.py    (snapshots/tickers/apy/futures/data_report.txt)
   - subsquid_volume.fetch_daily_volumes  (volumes/{today}.parquet)

3. Package
   - gmx-full.tar.gz   = user_data/data/gmx/{apy,futures,snapshots,tickers,volumes}/
   - gmx-light.tar.gz  = user_data/data/gmx/{apy,snapshots,tickers,volumes}/   (no futures)

4. Create release
   - tag:    data-YYYY-MM-DD
   - name:   "GMX Data YYYY-MM-DD"
   - assets: gmx-full.tar.gz, gmx-light.tar.gz, data_report.txt
   - if a release for today already exists (re-run): delete and recreate

5. Prune
   - delete release tags older than 14 days
```

Tarballs preserve the `user_data/data/gmx/...` prefix so consumers extract at any root.

### Retention — 14 days

Each release is a complete snapshot of the cumulative state (futures feathers contain full history; daily parquets are accumulated by restore-then-extend). 14 days of releases gives:
- Rollback if a daily run produces bad data.
- Buffer if the workflow breaks for a week (Hyperliquid lost 8 days before someone noticed).
- No long-term compounding storage cost.

The `latest` release tag (auto-managed by GitHub when using release names with date-sortable tags, or explicitly via the workflow) always points to the most recent.

### Light vs full asset

`gmx-light.tar.gz` exists for consumers who only need market context (apy/snapshots/tickers/volumes) and not freqtrade-format candles. Saves bandwidth and disk for that path.

## Component changes

### New files

- `.github/workflows/release-data.yml` — daily release pipeline (replaces the two existing daily workflows).
- `scripts/download_gmx_data.sh` — consumer-facing fetch script (`gh release download` + `tar -xz`). Supports `--release data-YYYY-MM-DD` (specific date) and `--asset full|light` (default `full`). Mirrors the hyperliquid `download_hyperliquid_data.sh` ergonomics.

### Modified files

- `src/gmx_historical_data/quickstart.py`:
  - Replace `seed_from_branch()` git-clone path with `gh release download` invocation.
  - Drop `DEFAULT_BRANCH = "data/daily-collection"` constant; introduce `DEFAULT_RELEASE_TAG = "latest"`.
  - Keep the merge-copy semantics (existing local files never overwritten — idempotent).
  - `print_coverage_summary()` unchanged (operates on the extracted tree).
- `README.md` — update download instructions, mark `data/daily-collection` branch as deprecated with a date.
- `.github/workflows/collect-gmx-data.yml` — remove the `schedule:` trigger (keep `workflow_dispatch:` for emergency rollback). Add a deprecation comment header.
- `.github/workflows/collect-volume.yml` — same: remove `schedule:`, keep `workflow_dispatch:`, deprecation comment.

### Untouched

- `master` working tree (code only — already correct).
- `data/full-history` branch.
- `.github/workflows/collect-full-history.yml`.
- `Oracle-server-setup` repo (separate coordination, out of scope).

## Seed strategy (first release)

Before the first scheduled run, manually create release `data-2026-04-27` (or whatever the cutover date is):

```bash
git fetch origin data/daily-collection
git worktree add /tmp/seed data/daily-collection
cd /tmp/seed
tar -czf /tmp/gmx-full.tar.gz user_data/data/gmx/
tar -czf /tmp/gmx-light.tar.gz \
    user_data/data/gmx/apy/ \
    user_data/data/gmx/snapshots/ \
    user_data/data/gmx/tickers/ \
    user_data/data/gmx/volumes/
gh release create "data-$(date -u +%Y-%m-%d)" \
    --title "GMX Data $(date -u +%Y-%m-%d) (seed)" \
    --notes "Seed release. Migrated from data/daily-collection branch." \
    /tmp/gmx-full.tar.gz /tmp/gmx-light.tar.gz /tmp/seed/data_report.txt
git worktree remove /tmp/seed
```

This is one-time manual setup, kept out of the workflow.

## Cutover

1. Land the new workflow + scripts + quickstart change on `master` (workflow disabled / `workflow_dispatch` only).
2. Seed first release manually (above).
3. Run new workflow via `workflow_dispatch` once. Verify outputs and consumer (`download_gmx_data.sh` + `quickstart.py`) work end-to-end against the new release.
4. Enable the schedule on `release-data.yml`. Disable schedules on `collect-gmx-data.yml` and `collect-volume.yml`.
5. Run for 3 days. Compare daily release contents against branch contents (parity check).
6. After 3 successful days: branch becomes frozen. Add deprecation note to its README.
7. Branch is **left in place** (not deleted — undoable rule). Marked as historical reference only.

## Risks

| Risk | Mitigation |
|---|---|
| `gmx-full.tar.gz` exceeds 2 GB GitHub asset limit | Measure during seed step. If close, split by timeframe (`gmx-full-1m.tar.gz`, `gmx-full-1h.tar.gz`, etc.) — same idea as Hyperliquid. Flag during plan execution. |
| First scheduled run misses the seed | Seed manually before enabling schedule (cutover step 2). |
| Consumer using `quickstart.py` from an old `master` checkout | Pin the version in Oracle-server-setup or document the cutover date. |
| Workflow rate-limited by `gh release create` | Same rate limits as Hyperliquid — proven OK at daily cadence. |
| Re-run on same day collides with existing release | Workflow deletes existing release for today before creating (idempotent). |
| Asset name collision with `data-YYYY-MM-DD` tag retroactively recreated | Pruning is by tag age, not name — safe. |

## Open / deferred

- Oracle-server-setup consumer migration: deferred. The branch stays readable for now, so existing consumers continue to work until that PR lands.
- `data/full-history` migration: deferred. One-time data, doesn't accumulate.

## Success criteria

- Daily releases land at `data-YYYY-MM-DD` for 7 consecutive days.
- `quickstart.py` (rewritten) succeeds against `latest` release without git clone.
- `download_gmx_data.sh` works fresh on a new machine with only `gh` + `tar`.
- Repo working tree (`master`) size unchanged.
- Old workflows disabled; no schedule churn writing to the branch.
