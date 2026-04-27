# GMX V2 Daily Market Data — DEPRECATED

> **This branch is frozen as of 2026-04-27.**
>
> Daily data now ships as **GitHub Releases** (`data-YYYY-MM-DD` tags).
> This branch will not receive further updates.
>
> **New consumers:** use [`scripts/download_gmx_data.sh`](../../scripts/download_gmx_data.sh)
> or `gh release download` — see the [main README](../../README.md#downloading-gmx-historical-data).

---

## What changed (2026-04-27)

Daily data collection was migrated from this git branch to GitHub Releases to stop the
repo from accumulating unbounded pack files. The branch was already at 8.7 GB with every
daily run rewriting all `futures/*.feather` files.

| | Before | After |
|---|---|---|
| Storage | This branch (rewrote feathers daily) | `data-YYYY-MM-DD` GitHub Release tags |
| Retention | Entire history in git | 14 rolling days of releases |
| Download | `git clone --depth 1 --branch data/daily-collection` | `scripts/download_gmx_data.sh` |
| Workflow | `collect-gmx-data.yml` + `collect-volume.yml` | `release-data.yml` (single job, 02:00 UTC) |
| Assets | — | `gmx-full.tar.gz`, `gmx-light.tar.gz`, `data_report.txt` |

## Downloading current data

```bash
# Latest full snapshot (apy, snapshots, tickers, volumes, futures feathers)
./scripts/download_gmx_data.sh

# Latest light snapshot (no futures/)
./scripts/download_gmx_data.sh --asset light

# Specific historical release
./scripts/download_gmx_data.sh --release data-2026-04-27
```

Requires `gh` CLI authenticated (`gh auth login`).

## Historical data on this branch

The last snapshot on this branch is **2026-04-27**. All data up to that date is preserved
here as a permanent historical reference. The GitHub Releases pipeline continues from
that point forward.

---

## What was collected (historical reference)

| Data | Format | Path |
|------|--------|------|
| Daily OHLCV candles (per symbol) | Feather (CCXT format) | `user_data/data/gmx/futures/{SYM}_USDC_USDC-{TF}-futures.feather` |
| Full market snapshot (OI, liquidity, rates) | Parquet | `user_data/data/gmx/snapshots/{YYYY-MM-DD}.parquet` |
| Ticker data | Parquet | `user_data/data/gmx/tickers/{YYYY-MM-DD}.parquet` |
| APY data | Parquet | `user_data/data/gmx/apy/{YYYY-MM-DD}.parquet` |
| 24h volume per market | Parquet | `user_data/data/gmx/volumes/{YYYY-MM-DD}.parquet` |
| Collection summary | Text | `data_report.txt` |

### Market Snapshot Fields

Each daily snapshot parquet contains one row per market with:

- **Open Interest**: `open_interest_long`, `open_interest_short`
- **Pool Liquidity**: `pool_amount_long`, `pool_amount_short`, `available_liquidity_long`, `available_liquidity_short`
- **Rates**: `funding_rate_long/short`, `borrowing_rate_long/short`, `net_rate_long/short`
- **Metadata**: `name`, `symbol`, `market_token`, `is_listed`, `is_swap_only`

All OI/rate values are strings in GMX 30-decimal precision (`1e30`).

### Usage

```python
import pandas as pd

# Read daily candles
eth = pd.read_feather("user_data/data/gmx/futures/ETH_USDC_USDC-1h-futures.feather")

# Read market snapshot
snap = pd.read_parquet("user_data/data/gmx/snapshots/2026-04-27.parquet")
perp = snap[~snap["is_swap_only"]]  # perpetual markets only
```
