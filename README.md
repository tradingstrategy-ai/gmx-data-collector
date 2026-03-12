# GMX V2 Daily Market Data

Automated daily snapshots of all GMX V2 markets on Arbitrum, collected via the public GMX REST API.

## What's Collected

| Data | Format | Path |
|------|--------|------|
| Daily OHLCV candles (per symbol) | Feather (CCXT format) | `user_data/data/gmx/futures/{SYM}_USDC_USDC-1d-futures.feather` |
| Full market snapshot (OI, liquidity, rates) | Parquet | `user_data/data/gmx/snapshots/{YYYY-MM-DD}.parquet` |
| Collection summary | Text | `user_data/data_report.txt` |

## Market Snapshot Fields

Each daily snapshot parquet contains one row per market with:

- **Open Interest**: `open_interest_long`, `open_interest_short`
- **Pool Liquidity**: `pool_amount_long`, `pool_amount_short`, `available_liquidity_long`, `available_liquidity_short`
- **Rates**: `funding_rate_long/short`, `borrowing_rate_long/short`, `net_rate_long/short`
- **Metadata**: `name`, `symbol`, `market_token`, `is_listed`, `is_swap_only`

All OI/rate values are strings in GMX 30-decimal precision (`1e30`).

## Schedule

- **Collection**: Daily at 02:00 UTC ([collect-gmx-data.yml](../../.github/workflows/collect-gmx-data.yml))
- **History squash**: 1st of each month at 04:00 UTC (keeps repo size small)

## Usage

```python
import pandas as pd

# Read daily candles
eth = pd.read_feather("user_data/data/gmx/futures/ETH_USDC_USDC-1d-futures.feather")

# Read market snapshot
snap = pd.read_parquet("user_data/data/gmx/snapshots/2026-03-12.parquet")
perp = snap[~snap["is_swap_only"]]  # perpetual markets only
```

## No API Keys Required

All data comes from the public GMX API (`arbitrum-api.gmxinfra.io`). No RPC, no HyperSync, no secrets.
