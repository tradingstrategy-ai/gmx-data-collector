# GMX Historical Data Collection

Collect historical price data for all 118 GMX V2 tokens with smart incremental updates.

## Features

- **Smart Incremental Updates** — Only fetches data you don't have (10-30x faster for daily updates)
- **Dual Data Sources** — GMX API (recent ~6 months) + Chainlink (historical)
- **118 GMX V2 Tokens** — 34 Chainlink markets + 84 non-Chainlink markets
- **6 Timeframes** — 1min, 5min, 15min, 1h, 4h, 1d
- **Freqtrade Compatible** — Direct export to Feather format (OHLCV + funding rate + mark price)
- **Concurrent Collection** — Parallel symbol processing and RPC batch requests
- **Historical OI & Pool Liquidity** — Full on-chain history of open interest and LP pool depth via HyperSync
- **Analysis Notebooks** — Interactive Plotly notebooks for OI trends, pool utilisation, and cross-exchange validation

## Quick Start

```bash
# Install
poetry install

# Set environment variables
export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
export HYPERSYNC_API_TOKEN="your_token_here"  # Free from https://envio.dev

# Collect full historical data for 34 Chainlink-feed tokens (no HyperSync needed)
gmx_historical_data collect --full --chainlink-only --output-dir user_data --concurrency 3

# Initial collection: all 118 tokens (4-5 hours)
gmx_historical_data collect --default --output-dir ./data --concurrency 5

# Daily incremental update (10-30 minutes for all tokens)
gmx_historical_data collect --update --output-dir ./data --concurrency 10
```

## Installation

**Prerequisites:** Python 3.11 or 3.12, Rust toolchain (for hypersync)

```bash
# Install Rust (if needed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install dependencies
poetry install
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `collect --default` | Collect GMX + Chainlink data (recommended) |
| `collect --full` | Full historical collection |
| `collect --full --chainlink-only` | Collect only 34 Chainlink-feed markets (no HyperSync needed) |
| `collect --full --all-markets` | Collect all 118 markets (requires HyperSync) |
| `collect --update` | Incremental update — only fetches new data |
| `verify` | Verify data quality |
| `export-freqtrade` | Export OHLCV + funding rate + mark price to Freqtrade feather format |
| `debug-oracle` | Debug oracle events |

### Common Options

| Option | Description | Default |
|--------|-------------|---------|
| `--output-dir PATH` | Output directory | `./data` |
| `--symbol TEXT` | Token(s) — comma-separated for `collect`, repeatable for `export-freqtrade` | All tokens |
| `--chainlink-only` | Only collect 34 Chainlink markets (no HyperSync needed) | Off |
| `--all-markets` | Collect all 118 markets (requires HyperSync) | On |
| `--concurrency INT` | Parallelism level | `4` |

### Concurrency

| `--concurrency` | Symbols parallel | RPC batch workers | Use case |
|-----------------|------------------|-------------------|----------|
| 1 | 1 | 1 | Slow/rate-limited RPC |
| 4 (default) | 4 | 4 | Balanced |
| 8 | 8 | 8 | Fast RPC endpoint |
| 10+ | 10+ | 8 (capped) | Maximum speed |

### Estimated Times (with `--concurrency 10`)

**Initial collection (`--full` / `--default`):**
- Single Chainlink token: ~2-3 minutes
- All 118 tokens: ~4-5 hours

**Incremental updates (`--update`):**
- Single token (daily): ~10-30 seconds
- All 118 tokens (daily): ~10-30 minutes

## Automated Updates

```bash
# Cron job: update daily at 2 AM
0 2 * * * cd /path/to/gmx_historical_data && source .venv/bin/activate && \
  gmx_historical_data collect --update --output-dir ./data --concurrency 10 >> /var/log/gmx-update.log 2>&1
```

## Export for Freqtrade

Exports OHLCV candles, funding rates, and mark prices in FreqTrade's feather format (CCXT-compatible).

```bash
# Export all data types
gmx_historical_data export-freqtrade --data-dir ./data --output-dir ./freqtrade_data

# Export specific symbols/timeframes
gmx_historical_data export-freqtrade --data-dir ./data --symbol ETH --symbol BTC --timeframe 1h
```

Output structure:

```
freqtrade_data/gmx/futures/
├── ETH_USDC_USDC-1h-futures.feather
├── ETH_USDC_USDC-1h-funding_rate.feather
├── ETH_USDC_USDC-1h-mark.feather
└── ...
```

## Backtesting

GMX requires [gmx-ccxt-freqtrade](https://github.com/tradingstrategy-ai/gmx-ccxt-freqtrade) monkeypatch. Use the included `freqtrade-gmx` wrapper.

> **Important:** Install `freqtrade` in a separate isolated environment — it requires `pandas<3.0` which conflicts with this project's `pandas>=3.0`.

```bash
python -m venv freqtrade-venv && source freqtrade-venv/bin/activate
pip install "freqtrade>=2025.11" "web3-ethereum-defi[web3v7,ccxt]>=0.38" plotly
```

```bash
# Export data to freqtrade format
gmx_historical_data export-freqtrade --data-dir ./data --output-dir ./user_data/data \
    --symbol BTC --symbol ETH --timeframe 1h

# Run backtest
./freqtrade-gmx backtesting --config configs/adxmomentum_gmx.json \
    --strategy ADXMomentum --timerange 20210713-
```

See `examples/strategies/ADXMomentum.py` for an example strategy.

### Plot Results

```bash
./freqtrade-gmx plot-profit --config configs/adxmomentum_gmx.example.json --auto-open
./freqtrade-gmx plot-dataframe --config configs/adxmomentum_gmx.example.json \
    --strategy ADXMomentum -p BTC/USDC:USDC --auto-open
```

## Data Structure

```
data/
├── candles/arbitrum/{SYMBOL}/
│   ├── 1m.parquet
│   ├── 1h.parquet
│   └── 1d.parquet
├── open_interest/arbitrum/
│   ├── raw/{SYMBOL}/data.parquet         # Raw OI events (every position change)
│   ├── snapshots/{SYMBOL}/daily.parquet  # End-of-day OI snapshots
│   └── checkpoints/
├── pool_liquidity/arbitrum/
│   ├── raw/{SYMBOL}/data.parquet         # Raw PoolAmountUpdated events
│   ├── snapshots/{SYMBOL}/daily.parquet  # Daily pool depth snapshots
│   └── checkpoints/pool_liquidity_checkpoint.json
├── funding/arbitrum/
│   ├── raw/
│   │   ├── funding/{SYMBOL}/partition=0/data.parquet
│   │   └── fee_per_size/{SYMBOL}/data.parquet
│   ├── rates/{SYMBOL}/
│   │   ├── 1h.parquet                    # Unified hourly rates
│   │   ├── 1h_factor.parquet             # HyperSync-only rates
│   │   └── 1h_datastore.parquet          # DataStore-only rates
│   ├── direction/{SYMBOL}/1h.parquet
│   └── checkpoints/
├── borrowing/arbitrum/
│   ├── raw/borrowing/{SYMBOL}/partition=0/data.parquet
│   ├── rates/{SYMBOL}/1h.parquet
│   └── checkpoints/borrowing_factor_checkpoint.json
└── raw/arbitrum/{SYMBOL}/
```

## Docker Usage

```bash
cp .env.example .env
# Edit .env with your API keys

# All 118 markets
docker-compose --profile all up gmx-collect-all

# 34 Chainlink markets only
docker-compose --profile chainlink up gmx-collect-chainlink-only

# Custom symbols
SYMBOLS=ETH,BTC,SUI docker-compose --profile custom up gmx-collect-custom
```

| Profile | Markets | Duration (first run) |
|---------|---------|----------------------|
| `all` | 118 | 6-8 hours |
| `chainlink` | 34 | 2-3 hours |
| `custom` | User defined | Varies |
| `update` | All existing | 10-30 minutes |
| `verify` | N/A | 1-2 minutes |

See [Docker Usage Guide](docs/docker-usage.md) for details.

## Funding Rate Extraction

Extract exact per-second `fundingFactorPerSecond` from on-chain GMX V2 events (HyperSync). GMX API provides only 8-hour epoch snapshots; this gives sub-second resolution since protocol launch.

```bash
# Full extraction
poetry run python scripts/extract_unified_funding.py

# Incremental update (resumes from checkpoints)
poetry run python scripts/extract_unified_funding.py --resume

# Export to FreqTrade feather format
poetry run python scripts/extract_unified_funding.py --feather-dir ./user_data/data

# Output as feather
poetry run python scripts/extract_unified_funding.py --output feather
```

### How It Works

1. **Phase 2 — Funding Factor** (HyperSync): Streams `Funding` events for `fundingFactorPerSecond` magnitude (V2.2+, Aug 2025+)
2. **Phase 3 — Direction** (HyperSync): Streams `FundingFeeAmountPerSizeUpdated`, compares long/short delta sums to determine who pays
3. **Phase 1 — DataStore** (opt-in): Reads signed `savedFundingFactorPerSecond` via batched `eth_call` at hourly intervals (pre-V2.2, requires archive RPC)
4. **Merge**: Combines sources, applies direction correction, deduplicates, writes unified `1h.parquet`

The DataStore phase uses fully-batched JSON-RPC — all network I/O completes before any record is written (~203 HTTP requests total vs ~17,000 sequential).

```bash
# Via Makefile
make funding-unified
make funding-unified INCLUDE_DATASTORE=1   # Include pre-V2.2 DataStore phase
make funding-unified-resume                # Incremental
make funding-unified MARKET="ETH/USD"      # Single market

# Run individual scripts
poetry run python scripts/extract_funding_factor.py --resume
poetry run python scripts/extract_funding_fee_per_size.py --resume
poetry run python scripts/extract_funding_datastore.py --resume  # Archive RPC required
```

### Unified Hourly Rates Schema (`rates/{SYMBOL}/1h.parquet`)

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | datetime[ms, UTC] | Hour start |
| `funding_rate` | float64 | Mean per-second rate |
| `funding_rate_hourly` | float64 | `rate * 3600` |
| `funding_rate_annualized` | float64 | `rate * 3600 * 8760` |
| `longs_pay_shorts` | bool | True = longs pay |
| `funding_fee_long` | float64 | Signed hourly rate for longs |
| `funding_fee_short` | float64 | Signed hourly rate for shorts |
| `update_count` | uint32 | Events in hour |
| `source` | string | `"datastore"` or `"hypersync"` |

### Daily Cronjob

```bash
0 2 * * * cd /path/to/gmx_historical_data && \
  poetry run python scripts/extract_unified_funding.py --resume >> logs/unified_funding.log 2>&1
```

## Borrowing Rate Extraction

Extract `borrowingFactorPerSecond` from GMX V2 `Borrowing` events — the actual on-chain rate charged to the dominant OI side, paid to LPs.

```bash
# Full historical extraction
poetry run python scripts/extract_borrowing_factor.py --from-block 120000000

# Incremental
poetry run python scripts/extract_borrowing_factor.py --resume

# Filter by market
poetry run python scripts/extract_borrowing_factor.py --resume --market "ETH/USD"
```

**Hourly rates schema** (`rates/{SYMBOL}/1h.parquet`):

| Column | Type | Description |
|--------|------|-------------|
| `timestamp` | datetime[ms, UTC] | Hour start |
| `borrowing_rate` | float64 | Mean per-second rate |
| `borrowing_rate_hourly` | float64 | `rate * 3600` |
| `borrowing_rate_annualized` | float64 | `rate * 3600 * 8760` |
| `update_count` | uint32 | Events in hour |
| `symbol` | string | Human-readable symbol |
| `market` | string | Market contract address |

To compute net position cost: `net_rate = funding_rate + borrowing_rate`. Both outputs use the same hourly format and can be joined on `(timestamp, symbol)`.

## Open Interest Extraction

Extract full historical `OpenInterestUpdated` and `OpenInterestInTokensUpdated` events from GMX V2 via HyperSync. Records every position change with USD and token-denominated OI.

```bash
# Full historical backfill (from GMX V2 launch, ~20 min)
poetry run python scripts/extract_open_interest.py --from-block 120000000

# Incremental update (resumes from checkpoint)
poetry run python scripts/extract_open_interest.py --resume

# Filter to a single market
poetry run python scripts/extract_open_interest.py --resume --market "ETH/USD"

# Quick test on a small block range
poetry run python scripts/extract_open_interest.py --from-block 290000000 --to-block 290100000
```

**Output structure:**

```
data/open_interest/arbitrum/
├── raw/{SYMBOL}/data.parquet          — raw events (every position change)
├── snapshots/{SYMBOL}/daily.parquet   — end-of-day OI snapshots
└── checkpoints/
```

**Daily snapshot schema** (`snapshots/{SYMBOL}/daily.parquet`):

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | string | Market symbol (e.g. `ETH/USD`) |
| `date` | string | UTC date |
| `longOiUsd` | string | Long OI in USD (30-decimal string) |
| `shortOiUsd` | string | Short OI in USD (30-decimal string) |
| `totalOiUsd` | string | Total OI in USD |
| `longShortRatio` | string | Long/short ratio |
| `eventCount` | int | Position changes that day |

## Pool Liquidity Extraction

Extract full historical `PoolAmountUpdated` events — LP pool token changes on every deposit, withdrawal, and position-triggered rebalance.

```bash
# Full historical backfill (~20 min via HyperSync)
poetry run python scripts/extract_pool_liquidity.py --from-block 120000000

# Incremental update
poetry run python scripts/extract_pool_liquidity.py --resume

# Filter to a single market
poetry run python scripts/extract_pool_liquidity.py --resume --market "ETH/USD"
```

**Output structure:**

```
data/pool_liquidity/arbitrum/
├── raw/{SYMBOL}/data.parquet          — raw events (delta + nextValue per token)
├── snapshots/{SYMBOL}/daily.parquet   — daily pool depth snapshots
└── checkpoints/pool_liquidity_checkpoint.json
```

**Daily snapshot schema** (`snapshots/{SYMBOL}/daily.parquet`):

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | string | Market symbol |
| `date` | datetime[UTC] | UTC date |
| `token` | string | Pool token address |
| `pool_tokens` | float64 | End-of-day pool token amount (raw units) |

## Analysis Notebooks

Interactive Plotly notebooks for exploring OI and liquidity. Launch with:

```bash
poetry run jupyter lab notebooks/
```

| Notebook | Description |
|----------|-------------|
| `notebooks/01_oi_analysis.ipynb` | OI time series, long/short breakdown, market rankings, monthly heatmap |
| `notebooks/02_liquidity_analysis.ipynb` | Pool depth over time, OI vs pool dual-axis, utilisation scatter, trading universe filter |
| `notebooks/03_cross_exchange_validation.ipynb` | GMX OI vs Binance/Hyperliquid volume correlation, cross-exchange funding rate comparison |

**Usage example:**

```python
# Load OI snapshots for all markets
import pandas as pd
from pathlib import Path

SCALE_30 = 10 ** 30
snapshots_dir = Path("data/open_interest/arbitrum/snapshots")
frames = [pd.read_parquet(f) for f in snapshots_dir.rglob("daily.parquet")]
oi = pd.concat(frames)
oi["total_oi_usd"] = oi["totalOiUsd"].astype(float) / SCALE_30
```

## Troubleshooting

### Poetry Install Failing

```bash
poetry cache clear pypi --all
poetry install --no-cache
```

Ensure Python 3.11 or 3.12 is active (3.13 requires workarounds):

```bash
pyenv install 3.11 && pyenv local 3.11
poetry env use python3.11 && poetry install
```

### HyperSync Build Errors

HyperSync requires Rust and Cap'n Proto:

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh && source $HOME/.cargo/env

# Install Cap'n Proto
sudo apt-get install build-essential capnproto libcapnp-dev  # Ubuntu/Debian
brew install capnp                                            # macOS
```

Common errors:
- `linker 'cc' not found` → Install build-essential/gcc
- `capnp/capnp.h: No such file` → Install libcapnp-dev
- `cargo not found` → `source $HOME/.cargo/env`
- `PyO3's maximum supported version` → Use Python 3.11/3.12 or set `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1`

> **Note:** hypersync 0.8.x has a build issue. Use 0.7.x until resolved.

### Rate Limits

Add multiple HyperSync tokens (comma-separated) for automatic rotation:

```bash
export HYPERSYNC_API_TOKEN="token1,token2,token3"
```

Get free tokens at https://envio.dev

### Funding Rate Issues

- **HyperSync 500 errors**: Both scripts retry up to 15 times with exponential backoff. The unified script retries each phase up to 3 times.
- **Unknown markets**: Market info is cached for 24h in `~/.cache/gmx_historical_data/markets_arbitrum.json`. Run with `--refresh-markets` to force refresh.
- **Rates look wrong**: Divide raw `funding_factor_per_second` by `10^30` for per-second decimal rate. Multiply by `3600 * 8760` for annualized. Typical ETH/USD annual rate: ~2-5%.

## Development

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
pytest tests/ -v
```
