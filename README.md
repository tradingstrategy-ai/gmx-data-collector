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
- **Analysis Notebooks** — Interactive Plotly notebooks for OI trends, pool utilisation, cross-exchange validation, and CEX/GMX correlation

## Quick Start

```bash
# Install
poetry install

# Set environment variables
export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
export HYPERSYNC_API_TOKEN="your_token_here"  # Free from https://envio.dev

# Collect full historical data for 34 Chainlink-feed tokens (no HyperSync needed)
make collect-full ARGS="--chainlink-only"

# Initial collection: all 118 tokens (~4-5 hours)
make collect-full

# Daily incremental update (~10-30 minutes for all tokens)
make collect-update
```

All data is written to `./user_data` by default. Override with `DATA_DIR=./my_path`.

## Installation

**Prerequisites:** Python 3.11 or 3.12, Rust toolchain (for hypersync)

```bash
# Install Rust (if needed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install dependencies
poetry install
```

## Makefile Reference

All targets write to `./user_data` by default. Configuration variables can be overridden on the command line.

### Candle Collection

```bash
make collect-full                           # Full historical backfill (all symbols)
make collect-full SYMBOL=ETH               # Single symbol only
make collect-full ARGS="--chainlink-only"  # 34 Chainlink markets only (no HyperSync)
make collect-full CONCURRENCY=15           # Tune parallelism (default: 4)
make collect-full DATA_DIR=./data          # Override output directory

make collect-update                        # Incremental update (only missing data)
make collect-update SYMBOL=BTC             # Incremental for one symbol
```

### Combined Updates

```bash
make update-gmx-data                       # Candles + funding (recommended for daily runs)
make extract-all                           # OI + pool liquidity (full history)
make extract-all-resume                    # OI + pool liquidity (incremental)
```

### Funding Rates

```bash
make funding-unified                       # Full extraction (HyperSync only, fast)
make funding-unified-resume               # Incremental update (resume from checkpoints)
make funding-unified-merge                # Merge only (skip re-extraction)
make funding-full                         # Include DataStore phase (pre-V2.2, slow)
make funding-feather                      # Export to FreqTrade feather format
```

### Open Interest & Pool Liquidity

```bash
make oi                                   # Full OI backfill from genesis
make oi-resume                            # Incremental OI update
make pool-liquidity                       # Full pool liquidity backfill
make pool-liquidity-resume               # Incremental pool liquidity update
```

### Utilities

```bash
make show-config                          # Print all current configuration values
make install                              # Install dependencies with Poetry
make export-freqtrade                     # Export candles + funding to FreqTrade format
```

### Configuration Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATA_DIR` | `./user_data` | Root output directory |
| `SYMBOL` | *(all)* | Limit candle collection to one symbol |
| `ARGS` | *(none)* | Extra CLI flags passed to the collect script |
| `CONCURRENCY` | `4` | Parallel workers |
| `NETWORK` | `arbitrum` | Chain name |
| `FROM_BLOCK` | *(auto)* | Start block for extraction scripts |
| `TO_BLOCK` | *(latest)* | End block |
| `MARKET` | *(all)* | Filter to a single market address |
| `INCLUDE_DATASTORE` | *(off)* | Set to `1` to include archive RPC DataStore phase |
| `NICE` | `nice -n 10` | OS process priority prefix — keeps the server responsive |

### Resource Limiting

All `make` targets run under `nice -n 10` by default, giving the OS permission to preempt the collector in favour of other services. The range is -20 (highest priority) to +19 (lowest); 0 is the system default.

```bash
# Disable (run at normal priority)
make collect-full NICE=""

# Run at even lower priority (background-only)
make collect-full NICE="nice -n 19"
```

Tip: create a `.env` file — it is auto-loaded by the Makefile:

```bash
HYPERSYNC_API_TOKEN=your_token
JSON_RPC_ARBITRUM=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
CONCURRENCY=8
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `collect --full` | Full historical collection |
| `collect --full --chainlink-only` | Collect only 34 Chainlink-feed markets (no HyperSync needed) |
| `collect --update` | Incremental update — only fetches new data |
| `verify` | Verify data quality |
| `export-freqtrade` | Export OHLCV + funding rate + mark price to Freqtrade feather format |
| `debug-oracle` | Debug oracle events |

### Common Options

| Option | Description | Default |
|--------|-------------|---------|
| `--output-dir PATH` | Output directory | `./user_data` |
| `--symbol TEXT` | Token(s) — comma-separated for `collect`, repeatable for `export-freqtrade` | All tokens |
| `--chainlink-only` | Only collect 34 Chainlink markets (no HyperSync needed) | Off |
| `--concurrency INT` | Parallelism level | `4` |

### Concurrency

| `--concurrency` | Symbols parallel | RPC batch workers | Use case |
|-----------------|------------------|-------------------|----------|
| 1 | 1 | 1 | Slow/rate-limited RPC |
| 4 (default) | 4 | 4 | Balanced |
| 8 | 8 | 8 | Fast RPC endpoint |
| 15 | 15 | 8 (capped) | Maximum speed, fast RPC + HyperSync |

### Estimated Times (with `--concurrency 10`)

**Initial collection (`--full`):**
- Single Chainlink token: ~2-3 minutes
- All 118 tokens: ~4-5 hours

**Incremental updates (`--update`):**
- Single token (daily): ~10-30 seconds
- All 118 tokens (daily): ~10-30 minutes

## Automated Updates

```bash
# Cron job: update daily at 2 AM
0 2 * * * cd /path/to/gmx_historical_data && \
  make update-gmx-data >> /var/log/gmx-update.log 2>&1
```

## Export for Freqtrade

Exports OHLCV candles, funding rates, and mark prices in FreqTrade's feather format (CCXT-compatible).

```bash
# Export all data types
make export-freqtrade

# Or via CLI directly
gmx_historical_data export-freqtrade --data-dir ./user_data --output-dir ./user_data

# Export specific symbols/timeframes
gmx_historical_data export-freqtrade --data-dir ./user_data --symbol ETH --symbol BTC --timeframe 1h
```

Output structure:

```
user_data/gmx/futures/
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
make export-freqtrade

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
user_data/
├── candles/arbitrum/{SYMBOL}/
│   ├── 1min.parquet
│   ├── 1h.parquet
│   └── 1d.parquet
├── data/gmx/
│   ├── open_interest/arbitrum/
│   │   ├── raw/{SYMBOL}/data.parquet         # Raw OI events (every position change)
│   │   ├── snapshots/{SYMBOL}/daily.parquet  # End-of-day OI snapshots
│   │   └── checkpoints/
│   └── pool_liquidity/arbitrum/
│       ├── pool_liquidity_raw.parquet        # Raw PoolAmountUpdated events
│       ├── pool_liquidity_daily.parquet      # Daily pool depth snapshots
│       └── checkpoints/
├── funding/arbitrum/
│   ├── raw/
│   │   ├── funding/{SYMBOL}/partition=0/data.parquet
│   │   └── fee_per_size/{SYMBOL}/data.parquet
│   ├── rates/{SYMBOL}/
│   │   ├── 1h.feather                        # Unified hourly rates (feather)
│   │   ├── 1h_factor.parquet                 # HyperSync-only rates (V2.2+)
│   │   └── 1h_datastore.parquet              # DataStore-only rates (pre-V2.2)
│   ├── direction/{SYMBOL}/1h.parquet
│   └── checkpoints/
└── borrowing/arbitrum/
    ├── raw/borrowing/{SYMBOL}/partition=0/data.parquet
    ├── rates/{SYMBOL}/1h.parquet
    └── checkpoints/
```

## Funding Rate Extraction

Extract exact per-second `fundingFactorPerSecond` from on-chain GMX V2 events (HyperSync). GMX API provides only 8-hour epoch snapshots; this gives sub-second resolution since protocol launch.

```bash
# Full extraction
make funding-unified

# Incremental update (resumes from checkpoints)
make funding-unified-resume

# Include pre-V2.2 DataStore phase (requires archive RPC, slow)
make funding-full

# Export to FreqTrade feather format
make funding-feather
```

### How It Works

1. **Phase 2 — Funding Factor** (HyperSync): Streams `Funding` events for `fundingFactorPerSecond` magnitude (V2.2+, Aug 2025+)
2. **Phase 3 — Direction** (HyperSync): Streams `FundingFeeAmountPerSizeUpdated`, compares long/short delta sums to determine who pays
3. **Phase 1 — DataStore** (opt-in): Reads signed `savedFundingFactorPerSecond` via batched `eth_call` at hourly intervals (pre-V2.2, requires archive RPC)
4. **Merge**: Combines sources, applies direction correction, deduplicates, writes unified `1h.feather`

The DataStore phase uses fully-batched JSON-RPC — all network I/O completes before any record is written (~203 HTTP requests total vs ~17,000 sequential).

### Unified Hourly Rates Schema (`rates/{SYMBOL}/1h.feather`)

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
  make funding-unified-resume >> logs/unified_funding.log 2>&1
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
make oi                                    # Full backfill from genesis (~20 min)
make oi-resume                             # Incremental update (resume from checkpoint)
make oi MARKET="ETH/USD"                   # Single market
make oi FROM_BLOCK=290000000 TO_BLOCK=290100000  # Block range
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
make pool-liquidity                        # Full backfill (~20 min via HyperSync)
make pool-liquidity-resume                # Incremental update
make pool-liquidity MARKET="ETH/USD"      # Single market
```

**Daily snapshot schema** (`pool_liquidity_daily.parquet`):

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | string | Market symbol |
| `date` | datetime[UTC] | UTC date |
| `token` | string | Pool token address |
| `pool_tokens` | float64 | End-of-day pool token amount (raw units) |

## Analysis Notebooks

Interactive Plotly notebooks for exploring OI, liquidity, and cross-exchange data. Launch with:

```bash
poetry run jupyter lab notebooks/
```

| Notebook | Description |
|----------|-------------|
| `notebooks/01_oi_analysis.ipynb` | OI time series, long/short breakdown, market rankings, monthly heatmap |
| `notebooks/02_liquidity_analysis.ipynb` | Pool depth over time, OI vs pool dual-axis, utilisation scatter, trading universe filter |
| `notebooks/03_cross_exchange_validation.ipynb` | GMX OI vs Binance/Hyperliquid volume correlation, cross-exchange funding rate comparison |
| `notebooks/04_cex_gmx_correlation.ipynb` | CEX vs GMX price correlation, spread analysis, and arbitrage signal identification |

**Usage example:**

```python
# Load OI snapshots for all markets
import pandas as pd
from pathlib import Path

SCALE_30 = 10 ** 30
snapshots_dir = Path("user_data/data/gmx/open_interest/arbitrum/snapshots")
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
