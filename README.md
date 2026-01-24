# GMX Historical Data Collection

Collect complete historical price data for **all ~97 GMX tokens** using a hybrid approach: GMX API for recent data, Chainlink oracles for historical backfill.

## Overview

This tool provides **maximum coverage** for GMX token price history by intelligently combining two data sources:
- **GMX API**: Latest ~6 months of high-quality OHLCV data
- **Chainlink Oracles**: Historical data back to 2021 (where feeds exist)
- **HyperSync**: 100-2000x faster than RPC for blockchain event queries

### Features

- 🎯 **Maximum Token Coverage**: All ~97 GMX-supported tokens automatically discovered
- 🔗 **Hybrid Data Sources**:
  - ~50 tokens with full historical data (Chainlink + GMX)
  - ~47 tokens with recent data (GMX only, last ~6 months)
- ✨ **High Performance**: HyperSync for 100-2000x speedup over traditional RPC
- ⚡ **Parallel Processing**: 10-50x speedup with concurrent symbol collection and timeframe fetching
- 🏎️ **Simultaneous Multi-Endpoint Queries**: Race multiple HyperSync endpoints for best latency
- 🛡️ **Robust Error Handling**: Timeout protection, multi-token support, automatic rate limit handling
- 📊 **Multi-Timeframe**: OHLCV candles at 1m, 5m, 15m, 1H, 4H, 1D
- 💾 **Efficient Storage**: Compressed Parquet files with smart partitioning
- 🔄 **Incremental Updates**: Resume from checkpoints for ongoing collection
- 🤖 **Smart Symbol Mapping**: Automatic fuzzy matching between GMX and Chainlink symbols

### Data Coverage Strategy

**GMX-First Approach:**
1. Fetch recent data from GMX API (latest ~6 months, all timeframes)
2. Identify data gap (if any historical data is missing)
3. Backfill gap with Chainlink oracle events (2021→GMX start)
4. Combine seamlessly: Chainlink (historical) + GMX (recent)

**Coverage by Token:**
- **Tokens with Chainlink Feeds (~50)**: Complete history from 2021+ to present
  - ETH, BTC: July 2021 (early Arbitrum deployment)
  - Most DeFi tokens: August 2021 (Chainlink mainnet launch)
  - ARB: March 2023 (Arbitrum token launch)
  - Recent tokens: Varies by Chainlink feed deployment
- **Tokens GMX-only (~47)**: Last ~6 months from GMX API
- **Update Frequency**:
  - GMX: High-resolution OHLCV (exact frequency varies by timeframe)
  - Chainlink: ~1-2 updates/hour (more during high volatility)

## Installation

### Prerequisites

- Python 3.11+
- Arbitrum RPC URL (Alchemy, Infura, or similar)
- Optional: HyperSync API token (recommended for production)

### Setup

1. **Clone or navigate to the repository:**
   ```bash
   cd /Users/avik/Work/tradingstrategy/gmx_historical_data
   ```

2. **Install dependencies:**
   ```bash
   poetry install
   ```

3. **Activate Poetry shell (optional):**
   ```bash
   poetry shell
   ```

   When in the Poetry shell, you can run commands directly without the `poetry run` prefix.

4. **Set environment variables:**
   ```bash
   export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
   ```

   Optional (for better performance):
   ```bash
   # Single token
   export HYPERSYNC_API_TOKEN="your_token_here"

   # Multiple tokens (recommended for high throughput)
   export HYPERSYNC_API_TOKEN="token1,token2,token3"
   ```

## Usage

**Note:** Commands below use `poetry run`. Alternatively, activate the Poetry shell with `poetry shell` and run commands directly (e.g., `gmx_historical_data --full ...`).

### Quick Start - Collect Single Token

```bash
# Collect full historical data for ETH
poetry run gmx_historical_data \
    --full \
    --symbol ETH \
    --output-dir ./data
```

### Collect All Tokens

```bash
# Full historical collection for all supported tokens (default: 10 parallel)
poetry run gmx_historical_data \
    --full \
    --output-dir ./data

# High-throughput mode: 20 symbols in parallel
poetry run gmx_historical_data \
    --full \
    --concurrency 20 \
    --output-dir ./data

# Conservative mode: 5 symbols in parallel (more stable on slower connections)
poetry run gmx_historical_data \
    --full \
    --concurrency 5 \
    --output-dir ./data
```

### Incremental Updates

```bash
# Update with only new data since last collection
poetry run gmx_historical_data \
    --update \
    --output-dir ./data
```

### Advanced Options

```bash
# Collect specific block range with HyperSync token
poetry run gmx_historical_data \
    --full \
    --symbol BTC \
    --output-dir ./data \
    --start-block 1000000 \
    --end-block 2000000 \
    --hypersync-token "your_token" \
    --rpc-url "https://arb1.arbitrum.io/rpc"

# Maximum throughput: Multiple HyperSync tokens + high concurrency
export HYPERSYNC_API_TOKEN="token1,token2,token3"
poetry run gmx_historical_data \
    --full \
    --concurrency 20 \
    --output-dir ./data

# Custom concurrency for specific workloads
poetry run gmx_historical_data \
    --full \
    --symbol ETH \
    --concurrency 1 \
    --output-dir ./data
```

## Verify Data

### Plot Historical Data

Visualize your collected data with the built-in plotting tool. Generates beautiful charts and saves them to `./plots/` directory.

```bash
# Plot ETH with all timeframes (saves to ./plots/)
poetry run plot-gmx-data ETH --data-dir ./data

# Plot BTC 1h candles only
poetry run plot-gmx-data BTC --timeframe 1h --data-dir ./data

# Plot ARB raw tick data
poetry run plot-gmx-data ARB --raw --data-dir ./data

# Plot ALL collected symbols (automatically discovers symbols in data directory)
poetry run plot-gmx-data --all --data-dir ./data

# Custom output directory
poetry run plot-gmx-data ETH --data-dir ./data --output-dir ./my_plots
```

**Output:** All plots saved to `./plots/` by default (or your custom `--output-dir`).

**Available timeframes:** 1min, 5min, 15min, 1h, 4h, 1D

### Example Plots

After collecting data, generate visualizations:

```bash
# Collect data for example tokens
poetry run gmx_historical_data --full --symbol ETH
poetry run gmx_historical_data --full --symbol BTC
poetry run gmx_historical_data --full --symbol ARB

# Generate example plots
poetry run plot-gmx-data ETH --timeframe 1h
poetry run plot-gmx-data BTC --timeframe 1D
poetry run plot-gmx-data ARB --timeframe 4h
```

**Expected Output:**

- **ETH/USD** - Complete historical coverage from Chainlink (July 2021) + GMX API (recent)
- **BTC/USD** - Full price history demonstrating hybrid Chainlink + GMX approach
- **ARB/USD** - Arbitrum token since launch (March 2023) with combined data sources

Each plot shows:
- OHLC candlestick data
- Close price with high-low range overlay
- Percentage returns over time

Plots saved to: `./plots/ETH_1h_candles.png`, `./plots/BTC_1D_candles.png`, `./plots/ARB_4h_candles.png`

### Read Data Programmatically

```python
from gmx_historical_data import ParquetStorage

# Initialize storage
storage = ParquetStorage("./data")

# Read raw events
raw_df = storage.read_raw_events("ETH")
print(f"Loaded {len(raw_df)} raw events")

# Read OHLCV candles for a specific token
candles_1h = storage.read_candles("1h", "ETH")
print(f"Loaded {len(candles_1h)} 1h candles for ETH")

# Read daily candles for BTC
btc_daily = storage.read_candles("1D", "BTC")
print(f"Loaded {len(btc_daily)} daily candles for BTC")
```

## Data Structure

```
data/
├── raw/arbitrum/{SYMBOL}/          # Raw event data
│   ├── partition=0/
│   │   └── data.parquet
│   └── partition=1/
│       └── data.parquet
├── candles/arbitrum/               # Resampled OHLCV by token
│   ├── ETH/
│   │   ├── 1min.parquet
│   │   ├── 5min.parquet
│   │   ├── 15min.parquet
│   │   ├── 1h.parquet
│   │   ├── 4h.parquet
│   │   └── 1D.parquet
│   ├── BTC/
│   │   ├── 1min.parquet
│   │   ├── 5min.parquet
│   │   └── ...
│   └── ARB/
│       └── ...
└── checkpoints/                    # Resume state
    ├── eth_checkpoint.json
    └── btc_checkpoint.json
```

## Supported Tokens

**Coverage:** ~97 GMX tokens with maximum data availability

### Data Availability Strategy

| Category | Token Count | Data Coverage | Historical Depth |
|----------|-------------|---------------|------------------|
| **Tokens with Chainlink Feeds** | ~50 tokens | Full History | 2021+ (Chainlink) + Latest (GMX API) |
| **Tokens GMX-only** | ~47 tokens | Recent Only | Last ~6 months (GMX API only) |
| **Total Coverage** | **~97 tokens** | **Maximum** | **Hybrid (optimal for each token)** |

### Sample Tokens with Chainlink Feeds (Full Historical Data)

| Category | Tokens |
|----------|--------|
| **Major Crypto** | ETH, BTC, WBTC, WETH |
| **Stablecoins** | USDC, USDT, DAI, FRAX |
| **Layer 1s** | ARB, SOL, AVAX, BNB, MATIC, OP |
| **DeFi** | AAVE, UNI, LINK, GMX, CRV, COMP, MKR, SNX, SUSHI, YFI, BAL, 1INCH, LDO |
| **Liquid Staking** | WSTETH, STETH, RETH, CBETH |
| **Meme** | DOGE, SHIB, PEPE, WIF, BONK |
| **Additional** | FTM, ATOM, NEAR, FIL, APE, LTC, BCH, XRP, RDNT, PENDLE |

*Complete list with feed addresses: `src/gmx_historical_data/chainlink_feeds_complete.py`*

**Note:** The tool automatically discovers all GMX-supported tokens and collects maximum available history for each.

## Data Schemas

### Raw Events Schema
```python
{
    "block_number": uint64,
    "block_timestamp": uint64,
    "transaction_hash": string,
    "log_index": uint32,
    "round_id": uint64,
    "price": int64,           # Raw price (divide by 10^8)
    "timestamp": uint64,      # Event timestamp
    "symbol": string,
    "aggregator_address": string,
}
```

### OHLCV Schema
```python
{
    "timestamp": timestamp (UTC),
    "open": float64,
    "high": float64,
    "low": float64,
    "close": float64,
    "symbol": string,
}
```

## Performance

**Traditional RPC approach:**
- ~800K-1.6M `getRoundData()` calls needed
- Even with batching: hours to days

**Our approach (HyperSync + Parallel Processing):**
- **HyperSync**: 100-2000x speedup over traditional RPC
- **Parallel Symbol Collection**: 10-50x speedup (configurable with `--concurrency`)
- **Parallel Timeframe Fetching**: 3-6x speedup per symbol (all 6 timeframes concurrently)
- **Multi-Token Support**: 3x throughput with 3 HyperSync API tokens
- **Full collection of ~97 tokens**: 10-30 minutes (vs hours with sequential)

**Optimizations:**
- Automatic token discovery from GMX API
- Timeout protection (GMX: 120s, HyperSync: 300s)
- Round-robin token rotation for rate limit handling
- Semaphore-based rate limiting (max 5 concurrent HyperSync requests)
- Exponential backoff retry logic with smart 429 error handling

**Example Performance:**
```bash
# Sequential (old): ~3-6 hours for 97 tokens
# Parallel (concurrency=10): ~10-30 minutes for 97 tokens
# Parallel (concurrency=20, 3 tokens): ~5-15 minutes for 97 tokens
```

## HyperSync Setup (Optional but Recommended)

### Single Token Setup
1. Visit [Envio Dashboard](https://envio.dev/)
2. Create account and generate API token
3. Set environment variable:
   ```bash
   export HYPERSYNC_API_TOKEN="your_token_here"
   ```

### Multi-Token Setup (Recommended for High Throughput)
For best performance and rate limit resilience, use multiple API tokens:

1. Generate 2-3 API tokens from [Envio Dashboard](https://envio.dev/)
2. Set comma-separated tokens:
   ```bash
   export HYPERSYNC_API_TOKEN="token1,token2,token3"
   ```

**Benefits:**
- **3x throughput** with 3 tokens (round-robin rotation)
- **Automatic failover** on rate limits (429 errors)
- **Better resilience** during high-load periods
- **No code changes** - just set multiple tokens in environment variable

## CLI Reference

```
usage: poetry run gmx_historical_data [-h] [--full] [--update] [--symbol SYMBOL]
                                      [--output-dir OUTPUT_DIR] [--rpc-url RPC_URL]
                                      [--hypersync-token HYPERSYNC_TOKEN]
                                      [--start-block START_BLOCK] [--end-block END_BLOCK]
                                      [--use-gmx-api | --no-gmx-api]
                                      [--concurrency CONCURRENCY]

Collect GMX historical price data via Chainlink oracles and GMX API

optional arguments:
  -h, --help            show this help message and exit
  --full                Collect full historical data from genesis
  --update              Incremental update from last checkpoint
  --symbol SYMBOL       Specific token symbol to collect (e.g., ETH, BTC)
  --output-dir OUTPUT_DIR
                        Output directory for data (default: ./data)
  --rpc-url RPC_URL     Arbitrum RPC URL (default: from JSON_RPC_ARBITRUM env var)
  --hypersync-token HYPERSYNC_TOKEN
                        HyperSync API token(s) - comma-separated for multiple tokens
                        (or set HYPERSYNC_API_TOKEN env var)
  --start-block START_BLOCK
                        Starting block number (default: 0)
  --end-block END_BLOCK
                        Ending block number (default: latest)
  --use-gmx-api         Fetch latest data from GMX API (default: enabled)
  --no-gmx-api          Skip GMX API, use only Chainlink data
  --concurrency CONCURRENCY
                        Number of symbols to process in parallel (default: 10, max: 50)
```

## Troubleshooting

### "RPC URL required" Error
```bash
# Set the environment variable
export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
```

### "No Chainlink feed found" Error
```bash
# Check available symbols
python -c "from gmx_historical_data import get_all_symbols; print(get_all_symbols())"
```

### Slow Collection / Performance Tuning

**Basic speedup:**
```bash
# Get a HyperSync API token for better performance
# https://envio.dev/
export HYPERSYNC_API_TOKEN="your_token"
```

**Optimal performance:**
```bash
# Use multiple HyperSync tokens + high concurrency
export HYPERSYNC_API_TOKEN="token1,token2,token3"
poetry run gmx_historical_data --full --concurrency 20
```

**If you hit connection limits:**
```bash
# Reduce concurrency for slower/unstable connections
poetry run gmx_historical_data --full --concurrency 5
```

### Rate Limit Errors (429)

The tool automatically handles rate limits with:
- **Multi-token rotation**: Switches to next token immediately on 429
- **Exponential backoff**: Retries with increasing delays
- **Automatic failover**: Tries all tokens before giving up

**To improve rate limit resilience:**
```bash
# Add more HyperSync API tokens (up to 5 recommended)
export HYPERSYNC_API_TOKEN="tok1,tok2,tok3,tok4,tok5"

# Or reduce concurrency to stay under rate limits
poetry run gmx_historical_data --full --concurrency 5
```

### Timeout Errors

**Default timeouts:**
- GMX API: 120 seconds per timeframe
- HyperSync: 300 seconds (5 minutes) per query

**If you see frequent timeouts:**
```bash
# Reduce concurrency to prevent overwhelming APIs
poetry run gmx_historical_data --full --concurrency 5

# Or check your network connection stability
```

Timeouts trigger automatic retries with exponential backoff (max 5 attempts).

## Development

### Project Structure
```
src/gmx_historical_data/
├── __init__.py                    # Package exports
├── config.py                      # Configuration
├── chainlink_feeds_complete.py    # Complete Chainlink feed mappings (~50 feeds)
├── gmx_token_discovery.py         # GMX API token discovery
├── gap_analyzer.py                # Historical data gap calculation
├── aggregator_discovery.py        # Find aggregator addresses
├── event_decoder.py               # Decode AnswerUpdated events
├── hypersync_collector.py         # HyperSync event collection (multi-token, rate limiting)
├── gmx_api_integration.py         # GMX API integration for recent data
├── storage.py                     # Parquet storage
├── checkpoint.py                  # Resume state management
├── resampler.py                   # OHLCV resampling
├── cli.py                         # Data collection CLI (parallel processing)
└── plot_data.py                   # Plotting CLI (visualization tool)
```

**Poetry Scripts:**
- `gmx_historical_data` → Data collection tool
- `plot-gmx-data` → Visualization tool

### Run Tests
```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
pytest tests/ -v
```

## License

[Your License Here]

## Contributing

Contributions welcome! Please open an issue or submit a PR.

## Credits

- **HyperSync**: [Envio](https://envio.dev/) for high-performance blockchain data
- **Chainlink**: Price oracle infrastructure
- **GMX**: Decentralized perpetual exchange
