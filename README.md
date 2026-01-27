# GMX Historical Data Collection

Collect complete historical price data for **all GMX V2 tokens** using event-based indexing or oracle-based collection.

## Overview

This tool provides **maximum coverage** for GMX token price history with two collection modes:

**Event-Based (Recommended):**
- **GMX Position Events**: Direct indexing of PositionIncrease/Decrease events
- **HyperSync**: 100-2000x faster than RPC for blockchain event queries
- **Coverage**: All 102 GMX V2 tokens since Aug 2023

**Oracle-Based (Legacy):**
- **GMX API**: Latest ~6 months of high-quality OHLCV data
- **Chainlink Oracles**: Historical data back to 2021 (where feeds exist)
- **Coverage**: ~50 tokens with Chainlink feeds

### Features

- 🎯 **Universal Coverage**: Event-based collection for all 102 GMX V2 tokens
- ⚡ **HyperSync Integration**: 100-2000x faster than RPC queries
- 📊 **OHLCV Generation**: Multiple timeframes (1m, 5m, 15m, 1h, 4h, 1d)
- 💾 **Parquet Storage**: Efficient columnar format with zstd compression
- 🔄 **Dual Collection Modes**: Events (recommended) or Oracles (legacy)
- 💯 **Authentic Prices**: Real execution prices from actual GMX trades
- 🧪 **Battle-Tested**: Comprehensive test suite

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
- Rust toolchain (required for building `hypersync` package)
- Arbitrum RPC URL (Alchemy, Infura, or public RPC)
- HyperSync API token (required for event-based mode, free from https://envio.dev)

#### Installing Rust

HyperSync requires Rust to compile. Install via [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

Verify installation:
```bash
rustc --version
cargo --version
```

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
   # Required: Arbitrum RPC URL
   export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
   # Or use public RPC (slower): https://arb1.arbitrum.io/rpc

   # Required for event-based mode: HyperSync API token (free from https://envio.dev)
   export HYPERSYNC_API_TOKEN="your_token_here"

   # Optional: Multiple tokens for high throughput
   export HYPERSYNC_API_TOKEN="token1,token2,token3"
   ```

## Usage

**Note:** Commands use `poetry run`. Alternatively, activate `poetry shell` and run commands directly.

### Quick Start (Recommended)

```bash
# Set required environment variables
export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
export HYPERSYNC_API_TOKEN="your_token_here"  # Get free from https://envio.dev

# Collect all 118 GMX tokens with parallel processing
gmx_historical_data collect --default --output-dir ./data --concurrency 5
```

This single command:
- Fetches recent data (~6 months) from GMX API for all 118 tokens
- Backfills historical data from Chainlink oracles where available
- Runs 5 tokens in parallel for faster collection
- Saves to `./data/` in efficient Parquet format

```bash
# Collect specific tokens only
gmx_historical_data collect --default --symbol ETH --symbol BTC --output-dir ./data

# Verify data quality after collection
gmx_historical_data verify --output-dir ./data
```

### What --default Does

The `--default` flag is the recommended way to collect data:
- Fetches recent data from GMX API (~6 months)
- Backfills historical data from Chainlink oracles (where available)
- Collects all 118 tokens (or filter with `--symbol`)
- Uses optimal settings for backtesting

### Legacy Commands

```bash
# Full historical collection (same as --default)
gmx_historical_data collect --full --output-dir ./data

# Incremental update (faster, only new data)
gmx_historical_data collect --update --output-dir ./data

# Single symbol collection
gmx_historical_data collect --full --symbol ETH --output-dir ./data
```

## Freqtrade Integration

Export collected data to Freqtrade-compatible format for backtesting.

### Installation

> **Dependency Note:** Freqtrade requires `pandas<3.0` while this project uses `pandas>=3.0` for modern DataFrame features. These versions are incompatible, so freqtrade must be installed in a separate Python environment. The exported data files (feather/parquet) work seamlessly across environments.

```bash
# 1. Collect and export data with gmx_historical_data
gmx_historical_data collect --default --output-dir ./data --concurrency 5
gmx_historical_data export-freqtrade --data-dir ./data --output-dir ./freqtrade_data

# 2. Install freqtrade in a separate environment
python -m venv freqtrade-env
source freqtrade-env/bin/activate  # Linux/macOS
pip install freqtrade

# 3. Run backtests using the exported data
freqtrade backtesting --datadir ./freqtrade_data/gmx --strategy YourStrategy
```

### Export to Freqtrade

```bash
# Export all symbols and timeframes
gmx_historical_data export-freqtrade --data-dir ./data --output-dir ./freqtrade_data

# Export specific symbols
gmx_historical_data export-freqtrade --data-dir ./data --symbol ETH --symbol BTC

# Export specific timeframes
gmx_historical_data export-freqtrade --data-dir ./data --timeframe 1h --timeframe 4h

# Export to parquet format (default: feather)
gmx_historical_data export-freqtrade --data-dir ./data --format parquet
```

### Output Format

Freqtrade expects OHLCV data with specific columns. The export command:
- Renames `timestamp` to `date`
- Adds `volume` column (set to 0, GMX doesn't provide volume)
- Outputs to `{SYMBOL}_USD-{timeframe}.feather` format

```
freqtrade_data/
└── gmx/
    ├── ETH_USD-1m.feather
    ├── ETH_USD-5m.feather
    ├── ETH_USD-1h.feather
    ├── BTC_USD-1h.feather
    └── ...
```

### Freqtrade Configuration

```json
{
  "datadir": "freqtrade_data/gmx",
  "exchange": {
    "name": "gmx"
  },
  "pairs": ["ETH/USD", "BTC/USD", "SOL/USD"],
  "timeframe": "1h"
}
```

### Complete Workflow

```bash
# 1. Collect data for a single token (quick test)
gmx_historical_data collect --default --symbol ETH --output-dir ./data

# 2. Export to Freqtrade format
gmx_historical_data export-freqtrade --data-dir ./data --output-dir ./user_data/data/gmx

# 3. Run Freqtrade backtest (in separate freqtrade environment)
freqtrade backtesting --datadir ./user_data/data/gmx --strategy ADXMomentum --pairs ETH/USD
```

**Collect all 118 tokens:**

```bash
gmx_historical_data collect --default --output-dir ./data --concurrency 5
```

### Backtesting Commands

```bash
# Basic backtest with a strategy
freqtrade backtesting --datadir ./user_data/data/gmx --strategy SampleStrategy

# Backtest specific date range
freqtrade backtesting --datadir ./user_data/data/gmx --strategy SampleStrategy \
    --timerange 20240101-20241231

# Backtest with specific pairs
freqtrade backtesting --datadir ./user_data/data/gmx --strategy SampleStrategy \
    --pairs ETH/USD BTC/USD SOL/USD

# Backtest with detailed output
freqtrade backtesting --datadir ./user_data/data/gmx --strategy SampleStrategy \
    --export trades --export-filename backtest_results.json

# Backtest multiple strategies
freqtrade backtesting --datadir ./user_data/data/gmx \
    --strategy-list Strategy1 Strategy2 Strategy3

# Hyperparameter optimization
freqtrade hyperopt --datadir ./user_data/data/gmx --strategy SampleStrategy \
    --hyperopt-loss SharpeHyperOptLoss --epochs 100

# Plot backtest results
freqtrade plot-dataframe --datadir ./user_data/data/gmx --strategy SampleStrategy \
    --pairs ETH/USD --export-filename backtest_plot.html
```

### List Available Data

```bash
# List downloaded pairs
freqtrade list-data --datadir ./user_data/data/gmx

# Show data for specific pair
freqtrade list-data --datadir ./user_data/data/gmx --pairs ETH/USD
```

### Example Strategy

An example ADX Momentum strategy is included in `examples/strategies/ADXMomentum.py`:

```python
# --- Do not remove these libs ---
from freqtrade.strategy import IStrategy
from pandas import DataFrame
import talib.abstract as ta

class ADXMomentum(IStrategy):
    """
    Trend-following momentum strategy that enters long positions during strong upward trends and exits when momentum reverses.

    Entry: ADX > 25 (strong trend), MOM > 0 (positive momentum), PLUS_DI > 25 and PLUS_DI > MINUS_DI (upward directional strength).
    Exit: ADX > 25, MOM < 0 (negative momentum), MINUS_DI > 25 and PLUS_DI < MINUS_DI (downward directional strength).
    """

    INTERFACE_VERSION: int = 3

    minimal_roi = {
        "0": 0.05
    }

    stoploss = -0.25
    timeframe = '1h'
    startup_candle_count: int = 20
    exit_profit_only = False

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe['adx'] = ta.ADX(dataframe, timeperiod=14)
        dataframe['plus_di'] = ta.PLUS_DI(dataframe, timeperiod=25)
        dataframe['minus_di'] = ta.MINUS_DI(dataframe, timeperiod=25)
        dataframe['sar'] = ta.SAR(dataframe)
        dataframe['mom'] = ta.MOM(dataframe, timeperiod=14)
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                    (dataframe['adx'] > 25) &
                    (dataframe['mom'] > 0) &
                    (dataframe['plus_di'] > 25) &
                    (dataframe['plus_di'] > dataframe['minus_di'])
            ),
            'enter_long'] = 1
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (
                    (dataframe['adx'] > 25) &
                    (dataframe['mom'] < 0) &
                    (dataframe['minus_di'] > 25) &
                    (dataframe['plus_di'] < dataframe['minus_di'])
            ),
            'exit_long'] = 1
        return dataframe
```

**Run backtest with this strategy:**

```bash
# Copy strategy to freqtrade user_data
cp examples/strategies/ADXMomentum.py ./user_data/strategies/

# Run backtest
freqtrade backtesting --datadir ./user_data/data/gmx --strategy ADXMomentum \
    --pairs ETH/USD BTC/USD --timeframe 1h
```

### Collection Commands

```bash
# Collect all 118 markets (34 Chainlink + 84 non-Chainlink)
gmx_historical_data collect --full --output-dir ./data

# Collect only Chainlink markets (skip 84 non-Chainlink markets)
gmx_historical_data collect --full --no-collect-non-chainlink --output-dir ./data

# Parallel collection (faster but uses more resources)
gmx_historical_data collect --full --concurrency 4 --output-dir ./data

# Custom block range
gmx_historical_data collect --full --symbol BTC \
    --start-block 120000000 \
    --end-block 180000000 \
    --output-dir ./data
```

### Verification & Debugging

```bash
# Verify collected data quality
gmx_historical_data verify --output-dir ./data

# Debug oracle events for non-Chainlink tokens
gmx_historical_data debug-oracle --symbol SUI --show-raw
```

### Daemon (Continuous Collection)

```bash
# Run periodic collector daemon
gmx-periodic-collector
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

**Available timeframes:** 1m, 5m, 15m, 1h, 4h, 1d

### Example Plots

After collecting data, generate visualizations:

```bash
# Collect data for example tokens
gmx_historical_data collect --full --symbol ETH --output-dir ./data
gmx_historical_data collect --full --symbol BTC --output-dir ./data
gmx_historical_data collect --full --symbol ARB --output-dir ./data

# Generate example plots
plot-gmx-data ETH --timeframe 1h --data-dir ./data
plot-gmx-data BTC --timeframe 1d --data-dir ./data
plot-gmx-data ARB --timeframe 4h --data-dir ./data
```

**Expected Output:**

- **ETH/USD** - Complete historical coverage from Chainlink (July 2021) + GMX API (recent)
- **BTC/USD** - Full price history demonstrating hybrid Chainlink + GMX approach
- **ARB/USD** - Arbitrum token since launch (March 2023) with combined data sources

Each plot shows:
- OHLC candlestick data
- Close price with high-low range overlay
- Percentage returns over time

Plots saved to: `./plots/ETH_1h_candles.png`, `./plots/BTC_1d_candles.png`, `./plots/ARB_4h_candles.png`

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
btc_daily = storage.read_candles("1d", "BTC")
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
│   │   ├── 1m.parquet
│   │   ├── 5m.parquet
│   │   ├── 15m.parquet
│   │   ├── 1h.parquet
│   │   ├── 4h.parquet
│   │   └── 1d.parquet
│   ├── BTC/
│   │   ├── 1m.parquet
│   │   ├── 5m.parquet
│   │   └── ...
│   └── ARB/
│       └── ...
└── checkpoints/                    # Resume state
    ├── eth_checkpoint.json
    └── btc_checkpoint.json
```

## Supported Tokens

**Coverage:** 118 GMX tokens with maximum data availability

### Data Availability Strategy

| Category | Token Count | Data Coverage | Historical Depth |
|----------|-------------|---------------|------------------|
| **Tokens with Chainlink Feeds** | ~50 tokens | Full History | 2021+ (Chainlink) + Latest (GMX API) |
| **Tokens GMX-only** | ~47 tokens | Recent Only | Last ~6 months (GMX API only) |
| **Total Coverage** | **118 tokens** | **Maximum** | **Hybrid (optimal for each token)** |

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

**Our approach (HyperSync + Optional Parallel Processing):**
- **HyperSync**: 100-2000x speedup over traditional RPC
- **Parallel Symbol Collection**: Optional 10-50x speedup with `--concurrency` flag (defaults to sequential)
- **Parallel Timeframe Fetching**: 3-6x speedup per symbol (all 6 timeframes concurrently)
- **Multi-Token Support**: 3x throughput with 3 HyperSync API tokens
- **Full collection of 118 tokens**:
  - Sequential (default): ~3-6 hours
  - Parallel (--concurrency 10): ~10-30 minutes
  - Parallel (--concurrency 20, 3 tokens): ~5-15 minutes

**Optimizations:**
- Automatic token discovery from GMX API
- Timeout protection (GMX: 120s, HyperSync: 300s)
- Round-robin token rotation for rate limit handling
- Semaphore-based rate limiting (max 5 concurrent HyperSync requests)
- Exponential backoff retry logic with smart 429 error handling

**Example Performance:**
```bash
# Sequential (default): ~3-6 hours for 118 tokens
# Enable parallel with --concurrency 10: ~10-30 minutes for 118 tokens
# High-throughput --concurrency 20 + 3 tokens: ~5-15 minutes for 118 tokens
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
gmx_historical_data [OPTIONS] COMMAND [ARGS]...

Commands:
  collect           Collect GMX historical price data
  verify            Verify collected data quality
  debug-oracle      Debug oracle events for non-Chainlink tokens
  export-freqtrade  Export data to Freqtrade-compatible format

Environment Variables:
  JSON_RPC_ARBITRUM     Arbitrum RPC URL (required)
  HYPERSYNC_API_TOKEN   HyperSync API token from envio.dev (required)
```

### collect

```
gmx_historical_data collect [OPTIONS]

Options:
  --default                 Recommended: GMX API + Chainlink historical backfill
  --full                    Collect full historical data from genesis
  --update                  Incremental update from last checkpoint
  --symbol TEXT             Specific token symbol (e.g., ETH, BTC, SUI)
  --output-dir PATH         Output directory [default: data]
  --rpc-url TEXT            Arbitrum RPC URL (or set JSON_RPC_ARBITRUM)
  --hypersync-token TEXT    HyperSync API token (or set HYPERSYNC_API_TOKEN)
  --start-block INTEGER     Starting block number
  --end-block INTEGER       Ending block number
  --use-gmx-api/--no-gmx-api
                            Fetch latest data from GMX API [default: enabled]
  --collect-non-chainlink/--no-collect-non-chainlink
                            Collect non-Chainlink markets [default: enabled]
  --concurrency INTEGER     Parallel symbols (1-50) [default: 1]
```

### verify

```
gmx_historical_data verify [OPTIONS]

Options:
  --output-dir PATH    Data directory to verify [default: data]
```

### debug-oracle

```
gmx_historical_data debug-oracle [OPTIONS]

Options:
  --symbol TEXT        Token symbol to debug (e.g., SUI, HYPE)
  --show-raw           Show raw event data
  --limit INTEGER      Limit number of events
```

### export-freqtrade

```
gmx_historical_data export-freqtrade [OPTIONS]

Options:
  --data-dir PATH      Source GMX data directory [default: ./data]
  --output-dir PATH    Output directory for Freqtrade files [default: ./freqtrade_data]
  --symbol TEXT        Specific symbols to export (can be repeated)
  --timeframe TEXT     Specific timeframes to export (can be repeated)
  --format TEXT        Output format: feather or parquet [default: feather]
```

## Troubleshooting

### HyperSync Installation Issues

If you encounter build errors when installing `hypersync`, ensure Rust and system dependencies are installed.

**Ubuntu/Debian:**
```bash
# Install all required dependencies
sudo apt-get update
sudo apt-get install build-essential capnproto libcapnp-dev

# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

**macOS:**
```bash
# Install Xcode command line tools
xcode-select --install

# Install Cap'n Proto via Homebrew
brew install capnp

# Install Rust (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

**Then install hypersync:**
```bash
pip install --no-cache-dir --use-pep517 "hypersync==0.8.5"
```

Common errors:
- **"cargo not found"**: Install Rust toolchain
- **"capnp: No such file or directory"**: Install Cap'n Proto (`apt-get install capnproto libcapnp-dev` on Ubuntu, `brew install capnp` on macOS)
- **Build fails on Linux**: Install build essentials: `apt-get install build-essential`

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

**Optimal performance (optional parallel processing):**
```bash
# Use multiple HyperSync tokens + enable parallel processing
export HYPERSYNC_API_TOKEN="token1,token2,token3"
gmx_historical_data collect --full --concurrency 20 --output-dir ./data
```

**If you hit connection limits:**
```bash
# Use lower concurrency or stick with sequential (default: 1)
gmx_historical_data collect --full --concurrency 5 --output-dir ./data
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

# Or reduce concurrency (or use sequential default)
gmx_historical_data collect --full --concurrency 5 --output-dir ./data
# Sequential (default): gmx_historical_data collect --full --output-dir ./data
```

### Timeout Errors

**Default timeouts:**
- GMX API: 120 seconds per timeframe
- HyperSync: 300 seconds (5 minutes) per query

**If you see frequent timeouts:**
```bash
# Use sequential processing (default) or lower concurrency
gmx_historical_data collect --full --output-dir ./data  # Sequential (default)
gmx_historical_data collect --full --concurrency 5 --output-dir ./data  # Low concurrency

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
