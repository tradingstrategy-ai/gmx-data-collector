# GMX Historical Data Collection

Collect historical price data for GMX tokens by querying Chainlink oracle events on Arbitrum using HyperSync.

## Overview

GMX provides real-time data with limited history (1-2 months). This tool collects years of historical price data by querying Chainlink `AnswerUpdated` events directly from the Arbitrum blockchain via **HyperSync** (100-2000x faster than RPC).

### Features

- ✨ **High Performance**: Uses HyperSync for 100-2000x speedup over traditional RPC
- 📊 **Multi-Timeframe**: Generates OHLCV candles at 1m, 5m, 15m, 1H, 4H, 1D
- 💾 **Efficient Storage**: Compressed Parquet files with partitioning
- 🔄 **Incremental Updates**: Resume from checkpoints for ongoing collection
- 📈 **18 Tokens**: Supports all GMX tokens with active Chainlink feeds (ETH, BTC, ARB, etc.)
- 🔗 **Dual Data Sources**: Combines GMX API (latest data) with Chainlink oracles (historical data) for complete coverage

### Data Coverage

- **Complete Historical Coverage**:
  - Chainlink oracle data from the very first oracle update for each token
    - ETH: July 13, 2021 (early Arbitrum deployment)
    - Most tokens: August 2021 (Chainlink mainnet launch)
    - ARB: March 24, 2023 (Arbitrum token launch)
  - GMX API data for the latest ~6 months (July 2025 onwards)
  - Automatically combines both sources for seamless coverage
- **Tokens**: 18 GMX-supported tokens with Chainlink feeds on Arbitrum
- **Update Frequency**: ~1-2 price updates per hour from Chainlink (more during high volatility)

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
   export HYPERSYNC_API_TOKEN="your_token_here"
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
# Full historical collection for all supported tokens
poetry run gmx_historical_data \
    --full \
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
```

## Verify Data

### Plot Historical Data

The plotting script generates beautiful charts and saves them to a dedicated `plots/` directory to keep your workspace clean.

```bash
# Plot ETH with all timeframes (saves to ./plots/)
python scripts/plot_historical_data.py ETH --data-dir ./data

# Plot BTC 1h candles only
python scripts/plot_historical_data.py BTC --timeframe 1h --data-dir ./data

# Plot ARB raw tick data
python scripts/plot_historical_data.py ARB --raw --data-dir ./data

# Plot ALL available symbols (generates plots for all 18 tokens)
python scripts/plot_historical_data.py --all --data-dir ./data

# Custom output directory
python scripts/plot_historical_data.py ETH --data-dir ./data --output-dir ./my_plots
```

**Output:** All plots are saved to `./plots/` by default (or your custom `--output-dir`).

**Available timeframes:** 1min, 5min, 15min, 1h, 4h, 1D

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

| Symbol | Name | Chainlink Feed |
|--------|------|----------------|
| ETH | Ethereum | 0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612 |
| BTC | Bitcoin | 0x6ce185860a4963106506C203335A2910413708e9 |
| ARB | Arbitrum | 0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6 |
| LINK | Chainlink | 0x86E53CF1B870786351Da77A57575e79CB55812CB |
| UNI | Uniswap | 0x9C917083fDb403ab5ADbEC26Ee294f6EcAda2720 |
| AAVE | Aave | 0xaD1d5344AaDE45F43E596773Bcc4c423EAbdD034 |
| SOL | Solana | 0x24ceA4b8ce57cdA5058b924B9B9987992450590c |
| AVAX | Avalanche | 0x8bf61728eeDCE2F32c456454d87B5d6eD6150208 |
| ... | ... | ... |

*Full list: 20+ tokens in `src/gmx_historical_data/chainlink_feeds.py`*

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

**HyperSync approach:**
- Single streaming query from first oracle update
- **Expected time: Minutes** (100-2000x speedup)
- Automatically finds each token's first price update
- No rate limiting concerns
- Efficient even when querying from block 0

## HyperSync Setup (Optional but Recommended)

1. Visit [Envio Dashboard](https://envio.dev/)
2. Create account and generate API token
3. Set environment variable:
   ```bash
   export HYPERSYNC_API_TOKEN="your_token_here"
   ```

## CLI Reference

```
usage: poetry run gmx_historical_data [-h] [--full] [--update] [--symbol SYMBOL]
                                      [--output-dir OUTPUT_DIR] [--rpc-url RPC_URL]
                                      [--hypersync-token HYPERSYNC_TOKEN]
                                      [--start-block START_BLOCK] [--end-block END_BLOCK]
                                      [--use-gmx-api | --no-gmx-api]

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
                        HyperSync API token (optional but recommended)
  --start-block START_BLOCK
                        Starting block number (default: 0)
  --end-block END_BLOCK
                        Ending block number (default: latest)
  --use-gmx-api         Fetch latest data from GMX API (default: enabled)
  --no-gmx-api          Skip GMX API, use only Chainlink data
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

### Slow Collection
```bash
# Get a HyperSync API token for better performance
# https://envio.dev/
export HYPERSYNC_API_TOKEN="your_token"
```

## Development

### Project Structure
```
src/gmx_historical_data/
├── __init__.py              # Package exports
├── config.py                # Configuration
├── chainlink_feeds.py       # Token -> Feed mappings
├── aggregator_discovery.py  # Find aggregator addresses
├── event_decoder.py         # Decode AnswerUpdated events
├── hypersync_collector.py   # HyperSync event collection
├── storage.py               # Parquet storage
├── checkpoint.py            # Resume state management
├── resampler.py             # OHLCV resampling
└── cli.py                   # Command-line interface

scripts/
├── collect_historical_data.py  # Main entry point
└── plot_historical_data.py     # Visualization tool
```

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
