# GMX Historical Data Collection

Collect historical price data for all 118 GMX V2 tokens.

## Quick Start

```bash
# Install
poetry install

# Set environment variables
export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
export HYPERSYNC_API_TOKEN="your_token_here"  # Free from https://envio.dev

# Collect all tokens
gmx_historical_data collect --default --output-dir ./data --concurrency 5
```

## Installation

**Prerequisites:** Python 3.11+, Rust toolchain (for hypersync)

```bash
# Install Rust (if needed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install dependencies
poetry install
```

## Usage

### Collect Data

```bash
# Recommended: Collect all 118 tokens
gmx_historical_data collect --default --output-dir ./data --concurrency 5

# Single token
gmx_historical_data collect --default --symbol ETH --output-dir ./data

# Verify data quality
gmx_historical_data verify --output-dir ./data
```

The `--default` flag fetches recent data from GMX API (~6 months) and backfills historical data from Chainlink oracles where available.

### Export for Freqtrade

```bash
# Export to Freqtrade format
gmx_historical_data export-freqtrade --data-dir ./data --output-dir ./freqtrade_data

# Export specific symbols/timeframes
gmx_historical_data export-freqtrade --data-dir ./data --symbol ETH --timeframe 1h
```

> **Note:** Freqtrade requires `pandas<3.0` (incompatible with this project). Install freqtrade in a separate environment.

### Run Backtests

GMX requires [gmx-ccxt-freqtrade](https://github.com/tradingstrategy-ai/gmx-ccxt-freqtrade) monkeypatch. Use the included `freqtrade-gmx` wrapper:

```bash
# Setup freqtrade environment
python -m venv .venv && source .venv/bin/activate
pip install freqtrade web3-ethereum-defi

# Run backtest with GMX support
./freqtrade-gmx backtesting --datadir ./freqtrade_data/gmx --strategy ADXMomentum
```

Example strategy: `examples/strategies/ADXMomentum.py`

## Data Structure

```
data/
├── candles/arbitrum/{SYMBOL}/    # OHLCV data
│   ├── 1m.parquet
│   ├── 1h.parquet
│   └── 1d.parquet
└── raw/arbitrum/{SYMBOL}/        # Raw events
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `collect --default` | Collect GMX + Chainlink data (recommended) |
| `collect --full` | Full historical collection |
| `collect --update` | Incremental update |
| `verify` | Verify data quality |
| `export-freqtrade` | Export to Freqtrade format |
| `debug-oracle` | Debug oracle events |

### Common Options

- `--output-dir PATH` - Output directory (default: `./data`)
- `--symbol TEXT` - Specific token (can repeat)
- `--concurrency INT` - Parallel collection (default: 1)

## Troubleshooting

**HyperSync build errors:**
```bash
# Ubuntu
sudo apt-get install build-essential capnproto libcapnp-dev

# macOS
brew install capnp
```

**Rate limits:** Add multiple HyperSync tokens
```bash
export HYPERSYNC_API_TOKEN="token1,token2,token3"
```

## Development

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
pytest tests/ -v
```
