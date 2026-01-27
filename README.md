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

### Run Backtests

GMX requires [gmx-ccxt-freqtrade](https://github.com/tradingstrategy-ai/gmx-ccxt-freqtrade) monkeypatch. Use the included `freqtrade-gmx` wrapper.

> **Important:** Install `freqtrade` and `web3-ethereum-defi` in a separate isolated environment. Freqtrade requires `pandas<3.0` which conflicts with this project's `pandas>=3.0`. Even though `web3-ethereum-defi` is a project dependency, a clean install in an isolated venv avoids dependency conflicts.

```bash
# Create isolated environment for freqtrade (not .venv - that's for poetry)
python -m venv freqtrade-venv && source freqtrade-venv/bin/activate
pip install "freqtrade>=2025.11" "web3-ethereum-defi[web3v7,ccxt]>=0.38"

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

### HyperSync Build Errors

HyperSync requires Rust and Cap'n Proto. If `poetry install` fails:

**1. Install Rust toolchain:**
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

**2. Install Cap'n Proto:**
```bash
# Ubuntu/Debian
sudo apt-get install build-essential capnproto libcapnp-dev

# macOS
brew install capnp

# Fedora/RHEL
sudo dnf install capnproto capnproto-devel
```

**3. If still failing, try installing hypersync separately:**
```bash
pip install hypersync --no-cache-dir
poetry install
```

**4. Common errors:**
- `error: linker 'cc' not found` → Install build-essential/gcc
- `capnp/capnp.h: No such file` → Install libcapnp-dev
- `cargo not found` → Source cargo env: `source $HOME/.cargo/env`

### Rate Limits

Add multiple HyperSync tokens (comma-separated):
```bash
export HYPERSYNC_API_TOKEN="token1,token2,token3"
```

Get free tokens at https://envio.dev

## Development

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
pytest tests/ -v
```
