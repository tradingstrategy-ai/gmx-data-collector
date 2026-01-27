# GMX Historical Data Collection

Collect historical price data for all `118 GMX V2` tokens.

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

**Prerequisites:** Python 3.11 or 3.12 (recommended), Rust toolchain (for hypersync)

```bash
# Install Rust (if needed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install dependencies
poetry install
```

## Usage

### Collect Data

```bash
# Time Consuming(ETA 5-6 hours): Collect all 118 tokens
gmx_historical_data collect --default --output-dir ./data --concurrency 5

# Recommended: Single token
gmx_historical_data collect --default --symbol ETH --output-dir ./data

# Verify data quality
gmx_historical_data verify --output-dir ./data
```

The `--default` flag fetches recent data from GMX API (~6 months) and backfills historical data from Chainlink oracles where available. **N.B.** There are only around 34 tokens which have the chainlink price feeds as of making this tutorial.

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
pip install "freqtrade>=2025.11" "web3-ethereum-defi[web3v7,ccxt]>=0.38" plotly
```

```bash
# Copy example config and customize
cp configs/adxmomentum_gmx.example.json configs/adxmomentum_gmx.json
# Edit configs/adxmomentum_gmx.json with your settings
```

**N.B.**: The following step is very essential as we are saving the data as [`parquet`](https://en.wikipedia.org/wiki/Apache_Parquet) file but `freqtrade` expects the data as [`feather`](https://en.wikipedia.org/wiki/Feather_file_format) file format.

```bash
# Export data to freqtrade format
gmx_historical_data export-freqtrade --data-dir ./data --output-dir ./user_data/data/gmx \
    --symbol BTC --symbol ETH --timeframe 1h
```

```bash
# Run backtest with GMX support
./freqtrade-gmx backtesting --config configs/adxmomentum_gmx.json \
    --strategy ADXMomentum --timerange 20210713-
```

You can keep the timerange blank. Then freqtrade will run the backtest on the highest range of data available.

### Example Backtest Results

```
Result for strategy ADXMomentum
                                                BACKTESTING REPORT                                                 
┏━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
┃               ┃        ┃              ┃                 ┃              ┃                 ┃      Win  Draw  Loss ┃
┃          Pair ┃ Trades ┃ Avg Profit % ┃ Tot Profit USDC ┃ Tot Profit % ┃    Avg Duration ┃                 Win% ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
│ BTC/USDC:USDC │    366 │         0.16 │           8.590 │         8.59 │ 2 days, 4:10:00 │      145     0   221 │
│               │        │              │                 │              │                 │                 39.6 │
│ ETH/USDC:USDC │    394 │         0.07 │           4.103 │          4.1 │ 1 day, 22:11:00 │      170     0   224 │
│               │        │              │                 │              │                 │                 43.1 │
│         TOTAL │    760 │         0.11 │          12.693 │        12.69 │ 2 days, 1:04:00 │      315     0   445 │
│               │        │              │                 │              │                 │                 41.4 │
└───────────────┴────────┴──────────────┴─────────────────┴──────────────┴─────────────────┴──────────────────────┘
                                         LEFT OPEN TRADES REPORT                                          
┏━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┓
┃  Pair ┃ Trades ┃ Avg Profit % ┃ Tot Profit USDC ┃ Tot Profit % ┃ Avg Duration ┃  Win  Draw  Loss  Win% ┃
┡━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━┩
│ TOTAL │      0 │          0.0 │           0.000 │          0.0 │         0:00 │    0     0     0     0 │
└───────┴────────┴──────────────┴─────────────────┴──────────────┴──────────────┴────────────────────────┘
                                                 ENTER TAG STATS                                                  
┏━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Enter Tag ┃ Entries ┃ Avg Profit % ┃ Tot Profit USDC ┃ Tot Profit % ┃    Avg Duration ┃  Win  Draw  Loss  Win% ┃
┡━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━┩
│     OTHER │     760 │         0.11 │          12.693 │        12.69 │ 2 days, 1:04:00 │  315     0   445  41.4 │
│     TOTAL │     760 │         0.11 │          12.693 │        12.69 │ 2 days, 1:04:00 │  315     0   445  41.4 │
└───────────┴─────────┴──────────────┴─────────────────┴──────────────┴─────────────────┴────────────────────────┘
                                                EXIT REASON STATS                                                 
┏━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Exit Reason ┃ Exits ┃ Avg Profit % ┃ Tot Profit USDC ┃ Tot Profit % ┃    Avg Duration ┃  Win  Draw  Loss  Win% ┃
┡━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━┩
│         roi │   270 │         5.02 │         203.248 │       203.25 │ 2 days, 2:09:00 │  270     0     0   100 │
│ exit_signal │   490 │        -2.59 │        -190.555 │      -190.56 │ 2 days, 0:28:00 │   45     0   445   9.2 │
│       TOTAL │   760 │         0.11 │          12.693 │        12.69 │ 2 days, 1:04:00 │  315     0   445  41.4 │
└─────────────┴───────┴──────────────┴─────────────────┴──────────────┴─────────────────┴────────────────────────┘
                                                  MIXED TAG STATS                                                  
┏━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━┓
┃           ┃             ┃        ┃              ┃    Tot Profit ┃              ┃               ┃      Win  Draw ┃
┃ Enter Tag ┃ Exit Reason ┃ Trades ┃ Avg Profit % ┃          USDC ┃ Tot Profit % ┃  Avg Duration ┃     Loss  Win% ┃
┡━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━┩
│           │         roi │    270 │         5.02 │       203.248 │       203.25 │       2 days, │      270     0 │
│           │             │        │              │               │              │       2:09:00 │        0   100 │
│           │ exit_signal │    490 │        -2.59 │      -190.555 │      -190.56 │       2 days, │       45     0 │
│           │             │        │              │               │              │       0:28:00 │      445   9.2 │
│     TOTAL │             │    760 │         0.11 │        12.693 │        12.69 │       2 days, │      315     0 │
│           │             │        │              │               │              │       1:04:00 │      445  41.4 │
└───────────┴─────────────┴────────┴──────────────┴───────────────┴──────────────┴───────────────┴────────────────┘
                          SUMMARY METRICS                          
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Metric                        ┃ Value                           ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ Backtesting from              │ 2021-07-14 11:00:00             │
│ Backtesting to                │ 2026-01-27 15:00:00             │
│ Trading Mode                  │ Isolated Futures                │
│ Max open trades               │ 2                               │
│                               │                                 │
│ Total/Daily Avg Trades        │ 760 / 0.46                      │
│ Starting balance              │ 100 USDC                        │
│ Final balance                 │ 112.693 USDC                    │
│ Absolute profit               │ 12.693 USDC                     │
│ Total profit %                │ 12.69%                          │
│ CAGR %                        │ 2.67%                           │
│ Sortino                       │ 0.49                            │
│ Sharpe                        │ 0.24                            │
│ Calmar                        │ 0.85                            │
│ SQN                           │ 0.76                            │
│ Profit factor                 │ 1.06                            │
│ Expectancy (Ratio)            │ 0.02 (0.04)                     │
│ Avg. daily profit             │ 0.008 USDC                      │
│ Avg. stake amount             │ 15 USDC                         │
│ Total trade volume            │ 22839.886 USDC                  │
│                               │                                 │
│ Best Pair                     │ BTC/USDC:USDC 8.59%             │
│ Worst Pair                    │ ETH/USDC:USDC 4.10%             │
│ Best trade                    │ BTC/USDC:USDC 6.57%             │
│ Worst trade                   │ ETH/USDC:USDC -10.70%           │
│ Best day                      │ 3 USDC                          │
│ Worst day                     │ -2.084 USDC                     │
│ Days win/draw/lose            │ 227 / 1105 / 318                │
│ Min/Max/Avg. Duration Winners │ 0d 00:00 / 12d 07:00 / 2d 06:44 │
│ Min/Max/Avg. Duration Losers  │ 0d 01:00 / 8d 19:00 / 1d 21:03  │
│ Max Consecutive Wins / Loss   │ 9 / 13                          │
│ Rejected Entry signals        │ 0                               │
│ Entry/Exit Timeouts           │ 0 / 0                           │
│                               │                                 │
│ Min balance                   │ 94.531 USDC                     │
│ Max balance                   │ 119.956 USDC                    │
│ Max % of account underwater   │ 17.25%                          │
│ Absolute drawdown             │ 19.701 USDC (17.25%)            │
│ Drawdown duration             │ 432 days 09:00:00               │
│ Profit at drawdown start      │ 14.232 USDC                     │
│ Profit at drawdown end        │ -5.469 USDC                     │
│ Drawdown start                │ 2021-10-21 09:00:00             │
│ Drawdown end                  │ 2022-12-27 18:00:00             │
│ Market change                 │ 110.57%                         │
└───────────────────────────────┴─────────────────────────────────┘

Backtested 2021-07-14 11:00:00 -> 2026-01-27 15:00:00 | Max open trades : 2
                                                 STRATEGY SUMMARY                                                  
┏━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃             ┃        ┃              ┃   Tot Profit ┃              ┃              ┃     Win  Draw ┃              ┃
┃    Strategy ┃ Trades ┃ Avg Profit % ┃         USDC ┃ Tot Profit % ┃ Avg Duration ┃    Loss  Win% ┃     Drawdown ┃
┡━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
│ ADXMomentum │    760 │         0.11 │       12.693 │        12.69 │      2 days, │     315     0 │  19.701 USDC │
│             │        │              │              │              │      1:04:00 │     445  41.4 │       17.25% │
└─────────────┴────────┴──────────────┴──────────────┴──────────────┴──────────────┴───────────────┴──────────────┘
```

~4.5 years of backtesting data with 760 trades using ADXMomentum strategy.


Example strategy: `examples/strategies/ADXMomentum.py`

### Plot Results

Generate interactive HTML charts for analysis:

```bash
# Plot profit/loss over time
./freqtrade-gmx plot-profit --config configs/adxmomentum_gmx.example.json --auto-open

# Plot individual pair with indicators
./freqtrade-gmx plot-dataframe --config configs/adxmomentum_gmx.example.json \
    --strategy ADXMomentum -p BTC/USDC:USDC --auto-open
```

Charts are saved to `user_data/plot/`:
- `freqtrade-profit-plot.html` - Cumulative profit chart
- `freqtrade-plot-BTC_USDC_USDC-1h.html` - Price chart with indicators and trade markers

#### Profit Chart

![Profit Chart](docs/images/profit-chart.png)

#### Price Chart with Indicators

![Price Chart](docs/images/price-chart-1.png)
![Price Chart](docs/images/price-chart-2.png)



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

### Poetry Install Failing

If `poetry install` hangs or fails:

```bash
# Clear poetry cache
poetry cache clear pypi --all

# Try with verbose output to see what's stuck
poetry install -vvv

# If dependency resolution is slow, try:
poetry install --no-cache
```

If poetry can't find a compatible Python version:
```bash
# Check Python version (needs 3.11 or 3.12)
python --version

# Use pyenv to install correct version
pyenv install 3.11
pyenv local 3.11
poetry env use python3.11
poetry install
```

> **Note:** Python 3.13 requires additional workarounds. Use Python 3.11 or 3.12 for easiest installation.

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
pip install --no-cache-dir --use-pep517 "hypersync==0.7.17"
poetry install
```

> **Note:** hypersync 0.8.x has a build issue with a missing GitHub dependency. Use 0.7.x versions until this is resolved.

**4. Common errors:**
- `error: linker 'cc' not found` → Install build-essential/gcc
- `capnp/capnp.h: No such file` → Install libcapnp-dev
- `cargo not found` → Source cargo env: `source $HOME/.cargo/env`
- `PyO3's maximum supported version` → Use Python 3.11 or 3.12, or set `PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1`

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
