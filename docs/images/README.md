# Example Plot Images

This directory contains example plots showing the data visualization capabilities.

## Generating Example Plots

To generate your own plots from collected data:

```bash
# Collect data first
poetry run gmx_historical_data --full --symbol ETH --symbol BTC --symbol ARB

# Generate plots
poetry run plot-gmx-data ETH --timeframe 1h
poetry run plot-gmx-data BTC --timeframe 1D
poetry run plot-gmx-data ARB --timeframe 4h
```

The plots will be saved to `./plots/` directory.

## Expected Files

- eth_1h_example.png - ETH 1 hour candles
- btc_1D_example.png - BTC daily candles
- arb_4h_example.png - ARB 4 hour candles

