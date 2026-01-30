# Docker Usage Guide

This guide explains how to use Docker Compose to collect GMX historical data.

## Quick Start

### 1. Setup Environment

Copy the example environment file and configure it:

```bash
cp .env.example .env
nano .env  # Edit with your API keys
```

Required environment variables:
- `JSON_RPC_ARBITRUM` - Your Arbitrum RPC URL (Alchemy, Infura, etc.)
- `HYPERSYNC_API_TOKEN` - Your HyperSync API token(s) (space-separated for rotation)

Optional:
- `SYMBOLS` - Comma-separated symbols for custom collection (default: ETH,BTC)
- `CONCURRENCY` - Parallel processing level (default: 4)

### 2. Build the Docker Image

```bash
docker-compose build
```

## Collection Options

Docker Compose provides 3 collection profiles plus utility services:

### Option 1: Collect Everything (All 118 Markets)

Collects all GMX V2 markets (34 Chainlink + 84 non-Chainlink):

```bash
docker-compose --profile all up gmx-collect-all
```

**Details:**
- Duration: ~6-8 hours (first run)
- Data size: ~50-100 GB
- Requires: HyperSync API token
- Output: `./data/candles/arbitrum/{SYMBOL}/{timeframe}.parquet`
- Logs: `./logs/gmx-all-YYYY-MM-DD-HH-MM-SS.log`

### Option 2: Chainlink Feed Tokens Only (34 Markets)

Collects only markets with public Chainlink price feeds:

```bash
docker-compose --profile chainlink up gmx-collect-chainlink-only
```

**Details:**
- Duration: ~2-3 hours (first run)
- Data size: ~20-30 GB
- Requires: HyperSync API token (optional, for faster Chainlink collection)
- Output: `./data/candles/arbitrum/{SYMBOL}/{timeframe}.parquet`
- Logs: `./logs/gmx-chainlink-YYYY-MM-DD-HH-MM-SS.log`

**Markets included:**
- Major tokens: ETH, BTC, SOL, AVAX, ARB, LINK, UNI, etc.
- Commodities: XAU (Gold), XAG (Silver)
- Total: 34 markets

### Option 3: Custom Symbols (User Configurable)

Collect specific symbols of your choice:

```bash
# Edit .env to set SYMBOLS
export SYMBOLS=ETH,BTC,SUI,ARB

docker-compose --profile custom up gmx-collect-custom
```

**Details:**
- Duration: Depends on number of symbols
- Data size: Depends on number of symbols
- Configurable via `SYMBOLS` environment variable
- Output: `./data/candles/arbitrum/{SYMBOL}/{timeframe}.parquet`
- Logs: `./logs/gmx-custom-YYYY-MM-DD-HH-MM-SS.log`

**Examples:**
```bash
# Just ETH and BTC
SYMBOLS=ETH,BTC docker-compose --profile custom up gmx-collect-custom

# DeFi tokens
SYMBOLS=UNI,AAVE,LINK,CRV docker-compose --profile custom up gmx-collect-custom

# Stablecoins
SYMBOLS=USDC,USDT,DAI docker-compose --profile custom up gmx-collect-custom
```

## Utility Services

### Incremental Update

Update existing data with latest candles:

```bash
docker-compose --profile update up gmx-update
```

**Benefits:**
- Fast (only fetches new data)
- Incremental collection (checks existing coverage)
- Ideal for scheduled updates

**Use case:**
Run periodically (daily/weekly) to keep data fresh:
```bash
# Cron example: Update daily at 2 AM
0 2 * * * cd /path/to/gmx_historical_data && docker-compose --profile update up gmx-update
```

### Data Verification

Verify collected data quality:

```bash
docker-compose --profile verify up gmx-verify
```

**Output:**
- Coverage statistics per symbol
- Data quality metrics
- Gap detection
- Completeness scores

## Advanced Usage

### Running with Custom Concurrency

Higher concurrency = faster but more resource intensive:

```bash
CONCURRENCY=8 docker-compose --profile all up gmx-collect-all
```

**Recommendations:**
- Low-end machine: `CONCURRENCY=2`
- Standard machine: `CONCURRENCY=4` (default)
- High-end machine: `CONCURRENCY=8-12`

### Multiple HyperSync Keys (Rate Limit Protection)

Configure multiple API keys for automatic rotation:

```bash
# In .env
HYPERSYNC_API_TOKEN=key1 key2 key3

docker-compose --profile all up gmx-collect-all
```

The collector will automatically:
- Start with the first key
- Rotate to next key on rate limit
- Skip failed keys
- Report when all keys are exhausted

### Running in Detached Mode

Run in background:

```bash
docker-compose --profile all up -d gmx-collect-all
```

Monitor logs:
```bash
docker-compose logs -f gmx-collect-all
```

Stop container:
```bash
docker-compose down
```

### Custom Log Directory

```bash
# Create custom log directory
mkdir -p /var/log/gmx

# Update docker-compose.yml volume:
volumes:
  - /var/log/gmx:/app/logs
```

## Data Management

### Directory Structure

After collection:
```
./data/
└── candles/
    └── arbitrum/
        ├── ETH/
        │   ├── 1m.parquet
        │   ├── 5m.parquet
        │   ├── 15m.parquet
        │   ├── 1h.parquet
        │   ├── 4h.parquet
        │   └── 1d.parquet
        ├── BTC/
        │   └── ...
        └── ...

./logs/
├── gmx-all-2026-01-30-14-30-15.log
├── gmx-chainlink-2026-01-30-15-00-00.log
└── ...

./.cache/
└── block_timestamps.parquet
```

### Accessing Data

Read candles in Python:
```python
import pandas as pd

# Read ETH 1-hour candles
df = pd.read_parquet('./data/candles/arbitrum/ETH/1h.parquet')

print(df.head())
print(f"Total candles: {len(df):,}")
print(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")
```

### Cleaning Up

Remove all data:
```bash
rm -rf ./data ./logs ./.cache
```

Remove specific symbol:
```bash
rm -rf ./data/candles/arbitrum/ETH
```

## Troubleshooting

### Error: "JSON_RPC_ARBITRUM environment variable is required"

**Solution:** Set your RPC URL in `.env`:
```bash
echo "JSON_RPC_ARBITRUM=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY" >> .env
```

### Error: "All HyperSync API keys have failed"

**Solution:**
1. Check if keys are valid
2. Wait 1 hour for rate limit reset
3. Add more keys to `.env` (space-separated)

### Container Exits Immediately

Check logs:
```bash
docker-compose logs gmx-collect-all
```

Common causes:
- Invalid API keys
- Network connectivity issues
- Insufficient disk space

### Slow Collection Speed

**Solutions:**
1. Increase concurrency: `CONCURRENCY=8`
2. Add more HyperSync keys for rotation
3. Use faster RPC provider
4. Reduce symbol count (custom profile)

### "Cache is stale" Warnings

**Normal behavior** - Cache auto-updates when >10k blocks behind. No action needed.

## Performance Tips

1. **Use SSD storage** - Significantly faster parquet writes
2. **Allocate enough RAM** - 8GB+ recommended for full collection
3. **Fast RPC provider** - Alchemy/Infura recommended
4. **Multiple HyperSync keys** - Prevents rate limiting
5. **Incremental updates** - Much faster than full re-collection

## Cost Optimization

### RPC Calls

Block timestamp cache minimizes RPC usage:
- First run: ~20,000 RPC calls (builds cache)
- Subsequent runs: <100 RPC calls (uses cache)

**Savings:** 99.5% reduction in RPC usage

### HyperSync API

Incremental collection reduces HyperSync usage:
- First run: Full historical scan
- Subsequent runs: Only new data

**Savings:** 50-98% reduction in HyperSync bandwidth

### Example: Daily Updates

For 118 markets with daily updates:
```
Full collection: 8 hours + 2.5B queries
Incremental:     10 minutes + 50M queries (98% savings)
```

## Security Best Practices

1. **Never commit `.env` to git**
   ```bash
   echo ".env" >> .gitignore
   ```

2. **Use read-only API keys** when possible

3. **Restrict volume permissions**
   ```bash
   chmod 700 ./data ./logs
   ```

4. **Rotate API keys** regularly

## Integration Examples

### With Cron (Automated Daily Updates)

```bash
# Edit crontab
crontab -e

# Add daily update at 2 AM
0 2 * * * cd /path/to/gmx_historical_data && docker-compose --profile update up gmx-update >> /var/log/gmx-cron.log 2>&1
```

### With CI/CD

```yaml
# GitHub Actions example
name: Collect GMX Data
on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM

jobs:
  collect:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Collect data
        env:
          JSON_RPC_ARBITRUM: ${{ secrets.ARBITRUM_RPC }}
          HYPERSYNC_API_TOKEN: ${{ secrets.HYPERSYNC_TOKEN }}
        run: |
          docker-compose --profile update up gmx-update
      - name: Upload data
        uses: actions/upload-artifact@v2
        with:
          name: gmx-data
          path: ./data
```

### With Kubernetes

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: gmx-data-update
spec:
  schedule: "0 2 * * *"
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: gmx-collector
            image: gmx-historical-data:latest
            env:
            - name: JSON_RPC_ARBITRUM
              valueFrom:
                secretKeyRef:
                  name: gmx-secrets
                  key: rpc-url
            - name: HYPERSYNC_API_TOKEN
              valueFrom:
                secretKeyRef:
                  name: gmx-secrets
                  key: hypersync-token
            args: ["collect", "--update", "--output-dir", "/data"]
            volumeMounts:
            - name: data-volume
              mountPath: /data
          volumes:
          - name: data-volume
            persistentVolumeClaim:
              claimName: gmx-data-pvc
          restartPolicy: OnFailure
```

## FAQ

**Q: Can I run multiple profiles at once?**

A: No, run them sequentially to avoid conflicts:
```bash
docker-compose --profile chainlink up && docker-compose --profile custom up
```

**Q: How much disk space do I need?**

A: Approximately:
- All markets (118): 50-100 GB
- Chainlink only (34): 20-30 GB
- Per symbol: 500MB-1GB

**Q: Can I use Docker on Windows/Mac?**

A: Yes, install Docker Desktop and use the same commands.

**Q: How do I update to latest version?**

A:
```bash
git pull
docker-compose build --no-cache
```

**Q: Can I export data to CSV?**

A: Yes, using Python:
```python
import pandas as pd
df = pd.read_parquet('./data/candles/arbitrum/ETH/1h.parquet')
df.to_csv('eth_1h.csv', index=False)
```

## Support

- **Documentation**: See `docs/` directory
- **Issues**: https://github.com/tradingstrategy-ai/gmx-historical-data/issues
- **Incremental Collection Guide**: `docs/incremental-collection.md`
