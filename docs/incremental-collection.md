# Incremental Oracle Events Collection

## Overview

The GMX Historical Data collector now supports **incremental collection** for non-Chainlink tokens. Instead of fetching all oracle events from genesis every time, the system:

1. **Analyzes existing data coverage** - Checks what data already exists per symbol
2. **Calculates missing ranges** - Determines exactly which blocks need oracle events
3. **Fetches only gaps** - Collects only the missing data (massive bandwidth savings)
4. **Handles rate limits** - Rotates between multiple HyperSync API keys automatically

This feature dramatically reduces collection time and bandwidth for daily/hourly updates, making it practical to maintain fresh data for all 84 non-Chainlink markets.

## How It Works

### 1. Block-Timestamp Cache

**Purpose**: Convert timestamps to block numbers efficiently without RPC calls.

**Location**: `./data/.cache/block_timestamps.parquet`

**Building**: Automatic on first use (samples every 1000 blocks)

**Updating**: Auto-updates when cache is >10k blocks behind

The cache samples Arbitrum blocks at regular intervals (every 1000 blocks by default) and stores them in a Parquet file. When converting timestamps to blocks, it uses linear interpolation between samples for fast, accurate conversions without hitting the RPC endpoint.

**Example**:
```python
from gmx_historical_data.block_timestamp_cache import BlockTimestampCache
from web3 import Web3

# Initialize with Web3 instance
web3 = Web3(Web3.HTTPProvider(os.getenv("JSON_RPC_ARBITRUM")))
cache = BlockTimestampCache(Path("./data/.cache/block_timestamps.parquet"), web3)

# Convert timestamp to block
block = cache.get_block_for_timestamp(1700000000)
print(f"Timestamp 1700000000 → Block {block}")

# Convert block to timestamp
timestamp = cache.get_timestamp_for_block(200000000)
print(f"Block 200000000 → Timestamp {timestamp}")
```

**Performance**:
- **Initial build**: ~5-10 minutes (one-time, samples ~30k blocks)
- **Lookups**: <1ms (uses pandas interpolation)
- **Updates**: Automatic when >10k blocks behind
- **Disk usage**: ~1-2 MB

### 2. Data Coverage Analysis

**Purpose**: Determine what oracle events are missing per symbol.

The coverage analyzer scans all existing parquet files for a symbol across all timeframes (1m, 5m, 15m, 1h, 4h, 1d) and determines:
- Whether any data exists
- The earliest timestamp across ALL timeframes
- Which timeframes have data
- How many candles exist per timeframe

This information is then used to calculate exactly which block range needs oracle events.

**Process**:
1. Read all timeframe parquet files for symbol (1m, 5m, 15m, 1h, 4h, 1d)
2. Find earliest timestamp across ALL timeframes
3. Convert earliest timestamp to block number
4. Calculate gap: `[genesis_block, earliest_data_block + safety_margin]`

**Safety Margin**: A 1000-block overlap (default) ensures no data is missed between collection runs due to timestamp rounding or block reorganizations.

**Example**:
```python
from gmx_historical_data.data_coverage_analyzer import DataCoverageAnalyzer
from pathlib import Path

analyzer = DataCoverageAnalyzer(Path("./data"))
coverage = analyzer.analyze_symbol_coverage("SUI")

if coverage.has_data:
    print(f"Symbol: {coverage.symbol}")
    print(f"Earliest data: {coverage.earliest_timestamp}")
    print(f"Latest data: {coverage.latest_timestamp}")
    print(f"Timeframes with data: {list(coverage.timeframe_coverage.keys())}")

    for tf_name, tf_cov in coverage.timeframe_coverage.items():
        print(f"  {tf_name}: {tf_cov.candle_count} candles")
else:
    print(f"No data found for {coverage.symbol} - will fetch from genesis")
```

**Output Example**:
```
Symbol: SUI
Earliest data: 1727654400 (2024-10-01 00:00:00)
Latest data: 1738281600 (2025-01-30 12:00:00)
Timeframes with data: ['1m', '5m', '15m', '1h', '4h', '1d']
  1m: 50,000 candles
  5m: 10,000 candles
  15m: 3,333 candles
  1h: 2,500 candles
  4h: 625 candles
  1d: 104 candles
```

### 3. HyperSync API Key Rotation

**Purpose**: Handle rate limits by rotating between multiple API keys.

HyperSync enforces rate limits on API requests. When collecting data for many symbols or making large historical queries, you may hit these limits. The key rotator automatically switches to the next available key when rate limits are encountered.

**Configuration**: Space-separated keys in `HYPERSYNC_API_TOKEN`

**Example**:
```bash
# Multiple keys for automatic rotation
export HYPERSYNC_API_TOKEN="key1 key2 key3"

# Single key (no rotation)
export HYPERSYNC_API_TOKEN="single_key"
```

**Behavior**:
- Starts with first key
- On rate limit error (HTTP 429): rotates to next key automatically
- Marks failed keys to avoid retry loops
- Raises error if all keys fail
- Logs rotation events for debugging

**Rotation Example**:
```
[INFO] Initialized HyperSyncKeyRotator with 3 API key(s)
[INFO] Using API key #1/3
[WARN] Rate limit detected - rotating to next API key
[INFO] Rotated to API key #2/3
[SUCCESS] Collection succeeded after key rotation
```

### 4. Enhanced Error Handling

All HyperSync and RPC operations include comprehensive error handling:

**Progressive Retry Backoff**:
1. First retry: 2 seconds
2. Second retry: 5 seconds
3. Third retry: 10 seconds
4. Fourth retry: 30 seconds
5. Fifth retry: 60 seconds

**Full Error Logging**:
- Console output with colored formatting
- Complete tracebacks for debugging
- Log file support for background jobs
- Rate limit detection and key rotation

## Usage

### Basic Incremental Collection

```bash
# Set up environment
export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
export HYPERSYNC_API_TOKEN="your_hypersync_token"

# First run: Full collection from genesis
gmx_historical_data collect --symbol SUI --output-dir ./data

# Output:
# → Building block-timestamp cache...
# ✓ Cache built with 30,000 samples
# → No existing data found for SUI
# → Fetching oracle events: blocks 180,000,000 to 210,000,000
# ✓ Collected 50,000 oracle events
# ✓ Generated candles for 6 timeframes

# Second run: Only fetches new data since last run
gmx_historical_data collect --symbol SUI --output-dir ./data

# Output:
# ✓ Using cached block-timestamp data
# → Analyzing existing coverage...
#   ✓ Found data covering 6 timeframes
#   ✓ Earliest: 2024-10-01, Latest: 2025-01-30
# → Fetching oracle events: blocks 195,000,000 to 210,500,000
# ✓ Collected 15,000 new oracle events
# ✓ Merged with existing data (no duplicates)
```

### Multiple HyperSync Keys (Rate Limit Protection)

Get free HyperSync API keys at https://envio.dev

```bash
# Configure multiple keys
export HYPERSYNC_API_TOKEN="key1 key2 key3"

# Automatically rotates keys on rate limits
gmx_historical_data collect --default --output-dir ./data --concurrency 10

# Monitor rotation in logs:
# [INFO] Initialized HyperSyncKeyRotator with 3 API key(s)
# [INFO] Processing symbol 1/84: SUI
# ... (collection proceeds) ...
# [WARN] Rate limit detected on key #1 - rotating
# [INFO] Rotated to API key #2/3
# [SUCCESS] Collection completed successfully
```

### Quiet Mode with Logging

Perfect for cron jobs and background collection:

```bash
# Background collection with full logging to file
gmx_historical_data collect --default --quiet --log-file ./collection.log

# Check progress in another terminal
tail -f ./collection.log

# Or search for errors
grep ERROR ./collection.log
grep "Rate limit" ./collection.log
```

**Log File Features**:
- Automatic timestamping
- Full traceback for errors
- Progress indicators
- Rate limit events
- Collection statistics

### Cron Job Setup

```bash
# Edit crontab
crontab -e

# Add hourly updates (at minute 5)
5 * * * * cd /path/to/gmx_historical_data && source .venv/bin/activate && gmx_historical_data collect --update --output-dir ./data --concurrency 10 --quiet --log-file ./logs/hourly-update.log 2>&1

# Add daily full collection (at 2 AM)
0 2 * * * cd /path/to/gmx_historical_data && source .venv/bin/activate && gmx_historical_data collect --default --output-dir ./data --concurrency 5 --quiet --log-file ./logs/daily-collection.log 2>&1

# Weekly verification (Sunday at 3 AM)
0 3 * * 0 cd /path/to/gmx_historical_data && source .venv/bin/activate && gmx_historical_data verify --output-dir ./data --log-file ./logs/weekly-verify.log 2>&1
```

### Selective Symbol Collection

```bash
# Update specific symbols incrementally
gmx_historical_data collect --symbol SUI,DOGE,PEPE --output-dir ./data

# Output shows per-symbol coverage:
# [1/3] SUI: Found existing data (2024-10-01 to 2025-01-30)
#       → Fetching gap: 15,000 blocks
# [2/3] DOGE: No existing data
#       → Fetching from genesis: 30,000,000 blocks
# [3/3] PEPE: Data is current - skipping
```

## Performance Benefits

### Before (Always Full Collection)

Without incremental collection, every run fetches all oracle events from genesis:

```
Symbol: SUI
Fetching oracle events: blocks 180,000,000 to 210,000,000
  → 30M blocks scanned
  → ~50,000 oracle events
  → ~10 minutes collection time
  → Large HyperSync bandwidth usage
  → No analysis of existing data
```

### After (Incremental Collection)

With incremental collection, only missing data is fetched:

```
Symbol: SUI
Analyzing existing coverage...
  ✓ Found existing data covering 6 timeframes
    1m: 50,000 candles (2024-10-01 to 2025-01-30)
    5m: 10,000 candles (2024-10-01 to 2025-01-30)
    15m: 3,333 candles (2024-10-01 to 2025-01-30)
    1h: 2,500 candles (2024-10-01 to 2025-01-30)
    4h: 625 candles (2024-10-01 to 2025-01-30)
    1d: 104 candles (2024-10-01 to 2025-01-30)

Earliest block: 195,000,000 (2024-10-01)
Fetching oracle events: blocks 180,000,000 to 195,001,000
  → 15M blocks scanned (50% reduction)
  → ~25,000 new oracle events
  → ~5 minutes (50% faster)
  → Half the bandwidth
```

### Multi-Symbol Collection

For all 84 non-Chainlink symbols:

**Before**:
- 84 symbols × 30M blocks each = 2.52B block queries
- Total time: ~14 hours (10 min per symbol)
- No consideration of existing data

**After (Daily Updates)**:
- 84 symbols × 0-1M blocks each = ~84M block queries (97% reduction!)
- Total time: ~1-2 hours (1-2 min per symbol)
- Skips symbols that are current

**After (Hourly Updates)**:
- 84 symbols × 0-100k blocks each = ~8.4M block queries (99.7% reduction!)
- Total time: ~10-20 minutes
- Most symbols skipped (already current)

### Bandwidth Savings

**Daily Update Example** (24 hours of new data):
- **Before**: Refetch all 30M blocks = ~500 MB HyperSync response
- **After**: Fetch 500k blocks = ~8 MB HyperSync response
- **Savings**: 98.4% bandwidth reduction

**Weekly Update Example** (7 days of new data):
- **Before**: Refetch all 30M blocks = ~500 MB HyperSync response
- **After**: Fetch 3.5M blocks = ~60 MB HyperSync response
- **Savings**: 88% bandwidth reduction

## Error Handling

### Progressive Rate Limiting

The collector implements exponential backoff for transient errors:

```python
# Automatic retry sequence for failed requests
retry_delays = [2, 5, 10, 30, 60]  # seconds

# On failure:
# Attempt 1: Immediate try
# Attempt 2: Wait 2s, retry
# Attempt 3: Wait 5s, retry
# Attempt 4: Wait 10s, retry
# Attempt 5: Wait 30s, retry
# Attempt 6: Wait 60s, final retry
# If still fails: Raise error
```

**Example Output**:
```
[red]Error in HyperSync get_height (attempt 1/5):[/red] failed to get arrow data from server
Full trace:
Traceback (most recent call last):
  File "oracle_price_collector.py", line 563, in collect_oracle_events
    end_block = await retry_with_backoff(...)
  File "oracle_price_collector.py", line 95, in retry_with_backoff
    return await func(*args, **kwargs)
HyperSyncError: Connection timeout

[yellow]Retrying in 2s...[/yellow]
[green]✓[/green] Retry succeeded
```

### Full Error Logging

All errors include complete context for debugging:

**Console Output** (colored, human-readable):
```
[ERROR] Failed to collect oracle events for SUI
  Cause: HyperSync rate limit exceeded
  Key: #1/3 (rotating to #2)
  Block range: 180000000-210000000
```

**Log File Output** (complete traceback):
```
2025-01-30 15:30:45 [ERROR] Failed to collect oracle events for SUI
Traceback (most recent call last):
  File "/path/to/oracle_price_collector.py", line 563
    events = await hypersync_client.get_events(...)
  File "/path/to/hypersync_collector.py", line 123
    response = await self.client.query(query)
hypersync.HyperSyncError: Rate limit exceeded (429)

Attempting key rotation...
2025-01-30 15:30:45 [INFO] Rotated to API key #2/3
```

### Key Rotation on Rate Limits

Automatic handling of rate limit errors:

```
[INFO] Processing symbol SUI (1/84)
[INFO] Using HyperSync API key #1/3

... (collection in progress) ...

[red]Error in oracle events collection:[/red] rate limit exceeded (HTTP 429)
[yellow]Rate limit detected - rotating to next API key[/yellow]
  Current key: #1/3 (marked as failed)
  Rotated to API key #2/3
  Retrying request...
[green]✓[/green] Collection succeeded after key rotation

[INFO] Processing symbol DOGE (2/84)
[INFO] Using HyperSync API key #2/3 (continuing from previous rotation)
```

**All Keys Exhausted**:
```
[red]Error: All HyperSync API keys have failed[/red]
  Keys tried: 3/3
  Last error: Rate limit exceeded (HTTP 429)

[yellow]Recommendation:[/yellow]
  1. Wait ~1 hour for rate limits to reset
  2. Add more API keys: export HYPERSYNC_API_TOKEN="key1 key2 key3 key4"
  3. Reduce concurrency: --concurrency 2
```

## Troubleshooting

### Cache Not Building

**Problem**: "Building block-timestamp cache" progress bar never completes or takes extremely long.

**Possible Causes**:
1. RPC endpoint is down or slow
2. Rate limiting on RPC endpoint
3. Network connectivity issues

**Solution**:

```bash
# 1. Test RPC connection
python -c "from web3 import Web3; w3 = Web3(Web3.HTTPProvider('$JSON_RPC_ARBITRUM')); print('Connected:', w3.is_connected()); print('Block:', w3.eth.block_number)"

# Expected output:
# Connected: True
# Block: 210000000

# 2. Check RPC rate limits
curl -X POST $JSON_RPC_ARBITRUM \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'

# 3. If slow, try different RPC provider
export JSON_RPC_ARBITRUM="https://arb1.arbitrum.io/rpc"  # Public RPC (slower)
# or
export JSON_RPC_ARBITRUM="https://arbitrum-mainnet.infura.io/v3/YOUR_KEY"  # Infura
```

**Rebuild Cache** (if corrupted):
```bash
# Delete existing cache
rm -rf ./data/.cache/block_timestamps.parquet

# Next run will rebuild automatically
gmx_historical_data collect --symbol ETH --output-dir ./data
```

### All Keys Rate Limited

**Problem**: "All HyperSync API keys have failed" error.

**Explanation**: HyperSync enforces rate limits per API key. If all keys are exhausted, you must wait or add more keys.

**Solution 1 - Wait for Reset** (~1 hour):
```bash
# Rate limits typically reset after 1 hour
# Check status by trying a single symbol:
gmx_historical_data collect --symbol ETH --output-dir ./data

# If successful, resume full collection
gmx_historical_data collect --default --output-dir ./data
```

**Solution 2 - Add More Keys**:
```bash
# Get free keys at https://envio.dev
# Add to environment (space-separated)
export HYPERSYNC_API_TOKEN="key1 key2 key3 key4 key5"

# Verify key count
gmx_historical_data collect --symbol ETH --output-dir ./data
# Should show: "Initialized HyperSyncKeyRotator with 5 API key(s)"
```

**Solution 3 - Reduce Concurrency**:
```bash
# Lower concurrency = fewer simultaneous requests
gmx_historical_data collect --default --output-dir ./data --concurrency 2

# Or process symbols one at a time
gmx_historical_data collect --default --output-dir ./data --concurrency 1
```

### Stale Cache Warning

**Problem**: Log shows "Cache is 50,000 blocks behind current block".

**Explanation**: The cache auto-updates if >10k blocks behind. This is informational, not an error.

**Auto-Update**:
```
[WARN] Cache is 50,000 blocks behind current block
[INFO] Auto-updating cache with new samples...
✓ Cache updated with 50 new samples
```

**Manual Rebuild** (optional):
```bash
# Force complete rebuild
rm -rf ./data/.cache/block_timestamps.parquet

# Next run rebuilds from scratch
gmx_historical_data collect --symbol ETH --output-dir ./data
```

### Missing Data After Collection

**Problem**: Some timeframes have gaps despite running incremental collection.

**Possible Causes**:
1. GMX API has gaps (not incrementally collectible)
2. Oracle events missing for that time period
3. Candle resampling failed

**Diagnosis**:
```bash
# Check data quality
gmx_historical_data verify --output-dir ./data --symbol SUI

# Output shows gaps:
# [WARN] SUI - 1m: Gap from 2024-12-15 to 2024-12-16 (1440 missing candles)
```

**Solution**:
```bash
# For GMX API gaps: Re-run in --full mode
gmx_historical_data collect --full --symbol SUI --output-dir ./data

# For oracle gaps: Check if oracle was publishing during that period
gmx_historical_data debug-oracle --symbol SUI --start-date 2024-12-15 --end-date 2024-12-16
```

### Incorrect Block Range Calculated

**Problem**: Incremental collection fetches wrong block range.

**Possible Causes**:
1. Cache interpolation error (blocks/timestamps misaligned)
2. Timezone issues in timestamp conversion
3. Corrupted parquet files

**Solution**:
```bash
# 1. Rebuild cache
rm -rf ./data/.cache/block_timestamps.parquet

# 2. Verify timestamps in parquet files
python << EOF
import pandas as pd
df = pd.read_parquet("./data/candles/arbitrum/SUI/1h.parquet")
print("Earliest:", pd.to_datetime(df['timestamp'].min(), unit='s'))
print("Latest:", pd.to_datetime(df['timestamp'].max(), unit='s'))
print("Timezone:", df['timestamp'].dtype)
EOF

# 3. If timestamps look wrong, re-collect
gmx_historical_data collect --full --symbol SUI --output-dir ./data
```

## Advanced Configuration

### Custom Safety Margin

Default: 1000 blocks (~4 minutes overlap to prevent gaps)

**Why adjust?**
- **Increase** for chains with frequent reorgs
- **Decrease** to minimize duplicate fetching

**Modify in code** (requires fork/edit):
```python
# In oracle_price_collector.py
start_block, end_block = coverage_analyzer.get_missing_block_range(
    coverage,
    block_cache,
    genesis_block=default_start,
    safety_margin=2000,  # Increase to 2000 blocks (~8 min overlap)
)
```

**Effect**:
- 1000 blocks: ~4 min overlap (default, recommended)
- 2000 blocks: ~8 min overlap (safer for fast updates)
- 500 blocks: ~2 min overlap (minimize duplicates, riskier)

### Block Sample Interval

Default: 1000 blocks (~4 minutes per sample)

**Why adjust?**
- **Decrease** for higher timestamp→block accuracy
- **Increase** for smaller cache file size

**Modify in config.py**:
```python
# src/gmx_historical_data/config.py
BLOCK_SAMPLE_INTERVAL = 500  # More samples = better accuracy, larger file
```

**Effect on cache size**:
- 1000 blocks: ~30k samples, ~1.5 MB cache (default)
- 500 blocks: ~60k samples, ~3 MB cache (higher accuracy)
- 2000 blocks: ~15k samples, ~750 KB cache (smaller file)

**Effect on accuracy**:
- 500 blocks: ±2 blocks error (±8 seconds)
- 1000 blocks: ±4 blocks error (±16 seconds) [default]
- 2000 blocks: ±8 blocks error (±32 seconds)

### Cache Stale Threshold

Default: 10,000 blocks (~40 minutes behind = auto-update)

**Modify in config.py**:
```python
# src/gmx_historical_data/config.py
CACHE_STALE_THRESHOLD = 5000  # Update more frequently (20 min threshold)
```

**Effect**:
- 10,000 blocks: Update cache if >40 min behind (default)
- 5,000 blocks: Update cache if >20 min behind (fresher cache)
- 20,000 blocks: Update cache if >80 min behind (less frequent updates)

### Concurrency Tuning

The `--concurrency` flag controls both symbol parallelism and RPC batch workers.

**Guidelines**:
- **Free RPC tier**: Use `--concurrency 1-2`
- **Paid RPC tier**: Use `--concurrency 4-8`
- **Unlimited RPC**: Use `--concurrency 10+`

**Example**:
```bash
# Conservative (1 symbol at a time, 1 RPC worker)
gmx_historical_data collect --default --output-dir ./data --concurrency 1

# Balanced (4 symbols, 4 RPC workers)
gmx_historical_data collect --default --output-dir ./data --concurrency 4

# Aggressive (10 symbols, 8 RPC workers - capped)
gmx_historical_data collect --default --output-dir ./data --concurrency 10
```

## FAQ

**Q: Does this work for Chainlink tokens too?**

A: No, Chainlink tokens use a different collection method (Multicall3 with round-based fetching). This optimization is only for non-Chainlink tokens (84 markets). Chainlink tokens already have optimizations (round range discovery, checkpoint caching).

**Q: What happens if I delete parquet files?**

A: The coverage analyzer detects missing data and automatically fetches from genesis (full collection). No manual intervention needed - it gracefully falls back to full mode.

**Q: Can I use this with `--update` mode?**

A: Yes! Incremental collection works with both `--full` and `--update` modes. The coverage analyzer runs regardless of the mode flag.

**Q: How much disk space does the cache use?**

A: ~1-2 MB for full Arbitrum history (very small). The cache is highly efficient due to sparse sampling (every 1000 blocks).

**Q: Can I share the cache between machines?**

A: Yes, the cache is portable. Copy `./data/.cache/block_timestamps.parquet` to other machines running the same codebase. The cache is chain-specific (Arbitrum), so it works for all GMX data collection.

**Q: What if the cache becomes corrupted?**

A: Delete it (`rm ./data/.cache/block_timestamps.parquet`) and it will automatically rebuild on next run. Rebuilding takes ~5-10 minutes.

**Q: Does incremental collection handle chain reorganizations?**

A: Yes, the safety margin (default 1000 blocks) provides overlap to handle small reorgs. For deep reorgs (>1000 blocks), you may need to re-run with `--full` mode.

**Q: Can I disable incremental collection?**

A: Not via CLI flag, but you can delete existing parquet files to force full collection, or modify the code to skip coverage analysis.

**Q: How do I know if incremental collection is working?**

A: Check the logs for messages like:
```
✓ Found existing data covering 6 timeframes
→ Fetching oracle events: blocks X to Y (Z blocks - reduced from full range)
```

If you see "No existing data found", it's falling back to full collection (expected on first run).

**Q: What happens if HyperSync changes their API?**

A: The collector uses the official `hypersync` Python client, which handles API versioning. If breaking changes occur, update the `hypersync` package version in `pyproject.toml`.

**Q: Can I use incremental collection for other blockchains?**

A: The current implementation is Arbitrum-specific (GMX V2 is on Arbitrum). To support other chains, you'd need to:
1. Update `GMX_V2_GENESIS_BLOCK` in config.py
2. Ensure HyperSync supports the target chain
3. Adjust block timing constants (Arbitrum: ~4 blocks/second)

**Q: How often should I run incremental updates?**

A: Depends on your needs:
- **Hourly**: Fresh data for live trading, minimal collection time
- **Daily**: Good balance for backtesting, ~1-2 hour collection
- **Weekly**: Acceptable for long-term analysis, ~4-6 hour collection

**Q: Does this reduce costs for paid RPC/HyperSync plans?**

A: Yes! Incremental collection dramatically reduces:
- HyperSync bandwidth usage (50-99% reduction)
- RPC calls for block lookups (cached)
- Compute time (faster collection = lower cloud costs)

For paid tiers, this can reduce monthly costs by 90%+ for daily updates.
