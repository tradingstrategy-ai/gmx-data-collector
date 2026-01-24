# GMX-First Data Collection Architecture

## Overview

This document describes the GMX-first hybrid data collection approach that maximizes token coverage and data availability.

## Design Principles

1. **GMX as Primary Source**: Start with GMX API to get recent high-quality OHLCV data
2. **Chainlink as Backfill**: Use Chainlink oracles only to fill historical gaps
3. **Maximum Coverage**: Collect data for ALL ~97 GMX-supported tokens
4. **Smart Symbol Mapping**: Automatic fuzzy matching between GMX and Chainlink symbols
5. **Resilient Collection**: Continue on errors, report summary at end

## Data Flow

```
1. GMX Token Discovery
   ↓
2. Fetch GMX OHLCV (all timeframes)
   ↓
3. Find Matching Chainlink Feed
   ↓
4. Calculate Data Gap
   ↓
5. Backfill with Chainlink (if needed)
   ↓
6. Combine & Save
```

## Components

### GMXTokenDiscovery
- Fetches all tokens from GMX API
- Returns ~97 tokens with metadata
- Supports Arbitrum and Avalanche

### Symbol Mapper
- Hybrid approach:
  1. Check manual overrides (WBTC.b → BTC)
  2. Try direct match (ETH → ETH)
  3. Try fuzzy match (strip .e, .b, W prefix)
- Returns Chainlink feed address or None

### DataGapAnalyzer
- Calculates backfill range
- Inputs: GMX DataFrame, Chainlink availability
- Outputs: (start_block, end_timestamp)

### DataCollector (Refactored)
- GMX-first collection logic
- Automatic gap detection
- Seamless data combination
- Error handling with continue-on-failure

## Token Coverage

| Category | Count | Data Source | Historical Depth |
|----------|-------|-------------|------------------|
| Tokens with Chainlink | ~50 | GMX + Chainlink | 2021+ to present |
| Tokens GMX-only | ~47 | GMX only | Last ~6 months |
| **Total** | **~97** | **Hybrid** | **Maximum available** |

## Example Flow: ETH

1. Discover ETH from GMX API
2. Fetch GMX OHLCV for 1m, 5m, 15m, 1h, 4h, 1D
   - Coverage: 2024-07-01 to 2026-01-24 (latest)
3. Find Chainlink feed: ETH → 0x639Fe...a612
4. Calculate gap: Need data from 2021-07-13 to 2024-07-01
5. Query Chainlink events via HyperSync
6. Resample Chainlink events to OHLCV
7. Combine: Chainlink (2021-2024) + GMX (2024-present)
8. Save: ~4.5 years of complete OHLCV data

## Example Flow: Newer Token (GMX-only)

1. Discover NEWTOKEN from GMX API
2. Fetch GMX OHLCV for all timeframes
   - Coverage: 2024-07-01 to 2026-01-24
3. Try to find Chainlink feed: None found
4. Skip Chainlink backfill
5. Save: ~6 months of GMX OHLCV data

## Performance

- **GMX API**: Fast, ~1-2 seconds per token per timeframe
- **Chainlink via HyperSync**: 100-2000x faster than RPC
- **Total collection time**: Minutes for all ~97 tokens (vs hours/days with RPC)

## Error Handling

- Continue on failure (resilient to individual token failures)
- Log errors with clear messages
- Report summary at end:
  - ✓ Successful: 85/97
  - ✗ Failed: 12/97
  - Failed tokens: [list]

## Future Enhancements

- [ ] Support Avalanche chain
- [ ] Parallel token collection
- [ ] Real-time streaming updates
- [ ] Data quality validation
