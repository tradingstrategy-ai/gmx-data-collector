# GMX Event-Based Historical Data Collection Design

**Date:** 2026-01-25
**Goal:** Build complete historical price data for all GMX tokens by indexing on-chain position events
**Approach:** Index PositionIncrease/Decrease events using HyperSync + eth_defi parser

---

## Overview

This design replaces Chainlink oracle-based collection (limited to 46 tokens) with GMX event indexing (covers all 118 tokens including synthetics).

**Why Event-Based Collection?**

- **Universal coverage:** Works for ALL GMX tokens (synthetic and non-synthetic)
- **Authentic prices:** Uses actual execution prices from real trades
- **Complete history:** GMX V2 deployed Aug 2023 on Arbitrum
- **No API credentials needed:** All data is on-chain
- **Leverages existing infrastructure:** Reuses HyperSync collector

**What We're Indexing:**

- `PositionIncrease` events: Long/short position opens or increases
- `PositionDecrease` events: Long/short position closes or decreases
- Each event contains: `executionPrice`, `sizeDeltaUsd`, `timestamp`, `market`

---

## Architecture

### High-Level Flow

```
[HyperSync Event Query]
    → [GMX EventEmitter Contract]
    → [Filter: PositionIncrease/Decrease]
    → [eth_defi Event Parser]
    → [Extract: executionPrice, timestamp, market, size]
    → [Aggregate by market + timeframe]
    → [Resample to OHLCV]
    → [Store Parquet files]
```

### Key Components

**1. Event Collector (HyperSync-based)**
- Query GMX EventEmitter contract: `0xC8ee91A54287DB53897056e12D9819156D3822Fb`
- Filter by event signatures: PositionIncrease, PositionDecrease
- Time range: GMX V2 genesis (~Aug 2023) → Latest
- Reuse existing `HyperSyncCollector` infrastructure

**2. Event Parser (eth_defi integration)**
- Use `eth_defi.gmx.events.decode_gmx_event()`
- Extract execution prices (30 decimal precision)
- Map market addresses to token symbols via `eth_defi.gmx.core.markets.Markets`

**3. Price Aggregator**
- Group events by market + time bucket
- Calculate OHLCV from execution prices
- Handle multiple trades per second

**4. Storage Layer**
- Raw events: Parquet files partitioned by market
- OHLCV candles: Same structure as existing collector
- Reuse existing Parquet schemas

---

## Data Model

### Position Event Structure

```python
@dataclass
class GMXPositionEvent:
    """Parsed GMX position event for price reconstruction."""

    # Event metadata
    block_number: int
    block_timestamp: int
    transaction_hash: str
    log_index: int

    # Event identification
    event_name: str  # "PositionIncrease" or "PositionDecrease"

    # Market & position data
    market: str  # Market contract address
    account: str  # Trader address
    is_long: bool  # Long or short position

    # Price data (30 decimal precision)
    execution_price: int  # Actual trade price
    size_delta_usd: int  # Trade size in USD
    size_delta_in_tokens: int  # Trade size in tokens
    price_impact_usd: int  # Price impact

    # Additional context
    position_key: str  # Unique position identifier
    collateral_token: str  # Collateral token address
```

### Parquet Schemas

**Raw Events Schema:**
```python
RAW_EVENTS_SCHEMA = pa.schema([
    ("block_number", pa.uint64()),
    ("block_timestamp", pa.uint64()),
    ("transaction_hash", pa.string()),
    ("log_index", pa.uint32()),
    ("event_name", pa.string()),
    ("market", pa.string()),
    ("account", pa.string()),
    ("is_long", pa.bool_()),
    ("execution_price", pa.int64()),  # 30 decimals
    ("size_delta_usd", pa.int64()),   # 30 decimals
    ("size_delta_in_tokens", pa.int64()),
    ("price_impact_usd", pa.int64()),
    ("position_key", pa.string()),
    ("collateral_token", pa.string()),
    ("symbol", pa.string()),  # Mapped from market address
])
```

**OHLCV Schema (reuse existing):**
```python
OHLCV_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("s", tz="UTC")),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),  # Optional: sum of size_delta_usd
    ("symbol", pa.string()),
])
```

---

## Implementation Details

### 1. Event Collection (HyperSync)

**GMX Contract Addresses (Arbitrum):**
```python
# EventEmitter - single contract emits ALL GMX events
EVENT_EMITTER_ADDRESS = "0xC8ee91A54287DB53897056e12D9819156D3822Fb"

# Event name hashes for filtering (keccak256 of event name string)
POSITION_INCREASE_HASH = keccak(text="PositionIncrease").hex()
POSITION_DECREASE_HASH = keccak(text="PositionDecrease").hex()
```

**HyperSync Query:**
```python
from hypersync import Query, LogSelection, FieldSelection

def build_gmx_events_query(
    start_block: int = 0,
    end_block: int | None = None,
) -> Query:
    """Build HyperSync query for GMX position events."""

    # GMX uses EventLog/EventLog1/EventLog2 with event name hash in topic[1]
    log_selection = LogSelection(
        address=[EVENT_EMITTER_ADDRESS.lower()],
        topics=[
            [],  # topic0: EventLog signature (any variant)
            [POSITION_INCREASE_HASH, POSITION_DECREASE_HASH],  # topic1: event name
        ],
    )

    field_selection = FieldSelection(
        log=[
            LogField.BLOCK_NUMBER,
            LogField.TRANSACTION_HASH,
            LogField.LOG_INDEX,
            LogField.ADDRESS,
            LogField.TOPIC0,
            LogField.TOPIC1,
            LogField.TOPIC2,
            LogField.DATA,
        ],
        block=[
            BlockField.NUMBER,
            BlockField.TIMESTAMP,
        ],
    )

    return Query(
        from_block=start_block,
        to_block=end_block,
        logs=[log_selection],
        field_selection=field_selection,
    )
```

### 2. Event Parsing (eth_defi Integration)

**Parse Position Events:**
```python
from eth_defi.gmx.events import decode_gmx_event, GMXEventData

def parse_position_event(
    web3: Web3,
    log_dict: dict,
    block_timestamps: dict[int, int],
) -> GMXPositionEvent:
    """Parse HyperSync log into position event."""

    # Use eth_defi decoder
    event_data: GMXEventData = decode_gmx_event(web3, log_dict)

    # Extract position-specific fields
    return GMXPositionEvent(
        block_number=log_dict["block_number"],
        block_timestamp=block_timestamps[log_dict["block_number"]],
        transaction_hash=log_dict["transaction_hash"],
        log_index=log_dict.get("log_index", 0),
        event_name=event_data.event_name,
        market=event_data.get_address("market"),
        account=event_data.get_address("account"),
        is_long=event_data.get_bool("isLong"),
        execution_price=event_data.get_uint("executionPrice"),
        size_delta_usd=event_data.get_uint("sizeDeltaUsd"),
        size_delta_in_tokens=event_data.get_uint("sizeDeltaInTokens"),
        price_impact_usd=event_data.get_int("priceImpactUsd"),
        position_key=event_data.get_bytes32("positionKey"),
        collateral_token=event_data.get_address("collateralToken"),
    )
```

**Market Address → Symbol Mapping:**
```python
from eth_defi.gmx.core.markets import Markets
from eth_defi.gmx.config import get_gmx_config

def get_market_symbol_mapping(web3: Web3) -> dict[str, str]:
    """Get mapping of market addresses to token symbols."""

    config = get_gmx_config(web3, chain="arbitrum")
    markets = Markets(config)

    # Build mapping
    mapping = {}
    for market_info in markets.get_all_markets():
        market_addr = market_info.market_address.lower()
        symbol = market_info.index_token_symbol
        mapping[market_addr] = symbol

    return mapping
```

### 3. OHLCV Aggregation

**Convert Events to Candles:**
```python
GMX_USD_PRECISION = 10**30

def aggregate_to_ohlcv(
    events: list[GMXPositionEvent],
    timeframe: str,
    symbol: str,
) -> pd.DataFrame:
    """Convert position events to OHLCV candles."""

    # Convert events to DataFrame
    df = pd.DataFrame([
        {
            "timestamp": pd.Timestamp(e.block_timestamp, unit="s", tz="UTC"),
            "price": e.execution_price / GMX_USD_PRECISION,
            "size_usd": e.size_delta_usd / GMX_USD_PRECISION,
        }
        for e in events
    ])

    # Sort by timestamp
    df = df.sort_values("timestamp")

    # Resample to OHLCV
    ohlcv = df.set_index("timestamp").resample(timeframe).agg({
        "price": ["first", "max", "min", "last"],
        "size_usd": "sum",  # Optional volume
    })

    # Flatten column names
    ohlcv.columns = ["open", "high", "low", "close", "volume"]
    ohlcv["symbol"] = symbol

    # Forward-fill gaps (no trades = repeat last close)
    ohlcv = ohlcv.fillna(method="ffill")

    return ohlcv.reset_index()
```

**OHLCV Logic:**
- **Open:** First execution_price in time bucket
- **High:** Max execution_price in bucket
- **Low:** Min execution_price in bucket
- **Close:** Last execution_price in bucket
- **Volume:** Sum of size_delta_usd (optional)

---

## Module Structure

### New Files

```
src/gmx_historical_data/
├── gmx_event_collector.py        # NEW: HyperSync-based event collector
├── gmx_event_parser.py            # NEW: eth_defi integration & parsing
├── gmx_market_mapper.py           # NEW: Market address → symbol mapping
├── event_aggregator.py            # NEW: Events → OHLCV aggregation
```

### Modified Files

```
src/gmx_historical_data/
├── cli.py                         # Add --use-events flag
├── config.py                      # Add EVENT_EMITTER_ADDRESS constant
```

### Reused Files (no changes)

```
src/gmx_historical_data/
├── hypersync_collector.py         # HyperSync client (reuse as-is)
├── storage.py                     # Parquet storage (reuse as-is)
├── resampler.py                   # OHLCV resampling (reuse logic)
```

---

## Storage Structure

```
data/
├── events/arbitrum/              # NEW: Raw position events
│   ├── ETH/
│   │   ├── partition=0/data.parquet
│   │   └── partition=1/data.parquet
│   ├── BTC/
│   │   └── partition=0/data.parquet
│   └── BONK/
│       └── partition=0/data.parquet
│
├── candles/arbitrum/             # EXISTING: OHLCV candles (same format)
│   ├── ETH/
│   │   ├── 1min.parquet
│   │   ├── 5min.parquet
│   │   ├── 1h.parquet
│   │   └── 1D.parquet
│   └── BTC/
│       └── ...
```

---

## CLI Integration

### New Collection Mode

```bash
# Event-based collection (NEW)
poetry run gmx_historical_data --full --use-events

# Oracle-based collection (EXISTING - default)
poetry run gmx_historical_data --full

# Single symbol via events
poetry run gmx_historical_data --full --symbol ETH --use-events
```

### Implementation

```python
async def collect_all_symbols(
    self,
    full: bool = False,
    use_events: bool = False,  # NEW flag
    concurrency: int = 1,
) -> None:
    """Collect historical data for all symbols.

    :param full: Full historical collection vs incremental
    :param use_events: Use event-based collection (vs oracle-based)
    :param concurrency: Number of parallel collections
    """

    if use_events:
        # NEW: Event-based collection path
        await self.collect_via_events()
    else:
        # EXISTING: Oracle + GMX API path
        await self.collect_via_oracles()
```

---

## Fallback Strategy

For tokens with insufficient event data (new listings, low volume):

1. **Try event-based collection first**
2. **If < 100 events found** → fall back to GMX API
3. **Combine:** Event data (older history) + GMX API (recent data)

```python
events = await collect_position_events(symbol, start_block, end_block)

if len(events) < 100:
    print(f"  ⚠ Only {len(events)} events found, falling back to GMX API")
    gmx_data = fetch_gmx_api_candles(symbol)
    combined_data = merge(events_ohlcv, gmx_data)
else:
    combined_data = events_ohlcv
```

---

## Advantages Over Oracle-Based Collection

| Feature | Oracle-Based | Event-Based |
|---------|-------------|-------------|
| **Token Coverage** | 46 tokens (with Chainlink feeds) | 118 tokens (all GMX markets) |
| **Synthetic Tokens** | ❌ Requires Data Streams API (paid) | ✅ Full support |
| **Data Source** | Chainlink oracle updates | Actual GMX trades |
| **Historical Depth** | Since 2021 (for old tokens) | Since Aug 2023 (GMX V2 launch) |
| **Credentials Required** | No (for traditional feeds) | No |
| **Data Authenticity** | Oracle prices | Real execution prices |

---

## Testing Strategy

### Unit Tests

1. **Event Parsing**
   - Test `parse_position_event()` with sample logs
   - Verify decimal conversion (30 → float)
   - Test market → symbol mapping

2. **OHLCV Aggregation**
   - Test `aggregate_to_ohlcv()` with mock events
   - Verify OHLC calculation correctness
   - Test gap filling (forward-fill)

3. **Storage**
   - Test Parquet roundtrip (write → read)
   - Verify schema compliance

### Integration Tests

1. **Event Collection**
   - Collect 1 day of ETH events
   - Verify event count > 0
   - Verify all required fields present

2. **End-to-End**
   - Run full collection for 1 token (e.g., ETH)
   - Verify OHLCV files created for all timeframes
   - Spot-check candle values against GMX API

### Verification

```bash
# Run tests
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
pytest tests/ -v -k event

# Test with single token
poetry run gmx_historical_data --full --symbol ETH --use-events

# Compare with existing data
poetry run gmx_historical_data --full --symbol ETH  # Oracle-based
# Spot-check that event-based prices are similar
```

---

## Performance Estimates

**Event Volume (rough estimates):**

- GMX V2 launch: Aug 2023 (~1.5 years ago)
- Average: ~1000 position events/day across all markets
- Total events: ~550K events (all markets, all time)
- Per market: ~5K-50K events depending on volume

**Collection Time:**

- HyperSync query: ~30 seconds (all events, all markets, all time)
- Event parsing: ~1-2 minutes (550K events)
- OHLCV aggregation: ~30 seconds per market
- **Total: ~5-10 minutes** for complete historical collection

**Storage:**

- Raw events: ~50-100 MB compressed (550K rows)
- OHLCV candles: ~20-50 MB (all timeframes, all markets)
- **Total: ~100-150 MB** for complete dataset

---

## Migration Path

### Phase 1: Implement Event Collection (Parallel)

- Build new event-based collector
- Test with single token (ETH)
- Don't remove oracle-based code yet

### Phase 2: Validate Data Quality

- Compare event-based vs oracle-based for tokens that have both
- Verify price accuracy
- Check for gaps or anomalies

### Phase 3: Make Event-Based Default (Optional)

- Switch default to `--use-events`
- Keep oracle-based as `--use-oracles` fallback
- Update README

### Phase 4: Deprecate Oracle-Based (Future)

- Once event-based is proven stable
- Remove Chainlink oracle dependencies
- Simplify codebase

---

## Dependencies

**Already installed:**
- ✅ `web3` - Web3 interactions
- ✅ `eth_defi` - GMX event parsing
- ✅ `hypersync` - Fast event queries
- ✅ `pandas` - Data processing
- ✅ `pyarrow` - Parquet storage

**No new dependencies required!**

---

## Summary

**What we're building:**

A GMX event-based historical data collector that:
- Indexes PositionIncrease/Decrease events from GMX EventEmitter contract
- Uses HyperSync for fast event queries (reuses existing infrastructure)
- Leverages eth_defi for event parsing (already installed)
- Aggregates execution prices to OHLCV candles
- Covers ALL 118 GMX tokens (including synthetics)
- Stores in same Parquet format as existing collector

**Key benefits:**
- ✅ Universal token coverage (no more Chainlink feed limitations)
- ✅ Authentic prices (real trades, not oracle estimates)
- ✅ No API credentials needed (all on-chain data)
- ✅ Fast collection (HyperSync + efficient indexing)
- ✅ Reuses 80% of existing code

**Implementation complexity:** Medium
- 4 new modules (~600 lines total)
- 2 modified files
- Reuses existing HyperSync, storage, and resampling logic
