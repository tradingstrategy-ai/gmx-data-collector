# GMX Historical Data Collection - How It Works

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    User Command (CLI)                           │
│  poetry run gmx_historical_data --full --use-events             │
└─────────────────┬───────────────────────────────────────────────┘
                  │
                  ├──── Event-Based Mode (--use-events) ───┐
                  │                                         │
                  └──── Oracle-Based Mode (legacy) ────────┼──────┐
                                                            │      │
                                                            ▼      ▼
┌───────────────────────────────────────┐   ┌─────────────────────────────┐
│    EVENT-BASED COLLECTION             │   │  ORACLE-BASED COLLECTION    │
│    (Recommended - All 102 Tokens)     │   │  (Legacy - ~50 Tokens)      │
└───────────────────────────────────────┘   └─────────────────────────────┘
```

---

## Event-Based Collection Flow (NEW - RECOMMENDED)

### Step 1: Initialization

**File: `cli.py` (line 388-407)**

```python
# Initialize event collector with HyperSync
event_collector = GMXEventCollector(
    hypersync_endpoint="https://arbitrum.hypersync.xyz",
    rpc_url=config.rpc_url,
    api_token=config.hypersync_api_token,
)

# Initialize market mapper to get all GMX markets
mapper = GMXMarketMapper(web3)
market_mapping = mapper.get_market_symbol_mapping()  # Returns 110 markets → 102 tokens
```

**What Happens:**
- Connects to HyperSync endpoint (100-2000x faster than RPC)
- Connects to Arbitrum RPC (for market address discovery)
- Fetches all 110 GMX market addresses from eth_defi library
- Maps market addresses to token symbols (e.g., "0xabc..." → "BTC")

---

### Step 2: Collect ALL Position Events (Single Batch Query)

**File: `gmx_event_collector.py` (line 94-142)**

```python
# Build HyperSync query for ALL position events
query = Query(
    from_block=start_block,
    to_block=end_block,
    logs=[{
        address: "0xC8ee91A54287DB53897056e12D9819156D3822Fb",  # GMX EventEmitter
        topics: [
            [],  # topic0: any EventLog variant
            [position_increase_hash, position_decrease_hash],  # topic1: event type
        ]
    }],
    fields: [block_number, block_hash, transaction_hash, transaction_index,
             log_index, address, topics, data, block_timestamp]
)

# Execute single query to get ALL events at once
response = await hypersync_client.get(query)
# Returns ALL position events across ALL markets
```

**What Happens:**
- Creates a SINGLE HyperSync query (not per-market)
- Filters for GMX EventEmitter contract address
- Filters for PositionIncrease/PositionDecrease event hashes
- Collects ALL position events across ALL 110 markets in one batch
- Returns events with block timestamps, transaction details, and event data

**Why This is Fast:**
- HyperSync has pre-indexed ALL Arbitrum events
- Single query vs. 110 separate queries (per-market)
- 100-2000x faster than standard RPC

---

### Step 3: Parse Events (Bug Fixes Applied)

**File: `gmx_event_parser.py` (line 89-134)**

```python
for log in response.data.logs:
    # Convert HyperSync fields (snake_case) to eth_defi format (camelCase)
    # BUG FIX: eth_defi expects camelCase, HyperSync returns snake_case
    eth_defi_log_dict = {
        "blockNumber": log.block_number,           # was: block_number
        "blockHash": log.block_hash,               # was: block_hash
        "transactionHash": log.transaction_hash,   # was: transaction_hash
        "transactionIndex": log.transaction_index, # was: transaction_index
        "logIndex": log.log_index,                 # was: log_index
        "address": log.address,
        "topics": log.topics,
        "data": log.data,
    }

    # Decode event using eth_defi library
    event_data = decode_gmx_event(web3, eth_defi_log_dict)

    # Extract fields
    position_key_bytes = event_data.get_bytes32("positionKey")
    position_key_hex = position_key_bytes.hex()  # BUG FIX: Convert bytes → hex string

    # Convert hex timestamp to integer
    # BUG FIX: HyperSync returns timestamps as hex strings like "0x65ca0e89"
    timestamp = block.timestamp
    if isinstance(timestamp, str) and timestamp.startswith("0x"):
        timestamp = int(timestamp, 16)  # Convert hex → int

    # Create structured event
    return GMXPositionEvent(
        block_number=block_number,
        block_timestamp=timestamp,
        transaction_hash=tx_hash,
        log_index=log_index,
        event_name="PositionIncrease",  # or "PositionDecrease"
        market="0xMarketAddress",
        account="0xTraderAddress",
        is_long=True,
        execution_price=3500000000000000000000000000000,  # 30 decimals
        size_delta_usd=1000000000000000000000000000000,
        size_delta_in_tokens=285714285714285714285,
        price_impact_usd=-5000000000000000000000000000,
        position_key="a1b2c3d4...",  # hex string
        collateral_token="0xTokenAddress",
    )
```

**Bug Fixes Applied:**
1. **Field Name Mismatch**: eth_defi expected `blockNumber`, we passed `block_number`
2. **Bytes to String**: Parquet can't store bytes, needed hex string conversion
3. **Hex Timestamps**: HyperSync returned "0x65ca0e89", pandas expected integer

---

### Step 4: Group Events by Market

**File: `cli.py` (line 422-428)**

```python
# Group events by market address
events_by_market = defaultdict(list)

for event in all_events:
    market_address = event.market.lower()
    if market_address in address_to_symbol:
        events_by_market[market_address].append(event)

# Result: {
#   "0x47c031236e19d024b42f8ae6780e44a573170703": [event1, event2, ...],  # BTC market
#   "0x70d95587d40a2caf56bd97485ab3eec10bee6336": [event3, event4, ...],  # ETH market
#   ...
# }
```

**What Happens:**
- Groups collected events by market address
- Only includes events for known markets
- Creates a dictionary: market_address → list of events

---

### Step 5: Process Each Market (Save + Aggregate)

**File: `cli.py` (line 436-480)**

```python
for market_address, market_events in events_by_market.items():
    symbol = address_to_symbol[market_address]  # e.g., "BTC"

    # Save raw events to Parquet
    storage.save_position_events(
        market_events,
        symbol,
        partition_id=0
    )
    # Saves to: data/events/arbitrum/BTC/partition=0/data.parquet

    # Aggregate events to OHLCV candles
    for timeframe in ["1min", "5min", "15min", "1h", "4h", "1D"]:
        ohlcv = aggregate_events_to_ohlcv(
            events=market_events,
            timeframe=timeframe,
            symbol=symbol
        )

        storage.save_candles(ohlcv, timeframe, symbol)
        # Saves to: data/candles/arbitrum/BTC/1h.parquet
```

**What Happens:**
1. **Save Raw Events**: Store raw position events with 30-decimal precision as strings
2. **Aggregate to OHLCV**: Convert events to candles for each timeframe
3. **Save Candles**: Store OHLCV data in Parquet files

---

### Step 6: OHLCV Aggregation

**File: `event_aggregator.py` (line 20-62)**

```python
def aggregate_events_to_ohlcv(events, timeframe, symbol):
    # Convert events to DataFrame
    df = pd.DataFrame([
        {
            "timestamp": pd.Timestamp(e.block_timestamp, unit="s", tz="UTC"),
            "price": e.execution_price / 10**30,  # Convert 30 decimals → float
            "size_usd": e.size_delta_usd / 10**30,
        }
        for e in events
    ])

    # Add deterministic sorting (fixes non-deterministic "last" price)
    df["original_order"] = range(len(df))
    df = df.sort_values(["timestamp", "original_order"])

    # Resample to OHLCV
    ohlcv = df.set_index("timestamp").resample(timeframe).agg({
        "price": ["first", "max", "min", "last"],  # OHLC
        "size_usd": "sum",                          # Volume
    })

    ohlcv.columns = ["open", "high", "low", "close", "volume"]
    ohlcv["symbol"] = symbol

    return ohlcv
```

**What Happens:**
- Converts execution prices from 30-decimal integers → float prices
- Groups events by timeframe (e.g., 1h bins)
- Calculates OHLC (Open/High/Low/Close) for each bin
- Sums volume (size_delta_usd) for each bin
- Returns pandas DataFrame with OHLCV data

---

### Step 7: Storage (Parquet with 30-Decimal Precision)

**File: `storage.py` (line 40-90)**

```python
# Schema for raw events (30-decimal fields stored as strings)
POSITION_EVENTS_SCHEMA = pa.schema([
    ("block_number", pa.uint64()),
    ("block_timestamp", pa.uint64()),
    ("transaction_hash", pa.string()),
    ("log_index", pa.uint32()),
    ("event_name", pa.string()),
    ("market", pa.string()),
    ("account", pa.string()),
    ("is_long", pa.bool_()),
    ("execution_price", pa.string()),      # 30 decimals → string (too large for int64)
    ("size_delta_usd", pa.string()),       # 30 decimals → string
    ("size_delta_in_tokens", pa.string()),
    ("price_impact_usd", pa.string()),
    ("position_key", pa.string()),
    ("collateral_token", pa.string()),
    ("symbol", pa.string()),
])

def save_position_events(events, symbol, partition_id):
    # GMX uses 10^30 precision which exceeds int64 max (9.2 × 10^18)
    # Store as strings to preserve exact precision
    data = {
        "execution_price": [str(e.execution_price) for e in events],
        "size_delta_usd": [str(e.size_delta_usd) for e in events],
        ...
    }

    df = pd.DataFrame(data)
    table = pa.Table.from_pandas(df, schema=POSITION_EVENTS_SCHEMA)

    pq.write_table(table, output_path, compression="zstd", compression_level=22)
```

**What Happens:**
- Stores large integers as strings (GMX uses 10^30, int64 max is ~9.2 × 10^18)
- Compresses with zstd level 22 (maximum compression)
- Saves to partitioned Parquet files

---

## Data Output Structure

```
data/
├── events/
│   └── arbitrum/
│       ├── BTC/
│       │   └── partition=0/
│       │       └── data.parquet        # Raw position events
│       ├── ETH/
│       ├── SOL/
│       └── ... (102 tokens)
│
└── candles/
    └── arbitrum/
        ├── BTC/
        │   ├── 1min.parquet
        │   ├── 5min.parquet
        │   ├── 15min.parquet
        │   ├── 1h.parquet
        │   ├── 4h.parquet
        │   └── 1D.parquet
        ├── ETH/
        ├── SOL/
        └── ... (102 tokens)
```

---

## Key Design Decisions

### 1. Single Batch Query (Optimization)
**Before:** Query each market separately (110 queries)
**After:** Single query for all markets (1 query)
**Result:** ~100x faster collection

### 2. 30-Decimal Precision (Data Integrity)
**Problem:** GMX uses 10^30 precision, int64 max is 9.2 × 10^18
**Solution:** Store as strings in Parquet
**Result:** No data loss, exact precision preserved

### 3. Field Name Conversion (Bug Fix)
**Problem:** HyperSync returns snake_case, eth_defi expects camelCase
**Solution:** Convert field names before parsing
**Result:** 100% parse success rate (was 0% before fix)

### 4. Hex Timestamp Conversion (Bug Fix)
**Problem:** HyperSync returns timestamps as hex strings
**Solution:** Convert "0x65ca0e89" → 1707830921
**Result:** Pandas can process timestamps correctly

### 5. Deterministic Sorting (Correctness)
**Problem:** Events with same timestamp had non-deterministic "last" price
**Solution:** Add original_order as secondary sort key
**Result:** Reproducible OHLCV calculations

---

## Performance Characteristics

### Event-Based Collection
- **Speed**: 100-2000x faster than RPC (HyperSync)
- **Coverage**: All 102 GMX V2 tokens
- **History**: From GMX V2 launch (Aug 2023, block ~120M) to present
- **Data Quality**: Authentic execution prices from real trades

### Typical Collection Times
- **Small range (10k blocks)**: ~10-30 seconds
- **Medium range (1M blocks)**: ~1-3 minutes
- **Large range (10M blocks)**: ~5-10 minutes
- **Full history (120M→180M)**: ~20-30 minutes

### Storage Efficiency
- **Raw events**: ~1MB per 10k events (zstd compression)
- **Candles**: ~100KB per symbol per timeframe
- **Full collection**: ~500MB-2GB total

---

## Error Handling

### Parse Failures
- Events that fail to parse are logged but skipped
- Malformed events don't stop the entire collection
- Warning logged for each failed event

### Missing Markets
- Events for unknown markets are ignored
- Only processes markets in the eth_defi library mapping

### Empty Results
- If no events in block range, creates empty result
- No files created for tokens with zero activity

---

## Summary: How It Works Now

1. **User runs command** with `--use-events` flag
2. **Discover all 110 GMX markets** → map to 102 tokens
3. **Single HyperSync query** collects ALL position events
4. **Parse events** with bug fixes (field names, timestamps, bytes→hex)
5. **Group by market** address
6. **For each token**:
   - Save raw events (30-decimal precision as strings)
   - Aggregate to OHLCV for 6 timeframes
   - Save candles to Parquet
7. **Result**: Complete price history for all active tokens

**Key Innovation**: Instead of relying on oracle prices or paid APIs, we index the actual on-chain position events to reconstruct authentic execution prices. This gives us universal coverage for all 102 GMX V2 tokens with real trade data.
