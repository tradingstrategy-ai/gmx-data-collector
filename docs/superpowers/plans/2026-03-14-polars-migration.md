# Polars Migration Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace pandas with Polars for all CPU-bound data processing (aggregation, sorting, deduplication, file I/O) in the collection pipeline and freqtrade export pipeline.

**Architecture:** All heavy work migrates to Polars internals. Public return types at aggregator boundaries stay `pd.DataFrame` (so external call sites in `cli.py` don't change). `freqtrade_exporter.py` migrates fully end-to-end. `storage.py` write methods swap to Polars native write + zstd-3. `cli.py`'s `_merge_and_save_candles` migrates its concat/unique/sort to Polars.

**Tech Stack:** Polars `^1.38.1` (already in `pyproject.toml`), PyArrow (kept for schema definitions), pandas (kept at call-site boundaries only).

**Spec:** `docs/superpowers/specs/2026-03-14-polars-migration-design.md`

---

## Chunk 1: oracle_event_aggregator.py

### Task 1: Migrate `oracle_event_aggregator.py` to Polars internals

**Files:**
- Modify: `src/gmx_historical_data/oracle_event_aggregator.py`
- Create: `tests/test_oracle_event_aggregator.py`

**Context for implementer:**

The file has three public functions:
1. `build_oracle_price_dataframe(events, token_decimals=18)` → `pd.DataFrame` with columns `timestamp`, `price`, `original_order`
2. `resample_oracle_price_dataframe(price_df, timeframe, symbol)` → `pd.DataFrame` with OHLCV columns
3. `aggregate_oracle_events_to_ohlcv(events, timeframe, symbol, token_decimals=18)` → `pd.DataFrame` — **do not change this function**, it's still used in `_collect_symbol_via_oracle_fallback`

Each `OraclePriceEvent` has attributes: `block_timestamp` (int, unix seconds), `min_price` (int, 30-decimal), `max_price` (int, 30-decimal).

The timeframe strings used throughout the codebase are pandas-style: `"1min"`, `"5min"`, `"15min"`, `"1h"`, `"4h"`, `"1D"`. Polars uses different aliases: `"1m"`, `"5m"`, `"15m"`, `"1h"`, `"4h"`, `"1d"`. A mapping dict is needed.

The `group_by_dynamic` Polars method (equivalent of pandas `resample`) takes `index_column` as first positional arg, `every` as the interval string, and `.agg([...])` for aggregations. Returns results sorted by the index column.

---

- [ ] **Step 1: Write the failing test**

Create `tests/test_oracle_event_aggregator.py`:

```python
"""Tests for oracle event aggregation — Polars migration equivalence."""

import types
from datetime import datetime, timezone

import pandas as pd
import pytest

from gmx_historical_data.oracle_event_aggregator import (
    GMX_INTERNAL_PRECISION,
    aggregate_oracle_events_to_ohlcv,
    build_oracle_price_dataframe,
    get_price_divisor,
    resample_oracle_price_dataframe,
)


def _make_event(block_timestamp: int, price_usd: float, token_decimals: int = 18):
    """Create a minimal mock OraclePriceEvent."""
    divisor = get_price_divisor(token_decimals)
    raw = int(price_usd * divisor)
    return types.SimpleNamespace(
        block_timestamp=block_timestamp,
        min_price=raw,
        max_price=raw,
    )


BASE_TS = int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())

EVENTS = [
    _make_event(BASE_TS, 3000.0),
    _make_event(BASE_TS + 30, 3100.0),
    _make_event(BASE_TS + 60, 2950.0),  # next minute
]


def test_build_oracle_price_dataframe_returns_pandas():
    """Return type must remain pd.DataFrame for call-site compatibility."""
    df = build_oracle_price_dataframe(EVENTS)
    assert isinstance(df, pd.DataFrame)


def test_build_oracle_price_dataframe_columns():
    df = build_oracle_price_dataframe(EVENTS)
    assert "timestamp" in df.columns
    assert "price" in df.columns


def test_build_oracle_price_dataframe_sorted():
    """Events must be sorted by timestamp in output."""
    shuffled = [EVENTS[2], EVENTS[0], EVENTS[1]]
    df = build_oracle_price_dataframe(shuffled)
    assert df["timestamp"].is_monotonic_increasing


def test_build_oracle_price_dataframe_empty():
    df = build_oracle_price_dataframe([])
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_resample_oracle_price_dataframe_returns_pandas():
    price_df = build_oracle_price_dataframe(EVENTS)
    ohlcv = resample_oracle_price_dataframe(price_df, "1min", "ETH")
    assert isinstance(ohlcv, pd.DataFrame)


def test_resample_oracle_price_dataframe_ohlcv_columns():
    price_df = build_oracle_price_dataframe(EVENTS)
    ohlcv = resample_oracle_price_dataframe(price_df, "1min", "ETH")
    for col in ["timestamp", "open", "high", "low", "close", "symbol"]:
        assert col in ohlcv.columns


def test_resample_oracle_price_dataframe_two_candles():
    """3 events spanning 2 minutes → 2 candles."""
    price_df = build_oracle_price_dataframe(EVENTS)
    ohlcv = resample_oracle_price_dataframe(price_df, "1min", "ETH")
    assert len(ohlcv) == 2


def test_resample_oracle_price_dataframe_ohlcv_values():
    """First candle: open=3000, high=3100, low=3000, close=3100."""
    price_df = build_oracle_price_dataframe(EVENTS)
    ohlcv = resample_oracle_price_dataframe(price_df, "1min", "ETH")
    c0 = ohlcv.iloc[0]
    assert c0["open"] == pytest.approx(3000.0, rel=1e-9)
    assert c0["high"] == pytest.approx(3100.0, rel=1e-9)
    assert c0["low"] == pytest.approx(3000.0, rel=1e-9)
    assert c0["close"] == pytest.approx(3100.0, rel=1e-9)
    assert c0["symbol"] == "ETH"


def test_resample_oracle_price_dataframe_second_candle():
    """Second candle: single event at 2950."""
    price_df = build_oracle_price_dataframe(EVENTS)
    ohlcv = resample_oracle_price_dataframe(price_df, "1min", "ETH")
    c1 = ohlcv.iloc[1]
    assert c1["open"] == pytest.approx(2950.0, rel=1e-9)
    assert c1["close"] == pytest.approx(2950.0, rel=1e-9)


def test_resample_oracle_price_dataframe_empty():
    empty_df = build_oracle_price_dataframe([])
    ohlcv = resample_oracle_price_dataframe(empty_df, "1min", "ETH")
    assert isinstance(ohlcv, pd.DataFrame)
    assert ohlcv.empty


def test_aggregate_oracle_events_to_ohlcv_unchanged():
    """aggregate_oracle_events_to_ohlcv must still work (backward compat)."""
    ohlcv = aggregate_oracle_events_to_ohlcv(EVENTS, "1min", "ETH")
    assert len(ohlcv) == 2
    assert ohlcv.iloc[0]["open"] == pytest.approx(3000.0, rel=1e-9)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/avik/Work/tradingstrategy/gmx_historical_data
poetry run python -m pytest tests/test_oracle_event_aggregator.py -v
```

Expected: most tests PASS (functions exist), but `test_resample_oracle_price_dataframe_two_candles` and OHLCV value tests may fail if pandas resample behaves differently for the new Polars path — confirm tests run and note which fail.

- [ ] **Step 3: Migrate `oracle_event_aggregator.py`**

Add `import polars as pl` at the top (after existing imports). Add the timeframe mapping dict and migrate `build_oracle_price_dataframe` and `resample_oracle_price_dataframe`. Leave `aggregate_oracle_events_to_ohlcv` entirely unchanged.

Replace the contents of `src/gmx_historical_data/oracle_event_aggregator.py` with:

```python
"""Aggregate GMX oracle price events to OHLCV candles.

Converts raw oracle price update events into time-series
OHLCV data suitable for backtesting and analysis.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import polars as pl

if TYPE_CHECKING:
    from gmx_historical_data.oracle_price_collector import OraclePriceEvent


#: GMX internal precision (30 decimals)
#: Price formula: human_price = raw / 10^(30 - token_decimals)
GMX_INTERNAL_PRECISION = 30

#: Mapping from pandas timeframe aliases to Polars aliases.
_PANDAS_TO_POLARS_TIMEFRAME: dict[str, str] = {
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
    "1h": "1h",
    "4h": "4h",
    "1D": "1d",
}


def get_price_divisor(token_decimals: int) -> int:
    """Get the divisor for converting raw GMX prices to USD.

    GMX uses 30-decimal internal precision. The formula is:
    human_price = raw / 10^(30 - token_decimals)

    :param token_decimals: Token decimals (e.g., 18 for ETH, 8 for BTC, 9 for SUI)
    :return: Divisor to convert raw price to USD
    """
    return 10 ** (GMX_INTERNAL_PRECISION - token_decimals)


def aggregate_oracle_events_to_ohlcv(
    events: list[OraclePriceEvent],
    timeframe: str,
    symbol: str,
    token_decimals: int = 18,
) -> pd.DataFrame:
    """Convert oracle price events to OHLC candles.

    Uses oracle mid-price: (min_price + max_price) / 2

    Price conversion formula: human_price = raw / 10^(30 - token_decimals)
    - ETH (18 decimals): divisor = 10^12
    - BTC (8 decimals): divisor = 10^22
    - SUI (9 decimals): divisor = 10^21

    :param events: List of oracle price events
    :param timeframe: Timeframe for resampling (e.g., "1min", "1h", "1D")
    :param symbol: Token symbol
    :param token_decimals: Token decimals for price conversion (default: 18)
    :return: DataFrame with OHLC data (no volume)
    """
    if not events:
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "symbol"])

    # Calculate divisor based on token decimals
    divisor = get_price_divisor(token_decimals)

    # Convert events to DataFrame
    # Use oracle mid-price: average of min/max prices
    df = pd.DataFrame(
        [
            {
                "timestamp": pd.Timestamp(e.block_timestamp, unit="s", tz="UTC"),
                "price": (e.min_price + e.max_price) / 2 / divisor,
            }
            for e in events
        ]
    )

    # Add original order column to ensure deterministic sorting
    df["original_order"] = range(len(df))

    # Sort by timestamp, then by original order for deterministic behavior
    df = df.sort_values(["timestamp", "original_order"])

    # Resample to OHLC (no volume)
    ohlcv = (
        df.set_index("timestamp")
        .resample(timeframe)
        .agg(
            {
                "price": ["first", "max", "min", "last"],
            }
        )
    )

    # Flatten column names
    ohlcv.columns = ["open", "high", "low", "close"]

    # Add symbol
    ohlcv["symbol"] = symbol

    # Drop rows with no data (NaN in all OHLC)
    ohlcv = ohlcv.dropna(subset=["open", "high", "low", "close"], how="all")

    # Reset index to make timestamp a column
    ohlcv = ohlcv.reset_index()

    return ohlcv


def build_oracle_price_dataframe(
    events: list[OraclePriceEvent],
    token_decimals: int = 18,
) -> pd.DataFrame:
    """Build a sorted price DataFrame from oracle events.

    This is the expensive step — converts raw events to a price time series.
    Call once per symbol, then use :func:`resample_oracle_price_dataframe`
    for each timeframe.

    Uses Polars internally for performance; returns :class:`pandas.DataFrame`
    for call-site compatibility.

    :param events: List of oracle price events.
    :param token_decimals: Token decimals for price conversion (default: 18).
    :return: DataFrame with columns ``timestamp`` and ``price``, sorted by timestamp.
    """
    if not events:
        return pd.DataFrame(columns=["timestamp", "price"])

    divisor = get_price_divisor(token_decimals)

    df = pl.DataFrame(
        {
            "timestamp": [
                pd.Timestamp(e.block_timestamp, unit="s", tz="UTC") for e in events
            ],
            "price": [(e.min_price + e.max_price) / 2 / divisor for e in events],
            "original_order": list(range(len(events))),
        }
    )
    df = df.sort(["timestamp", "original_order"])

    return df.to_pandas()


def resample_oracle_price_dataframe(
    price_df: pd.DataFrame,
    timeframe: str,
    symbol: str,
) -> pd.DataFrame:
    """Resample a pre-built price DataFrame to OHLCV candles for one timeframe.

    Use after :func:`build_oracle_price_dataframe` to avoid rebuilding the
    DataFrame for each timeframe.

    Uses Polars ``group_by_dynamic`` internally for performance; returns
    :class:`pandas.DataFrame` for call-site compatibility.

    :param price_df: DataFrame from :func:`build_oracle_price_dataframe`.
    :param timeframe: Resample rule (e.g., ``"1min"``, ``"1h"``, ``"1D"``).
    :param symbol: Token symbol (added as column).
    :return: DataFrame with columns ``timestamp``, ``open``, ``high``, ``low``, ``close``, ``symbol``.
    """
    if price_df.empty:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "symbol"])

    polars_tf = _PANDAS_TO_POLARS_TIMEFRAME[timeframe]
    df = pl.from_pandas(price_df)

    ohlcv = df.sort("timestamp").group_by_dynamic("timestamp", every=polars_tf).agg(
        [
            pl.first("price").alias("open"),
            pl.max("price").alias("high"),
            pl.min("price").alias("low"),
            pl.last("price").alias("close"),
        ]
    )

    ohlcv = ohlcv.with_columns(pl.lit(symbol).alias("symbol"))
    ohlcv = ohlcv.drop_nulls(subset=["open", "high", "low", "close"])

    return ohlcv.to_pandas()
```

- [ ] **Step 4: Run tests**

```bash
poetry run python -m pytest tests/test_oracle_event_aggregator.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Run full unit test suite to check for regressions**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run python -m pytest tests/ -v \
  --ignore=tests/test_hybrid_collection.py \
  --ignore=tests/test_gmx_event_collector.py \
  --ignore=tests/test_integration_event_based.py \
  --ignore=tests/test_integration_gmx_first.py \
  --ignore=tests/test_gmx_market_mapper.py \
  --ignore=tests/test_gmx_token_discovery.py \
  --ignore=tests/test_live_funding.py \
  -q
```

Expected: 102+ passed, same pre-existing failures as before (chainlink mapper, freqtrade timestamps). No new failures.

- [ ] **Step 6: Lint**

```bash
poetry run ruff check src/gmx_historical_data/oracle_event_aggregator.py tests/test_oracle_event_aggregator.py
poetry run ruff format --check src/gmx_historical_data/oracle_event_aggregator.py tests/test_oracle_event_aggregator.py
```

Expected: no issues.

- [ ] **Step 7: Commit**

```bash
git add src/gmx_historical_data/oracle_event_aggregator.py tests/test_oracle_event_aggregator.py
git commit -m "perf: migrate oracle_event_aggregator to Polars group_by_dynamic"
```

---

## Chunk 2: event_aggregator.py

### Task 2: Migrate `event_aggregator.py` to Polars internals

**Files:**
- Modify: `src/gmx_historical_data/event_aggregator.py`
- Modify: `tests/test_event_aggregator.py`

**Context for implementer:**

`event_aggregator.py` has one public function: `aggregate_events_to_ohlcv(events, timeframe, symbol, use_execution_price=False)`. It converts `GMXPositionEvent` objects to OHLCV candles.

Key differences from oracle aggregator:
- `use_execution_price=True`: uses `e.execution_price / GMX_USD_PRECISION`
- `use_execution_price=False`: uses `(e.index_token_price_min + e.index_token_price_max) / 2 / GMX_USD_PRECISION`
- `GMX_USD_PRECISION = 10**30` (defined locally in this file, not via `get_price_divisor`)

Import `_PANDAS_TO_POLARS_TIMEFRAME` from `oracle_event_aggregator` to avoid duplication.

The existing tests in `tests/test_event_aggregator.py` must all continue to pass — the function signature and return type (`pd.DataFrame`) are unchanged.

---

- [ ] **Step 1: Add equivalence test to `tests/test_event_aggregator.py`**

Add this test at the end of the existing file:

```python
def test_aggregate_events_polars_equivalence():
    """Polars-backed aggregation must produce same values as pandas."""
    base_ts = int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())
    events = [
        GMXPositionEvent(
            block_number=1,
            block_timestamp=base_ts,
            transaction_hash="0x1" + "0" * 62,
            log_index=0,
            event_name="PositionIncrease",
            market="0xmarket",
            account="0xacc1",
            is_long=True,
            index_token_price_min=2995 * GMX_USD_PRECISION,
            index_token_price_max=3005 * GMX_USD_PRECISION,
            execution_price=3000 * GMX_USD_PRECISION,
            size_delta_usd=1000 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey1",
            collateral_token="0xcoll",
        ),
        GMXPositionEvent(
            block_number=2,
            block_timestamp=base_ts + 30,
            transaction_hash="0x2" + "0" * 62,
            log_index=0,
            event_name="PositionIncrease",
            market="0xmarket",
            account="0xacc2",
            is_long=False,
            index_token_price_min=3095 * GMX_USD_PRECISION,
            index_token_price_max=3105 * GMX_USD_PRECISION,
            execution_price=3100 * GMX_USD_PRECISION,
            size_delta_usd=500 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey2",
            collateral_token="0xcoll",
        ),
    ]
    result = aggregate_events_to_ohlcv(events, timeframe="1min", symbol="ETH")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.iloc[0]["open"] == pytest.approx(3000.0, rel=1e-9)
    assert result.iloc[0]["high"] == pytest.approx(3100.0, rel=1e-9)
    assert result.iloc[0]["low"] == pytest.approx(3000.0, rel=1e-9)
    assert result.iloc[0]["close"] == pytest.approx(3100.0, rel=1e-9)
```

Also add `import pytest` to the imports at the top of `tests/test_event_aggregator.py` if not already present.

- [ ] **Step 2: Run test to verify it passes with current pandas code**

```bash
poetry run python -m pytest tests/test_event_aggregator.py -v
```

Expected: all tests PASS (including new equivalence test — this is verifying current behaviour before migration).

- [ ] **Step 3: Migrate `event_aggregator.py`**

Replace `src/gmx_historical_data/event_aggregator.py` with:

```python
"""Aggregate GMX position events to OHLCV candles.

Converts raw position events with execution prices into time-series
OHLCV data suitable for backtesting and analysis.
"""

import pandas as pd
import polars as pl

from gmx_historical_data.gmx_event_parser import GMXPositionEvent
from gmx_historical_data.oracle_event_aggregator import _PANDAS_TO_POLARS_TIMEFRAME

#: GMX USD precision (30 decimals)
GMX_USD_PRECISION = 10**30


def aggregate_events_to_ohlcv(
    events: list[GMXPositionEvent],
    timeframe: str,
    symbol: str,
    use_execution_price: bool = False,
) -> pd.DataFrame:
    """Convert position events to OHLC candles.

    By default uses Chainlink oracle prices (min/max from indexTokenPrice)
    to provide clean market prices without price impact. For backtesting,
    use execution prices which include slippage and represent actual fills.

    Uses Polars ``group_by_dynamic`` internally for performance; returns
    :class:`pandas.DataFrame` for call-site compatibility.

    :param events: List of position events
    :param timeframe: Timeframe for resampling (e.g., "1min", "1h", "1D")
    :param symbol: Token symbol
    :param use_execution_price: If True, use execution_price (includes slippage)
                                for backtesting. If False, use oracle mid-price
                                for clean market reference.
    :return: DataFrame with OHLC data (no volume)
    """
    if not events:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "symbol"])

    if use_execution_price:
        prices = [e.execution_price / GMX_USD_PRECISION for e in events]
    else:
        prices = [
            (e.index_token_price_min + e.index_token_price_max) / 2 / GMX_USD_PRECISION
            for e in events
        ]

    df = pl.DataFrame(
        {
            "timestamp": [
                pd.Timestamp(e.block_timestamp, unit="s", tz="UTC") for e in events
            ],
            "price": prices,
            "original_order": list(range(len(events))),
        }
    )
    df = df.sort(["timestamp", "original_order"])

    polars_tf = _PANDAS_TO_POLARS_TIMEFRAME[timeframe]
    ohlcv = df.group_by_dynamic("timestamp", every=polars_tf).agg(
        [
            pl.first("price").alias("open"),
            pl.max("price").alias("high"),
            pl.min("price").alias("low"),
            pl.last("price").alias("close"),
        ]
    )

    ohlcv = ohlcv.with_columns(pl.lit(symbol).alias("symbol"))
    ohlcv = ohlcv.drop_nulls(subset=["open", "high", "low", "close"])

    return ohlcv.to_pandas()
```

- [ ] **Step 4: Run tests**

```bash
poetry run python -m pytest tests/test_event_aggregator.py -v
```

Expected: all tests PASS.

- [ ] **Step 5: Lint and commit**

```bash
poetry run ruff check src/gmx_historical_data/event_aggregator.py tests/test_event_aggregator.py
poetry run ruff format --check src/gmx_historical_data/event_aggregator.py tests/test_event_aggregator.py
git add src/gmx_historical_data/event_aggregator.py tests/test_event_aggregator.py
git commit -m "perf: migrate event_aggregator to Polars group_by_dynamic"
```

---

## Chunk 3: storage.py

### Task 3: Migrate `storage.py` parquet writes to Polars + zstd-3

**Files:**
- Modify: `src/gmx_historical_data/storage.py`
- Modify: `tests/test_storage_list.py` (read to understand existing coverage)

**Context for implementer:**

Three methods in `ParquetStorage` call `pq.write_table(..., compression="zstd", compression_level=22)`:
1. `save_raw_events` (line ~152)
2. `save_candles` (line ~226)
3. `save_position_events` (line ~372)

Each follows this pattern:
```python
table = pa.Table.from_pandas(df, schema=SOME_SCHEMA)
pq.write_table(table, output_path, compression="zstd", compression_level=22)
```

Replace with:
```python
table = pa.Table.from_pandas(df, schema=SOME_SCHEMA)
pl.from_arrow(table).write_parquet(str(output_path), compression="zstd", compression_level=3)
```

This preserves schema enforcement (PyArrow still validates columns/types before the Polars conversion) while using Polars for the actual write + switching from zstd-22 to zstd-3.

Read operations (`read_raw_events`, `read_candles`) are **not changed** — they continue to use `pd.read_parquet`.

Add `import polars as pl` at the top of the file (after `import pyarrow.parquet as pq`).

---

- [ ] **Step 1: Read `tests/test_storage_list.py` to understand existing coverage**

Understand what's already tested. We need the write→read roundtrip to still produce identical data.

- [ ] **Step 2: Write a roundtrip test**

Add to `tests/test_storage_list.py` (or a new file `tests/test_storage_write.py` if the existing file doesn't cover writes):

```python
import tempfile
from pathlib import Path

import pandas as pd

from gmx_historical_data.storage import ParquetStorage


def test_save_candles_roundtrip():
    """Write candles with Polars zstd-3, read back with pandas — data must match."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))
        df_in = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2024-01-01 00:00:00", "2024-01-01 01:00:00"], utc=True
                ),
                "open": [100.0, 101.0],
                "high": [105.0, 106.0],
                "low": [99.0, 100.0],
                "close": [103.0, 104.0],
                "symbol": ["ETH", "ETH"],
            }
        )
        storage.save_candles(df_in, "1h", "ETH")
        df_out = storage.read_candles("1h", "ETH")

        assert len(df_out) == 2
        pd.testing.assert_frame_equal(
            df_in.reset_index(drop=True),
            df_out.reset_index(drop=True),
            check_like=True,
        )
```

- [ ] **Step 3: Run test to verify it passes with current code**

```bash
poetry run python -m pytest tests/test_storage_list.py -v  # (or test_storage_write.py)
```

Expected: PASS (establishes baseline).

- [ ] **Step 4: Migrate the three write methods in `storage.py`**

Add `import polars as pl` after `import pyarrow.parquet as pq` at the top.

In `save_raw_events`, replace:
```python
pq.write_table(
    table,
    output_path,
    compression="zstd",
    compression_level=22,
)
```
with:
```python
pl.from_arrow(table).write_parquet(str(output_path), compression="zstd", compression_level=3)
```

Apply the same replacement in `save_candles` and `save_position_events`. There are exactly 3 occurrences.

- [ ] **Step 5: Run roundtrip test**

```bash
poetry run python -m pytest tests/test_storage_list.py -v
```

Expected: PASS.

- [ ] **Step 6: Run full test suite**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run python -m pytest tests/ -v \
  --ignore=tests/test_hybrid_collection.py \
  --ignore=tests/test_gmx_event_collector.py \
  --ignore=tests/test_integration_event_based.py \
  --ignore=tests/test_integration_gmx_first.py \
  --ignore=tests/test_gmx_market_mapper.py \
  --ignore=tests/test_gmx_token_discovery.py \
  --ignore=tests/test_live_funding.py \
  -q
```

Expected: same pass count as before, no new failures.

- [ ] **Step 7: Lint and commit**

```bash
poetry run ruff check src/gmx_historical_data/storage.py
poetry run ruff format --check src/gmx_historical_data/storage.py
git add src/gmx_historical_data/storage.py tests/
git commit -m "perf: migrate storage writes to Polars write_parquet + zstd-3"
```

---

## Chunk 4: cli.py _merge_and_save_candles

### Task 4: Migrate `_merge_and_save_candles` in `cli.py` to Polars

**Files:**
- Modify: `src/gmx_historical_data/cli.py` (lines 284–321 only)

**Context for implementer:**

`_merge_and_save_candles` is at line 284 of `cli.py`. The full method body:

```python
def _merge_and_save_candles(
    self,
    symbol: str,
    timeframe: str,
    *dataframes: pd.DataFrame | None,
    merge_with_existing: bool = False,
) -> int:
    # Collect non-empty DataFrames
    dfs = [df for df in dataframes if df is not None and not df.empty]
    if not dfs:
        return 0

    if len(dfs) == 1:
        combined = dfs[0]
    else:
        combined = pd.concat(dfs, ignore_index=True)

    # Merge with existing storage if incremental
    if merge_with_existing:
        existing = self.storage.read_candles(timeframe, symbol)
        if not existing.empty:
            combined = pd.concat([existing, combined], ignore_index=True)

    # Deduplicate and sort
    combined = combined.drop_duplicates(subset=["timestamp"], keep="last")
    combined = combined.sort_values("timestamp").reset_index(drop=True)

    self.storage.save_candles(combined, timeframe, symbol)
    return len(combined)
```

Replace the concat/dedup/sort block with Polars. The method signature stays the same — it still accepts `*dataframes: pd.DataFrame | None` and calls `self.storage.save_candles(combined, ...)` with a pandas DataFrame (since storage.py's `save_candles` still accepts `pd.DataFrame`).

Add `import polars as pl` near the top of `cli.py` (after the existing pandas import). Search for `import pandas as pd` in cli.py to find the right location.

---

- [ ] **Step 1: Find the exact location of the import block in cli.py**

Search for `import pandas as pd` in `cli.py` to find line number.

- [ ] **Step 2: Add `import polars as pl` after the pandas import in cli.py**

Find the pandas import line and add `import polars as pl` directly after it.

- [ ] **Step 3: Run existing CLI tests to confirm baseline**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run python -m pytest tests/test_cli_refactor.py -v
```

Note the baseline pass/fail count.

- [ ] **Step 4: Replace the method body in `_merge_and_save_candles`**

The new method body (keep signature and docstring unchanged, replace only the body logic):

```python
    # Collect non-empty DataFrames
    dfs = [df for df in dataframes if df is not None and not df.empty]
    if not dfs:
        return 0

    # Merge with existing storage if incremental
    if merge_with_existing:
        existing = self.storage.read_candles(timeframe, symbol)
        if not existing.empty:
            dfs = [existing] + list(dfs)

    # Deduplicate and sort using Polars (faster than pandas for large merges)
    pl_frames = [pl.from_pandas(df) for df in dfs]
    combined = pl.concat(pl_frames) if len(pl_frames) > 1 else pl_frames[0]
    combined = combined.unique(subset=["timestamp"], keep="last", maintain_order=False)
    combined = combined.sort("timestamp")

    self.storage.save_candles(combined.to_pandas(), timeframe, symbol)
    return len(combined)
```

- [ ] **Step 5: Run CLI tests**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run python -m pytest tests/test_cli_refactor.py -v
```

Expected: same pass count as baseline.

- [ ] **Step 6: Lint and commit**

```bash
poetry run ruff check src/gmx_historical_data/cli.py
poetry run ruff format --check src/gmx_historical_data/cli.py
git add src/gmx_historical_data/cli.py
git commit -m "perf: migrate _merge_and_save_candles concat/dedup/sort to Polars"
```

---

## Chunk 5: freqtrade_exporter.py

### Task 5: Migrate `freqtrade_exporter.py` to full Polars

**Files:**
- Modify: `src/gmx_historical_data/freqtrade_exporter.py`
- Modify: `tests/test_freqtrade_exporter.py`

**Context for implementer:**

This is the most complete migration — no pandas remains after. Key changes:

**Imports:** Remove `import pandas as pd` and `import pyarrow.feather as feather`. Add `import polars as pl`.

**`_read_funding_rate`:** `pd.read_parquet(path)` → `pl.read_parquet(path)`. `df.empty` → `df.is_empty()`.

**`_transform_dataframe`:** Takes a Polars DataFrame. Rename `timestamp→date`, add `volume=0.0`, cast timestamp to ns, sort. No dedup (data already clean from storage).

**`_transform_funding_rate`:** Takes a Polars DataFrame. Rename timestamp and rate column, zero out OHLCV, cast timestamp to ns, sort, unique(keep="first"), drop_nulls on open.

**`_transform_mark_price`:** Takes a Polars DataFrame. Same as `_transform_dataframe` but also dedup with unique(keep="first").

**`_write`:** `feather.write_feather(df, path)` → `df.write_ipc(path)`. `df.to_parquet(path, index=False)` → `df.write_parquet(str(path))`.

**`export` method:** `self.storage.read_candles(tf, symbol)` still returns `pd.DataFrame`. Convert immediately: `df = pl.from_pandas(self.storage.read_candles(tf, symbol))`. Change `not df.empty` checks to `not df.is_empty()`. Same for `funding_df`.

Existing tests use `pd.read_feather(path)` to read output — this still works because `write_ipc` writes Arrow IPC v2 = feather v2 format, readable by pandas.

**Note:** `tests/test_freqtrade_exporter_timestamps.py` has 3 pre-existing failures unrelated to this migration. Do not attempt to fix them.

---

- [ ] **Step 1: Run existing tests to confirm baseline**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run python -m pytest tests/test_freqtrade_exporter.py -v
```

Note the baseline pass count (should be 6 tests passing).

- [ ] **Step 2: Migrate `freqtrade_exporter.py`**

Replace the full file content:

```python
"""Export GMX data to Freqtrade-compatible format.

Freqtrade expects OHLCV data with columns: date, open, high, low, close, volume

Exported file types:

- **OHLCV candles**: ``{SYMBOL}_USDC_USDC-{tf}-futures.feather``
- **Funding rate**: ``{SYMBOL}_USDC_USDC-{tf}-funding_rate.feather``
  (``open`` = hourly funding rate, other OHLCV columns = 0)
- **Mark price**: ``{SYMBOL}_USDC_USDC-{tf}-mark.feather``
  (OHLCV data used as mark price proxy)
- **Index price**: ``{SYMBOL}_USDC_USDC-{tf}-index.feather``
  (same as mark — GMX uses Chainlink oracle as index price)

Funding rate parquet files are read from
``{data_dir}/funding/arbitrum/rates/{SYMBOL}/{tf}.parquet``.
"""

import logging
from pathlib import Path

import polars as pl

from gmx_historical_data.storage import ParquetStorage

logger = logging.getLogger(__name__)


class FreqtradeExporter:
    """Export GMX candle and funding rate data to Freqtrade format.

    :param data_dir: Source directory with GMX data (candles + funding).
    :param output_dir: Output directory for Freqtrade files.
    """

    def __init__(self, data_dir: Path, output_dir: Path):
        """Initialize exporter.

        :param data_dir: Source GMX data directory.
        :param output_dir: Target directory for Freqtrade files.
        """
        self.data_dir = Path(data_dir)
        self.storage = ParquetStorage(self.data_dir)
        self.output_dir = Path(output_dir)
        self.funding_dir = self.data_dir / "funding" / "arbitrum" / "rates"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def export(
        self,
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        output_format: str = "feather",
        trading_mode: str = "futures",
        quote_currency: str = "USDC",
    ) -> dict[str, dict]:
        """Export GMX data to Freqtrade format.

        Exports OHLCV candles, funding rates, and mark price files for
        each symbol/timeframe combination.

        :param symbols: Specific symbols to export (default: all).
        :param timeframes: Specific timeframes to export (default: all).
        :param output_format: Output format (``'feather'`` or ``'parquet'``).
        :param trading_mode: ``'futures'`` or ``'spot'`` (default: ``'futures'``).
        :param quote_currency: Quote/settlement currency (default: ``'USDC'``).
        :returns: Dict mapping symbol to export stats.
        """
        # Create output directory
        if trading_mode == "futures":
            gmx_dir = self.output_dir / "gmx" / "futures"
        else:
            gmx_dir = self.output_dir / "gmx"
        gmx_dir.mkdir(parents=True, exist_ok=True)

        # Merge symbols from both candle and funding data
        candle_symbols = set(self.storage.list_symbols())
        funding_symbols = set(self.list_funding_symbols())
        all_symbols = sorted(candle_symbols | funding_symbols)

        if symbols:
            export_symbols = [s for s in symbols if s in all_symbols]
        else:
            export_symbols = all_symbols

        results = {}

        for symbol in export_symbols:
            ohlcv_files = 0
            funding_files = 0
            mark_files = 0
            index_files = 0
            total_candles = 0

            # Determine timeframes from candle + funding data
            candle_tfs = (
                set(self.storage.list_timeframes(symbol)) if symbol in candle_symbols else set()
            )
            funding_tfs = (
                set(self.list_funding_timeframes(symbol)) if symbol in funding_symbols else set()
            )
            available_tfs = sorted(candle_tfs | funding_tfs)

            if timeframes:
                export_tfs = [tf for tf in timeframes if tf in available_tfs]
            else:
                export_tfs = available_tfs

            for tf in export_tfs:
                # --- OHLCV candles ---
                if tf in candle_tfs:
                    raw_df = self.storage.read_candles(tf, symbol)
                    if not raw_df.empty:
                        df = pl.from_pandas(raw_df)
                        ft_df = self._transform_dataframe(df)
                        filename = self._get_freqtrade_filename(
                            symbol,
                            tf,
                            output_format,
                            trading_mode,
                            quote_currency,
                        )
                        self._write(ft_df, gmx_dir / filename, output_format)
                        ohlcv_files += 1
                        total_candles += len(ft_df)

                        # --- Mark price (OHLCV proxy) ---
                        mark_df = self._transform_mark_price(df)
                        mark_filename = self._get_freqtrade_filename(
                            symbol,
                            tf,
                            output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="mark",
                        )
                        self._write(mark_df, gmx_dir / mark_filename, output_format)
                        mark_files += 1

                        # --- Index price (same as mark for GMX/Chainlink) ---
                        index_filename = self._get_freqtrade_filename(
                            symbol,
                            tf,
                            output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="index",
                        )
                        self._write(mark_df, gmx_dir / index_filename, output_format)
                        index_files += 1

                # --- Funding rate ---
                if tf in funding_tfs:
                    funding_df = self._read_funding_rate(symbol, tf)
                    if funding_df is not None and not funding_df.is_empty():
                        ft_funding = self._transform_funding_rate(funding_df)
                        funding_filename = self._get_freqtrade_filename(
                            symbol,
                            tf,
                            output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="funding_rate",
                        )
                        self._write(ft_funding, gmx_dir / funding_filename, output_format)
                        funding_files += 1

            results[symbol] = {
                "files": ohlcv_files + funding_files + mark_files + index_files,
                "candles": total_candles,
                "ohlcv_files": ohlcv_files,
                "funding_files": funding_files,
                "mark_files": mark_files,
                "index_files": index_files,
            }

        return results

    # ------------------------------------------------------------------
    # Funding rate helpers
    # ------------------------------------------------------------------

    def list_funding_symbols(self) -> list[str]:
        """List symbols that have funding rate data.

        :returns: Sorted list of symbol names.
        """
        if not self.funding_dir.exists():
            return []
        return sorted(
            d.name for d in self.funding_dir.iterdir() if d.is_dir() and list(d.glob("*.parquet"))
        )

    def list_funding_timeframes(self, symbol: str) -> list[str]:
        """List available funding rate timeframes for a symbol.

        :param symbol: Token symbol (e.g., ``'ETH'``).
        :returns: Sorted list of timeframe strings.
        """
        symbol_dir = self.funding_dir / symbol
        if not symbol_dir.exists():
            return []
        return sorted(f.stem for f in symbol_dir.glob("*.parquet"))

    # ------------------------------------------------------------------
    # Data readers
    # ------------------------------------------------------------------

    def _read_funding_rate(self, symbol: str, timeframe: str) -> pl.DataFrame | None:
        """Read funding rate parquet for a symbol/timeframe.

        :param symbol: Token symbol.
        :param timeframe: Timeframe (e.g., ``'1h'``).
        :returns: Polars DataFrame with funding columns, or ``None`` if missing.
        """
        path = self.funding_dir / symbol / f"{timeframe}.parquet"
        if not path.exists():
            return None
        df = pl.read_parquet(path)
        if df.is_empty():
            return None
        return df

    # ------------------------------------------------------------------
    # Transformers
    # ------------------------------------------------------------------

    def _transform_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform GMX OHLCV dataframe to Freqtrade format.

        :param df: GMX candle Polars dataframe.
        :returns: Freqtrade-compatible Polars dataframe.
        """
        return (
            df.rename({"timestamp": "date"})
            .with_columns(
                pl.col("date").dt.convert_time_zone("UTC").dt.cast_time_unit("ns"),
                pl.lit(0.0).alias("volume"),
            )
            .select(["date", "open", "high", "low", "close", "volume"])
            .sort("date")
        )

    def _transform_funding_rate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform GMX funding rate dataframe to Freqtrade format.

        FreqTrade stores funding rate in the ``open`` column with other
        OHLCV columns set to 0. Uses ``funding_rate_hourly`` as the
        rate value (falls back to ``funding_rate`` if hourly is missing).

        :param df: Funding rate Polars dataframe from parquet.
        :returns: Freqtrade-compatible Polars dataframe.
        """
        rate_col = "funding_rate_hourly" if "funding_rate_hourly" in df.columns else "funding_rate"
        return (
            df.rename({"timestamp": "date", rate_col: "open"})
            .with_columns(
                pl.col("date").dt.convert_time_zone("UTC").dt.cast_time_unit("ns"),
                pl.col("open").cast(pl.Float64),
                pl.lit(0.0).alias("high"),
                pl.lit(0.0).alias("low"),
                pl.lit(0.0).alias("close"),
                pl.lit(0.0).alias("volume"),
            )
            .select(["date", "open", "high", "low", "close", "volume"])
            .sort("date")
            .unique(subset=["date"], keep="first", maintain_order=True)
            .drop_nulls(subset=["open"])
        )

    def _transform_mark_price(self, df: pl.DataFrame) -> pl.DataFrame:
        """Generate mark price feather from OHLCV candle data.

        Uses OHLCV as a mark price proxy (GMX doesn't provide a separate
        mark price feed).

        :param df: GMX candle Polars dataframe.
        :returns: Freqtrade-compatible mark price Polars dataframe.
        """
        return (
            df.rename({"timestamp": "date"})
            .with_columns(
                pl.col("date").dt.convert_time_zone("UTC").dt.cast_time_unit("ns"),
                pl.lit(0.0).alias("volume"),
            )
            .select(["date", "open", "high", "low", "close", "volume"])
            .sort("date")
            .unique(subset=["date"], keep="first", maintain_order=True)
        )

    # ------------------------------------------------------------------
    # File I/O
    # ------------------------------------------------------------------

    def _write(self, df: pl.DataFrame, path: Path, fmt: str) -> None:
        """Write Polars dataframe in the requested format.

        :param df: Polars dataframe to write.
        :param path: Output file path.
        :param fmt: ``'feather'`` or ``'parquet'``.
        """
        if fmt == "feather":
            df.write_ipc(path)
        else:
            df.write_parquet(str(path))

    # ------------------------------------------------------------------
    # Filename generation
    # ------------------------------------------------------------------

    def _get_freqtrade_filename(
        self,
        symbol: str,
        timeframe: str,
        fmt: str,
        trading_mode: str = "futures",
        quote_currency: str = "USDC",
        candle_type: str | None = None,
    ) -> str:
        """Generate Freqtrade-compatible filename.

        For futures OHLCV: ``{BASE}_{QUOTE}_{SETTLE}-{tf}-futures.{ext}``
        For funding rate: ``{BASE}_{QUOTE}_{SETTLE}-{tf}-funding_rate.{ext}``
        For mark price:   ``{BASE}_{QUOTE}_{SETTLE}-{tf}-mark.{ext}``

        :param symbol: Token symbol (e.g., ``'ETH'``).
        :param timeframe: Timeframe (e.g., ``'1h'``).
        :param fmt: File format (``'feather'`` or ``'parquet'``).
        :param trading_mode: ``'futures'`` or ``'spot'``.
        :param quote_currency: Quote/settlement currency.
        :param candle_type: Optional candle type suffix
            (``'funding_rate'``, ``'mark'``). ``None`` = standard OHLCV.
        :returns: Filename string.
        """
        base = f"{symbol}_{quote_currency}_{quote_currency}"

        if candle_type:
            return f"{base}-{timeframe}-{candle_type}.{fmt}"

        if trading_mode == "futures":
            return f"{base}-{timeframe}-futures.{fmt}"
        else:
            return f"{symbol}_{quote_currency}-{timeframe}.{fmt}"
```

- [ ] **Step 3: Run existing freqtrade exporter tests**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run python -m pytest tests/test_freqtrade_exporter.py -v
```

Expected: all 6 tests PASS (same as baseline).

- [ ] **Step 4: Run full test suite**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run python -m pytest tests/ -v \
  --ignore=tests/test_hybrid_collection.py \
  --ignore=tests/test_gmx_event_collector.py \
  --ignore=tests/test_integration_event_based.py \
  --ignore=tests/test_integration_gmx_first.py \
  --ignore=tests/test_gmx_market_mapper.py \
  --ignore=tests/test_gmx_token_discovery.py \
  --ignore=tests/test_live_funding.py \
  -q
```

Expected: same pass count as before tasks started. The 3 pre-existing failures in `test_freqtrade_exporter_timestamps.py` remain — do not fix them (out of scope).

- [ ] **Step 5: Lint and commit**

```bash
poetry run ruff check src/gmx_historical_data/freqtrade_exporter.py
poetry run ruff format --check src/gmx_historical_data/freqtrade_exporter.py
git add src/gmx_historical_data/freqtrade_exporter.py
git commit -m "perf: migrate freqtrade_exporter to full Polars (read/transform/write)"
```
