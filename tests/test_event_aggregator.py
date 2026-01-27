"""Tests for event aggregation to OHLCV."""

import pytest
import pandas as pd
from datetime import datetime, timezone
from gmx_historical_data.event_aggregator import (
    aggregate_events_to_ohlcv,
    GMX_USD_PRECISION,
)
from gmx_historical_data.gmx_event_parser import GMXPositionEvent


def test_aggregate_events_to_ohlcv():
    """Test aggregating position events to OHLCV candles."""
    # Create mock events with different prices
    base_ts = int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())

    events = [
        GMXPositionEvent(
            block_number=1,
            block_timestamp=base_ts,  # 12:00:00
            transaction_hash="0x1" + "0" * 62,
            log_index=0,
            event_name="PositionIncrease",
            market="0xmarket",
            account="0xacc1",
            is_long=True,
            index_token_price_min=2995 * GMX_USD_PRECISION,  # Oracle min
            index_token_price_max=3005 * GMX_USD_PRECISION,  # Oracle max (mid = 3000)
            execution_price=3000 * GMX_USD_PRECISION,  # $3000
            size_delta_usd=1000 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey1",
            collateral_token="0xcoll",
        ),
        GMXPositionEvent(
            block_number=2,
            block_timestamp=base_ts + 30,  # 12:00:30
            transaction_hash="0x2" + "0" * 62,
            log_index=0,
            event_name="PositionIncrease",
            market="0xmarket",
            account="0xacc2",
            is_long=False,
            index_token_price_min=3095 * GMX_USD_PRECISION,  # Oracle min
            index_token_price_max=3105 * GMX_USD_PRECISION,  # Oracle max (mid = 3100)
            execution_price=3100 * GMX_USD_PRECISION,  # $3100 (high)
            size_delta_usd=500 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey2",
            collateral_token="0xcoll",
        ),
        GMXPositionEvent(
            block_number=3,
            block_timestamp=base_ts + 60,  # 12:01:00 (next minute)
            transaction_hash="0x3" + "0" * 62,
            log_index=0,
            event_name="PositionDecrease",
            market="0xmarket",
            account="0xacc3",
            is_long=True,
            index_token_price_min=2945 * GMX_USD_PRECISION,  # Oracle min
            index_token_price_max=2955 * GMX_USD_PRECISION,  # Oracle max (mid = 2950)
            execution_price=2950 * GMX_USD_PRECISION,  # $2950 (low)
            size_delta_usd=200 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey3",
            collateral_token="0xcoll",
        ),
    ]

    # Aggregate to 1-minute candles
    ohlcv = aggregate_events_to_ohlcv(events, timeframe="1min", symbol="ETH")

    # Should have 2 candles (12:00 and 12:01)
    assert len(ohlcv) == 2

    # First candle (12:00)
    candle1 = ohlcv.iloc[0]
    assert candle1["open"] == 3000.0  # First trade
    assert candle1["high"] == 3100.0  # Max in bucket
    assert candle1["low"] == 3000.0  # Min in bucket
    assert candle1["close"] == 3100.0  # Last trade
    assert candle1["symbol"] == "ETH"

    # Second candle (12:01)
    candle2 = ohlcv.iloc[1]
    assert candle2["open"] == 2950.0
    assert candle2["close"] == 2950.0


def test_aggregate_empty_events():
    """Test aggregating empty event list."""
    ohlcv = aggregate_events_to_ohlcv([], timeframe="1min", symbol="BTC")

    # Should return empty DataFrame with correct columns
    assert len(ohlcv) == 0
    assert "open" in ohlcv.columns
    assert "high" in ohlcv.columns
    assert "low" in ohlcv.columns
    assert "close" in ohlcv.columns
    assert "symbol" in ohlcv.columns


def test_aggregate_single_event():
    """Test aggregating a single event."""
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
            index_token_price_min=2995 * GMX_USD_PRECISION,  # Oracle min
            index_token_price_max=3005 * GMX_USD_PRECISION,  # Oracle max (mid = 3000)
            execution_price=3000 * GMX_USD_PRECISION,
            size_delta_usd=1000 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey1",
            collateral_token="0xcoll",
        ),
    ]

    ohlcv = aggregate_events_to_ohlcv(events, timeframe="1min", symbol="BTC")

    # Should have 1 candle
    assert len(ohlcv) == 1

    candle = ohlcv.iloc[0]
    assert candle["open"] == 3000.0
    assert candle["high"] == 3000.0
    assert candle["low"] == 3000.0
    assert candle["close"] == 3000.0
    assert candle["symbol"] == "BTC"


def test_aggregate_identical_timestamps():
    """Test aggregating events with identical timestamps."""
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
            index_token_price_min=2995 * GMX_USD_PRECISION,  # Oracle min
            index_token_price_max=3005 * GMX_USD_PRECISION,  # Oracle max (mid = 3000)
            execution_price=3000 * GMX_USD_PRECISION,
            size_delta_usd=100 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey1",
            collateral_token="0xcoll",
        ),
        GMXPositionEvent(
            block_number=1,
            block_timestamp=base_ts,  # Same timestamp
            transaction_hash="0x2" + "0" * 62,
            log_index=1,
            event_name="PositionIncrease",
            market="0xmarket",
            account="0xacc2",
            is_long=False,
            index_token_price_min=3095 * GMX_USD_PRECISION,  # Oracle min
            index_token_price_max=3105 * GMX_USD_PRECISION,  # Oracle max (mid = 3100)
            execution_price=3100 * GMX_USD_PRECISION,
            size_delta_usd=200 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey2",
            collateral_token="0xcoll",
        ),
        GMXPositionEvent(
            block_number=1,
            block_timestamp=base_ts,  # Same timestamp
            transaction_hash="0x3" + "0" * 62,
            log_index=2,
            event_name="PositionDecrease",
            market="0xmarket",
            account="0xacc3",
            is_long=True,
            index_token_price_min=2945 * GMX_USD_PRECISION,  # Oracle min
            index_token_price_max=2955 * GMX_USD_PRECISION,  # Oracle max (mid = 2950)
            execution_price=2950 * GMX_USD_PRECISION,
            size_delta_usd=150 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey3",
            collateral_token="0xcoll",
        ),
    ]

    ohlcv = aggregate_events_to_ohlcv(events, timeframe="1min", symbol="ETH")

    # Should have 1 candle (all in same minute)
    assert len(ohlcv) == 1

    candle = ohlcv.iloc[0]
    assert candle["open"] == 3000.0  # First
    assert candle["high"] == 3100.0  # Max
    assert candle["low"] == 2950.0  # Min
    assert candle["close"] == 2950.0  # Last
    assert candle["symbol"] == "ETH"


def test_aggregate_unsorted_events():
    """Test aggregating events that are not sorted by timestamp."""
    base_ts = int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc).timestamp())

    events = [
        # Events in reverse chronological order
        GMXPositionEvent(
            block_number=3,
            block_timestamp=base_ts + 120,  # 12:02:00 (latest)
            transaction_hash="0x3" + "0" * 62,
            log_index=0,
            event_name="PositionDecrease",
            market="0xmarket",
            account="0xacc3",
            is_long=True,
            index_token_price_min=2895 * GMX_USD_PRECISION,  # Oracle min
            index_token_price_max=2905 * GMX_USD_PRECISION,  # Oracle max (mid = 2900)
            execution_price=2900 * GMX_USD_PRECISION,
            size_delta_usd=300 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey3",
            collateral_token="0xcoll",
        ),
        GMXPositionEvent(
            block_number=2,
            block_timestamp=base_ts + 60,  # 12:01:00 (middle)
            transaction_hash="0x2" + "0" * 62,
            log_index=0,
            event_name="PositionIncrease",
            market="0xmarket",
            account="0xacc2",
            is_long=False,
            index_token_price_min=3095 * GMX_USD_PRECISION,  # Oracle min
            index_token_price_max=3105 * GMX_USD_PRECISION,  # Oracle max (mid = 3100)
            execution_price=3100 * GMX_USD_PRECISION,
            size_delta_usd=200 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey2",
            collateral_token="0xcoll",
        ),
        GMXPositionEvent(
            block_number=1,
            block_timestamp=base_ts,  # 12:00:00 (earliest)
            transaction_hash="0x1" + "0" * 62,
            log_index=0,
            event_name="PositionIncrease",
            market="0xmarket",
            account="0xacc1",
            is_long=True,
            index_token_price_min=2995 * GMX_USD_PRECISION,  # Oracle min
            index_token_price_max=3005 * GMX_USD_PRECISION,  # Oracle max (mid = 3000)
            execution_price=3000 * GMX_USD_PRECISION,
            size_delta_usd=100 * GMX_USD_PRECISION,
            size_delta_in_tokens=0,
            price_impact_usd=0,
            position_key="0xkey1",
            collateral_token="0xcoll",
        ),
    ]

    ohlcv = aggregate_events_to_ohlcv(events, timeframe="1min", symbol="BTC")

    # Should have 3 candles (one per minute)
    assert len(ohlcv) == 3

    # First candle should use the earliest timestamp event
    candle1 = ohlcv.iloc[0]
    assert candle1["open"] == 3000.0
    assert candle1["close"] == 3000.0

    # Second candle
    candle2 = ohlcv.iloc[1]
    assert candle2["open"] == 3100.0
    assert candle2["close"] == 3100.0

    # Third candle
    candle3 = ohlcv.iloc[2]
    assert candle3["open"] == 2900.0
    assert candle3["close"] == 2900.0
