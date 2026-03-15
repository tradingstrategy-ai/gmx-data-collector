"""Tests for oracle event aggregation — Polars migration equivalence."""

import types
from datetime import UTC, datetime

import pandas as pd
import pytest

from gmx_historical_data.oracle_event_aggregator import (
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


BASE_TS = int(datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC).timestamp())

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
    assert list(df.columns) == ["timestamp", "price"]


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
    """3 events spanning 2 minutes -> 2 candles."""
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
