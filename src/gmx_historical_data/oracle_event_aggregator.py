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
    "1d": "1d",
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

    price_df = build_oracle_price_dataframe(events, token_decimals)
    return resample_oracle_price_dataframe(price_df, timeframe, symbol)


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
            "timestamp": [pd.Timestamp(e.block_timestamp, unit="s", tz="UTC") for e in events],
            "price": [(e.min_price + e.max_price) / 2 / divisor for e in events],
            "original_order": list(range(len(events))),
        }
    )
    df = df.sort(["timestamp", "original_order"])

    return df.select(["timestamp", "price"]).to_pandas()


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

    ohlcv = (
        df.sort("timestamp")
        .group_by_dynamic("timestamp", every=polars_tf)
        .agg(
            [
                pl.first("price").alias("open"),
                pl.max("price").alias("high"),
                pl.min("price").alias("low"),
                pl.last("price").alias("close"),
            ]
        )
    )

    ohlcv = ohlcv.with_columns(pl.lit(symbol).alias("symbol"))
    ohlcv = ohlcv.drop_nulls(subset=["open", "high", "low", "close"])

    return ohlcv.to_pandas()
