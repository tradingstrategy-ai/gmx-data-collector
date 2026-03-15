"""Aggregate GMX position events to OHLCV candles.

Converts raw position events with execution prices into time-series
OHLCV data suitable for backtesting and analysis.
"""

import pandas as pd
import polars as pl

from gmx_historical_data.gmx_event_parser import GMXPositionEvent

#: GMX USD precision (30 decimals)
GMX_USD_PRECISION = 10**30

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

    polars_tf = _PANDAS_TO_POLARS_TIMEFRAME[timeframe]

    prices = [
        e.execution_price / GMX_USD_PRECISION
        if use_execution_price
        else (e.index_token_price_min + e.index_token_price_max) / 2 / GMX_USD_PRECISION
        for e in events
    ]

    df = pl.DataFrame(
        {
            "timestamp": [pd.Timestamp(e.block_timestamp, unit="s", tz="UTC") for e in events],
            "price": prices,
            "original_order": list(range(len(events))),
        }
    )
    df = df.sort(["timestamp", "original_order"])

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
