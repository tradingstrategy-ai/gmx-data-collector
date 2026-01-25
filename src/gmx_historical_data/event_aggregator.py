"""Aggregate GMX position events to OHLCV candles.

Converts raw position events with execution prices into time-series
OHLCV data suitable for backtesting and analysis.
"""

import pandas as pd
from gmx_historical_data.gmx_event_parser import GMXPositionEvent


#: GMX USD precision (30 decimals)
GMX_USD_PRECISION = 10**30


def aggregate_events_to_ohlcv(
    events: list[GMXPositionEvent],
    timeframe: str,
    symbol: str,
) -> pd.DataFrame:
    """Convert position events to OHLCV candles.

    :param events: List of position events
    :param timeframe: Timeframe for resampling (e.g., "1min", "1h", "1D")
    :param symbol: Token symbol
    :return: DataFrame with OHLCV data including volume (sum of size_delta_usd)
    """
    if not events:
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume", "symbol"])

    # Convert events to DataFrame
    df = pd.DataFrame([
        {
            "timestamp": pd.Timestamp(e.block_timestamp, unit="s", tz="UTC"),
            "price": e.execution_price / GMX_USD_PRECISION,
            "size_usd": e.size_delta_usd / GMX_USD_PRECISION,
        }
        for e in events
    ])

    # Add original order column to ensure deterministic sorting
    df["original_order"] = range(len(df))

    # Sort by timestamp, then by original order for deterministic behavior
    df = df.sort_values(["timestamp", "original_order"])

    # Resample to OHLCV
    ohlcv = df.set_index("timestamp").resample(timeframe).agg({
        "price": ["first", "max", "min", "last"],
        "size_usd": "sum",
    })

    # Flatten column names
    ohlcv.columns = ["open", "high", "low", "close", "volume"]

    # Add symbol
    ohlcv["symbol"] = symbol

    # Drop rows with no data (NaN in all OHLC)
    ohlcv = ohlcv.dropna(subset=["open", "high", "low", "close"], how="all")

    # Reset index to make timestamp a column
    ohlcv = ohlcv.reset_index()

    # Drop the helper column used for deterministic sorting
    ohlcv = ohlcv.drop(columns=["original_order"], errors="ignore")

    return ohlcv
