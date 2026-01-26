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
        # Return empty DataFrame with correct schema
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "symbol"])

    # Convert events to DataFrame
    if use_execution_price:
        # Use actual execution price (includes slippage) for backtesting
        # This reflects what you'd actually get when executing trades
        df = pd.DataFrame([
            {
                "timestamp": pd.Timestamp(e.block_timestamp, unit="s", tz="UTC"),
                "price": e.execution_price / GMX_USD_PRECISION,
            }
            for e in events
        ])
    else:
        # Use oracle mid-price: average of Chainlink's min/max prices
        # This gives clean market prices without price impact from executions
        df = pd.DataFrame([
            {
                "timestamp": pd.Timestamp(e.block_timestamp, unit="s", tz="UTC"),
                "price": (e.index_token_price_min + e.index_token_price_max) / 2 / GMX_USD_PRECISION,
            }
            for e in events
        ])

    # Add original order column to ensure deterministic sorting
    df["original_order"] = range(len(df))

    # Sort by timestamp, then by original order for deterministic behavior
    df = df.sort_values(["timestamp", "original_order"])

    # Resample to OHLC (no volume)
    ohlcv = df.set_index("timestamp").resample(timeframe).agg({
        "price": ["first", "max", "min", "last"],
    })

    # Flatten column names
    ohlcv.columns = ["open", "high", "low", "close"]

    # Add symbol
    ohlcv["symbol"] = symbol

    # Drop rows with no data (NaN in all OHLC)
    ohlcv = ohlcv.dropna(subset=["open", "high", "low", "close"], how="all")

    # Reset index to make timestamp a column
    ohlcv = ohlcv.reset_index()

    # Drop the helper column used for deterministic sorting
    ohlcv = ohlcv.drop(columns=["original_order"], errors="ignore")

    return ohlcv
