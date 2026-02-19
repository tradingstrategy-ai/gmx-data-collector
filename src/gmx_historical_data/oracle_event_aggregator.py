"""Aggregate GMX oracle price events to OHLCV candles.

Converts raw oracle price update events into time-series
OHLCV data suitable for backtesting and analysis.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from gmx_historical_data.oracle_price_collector import OraclePriceEvent


#: GMX internal precision (30 decimals)
#: Price formula: human_price = raw / 10^(30 - token_decimals)
GMX_INTERNAL_PRECISION = 30


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
