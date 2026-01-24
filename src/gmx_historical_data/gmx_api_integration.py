"""GMX API integration for fetching latest price data.

This module fetches recent price data from GMX's official API and combines it
with historical Chainlink oracle data to provide complete coverage.
"""

import pandas as pd
from datetime import datetime, timezone
from eth_defi.gmx.api import GMXAPI


class GMXDataFetcher:
    """Fetches latest price data from GMX API."""

    def __init__(self, chain: str = "arbitrum") -> None:
        """Initialize GMX API client.

        :param chain: Blockchain network (default: arbitrum)
        """
        self.api = GMXAPI(chain=chain)
        self.chain = chain

    def get_latest_data_range(
        self,
        symbol: str,
        period: str = "1h",
    ) -> tuple[datetime | None, datetime | None]:
        """Fetch GMX data and return the date range available.

        :param symbol: Token symbol (e.g., 'ETH', 'BTC')
        :param period: Timeframe period (e.g., '1h', '4h', '1D')
        :return: Tuple of (earliest_timestamp, latest_timestamp) or (None, None) if no data
        """
        try:
            # Fetch maximum available data
            df = self.api.get_candlesticks_dataframe(symbol, period=period, limit=10000)

            if df.empty:
                return None, None

            earliest = df["timestamp"].min()
            latest = df["timestamp"].max()

            # Convert to timezone-aware datetime
            if earliest.tzinfo is None:
                earliest = earliest.replace(tzinfo=timezone.utc)
            if latest.tzinfo is None:
                latest = latest.replace(tzinfo=timezone.utc)

            return earliest, latest

        except Exception as e:
            print(f"  Warning: Could not fetch GMX data for {symbol}: {e}")
            return None, None

    def fetch_gmx_candles(
        self,
        symbol: str,
        period: str = "1h",
        limit: int = 10000,
    ) -> pd.DataFrame:
        """Fetch all available GMX candlestick data for a symbol.

        :param symbol: Token symbol (e.g., 'ETH', 'BTC')
        :param period: Timeframe period (e.g., '1h', '4h', '1D')
        :param limit: Maximum number of candles to fetch
        :return: DataFrame with columns: timestamp, open, high, low, close, symbol
        """
        try:
            df = self.api.get_candlesticks_dataframe(symbol, period=period, limit=limit)

            if df.empty:
                return pd.DataFrame()

            # Ensure timezone-aware timestamps
            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(timezone.utc)

            # Add symbol column for consistency with Chainlink data
            df["symbol"] = symbol

            # Reorder columns to match expected schema
            df = df[["timestamp", "open", "high", "low", "close", "symbol"]]

            return df

        except Exception as e:
            print(f"  Warning: Could not fetch GMX candles for {symbol}: {e}")
            return pd.DataFrame()

    def get_supported_periods(self) -> list[str]:
        """Get list of supported timeframe periods.

        :return: List of period strings (e.g., ['1h', '4h', '1D'])
        """
        # GMX API supports these common periods
        # Based on testing and typical exchange offerings
        return ["1m", "5m", "15m", "1h", "4h", "1D"]


def combine_gmx_and_chainlink_data(
    gmx_df: pd.DataFrame,
    chainlink_df: pd.DataFrame,
) -> pd.DataFrame:
    """Combine GMX API data with Chainlink oracle data.

    GMX provides recent data, Chainlink provides historical data.
    This function merges them, preferring GMX data for overlapping periods.

    :param gmx_df: DataFrame from GMX API with columns: timestamp, open, high, low, close, symbol
    :param chainlink_df: DataFrame from Chainlink with same columns
    :return: Combined DataFrame sorted by timestamp
    """
    if gmx_df.empty and chainlink_df.empty:
        return pd.DataFrame()

    if gmx_df.empty:
        return chainlink_df.copy()

    if chainlink_df.empty:
        return gmx_df.copy()

    # Get the earliest GMX timestamp to determine cutoff
    gmx_earliest = gmx_df["timestamp"].min()

    # Filter Chainlink data to only include data before GMX coverage
    chainlink_historical = chainlink_df[chainlink_df["timestamp"] < gmx_earliest].copy()

    # Combine datasets
    combined = pd.concat([chainlink_historical, gmx_df], ignore_index=True)

    # Sort by timestamp
    combined = combined.sort_values("timestamp").reset_index(drop=True)

    # Remove any duplicates (prefer GMX data)
    combined = combined.drop_duplicates(subset=["timestamp"], keep="last")

    return combined


def map_timeframe_to_gmx_period(timeframe: str) -> str:
    """Map internal timeframe notation to GMX API period notation.

    :param timeframe: Internal timeframe (e.g., '1min', '5min', '1h', '4h', '1D')
    :return: GMX API period string (e.g., '1m', '5m', '1h', '4h', '1d')
    """
    # GMX uses 'm' for minutes instead of 'min' and lowercase 'd' for days
    mapping = {
        "1min": "1m",
        "5min": "5m",
        "15min": "15m",
        "1h": "1h",
        "4h": "4h",
        "1D": "1d",  # GMX API expects lowercase 'd'
    }
    return mapping.get(timeframe, timeframe)
