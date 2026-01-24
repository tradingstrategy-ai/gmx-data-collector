"""Resample raw Chainlink events to OHLCV candles.

Converts tick data (individual price updates) to OHLCV candles at
various timeframes with forward-fill for gaps during low volatility.
"""

import pandas as pd
import pyarrow as pa

from gmx_historical_data.config import TIMEFRAMES
from gmx_historical_data.event_decoder import scale_price


class OHLCVResampler:
    """Resample raw events to OHLCV candles.

    :param decimals: Number of decimals for price scaling (default: 8 for USD pairs)
    """

    def __init__(self, decimals: int = 8):
        """Initialize OHLCV resampler.

        :param decimals: Decimals for Chainlink price feed
        """
        self.decimals = decimals

    def prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare raw events DataFrame for resampling.

        :param df: Raw events DataFrame
        :return: Prepared DataFrame with scaled prices and datetime index
        """
        if df.empty:
            return df

        # Create a copy to avoid modifying original
        df = df.copy()

        # Scale prices to human-readable values
        df["price_scaled"] = df["price"].apply(lambda x: scale_price(x, self.decimals))

        # Convert timestamp to datetime
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

        # Sort by timestamp
        df = df.sort_values("datetime")

        # Set datetime as index
        df = df.set_index("datetime")

        return df

    def resample_to_ohlcv(
        self,
        df: pd.DataFrame,
        timeframe: str,
        forward_fill: bool = True,
    ) -> pd.DataFrame:
        """Resample tick data to OHLCV candles.

        :param df: Prepared DataFrame with price_scaled and datetime index
        :param timeframe: Pandas-compatible timeframe string (e.g., '1min', '1H', '1D')
        :param forward_fill: Fill gaps with last known price
        :return: OHLCV DataFrame
        """
        if df.empty:
            return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close"])

        # Resample to OHLCV
        ohlc = df["price_scaled"].resample(timeframe).ohlc()

        # Forward-fill missing values if requested
        if forward_fill:
            ohlc = ohlc.ffill()

        # Drop rows with NaN (if any remain)
        ohlc = ohlc.dropna()

        # Reset index to have timestamp as column
        ohlc = ohlc.reset_index()
        ohlc = ohlc.rename(columns={"datetime": "timestamp"})

        return ohlc

    def resample_all_timeframes(
        self,
        df: pd.DataFrame,
        symbol: str,
    ) -> dict[str, pd.DataFrame]:
        """Resample to all configured timeframes.

        :param df: Raw events DataFrame
        :param symbol: Token symbol
        :return: Dictionary mapping timeframe -> OHLCV DataFrame
        """
        # Prepare DataFrame
        prepared = self.prepare_dataframe(df)

        if prepared.empty:
            return {}

        # Resample to each timeframe
        results = {}
        for timeframe in TIMEFRAMES:
            ohlc = self.resample_to_ohlcv(prepared, timeframe)

            if not ohlc.empty:
                # Add symbol column
                ohlc["symbol"] = symbol
                results[timeframe] = ohlc

        return results

    def combine_symbols(
        self,
        dataframes: list[pd.DataFrame],
    ) -> pd.DataFrame:
        """Combine OHLCV data from multiple symbols.

        :param dataframes: List of OHLCV DataFrames with 'symbol' column
        :return: Combined DataFrame
        """
        if not dataframes:
            return pd.DataFrame()

        # Filter out empty DataFrames
        dataframes = [df for df in dataframes if not df.empty]

        if not dataframes:
            return pd.DataFrame()

        # Concatenate all DataFrames
        combined = pd.concat(dataframes, ignore_index=True)

        # Sort by timestamp and symbol
        combined = combined.sort_values(["timestamp", "symbol"])

        return combined


def resample_raw_events(
    df: pd.DataFrame,
    symbol: str,
    decimals: int = 8,
) -> dict[str, pd.DataFrame]:
    """Convenience function to resample raw events.

    :param df: Raw events DataFrame from ParquetStorage
    :param symbol: Token symbol
    :param decimals: Chainlink feed decimals (default: 8)
    :return: Dictionary mapping timeframe -> OHLCV DataFrame
    """
    resampler = OHLCVResampler(decimals=decimals)
    return resampler.resample_all_timeframes(df, symbol)
