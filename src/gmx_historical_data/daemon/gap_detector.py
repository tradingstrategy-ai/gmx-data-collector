"""Gap detection for identifying missing time ranges in collected data."""

import pandas as pd
from datetime import datetime, timezone, timedelta
from gmx_historical_data.storage import ParquetStorage


class GapDetector:
    """Detect gaps in existing OHLCV data to determine what needs collection.

    :param storage: ParquetStorage instance for reading existing data
    """

    # Timeframe intervals
    TIMEFRAME_DELTAS = {
        "1min": timedelta(minutes=1),
        "5min": timedelta(minutes=5),
        "15min": timedelta(minutes=15),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1D": timedelta(days=1),
    }

    def __init__(self, storage: ParquetStorage):
        """Initialize gap detector.

        :param storage: ParquetStorage instance
        """
        self.storage = storage

    def detect_gap(
        self,
        symbol: str,
        timeframe: str,
    ) -> tuple[datetime | None, datetime]:
        """Detect time gap for a symbol/timeframe.

        Returns the time range that needs to be fetched from GMX API.

        :param symbol: Token symbol (e.g., 'ETH')
        :param timeframe: Timeframe string (e.g., '1h')
        :return: Tuple of (fetch_start_datetime, fetch_end_datetime)
                 - fetch_start is None if no existing data (fetch all available)
                 - fetch_end is always current time
        """
        # Read existing candles
        existing_df = self.storage.read_candles(timeframe, symbol)

        # Current time as end of gap
        fetch_end = datetime.now(timezone.utc)

        if existing_df.empty:
            # No existing data - fetch all available from GMX API
            return None, fetch_end

        # Get latest existing timestamp
        latest_timestamp = existing_df["timestamp"].max()

        # Ensure latest_timestamp is timezone-aware
        if latest_timestamp.tzinfo is None:
            latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)

        # Calculate fetch start: latest + 1 interval
        interval_delta = self.TIMEFRAME_DELTAS.get(timeframe)
        if not interval_delta:
            raise ValueError(f"Unknown timeframe: {timeframe}")

        fetch_start = latest_timestamp + interval_delta

        # If fetch_start is in the future, no gap exists
        if fetch_start >= fetch_end:
            # Return None, None to indicate no gap
            return None, None

        return fetch_start, fetch_end

    def has_gap(self, symbol: str, timeframe: str) -> bool:
        """Check if a gap exists for a symbol/timeframe.

        :param symbol: Token symbol
        :param timeframe: Timeframe string
        :return: True if gap exists (data needs collection)
        """
        fetch_start, fetch_end = self.detect_gap(symbol, timeframe)

        # No gap if both are None
        if fetch_start is None and fetch_end is None:
            return False

        # No gap if fetch_start is None but we just created the data
        # (this shouldn't happen but handle gracefully)
        if fetch_start is None:
            return True  # No existing data = gap exists

        # Gap exists if start < end
        return fetch_start < fetch_end

    def get_gap_info(
        self, symbol: str, timeframe: str
    ) -> dict[str, datetime | None | int]:
        """Get detailed gap information.

        :param symbol: Token symbol
        :param timeframe: Timeframe string
        :return: Dictionary with gap details
        """
        existing_df = self.storage.read_candles(timeframe, symbol)
        fetch_start, fetch_end = self.detect_gap(symbol, timeframe)

        if fetch_start is None and fetch_end is None:
            # No gap
            return {
                "has_gap": False,
                "existing_candles": len(existing_df),
                "latest_timestamp": (
                    existing_df["timestamp"].max() if not existing_df.empty else None
                ),
                "fetch_start": None,
                "fetch_end": None,
                "estimated_missing_candles": 0,
            }

        # Calculate estimated missing candles
        estimated_missing = 0
        if fetch_start and fetch_end:
            interval_delta = self.TIMEFRAME_DELTAS[timeframe]
            time_diff = fetch_end - fetch_start
            estimated_missing = int(time_diff / interval_delta)

        return {
            "has_gap": True,
            "existing_candles": len(existing_df),
            "latest_timestamp": (
                existing_df["timestamp"].max() if not existing_df.empty else None
            ),
            "fetch_start": fetch_start,
            "fetch_end": fetch_end,
            "estimated_missing_candles": estimated_missing,
        }
