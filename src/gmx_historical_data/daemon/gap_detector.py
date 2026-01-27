"""Gap detection for identifying missing time ranges in collected data."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from enum import Enum

import pandas as pd

from gmx_historical_data.storage import ParquetStorage


logger = logging.getLogger(__name__)


class GapStatus(Enum):
    """Status of gap detection result.

    :cvar NO_GAP: Data is current, no fetch needed.
    :cvar NORMAL_GAP: Standard incremental fetch from our_latest to now.
    :cvar DATA_LOSS_GAP: Our data is older than API earliest - permanent data loss.
    :cvar NO_EXISTING_DATA: Fresh start, no existing data in storage.
    :cvar API_UNAVAILABLE: Could not query API, fallback mode.
    """

    NO_GAP = "no_gap"
    NORMAL_GAP = "normal_gap"
    DATA_LOSS_GAP = "data_loss_gap"
    NO_EXISTING_DATA = "no_existing_data"
    API_UNAVAILABLE = "api_unavailable"


@dataclass
class GapDetectionResult:
    """Result of adaptive gap detection.

    :param status: Gap status classification.
    :param fetch_start: Start datetime for fetching (None if no fetch needed).
    :param fetch_end: End datetime for fetching (None if no fetch needed).
    :param our_latest: Latest timestamp in our storage.
    :param api_earliest: Earliest timestamp available from API.
    :param api_latest: Latest timestamp available from API.
    :param lost_candles_estimate: Estimated number of candles permanently lost.
    :param lost_timespan: Human-readable timespan of lost data.
    """

    status: GapStatus
    fetch_start: datetime | None
    fetch_end: datetime | None
    our_latest: datetime | None = None
    api_earliest: datetime | None = None
    api_latest: datetime | None = None
    lost_candles_estimate: int = 0
    lost_timespan: str | None = None

    @property
    def needs_fetch(self) -> bool:
        """Check if data fetch is needed.

        :return: True if data should be fetched.
        """
        return self.status in (
            GapStatus.NORMAL_GAP,
            GapStatus.DATA_LOSS_GAP,
            GapStatus.NO_EXISTING_DATA,
            GapStatus.API_UNAVAILABLE,
        )

    @property
    def has_data_loss(self) -> bool:
        """Check if there is permanent data loss.

        :return: True if data has been permanently lost.
        """
        return self.status == GapStatus.DATA_LOSS_GAP


class GapDetector:
    """Detect gaps in existing OHLCV data to determine what needs collection.

    .. deprecated::
        Use :class:`AdaptiveGapDetector` instead. This class does not detect
        sliding window data loss from GMX API and will be removed in a future version.

    The simple GapDetector compares local storage against current time but does NOT
    query the GMX API to detect if data has been permanently lost due to the API's
    sliding window. Use AdaptiveGapDetector for production workloads.

    :param storage: ParquetStorage instance for reading existing data
    """

    # Timeframe intervals
    TIMEFRAME_DELTAS = {
        "1min": timedelta(minutes=1),
        "5min": timedelta(minutes=5),
        "15min": timedelta(minutes=15),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
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


class AdaptiveGapDetector:
    """Adaptive gap detector that queries GMX API to detect sliding window data loss.

    GMX API maintains a sliding data window - older data is permanently removed:

    | Timeframe | API Window | Safe Collection Interval |
    |-----------|------------|-------------------------|
    | 1min      | ~5 hours   | Every 2 hours           |
    | 5min      | ~34 days   | Every 10 days           |
    | 15min     | ~100 days  | Every 30 days           |
    | 1h        | ~416 days  | Every 180 days          |
    | 4h        | ~1,664 days| Every 365 days          |
    | 1D        | ~27 years  | Effectively never       |

    This detector compares our storage against the API's actual data range
    to detect permanent data loss scenarios.

    :param storage: ParquetStorage instance for reading existing data.
    :param gmx_fetcher: GMXDataFetcher instance for querying API.
    """

    # Timeframe intervals
    TIMEFRAME_DELTAS = {
        "1min": timedelta(minutes=1),
        "5min": timedelta(minutes=5),
        "15min": timedelta(minutes=15),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
    }

    def __init__(self, storage: ParquetStorage, gmx_fetcher):
        """Initialize adaptive gap detector.

        :param storage: ParquetStorage instance.
        :param gmx_fetcher: GMXDataFetcher instance.
        """
        self.storage = storage
        self.gmx_fetcher = gmx_fetcher

    def _format_timespan(self, delta: timedelta) -> str:
        """Format a timedelta as a human-readable string.

        :param delta: Timedelta to format.
        :return: Human-readable string (e.g., '3 days 4 hours').
        """
        total_seconds = int(delta.total_seconds())
        days = total_seconds // 86400
        hours = (total_seconds % 86400) // 3600
        minutes = (total_seconds % 3600) // 60

        parts = []
        if days > 0:
            parts.append(f"{days} day{'s' if days != 1 else ''}")
        if hours > 0:
            parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
        if minutes > 0 and days == 0:
            parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")

        return " ".join(parts) if parts else "0 minutes"

    def detect_gap_adaptive(
        self,
        symbol: str,
        timeframe: str,
    ) -> GapDetectionResult:
        """Detect time gap with API awareness to detect permanent data loss.

        Compares our storage against GMX API's actual data range:
        1. If our_latest is None -> NO_EXISTING_DATA, fetch all available
        2. If our_latest + interval >= now -> NO_GAP, skip
        3. If our_latest >= api_earliest -> NORMAL_GAP, fetch incrementally
        4. If our_latest < api_earliest -> DATA_LOSS_GAP, log warning, fetch available

        :param symbol: Token symbol (e.g., 'ETH').
        :param timeframe: Timeframe string (e.g., '1h').
        :return: GapDetectionResult with status and fetch boundaries.
        """
        from gmx_historical_data.gmx_api_integration import map_timeframe_to_gmx_period

        now = datetime.now(timezone.utc)
        interval_delta = self.TIMEFRAME_DELTAS.get(timeframe)
        if not interval_delta:
            raise ValueError(f"Unknown timeframe: {timeframe}")

        # Step 1: Read our storage
        existing_df = self.storage.read_candles(timeframe, symbol)

        our_latest = None
        if not existing_df.empty:
            our_latest = existing_df["timestamp"].max()
            if our_latest.tzinfo is None:
                our_latest = our_latest.replace(tzinfo=timezone.utc)

        # Step 2: Query GMX API for its data range
        gmx_period = map_timeframe_to_gmx_period(timeframe)
        api_earliest, api_latest = self.gmx_fetcher.get_latest_data_range(
            symbol, gmx_period
        )

        # Handle API unavailable
        if api_earliest is None or api_latest is None:
            logger.warning(
                f"GMX API unavailable for {symbol} {timeframe}, falling back to simple detection"
            )
            # Fall back to simple detection behavior
            if our_latest is None:
                return GapDetectionResult(
                    status=GapStatus.API_UNAVAILABLE,
                    fetch_start=None,
                    fetch_end=now,
                    our_latest=None,
                    api_earliest=None,
                    api_latest=None,
                )
            else:
                fetch_start = our_latest + interval_delta
                if fetch_start >= now:
                    return GapDetectionResult(
                        status=GapStatus.NO_GAP,
                        fetch_start=None,
                        fetch_end=None,
                        our_latest=our_latest,
                        api_earliest=None,
                        api_latest=None,
                    )
                return GapDetectionResult(
                    status=GapStatus.API_UNAVAILABLE,
                    fetch_start=fetch_start,
                    fetch_end=now,
                    our_latest=our_latest,
                    api_earliest=None,
                    api_latest=None,
                )

        # Case 1: No existing data - fresh start
        if our_latest is None:
            return GapDetectionResult(
                status=GapStatus.NO_EXISTING_DATA,
                fetch_start=api_earliest,
                fetch_end=now,
                our_latest=None,
                api_earliest=api_earliest,
                api_latest=api_latest,
            )

        # Case 2: Data is current - no gap
        fetch_start = our_latest + interval_delta
        if fetch_start >= now:
            return GapDetectionResult(
                status=GapStatus.NO_GAP,
                fetch_start=None,
                fetch_end=None,
                our_latest=our_latest,
                api_earliest=api_earliest,
                api_latest=api_latest,
            )

        # Case 3: Check for data loss
        if our_latest < api_earliest:
            # DATA LOSS: Our data is older than API's earliest available
            lost_timespan_delta = api_earliest - our_latest
            lost_candles = int(lost_timespan_delta / interval_delta)
            lost_timespan = self._format_timespan(lost_timespan_delta)

            logger.critical(
                f"DATA LOSS DETECTED: {symbol} {timeframe} - "
                f"Our latest: {our_latest.isoformat()}, "
                f"API earliest: {api_earliest.isoformat()}, "
                f"Lost: ~{lost_candles} candles ({lost_timespan})"
            )

            return GapDetectionResult(
                status=GapStatus.DATA_LOSS_GAP,
                fetch_start=api_earliest,
                fetch_end=now,
                our_latest=our_latest,
                api_earliest=api_earliest,
                api_latest=api_latest,
                lost_candles_estimate=lost_candles,
                lost_timespan=lost_timespan,
            )

        # Case 4: Normal gap - incremental fetch
        return GapDetectionResult(
            status=GapStatus.NORMAL_GAP,
            fetch_start=fetch_start,
            fetch_end=now,
            our_latest=our_latest,
            api_earliest=api_earliest,
            api_latest=api_latest,
        )
