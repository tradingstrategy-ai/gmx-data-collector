"""Calculate fetch boundaries for smart incremental data collection.

This module determines what data ranges need to be fetched based on:
- Existing data in storage
- GMX API data availability
- Collection mode (FULL vs INCREMENTAL)
"""

from dataclasses import dataclass
from datetime import UTC, datetime

from gmx_historical_data.config import FetchMode
from gmx_historical_data.daemon.gap_detector import AdaptiveGapDetector, GapStatus
from gmx_historical_data.gap_analyzer import DataGapAnalyzer
from gmx_historical_data.storage import ParquetStorage


@dataclass
class FetchBoundaries:
    """Boundaries for data fetching across all sources.

    :param gmx_api_needed: Whether GMX API fetch is needed
    :param gmx_api_start: Start timestamp for GMX API (None = fetch all available)
    :param gmx_api_end: End timestamp for GMX API (usually now)
    :param chainlink_needed: Whether Chainlink backfill is needed
    :param chainlink_start_timestamp: Start timestamp for Chainlink (None = fetch all)
    :param chainlink_end_timestamp: End timestamp for Chainlink (stop before GMX coverage)
    :param oracle_needed: Whether oracle events are needed
    :param oracle_start_block: Start block for oracle events
    :param oracle_end_block: End block for oracle events
    :param mode: Collection mode (FULL, INCREMENTAL, NO_FETCH)
    """

    gmx_api_needed: bool
    gmx_api_start: datetime | None
    gmx_api_end: datetime | None
    chainlink_needed: bool
    chainlink_start_timestamp: int | None
    chainlink_end_timestamp: int | None
    oracle_needed: bool
    oracle_start_block: int | None
    oracle_end_block: int | None
    mode: FetchMode

    @property
    def needs_any_fetch(self) -> bool:
        """Check if any data fetch is needed.

        :return: True if GMX API, Chainlink, or Oracle fetch is required
        """
        return self.gmx_api_needed or self.chainlink_needed or self.oracle_needed


class FetchBoundaryCalculator:
    """Calculate fetch boundaries for incremental and full collection modes.

    This class integrates with AdaptiveGapDetector to determine what data
    ranges need to be fetched, enabling efficient incremental updates.

    :param storage: ParquetStorage instance for reading existing data
    :param adaptive_gap_detector: AdaptiveGapDetector instance (optional)
    :param gap_analyzer: DataGapAnalyzer instance for backfill calculation
    """

    def __init__(
        self,
        storage: ParquetStorage,
        adaptive_gap_detector: AdaptiveGapDetector | None,
        gap_analyzer: DataGapAnalyzer,
    ):
        """Initialize fetch boundary calculator.

        :param storage: ParquetStorage instance
        :param adaptive_gap_detector: AdaptiveGapDetector instance (optional)
        :param gap_analyzer: DataGapAnalyzer instance
        """
        self.storage = storage
        self.adaptive_gap_detector = adaptive_gap_detector
        self.gap_analyzer = gap_analyzer

    def calculate_boundaries(
        self,
        symbol: str,
        timeframe: str,
        mode: FetchMode,
        chainlink_available: bool,
        gmx_earliest: datetime | None = None,
        force: bool = False,
    ) -> FetchBoundaries:
        """Calculate fetch boundaries based on mode and existing data.

        :param symbol: Token symbol (e.g., 'ETH')
        :param timeframe: Timeframe string (e.g., '1h')
        :param mode: FULL or INCREMENTAL
        :param chainlink_available: Whether Chainlink feed exists
        :param gmx_earliest: Earliest timestamp from GMX API query (optional)
        :param force: If True, short-circuit to full (genesis) boundaries
            regardless of stored data or requested mode. Used by ``--force`` to
            re-fetch everything from genesis.
        :return: FetchBoundaries with all source ranges
        """
        if force or mode == FetchMode.FULL:
            return self._calculate_full_boundaries(
                symbol, timeframe, chainlink_available, gmx_earliest
            )
        else:  # INCREMENTAL
            return self._calculate_incremental_boundaries(
                symbol, timeframe, chainlink_available, gmx_earliest
            )

    def _calculate_full_boundaries(
        self,
        symbol: str,
        timeframe: str,
        chainlink_available: bool,
        gmx_earliest: datetime | None,
    ) -> FetchBoundaries:
        """Calculate boundaries for full collection mode.

        Full mode: Fetch ALL available data
        - GMX API: Full window (~6 months)
        - Chainlink: All historical data before GMX coverage

        :param symbol: Token symbol
        :param timeframe: Timeframe string
        :param chainlink_available: Whether Chainlink feed exists
        :param gmx_earliest: Earliest timestamp from GMX API
        :return: FetchBoundaries for full collection
        """
        now = datetime.now(UTC)

        # GMX API: Always fetch full available window
        gmx_needed = True
        gmx_start = None  # Fetch all available
        gmx_end = now

        # Chainlink: Backfill all historical data before GMX coverage
        chainlink_needed = chainlink_available
        chainlink_start = None  # Fetch all available
        chainlink_end = None

        if chainlink_available and gmx_earliest:
            # Stop Chainlink backfill just before GMX coverage starts
            chainlink_end = int(gmx_earliest.timestamp()) - 1

        return FetchBoundaries(
            gmx_api_needed=gmx_needed,
            gmx_api_start=gmx_start,
            gmx_api_end=gmx_end,
            chainlink_needed=chainlink_needed,
            chainlink_start_timestamp=chainlink_start,
            chainlink_end_timestamp=chainlink_end,
            oracle_needed=False,  # Full mode doesn't use oracle events for Chainlink symbols
            oracle_start_block=None,
            oracle_end_block=None,
            mode=FetchMode.FULL,
        )

    def _calculate_incremental_boundaries(
        self,
        symbol: str,
        timeframe: str,
        chainlink_available: bool,
        gmx_earliest: datetime | None,
    ) -> FetchBoundaries:
        """Calculate boundaries for incremental update mode.

        Logic:
        1. Use AdaptiveGapDetector to check existing data status
        2. If NO_GAP: Skip all fetching
        3. If NORMAL_GAP: Fetch only from our_latest + interval to now
        4. If DATA_LOSS_GAP: Fetch from api_earliest (lost data) to now
        5. If NO_EXISTING_DATA: Fall back to full collection

        For Chainlink backfill:
        - Only fetch if our earliest > gmx_earliest (we need older data)
        - Fetch range: chainlink_start=None, chainlink_end=our_earliest-1

        :param symbol: Token symbol
        :param timeframe: Timeframe string
        :param chainlink_available: Whether Chainlink feed exists
        :param gmx_earliest: Earliest timestamp from GMX API
        :return: FetchBoundaries for incremental collection
        """
        # Use adaptive gap detector if available
        if self.adaptive_gap_detector:
            gap_result = self.adaptive_gap_detector.detect_gap_adaptive(symbol, timeframe)

            if gap_result.status == GapStatus.NO_GAP:
                # Data is current - skip all fetching
                return FetchBoundaries(
                    gmx_api_needed=False,
                    gmx_api_start=None,
                    gmx_api_end=None,
                    chainlink_needed=False,
                    chainlink_start_timestamp=None,
                    chainlink_end_timestamp=None,
                    oracle_needed=False,
                    oracle_start_block=None,
                    oracle_end_block=None,
                    mode=FetchMode.NO_FETCH,
                )

            elif gap_result.status == GapStatus.NORMAL_GAP:
                # Incremental fetch from our_latest to now
                now = datetime.now(UTC)

                # GMX API: Only fetch recent data
                gmx_needed = True
                gmx_start = gap_result.fetch_start  # our_latest + interval
                gmx_end = now

                # Chainlink: Check if we need older historical data
                existing_df = self.storage.read_candles(timeframe, symbol)
                our_earliest = None
                if not existing_df.empty:
                    our_earliest = existing_df["timestamp"].min()
                    if our_earliest.tzinfo is None:
                        our_earliest = our_earliest.replace(tzinfo=UTC)

                # Only backfill if we have GMX data but no older Chainlink data
                chainlink_needed = False
                chainlink_start = None
                chainlink_end = None
                if chainlink_available and gmx_earliest and our_earliest:
                    if our_earliest > gmx_earliest:
                        # We need historical data before our storage. Bound the
                        # start to the gap analyzer's feed floor (not None /
                        # genesis) so we only re-fetch the missing older slice.
                        chainlink_needed = True
                        chainlink_start = self.gap_analyzer.feed_floor_timestamp
                        chainlink_end = int(our_earliest.timestamp()) - 1

                return FetchBoundaries(
                    gmx_api_needed=gmx_needed,
                    gmx_api_start=gmx_start,
                    gmx_api_end=gmx_end,
                    chainlink_needed=chainlink_needed,
                    chainlink_start_timestamp=chainlink_start,  # Bounded feed floor
                    chainlink_end_timestamp=chainlink_end,
                    oracle_needed=False,  # Incremental mode doesn't use oracle events
                    oracle_start_block=None,
                    oracle_end_block=None,
                    mode=FetchMode.INCREMENTAL,
                )

            elif gap_result.status == GapStatus.DATA_LOSS_GAP:
                # Data loss detected - fetch from API's earliest
                now = datetime.now(UTC)

                return FetchBoundaries(
                    gmx_api_needed=True,
                    gmx_api_start=gap_result.api_earliest,
                    gmx_api_end=now,
                    chainlink_needed=chainlink_available,
                    chainlink_start_timestamp=(
                        # Bound the backfill start to the feed floor (not genesis)
                        # whenever a backfill is actually requested.
                        self.gap_analyzer.feed_floor_timestamp if chainlink_available else None
                    ),
                    chainlink_end_timestamp=(
                        int(gap_result.api_earliest.timestamp()) - 1
                        if gap_result.api_earliest
                        else None
                    ),
                    oracle_needed=False,
                    oracle_start_block=None,
                    oracle_end_block=None,
                    mode=FetchMode.INCREMENTAL,
                )

            else:  # NO_EXISTING_DATA or API_UNAVAILABLE
                # Only fall back to full (genesis) collection when storage truly
                # has no rows for this symbol. A symbol that DOES hold candles
                # must not be re-walked from genesis just because the adaptive
                # detector mis-reported NO_EXISTING_DATA / API_UNAVAILABLE.
                existing_df = self.storage.read_candles(timeframe, symbol)
                if existing_df.empty:
                    return self._calculate_full_boundaries(
                        symbol, timeframe, chainlink_available, gmx_earliest
                    )

                # We have stored data: treat as incremental. Bound any Chainlink
                # backfill to the feed floor and only request it when GMX has
                # older history than we store.
                now = datetime.now(UTC)
                our_earliest = existing_df["timestamp"].min()
                if our_earliest.tzinfo is None:
                    our_earliest = our_earliest.replace(tzinfo=UTC)

                chainlink_needed = False
                chainlink_start = None
                chainlink_end = None
                if chainlink_available and gmx_earliest and our_earliest > gmx_earliest:
                    chainlink_needed = True
                    chainlink_start = self.gap_analyzer.feed_floor_timestamp
                    chainlink_end = int(our_earliest.timestamp()) - 1

                return FetchBoundaries(
                    gmx_api_needed=True,
                    gmx_api_start=None,  # Fetch full available GMX window
                    gmx_api_end=now,
                    chainlink_needed=chainlink_needed,
                    chainlink_start_timestamp=chainlink_start,
                    chainlink_end_timestamp=chainlink_end,
                    oracle_needed=False,
                    oracle_start_block=None,
                    oracle_end_block=None,
                    mode=FetchMode.INCREMENTAL,
                )

        # Fallback if no adaptive detector - use full collection
        return self._calculate_full_boundaries(symbol, timeframe, chainlink_available, gmx_earliest)
