"""Analyze data gaps between GMX and Chainlink data sources."""

from datetime import UTC

import pandas as pd

from gmx_historical_data.config import FetchMode


class DataGapAnalyzer:
    """Calculate what historical data needs to be backfilled.

    Now supports both FULL and INCREMENTAL modes for smart data collection.
    """

    def calculate_gap(
        self,
        gmx_df: pd.DataFrame,
        chainlink_available: bool,
        gmx_v2_genesis_block: int = 120_000_000,
        mode: FetchMode = FetchMode.FULL,
        existing_df: pd.DataFrame | None = None,
    ) -> tuple[int | None, int | None]:
        """Calculate the gap that needs to be filled with Chainlink data.

        Supports both FULL and INCREMENTAL collection modes:
        - FULL: Collect ALL Chainlink data before GMX coverage
        - INCREMENTAL: Only backfill if we need older data than we have

        :param gmx_df: GMX OHLCV DataFrame with 'timestamp' column
        :param chainlink_available: Whether Chainlink feed exists for this token
        :param gmx_v2_genesis_block: Block number for GMX V2 launch (unused, kept for API compatibility)
        :param mode: Collection mode (FULL or INCREMENTAL)
        :param existing_df: Existing stored data (for incremental mode)
        :return: Tuple of (backfill_start_block, backfill_end_timestamp)
            - backfill_start_block: Always None (timestamp-based) or 0 for compatibility
            - backfill_end_timestamp: Unix timestamp to end collection
            - Returns (None, None) if no backfill needed
        """
        # No backfill if Chainlink not available
        if not chainlink_available:
            return None, None

        if mode == FetchMode.FULL:
            return self._calculate_full_gap(gmx_df)
        else:  # INCREMENTAL
            return self._calculate_incremental_gap(gmx_df, existing_df)

    def _calculate_full_gap(self, gmx_df: pd.DataFrame) -> tuple[int | None, int | None]:
        """Calculate gap for full collection mode.

        Full mode: Collect ALL Chainlink data before GMX coverage.

        Note: The backfill_start_block is now always 0 to collect ALL available
        Chainlink historical data. The HyperSync collector uses auto_detect_start=True
        to efficiently find the first event without scanning empty blocks.

        :param gmx_df: GMX OHLCV DataFrame
        :return: (None, backfill_end_timestamp) for timestamp-based collection
        """
        # If GMX data is empty, collect all Chainlink data
        if gmx_df.empty:
            return None, None

        # Get earliest GMX timestamp
        gmx_earliest = gmx_df["timestamp"].min()
        gmx_earliest_unix = int(gmx_earliest.timestamp())

        # Backfill from genesis (block 0) to just before GMX coverage starts
        # Return None for start to indicate "fetch all available"
        backfill_end_timestamp = gmx_earliest_unix - 1

        return None, backfill_end_timestamp

    def _calculate_incremental_gap(
        self, gmx_df: pd.DataFrame, existing_df: pd.DataFrame | None
    ) -> tuple[int | None, int | None]:
        """Calculate gap for incremental mode.

        Incremental mode: Only fetch if we have GMX data but no older historical data.
        This prevents refetching Chainlink data we already have.

        :param gmx_df: GMX OHLCV DataFrame
        :param existing_df: Existing stored data
        :return: (None, backfill_end_timestamp) if backfill needed, else (None, None)
        """
        # No backfill if GMX data is empty or no existing data to compare
        if gmx_df.empty or existing_df is None or existing_df.empty:
            return None, None

        # Get timestamps
        gmx_earliest = gmx_df["timestamp"].min()
        our_earliest = existing_df["timestamp"].min()

        # Ensure timezone-aware for comparison
        if our_earliest.tzinfo is None:
            our_earliest = our_earliest.replace(tzinfo=UTC)
        if gmx_earliest.tzinfo is None:
            gmx_earliest = gmx_earliest.replace(tzinfo=UTC)

        # Only backfill if our data doesn't cover the GMX range
        if our_earliest > gmx_earliest:
            # We need historical data before our storage
            our_earliest_unix = int(our_earliest.timestamp())
            return None, our_earliest_unix - 1

        # We already have sufficient historical data
        return None, None
