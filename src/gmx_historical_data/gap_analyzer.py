"""Analyze data gaps between GMX and Chainlink data sources."""

import pandas as pd


class DataGapAnalyzer:
    """Calculate what historical data needs to be backfilled."""

    def calculate_gap(
        self,
        gmx_df: pd.DataFrame,
        chainlink_available: bool,
    ) -> tuple[int | None, int | None]:
        """Calculate the gap that needs to be filled with Chainlink data.

        :param gmx_df: GMX OHLCV DataFrame with 'timestamp' column
        :param chainlink_available: Whether Chainlink feed exists for this token
        :return: Tuple of (backfill_start_block, backfill_end_timestamp)
            - backfill_start_block: Block to start Chainlink collection (0 = genesis)
            - backfill_end_timestamp: Unix timestamp to end collection (GMX earliest - 1)
            - Returns (None, None) if no backfill needed
        """
        # No backfill if Chainlink not available
        if not chainlink_available:
            return None, None

        # If GMX data is empty, collect all Chainlink data
        if gmx_df.empty:
            return 0, None  # From genesis to latest

        # Get earliest GMX timestamp
        gmx_earliest = gmx_df["timestamp"].min()
        gmx_earliest_unix = int(gmx_earliest.timestamp())

        # Backfill from genesis to just before GMX coverage starts
        backfill_start_block = 0
        backfill_end_timestamp = gmx_earliest_unix - 1

        return backfill_start_block, backfill_end_timestamp
