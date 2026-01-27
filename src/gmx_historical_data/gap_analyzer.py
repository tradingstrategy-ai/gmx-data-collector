"""Analyze data gaps between GMX and Chainlink data sources."""

import pandas as pd


class DataGapAnalyzer:
    """Calculate what historical data needs to be backfilled."""

    def calculate_gap(
        self,
        gmx_df: pd.DataFrame,
        chainlink_available: bool,
        gmx_v2_genesis_block: int = 120_000_000,
    ) -> tuple[int | None, int | None]:
        """Calculate the gap that needs to be filled with Chainlink data.

        Note: The backfill_start_block is now always 0 to collect ALL available
        Chainlink historical data. The HyperSync collector uses auto_detect_start=True
        to efficiently find the first event without scanning empty blocks.

        :param gmx_df: GMX OHLCV DataFrame with 'timestamp' column
        :param chainlink_available: Whether Chainlink feed exists for this token
        :param gmx_v2_genesis_block: Block number for GMX V2 launch (unused, kept for API compatibility)
        :return: Tuple of (backfill_start_block, backfill_end_timestamp)
            - backfill_start_block: Always 0 to get all historical Chainlink data
            - backfill_end_timestamp: Unix timestamp to end collection (GMX earliest - 1)
            - Returns (None, None) if no backfill needed
        """
        # No backfill if Chainlink not available
        if not chainlink_available:
            return None, None

        # If GMX data is empty, collect all Chainlink data
        if gmx_df.empty:
            return 0, None

        # Get earliest GMX timestamp
        gmx_earliest = gmx_df["timestamp"].min()
        gmx_earliest_unix = int(gmx_earliest.timestamp())

        # Backfill from genesis (block 0) to just before GMX coverage starts
        # HyperSync's auto_detect_start will efficiently find the first event
        backfill_start_block = 0
        backfill_end_timestamp = gmx_earliest_unix - 1

        return backfill_start_block, backfill_end_timestamp
