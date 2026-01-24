"""Integration test for GMX-first data collection."""

import pytest
import os
from pathlib import Path
import pandas as pd
from datetime import timezone
from gmx_historical_data import (
    GMXTokenDiscovery,
    DataGapAnalyzer,
    find_chainlink_symbol,
    get_feed_address_for_gmx_symbol,
)


@pytest.mark.skipif(
    not os.getenv("JSON_RPC_ARBITRUM"), reason="Requires JSON_RPC_ARBITRUM env var"
)
def test_gmx_first_flow():
    """Test the complete GMX-first data collection flow."""
    # Step 1: Discover GMX tokens
    discovery = GMXTokenDiscovery(chain="arbitrum")
    symbols = discovery.get_supported_symbols()

    assert len(symbols) > 90
    assert "ETH" in symbols

    # Step 2: Find Chainlink feed for ETH
    chainlink_symbol = find_chainlink_symbol("ETH")
    assert chainlink_symbol == "ETH"

    feed_address = get_feed_address_for_gmx_symbol("ETH")
    assert feed_address == "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"

    # Step 3: Test gap analysis (mock GMX data)
    dates = pd.date_range("2024-07-01", "2024-12-31", freq="1h", tz=timezone.utc)
    gmx_df = pd.DataFrame(
        {
            "timestamp": dates,
            "close": [2000.0] * len(dates),
        }
    )

    analyzer = DataGapAnalyzer()
    backfill_start, backfill_end = analyzer.calculate_gap(
        gmx_df=gmx_df, chainlink_available=True
    )

    assert backfill_start == 0
    assert backfill_end is not None

    print(f"\nGMX-first flow test passed:")
    print(f"  - Discovered {len(symbols)} GMX tokens")
    print(f"  - Found Chainlink feed for ETH: {feed_address}")
    print(
        f"  - Gap analysis: backfill from block {backfill_start} to timestamp {backfill_end}"
    )
