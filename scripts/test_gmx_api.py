"""Test script to demonstrate GMX API integration.

This script fetches latest price data from GMX's official API
without requiring HyperSync or Chainlink data.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from gmx_historical_data.gmx_api_integration import GMXDataFetcher


def main():
    """Test GMX API fetcher."""
    print("=" * 60)
    print("Testing GMX API Integration")
    print("=" * 60)

    # Initialize fetcher
    fetcher = GMXDataFetcher(chain="arbitrum")

    # Test tokens
    test_symbols = ["ETH", "BTC", "ARB"]

    for symbol in test_symbols:
        print(f"\n{'=' * 60}")
        print(f"Fetching {symbol} data from GMX API...")
        print(f"{'=' * 60}")

        # Get data range
        earliest, latest = fetcher.get_latest_data_range(symbol, period="1h")

        if earliest is None:
            print(f"No data available for {symbol}")
            continue

        print(f"\nData coverage:")
        print(f"  Earliest: {earliest.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  Latest: {latest.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  Days: {(latest - earliest).days}")

        # Fetch candles
        df = fetcher.fetch_gmx_candles(symbol, period="1h", limit=10)

        if not df.empty:
            print(f"\nLast 10 candles:")
            print(df.to_string(index=False))
        else:
            print("No candle data available")

    print(f"\n{'=' * 60}")
    print("Test complete!")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
