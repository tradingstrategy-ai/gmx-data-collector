"""Daily GMX V2 market snapshot collector.

Fetches a point-in-time snapshot of all listed GMX V2 markets via the
public REST API (no HyperSync, no RPC, no API keys required). Captures:

- Open Interest (long/short per market)
- Pool Liquidity (pool amounts, available liquidity)
- Funding & Borrowing rates
- Daily OHLCV candles per unique symbol

Output is date-partitioned parquet files under ``--output-dir``::

    data/snapshots/
    ├── markets_info/2026-03-11.parquet
    └── ohlcv_daily/2026-03-11.parquet

Usage::

    # Today's snapshot
    poetry run python scripts/collect_daily_snapshot.py

    # Specific date
    poetry run python scripts/collect_daily_snapshot.py --date 2026-03-10

    # Custom output
    poetry run python scripts/collect_daily_snapshot.py --output-dir ./my_data
"""

import argparse
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from eth_defi.gmx.api import GMXAPI
from rich.console import Console

console = Console()

# GMX API returns rates as annualized 1e30 fixed-point strings.
_GMX_PRECISION = 1e30
_HOURS_PER_YEAR = 365.25 * 24


def collect_markets_snapshot(api: GMXAPI, date_str: str) -> pd.DataFrame:
    """Fetch current OI, pool liquidity, and rates for all listed markets.

    Calls ``GMXAPI.get_markets_info()`` once and flattens the response into
    a tabular DataFrame. Swap-only markets (no ``/`` in name) are excluded.

    :param api: Initialised GMXAPI client.
    :param date_str: ISO date string to tag the snapshot (e.g. ``"2026-03-11"``).
    :returns: DataFrame with one row per market.
    """
    data = api.get_markets_info()
    markets = data.get("markets", [])

    rows = []
    for market in markets:
        if not market.get("isListed", True):
            continue

        name = market.get("name", "")
        # Skip swap-only markets — their names lack a "/"
        if "/" not in name:
            continue

        symbol = name.split("/")[0].strip()
        if not symbol:
            continue

        rows.append(
            {
                "date": date_str,
                "name": name,
                "symbol": symbol,
                "market_token": market.get("marketToken", ""),
                "index_token": market.get("indexToken", ""),
                "long_token": market.get("longToken", ""),
                "short_token": market.get("shortToken", ""),
                "listing_date": market.get("listingDate", ""),
                "open_interest_long": market.get("openInterestLong", "0"),
                "open_interest_short": market.get("openInterestShort", "0"),
                "pool_amount_long": market.get("poolAmountLong", "0"),
                "pool_amount_short": market.get("poolAmountShort", "0"),
                "available_liquidity_long": market.get("availableLiquidityLong", "0"),
                "available_liquidity_short": market.get("availableLiquidityShort", "0"),
                "funding_rate_long": market.get("fundingRateLong", "0"),
                "funding_rate_short": market.get("fundingRateShort", "0"),
                "borrowing_rate_long": market.get("borrowingRateLong", "0"),
                "borrowing_rate_short": market.get("borrowingRateShort", "0"),
                "net_rate_long": market.get("netRateLong", "0"),
                "net_rate_short": market.get("netRateShort", "0"),
            }
        )

    df = pd.DataFrame(rows)
    console.print(f"  [green]Fetched {len(df)} markets from get_markets_info()[/green]")
    return df


def collect_ohlcv_snapshot(api: GMXAPI, date_str: str) -> pd.DataFrame:
    """Fetch the latest daily OHLCV candle for each unique symbol.

    For symbols that appear in multiple markets (e.g. ETH has 2-3 markets
    with different collateral), only one candle call is made per symbol.

    :param api: Initialised GMXAPI client.
    :param date_str: ISO date string to tag the snapshot.
    :returns: DataFrame with one row per symbol (OHLCV + date).
    """
    # Discover unique symbols from markets_info
    data = api.get_markets_info()
    markets = data.get("markets", [])

    symbols = set()
    for market in markets:
        if not market.get("isListed", True):
            continue
        name = market.get("name", "")
        if "/" not in name:
            continue
        symbol = name.split("/")[0].strip()
        if symbol:
            symbols.add(symbol)

    symbols = sorted(symbols)
    console.print(f"  Fetching daily candles for {len(symbols)} unique symbols...")

    rows = []
    failed = []
    for symbol in symbols:
        try:
            df = api.get_candlesticks_dataframe(symbol, period="1d", limit=2)
            if df.empty:
                failed.append(symbol)
                continue

            # Take the latest candle row
            latest = df.iloc[-1]
            rows.append(
                {
                    "date": date_str,
                    "symbol": symbol,
                    "timestamp": latest["timestamp"],
                    "open": float(latest["open"]),
                    "high": float(latest["high"]),
                    "low": float(latest["low"]),
                    "close": float(latest["close"]),
                }
            )
        except Exception as e:
            failed.append(symbol)
            console.print(f"    [yellow]Warning: {symbol} — {e}[/yellow]")

        # Small delay to be polite to the API
        time.sleep(0.1)

    console.print(
        f"  [green]Collected {len(rows)} candles[/green]"
        + (f" [yellow]({len(failed)} failed: {', '.join(failed[:5])}{'...' if len(failed) > 5 else ''})[/yellow]" if failed else "")
    )
    return pd.DataFrame(rows)


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Collect daily GMX V2 market snapshot (OI, liquidity, OHLCV)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Today's snapshot
  poetry run python scripts/collect_daily_snapshot.py

  # Specific date
  poetry run python scripts/collect_daily_snapshot.py --date 2026-03-10

  # Custom output directory
  poetry run python scripts/collect_daily_snapshot.py --output-dir ./my_data
        """,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./data/snapshots"),
        help="Base output directory (default: ./data/snapshots)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Snapshot date in YYYY-MM-DD format (default: today UTC)",
    )
    parser.add_argument(
        "--network",
        choices=["arbitrum", "avalanche"],
        default="arbitrum",
        help="GMX network (default: arbitrum)",
    )

    args = parser.parse_args()
    date_str = args.date or datetime.now(UTC).strftime("%Y-%m-%d")

    console.print(f"\n[bold]GMX Daily Snapshot — {date_str}[/bold]")
    console.print(f"  Network: {args.network}")
    console.print(f"  Output:  {args.output_dir}\n")

    api = GMXAPI(chain=args.network)

    # --- 1. Markets snapshot (OI + liquidity + rates) ---
    console.print("[bold]Phase 1: Markets info (OI, liquidity, rates)[/bold]")
    markets_df = collect_markets_snapshot(api, date_str)

    if markets_df.empty:
        console.print("[red]Error: No market data returned from GMX API[/red]")
        sys.exit(1)

    markets_path = args.output_dir / "markets_info" / f"{date_str}.parquet"
    markets_path.parent.mkdir(parents=True, exist_ok=True)
    markets_df.to_parquet(markets_path, index=False)
    console.print(f"  Saved → {markets_path}\n")

    # --- 2. OHLCV daily candles ---
    console.print("[bold]Phase 2: Daily OHLCV candles[/bold]")
    ohlcv_df = collect_ohlcv_snapshot(api, date_str)

    if not ohlcv_df.empty:
        ohlcv_path = args.output_dir / "ohlcv_daily" / f"{date_str}.parquet"
        ohlcv_path.parent.mkdir(parents=True, exist_ok=True)
        ohlcv_df.to_parquet(ohlcv_path, index=False)
        console.print(f"  Saved → {ohlcv_path}\n")
    else:
        console.print("  [yellow]No OHLCV data collected[/yellow]\n")

    # --- Summary ---
    console.print("[bold]Summary[/bold]")
    console.print(f"  Date:    {date_str}")
    console.print(f"  Markets: {len(markets_df)}")
    console.print(f"  Candles: {len(ohlcv_df)}")
    console.print("[green]Done.[/green]")


if __name__ == "__main__":
    main()
