"""Daily GMX V2 market snapshot collector.

Fetches a point-in-time snapshot of all listed GMX V2 markets via the
public REST API (no HyperSync, no RPC, no API keys required). Captures:

- Daily OHLCV candles per symbol → CCXT feather format
- Open Interest (long/short per market)
- Pool Liquidity (pool amounts, available liquidity)
- Funding & Borrowing rates

Output follows the existing ``user_data/`` layout::

    user_data/data/gmx/
    ├── futures/{SYM}_USDC_USDC-1d-futures.feather   # OHLCV (appended)
    └── snapshots/{date}.parquet                      # OI + liquidity + rates

Usage::

    # Today's snapshot
    poetry run python scripts/collect_daily_snapshot.py

    # Specific date
    poetry run python scripts/collect_daily_snapshot.py --date 2026-03-10

    # Custom output root
    poetry run python scripts/collect_daily_snapshot.py --output-dir ./my_data
"""

import argparse
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow.feather as feather
from eth_defi.gmx.api import GMXAPI
from rich.console import Console

console = Console()


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


def _get_unique_symbols(api: GMXAPI) -> list[str]:
    """Extract sorted unique base symbols from listed GMX markets.

    :param api: Initialised GMXAPI client.
    :returns: Sorted list of unique symbol strings.
    """
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

    return sorted(symbols)


def collect_and_save_ohlcv(
    api: GMXAPI,
    date_str: str,
    futures_dir: Path,
) -> int:
    """Fetch daily OHLCV candles and append to per-symbol CCXT feather files.

    For each symbol, fetches the latest daily candle and appends it to
    ``{futures_dir}/{SYM}_USDC_USDC-1d-futures.feather``. Existing rows
    at the same date are replaced (idempotent on re-run).

    :param api: Initialised GMXAPI client.
    :param date_str: ISO date string for the snapshot.
    :param futures_dir: Directory for CCXT feather files
        (e.g. ``user_data/data/gmx/futures/``).
    :returns: Number of symbols successfully saved.
    """
    symbols = _get_unique_symbols(api)
    console.print(f"  Fetching daily candles for {len(symbols)} unique symbols...")

    futures_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    failed = []

    for symbol in symbols:
        try:
            df = api.get_candlesticks_dataframe(symbol, period="1d", limit=2)
            if df.empty:
                failed.append(symbol)
                continue

            # Take the latest candle and build CCXT-format row
            latest = df.iloc[-1]
            ts = latest["timestamp"]
            if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                ts = pd.Timestamp(ts, tz="UTC")

            new_row = pd.DataFrame(
                [
                    {
                        "date": ts,
                        "open": float(latest["open"]),
                        "high": float(latest["high"]),
                        "low": float(latest["low"]),
                        "close": float(latest["close"]),
                        "volume": 0.0,
                    }
                ]
            )

            # Append to existing feather file or create new one
            filepath = futures_dir / f"{symbol}_USDC_USDC-1d-futures.feather"
            if filepath.exists():
                existing = pd.read_feather(filepath)
                if existing["date"].dt.tz is None:
                    existing["date"] = existing["date"].dt.tz_localize("UTC")
                # Drop any existing row at the same timestamp (idempotent)
                existing = existing[existing["date"] != ts]
                combined = pd.concat([existing, new_row], ignore_index=True)
            else:
                combined = new_row

            combined = combined.sort_values("date").reset_index(drop=True)
            combined["date"] = combined["date"].dt.as_unit("ns")
            feather.write_feather(combined, filepath)
            saved += 1

        except Exception as e:
            failed.append(symbol)
            console.print(f"    [yellow]Warning: {symbol} — {e}[/yellow]")

        # Small delay to be polite to the API
        time.sleep(0.1)

    console.print(
        f"  [green]Saved {saved} candle files[/green]"
        + (
            f" [yellow]({len(failed)} failed: {', '.join(failed[:5])}"
            f"{'...' if len(failed) > 5 else ''})[/yellow]"
            if failed
            else ""
        )
    )
    return saved


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

  # Custom output root
  poetry run python scripts/collect_daily_snapshot.py --output-dir ./my_data
        """,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("./user_data"),
        help="Root output directory (default: ./user_data)",
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

    futures_dir = args.output_dir / "data" / "gmx" / "futures"
    snapshots_dir = args.output_dir / "data" / "gmx" / "snapshots"

    console.print(f"\n[bold]GMX Daily Snapshot — {date_str}[/bold]")
    console.print(f"  Network:   {args.network}")
    console.print(f"  Futures:   {futures_dir}")
    console.print(f"  Snapshots: {snapshots_dir}\n")

    api = GMXAPI(chain=args.network)

    # --- 1. Markets snapshot (OI + liquidity + rates) ---
    console.print("[bold]Phase 1: Markets info (OI, liquidity, rates)[/bold]")
    markets_df = collect_markets_snapshot(api, date_str)

    if markets_df.empty:
        console.print("[red]Error: No market data returned from GMX API[/red]")
        sys.exit(1)

    markets_path = snapshots_dir / f"{date_str}.parquet"
    markets_path.parent.mkdir(parents=True, exist_ok=True)
    markets_df.to_parquet(markets_path, index=False)
    console.print(f"  Saved → {markets_path}\n")

    # --- 2. OHLCV daily candles (CCXT feather format) ---
    console.print("[bold]Phase 2: Daily OHLCV candles (CCXT feather)[/bold]")
    candle_count = collect_and_save_ohlcv(api, date_str, futures_dir)
    console.print()

    # --- Summary ---
    console.print("[bold]Summary[/bold]")
    console.print(f"  Date:      {date_str}")
    console.print(f"  Markets:   {len(markets_df)}")
    console.print(f"  Candles:   {candle_count}")
    console.print("[green]Done.[/green]")


if __name__ == "__main__":
    main()
