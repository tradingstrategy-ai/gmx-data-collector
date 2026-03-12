"""Daily GMX V2 market snapshot collector.

Fetches a point-in-time snapshot of **all** GMX V2 markets (perpetual +
swap-only) via the public REST API. No HyperSync, no RPC, no API keys.

Captures:

- Daily OHLCV candles per symbol → CCXT feather format
- Open Interest (long/short per market, including alt-collateral variants)
- Pool Liquidity (pool amounts, available liquidity)
- Funding & Borrowing rates
- Swap-only pool data

Output follows the existing ``user_data/`` layout::

    user_data/data/gmx/
    ├── futures/{SYM}_USDC_USDC-1d-futures.feather   # OHLCV (appended)
    └── snapshots/{date}.parquet                      # All markets snapshot

A ``data_report.txt`` file is generated in the output root after each run.

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

# GMX 30-decimal precision for OI/rate values.
_GMX_PRECISION = 1e30


def _fetch_all_markets(api: GMXAPI) -> list[dict]:
    """Fetch the full market list from the GMX API.

    :param api: Initialised GMXAPI client.
    :returns: List of market dicts (all markets, no filtering).
    """
    data = api.get_markets_info()
    return data.get("markets", [])


def collect_markets_snapshot(
    markets: list[dict],
    date_str: str,
) -> pd.DataFrame:
    """Flatten all markets into a tabular snapshot DataFrame.

    Includes every market returned by the API: perpetual markets,
    alt-collateral variants, and swap-only pools. Deprecated (unlisted)
    markets are tagged but still included.

    :param markets: Raw market dicts from ``get_markets_info()``.
    :param date_str: ISO date string to tag the snapshot.
    :returns: DataFrame with one row per market.
    """
    rows = []
    for market in markets:
        name = market.get("name", "")
        is_listed = market.get("isListed", True)
        is_swap_only = "/" not in name
        symbol = "" if is_swap_only else name.split("/")[0].strip()

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
                "is_listed": is_listed,
                "is_swap_only": is_swap_only,
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
    perp = df[~df["is_swap_only"]]
    swap = df[df["is_swap_only"]]
    listed = df[df["is_listed"]]
    console.print(
        f"  [green]Fetched {len(df)} markets[/green] "
        f"({len(perp)} perpetual, {len(swap)} swap-only, "
        f"{len(listed)} listed)"
    )
    return df


def collect_and_save_ohlcv(
    api: GMXAPI,
    markets: list[dict],
    date_str: str,
    futures_dir: Path,
) -> tuple[int, list[str]]:
    """Fetch daily OHLCV candles and append to per-symbol CCXT feather files.

    For each unique symbol across all perpetual markets, fetches the latest
    daily candle and appends it to
    ``{futures_dir}/{SYM}_USDC_USDC-1d-futures.feather``. Existing rows
    at the same date are replaced (idempotent on re-run).

    :param api: Initialised GMXAPI client.
    :param markets: Raw market dicts from ``get_markets_info()``.
    :param date_str: ISO date string for the snapshot.
    :param futures_dir: Directory for CCXT feather files.
    :returns: Tuple of (saved count, list of failed symbols).
    """
    # Extract unique symbols from perpetual markets only
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

    futures_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    failed = []

    for symbol in symbols:
        try:
            filepath = futures_dir / f"{symbol}_USDC_USDC-1d-futures.feather"

            # Fetch full history on first run, just recent candles on subsequent
            if filepath.exists():
                limit = 5  # Last few days to catch up
            else:
                limit = 10000  # Max available history from API

            df = api.get_candlesticks_dataframe(symbol, period="1d", limit=limit)
            if df.empty:
                failed.append(symbol)
                continue

            # Build CCXT-format DataFrame from all fetched candles
            new_rows = pd.DataFrame(
                {
                    "date": df["timestamp"],
                    "open": df["open"].astype(float),
                    "high": df["high"].astype(float),
                    "low": df["low"].astype(float),
                    "close": df["close"].astype(float),
                    "volume": 0.0,
                }
            )
            if new_rows["date"].dt.tz is None:
                new_rows["date"] = new_rows["date"].dt.tz_localize("UTC")
            new_rows["date"] = new_rows["date"].dt.as_unit("ns")

            # Merge with existing data (idempotent — dedup by date)
            if filepath.exists():
                existing = pd.read_feather(filepath)
                if existing["date"].dt.tz is None:
                    existing["date"] = existing["date"].dt.tz_localize("UTC")
                existing["date"] = existing["date"].dt.as_unit("ns")
                combined = pd.concat([existing, new_rows], ignore_index=True)
                combined = combined.drop_duplicates(subset=["date"], keep="last")
            else:
                combined = new_rows

            combined = combined.sort_values("date").reset_index(drop=True)
            if combined["date"].dtype == "object":
                combined["date"] = pd.to_datetime(combined["date"], utc=True)
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
    return saved, failed


def generate_report(
    date_str: str,
    markets_df: pd.DataFrame,
    candle_count: int,
    failed_symbols: list[str],
    futures_dir: Path,
    snapshots_dir: Path,
    report_path: Path,
) -> None:
    """Write a human-readable data report after each collection run.

    :param date_str: Snapshot date.
    :param markets_df: Full markets snapshot DataFrame.
    :param candle_count: Number of OHLCV feather files written.
    :param failed_symbols: Symbols that failed candle fetch.
    :param futures_dir: Path to CCXT feather directory.
    :param snapshots_dir: Path to snapshots directory.
    :param report_path: Output path for the report file.
    """
    now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")

    perp_df = markets_df[~markets_df["is_swap_only"]]
    swap_df = markets_df[markets_df["is_swap_only"]]
    listed_df = markets_df[markets_df["is_listed"]]

    # Count existing feather files and snapshot days
    feather_files = list(futures_dir.glob("*-1d-futures.feather"))
    snapshot_files = list(snapshots_dir.glob("*.parquet"))

    # Compute total OI across all perpetual markets
    total_oi = 0
    for _, row in perp_df.iterrows():
        try:
            oi_long = int(row["open_interest_long"])
            oi_short = int(row["open_interest_short"])
            total_oi += (oi_long + oi_short)
        except (ValueError, TypeError):
            pass
    total_oi_usd = total_oi / _GMX_PRECISION

    # Top 10 markets by OI
    oi_rows = []
    for _, row in perp_df.iterrows():
        try:
            oi = (int(row["open_interest_long"]) + int(row["open_interest_short"])) / _GMX_PRECISION
            oi_rows.append((row["name"], oi))
        except (ValueError, TypeError):
            pass
    oi_rows.sort(key=lambda x: x[1], reverse=True)

    lines = [
        f"# GMX Data Report — {date_str}",
        f"Generated: {now_utc}",
        "",
        "## Collection Summary",
        f"- Snapshot date: {date_str}",
        f"- Total markets from API: {len(markets_df)}",
        f"  - Perpetual markets: {len(perp_df)}",
        f"  - Swap-only pools: {len(swap_df)}",
        f"  - Listed: {len(listed_df)}, Unlisted: {len(markets_df) - len(listed_df)}",
        f"- Unique symbols (OHLCV): {candle_count} saved"
        + (f", {len(failed_symbols)} failed" if failed_symbols else ""),
        f"- Total OI (all markets): ${total_oi_usd:,.0f}",
        "",
        "## Data Files",
        f"- OHLCV feather files: {len(feather_files)}",
        f"- Snapshot parquet files: {len(snapshot_files)} days",
        "",
        "## Top 10 Markets by Open Interest",
    ]

    for i, (name, oi) in enumerate(oi_rows[:10], 1):
        lines.append(f"  {i:2d}. {name:<40s} ${oi:>14,.0f}")

    if failed_symbols:
        lines.append("")
        lines.append("## Failed OHLCV Fetches")
        lines.append(f"  {', '.join(failed_symbols)}")

    # Per-symbol candle counts (how many days of data each feather has)
    lines.append("")
    lines.append("## OHLCV Coverage (rows per symbol)")
    symbol_rows = []
    for f in sorted(feather_files):
        try:
            df = pd.read_feather(f)
            sym = f.stem.replace("_USDC_USDC-1d-futures", "")
            symbol_rows.append((sym, len(df)))
        except Exception:
            pass
    for sym, count in sorted(symbol_rows):
        lines.append(f"  {sym}: {count} days")

    lines.append("")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines), encoding="utf-8")
    console.print(f"  Report → {report_path}")


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
    report_path = args.output_dir.parent / "data_report.txt"

    console.print(f"\n[bold]GMX Daily Snapshot — {date_str}[/bold]")
    console.print(f"  Network:   {args.network}")
    console.print(f"  Futures:   {futures_dir}")
    console.print(f"  Snapshots: {snapshots_dir}\n")

    api = GMXAPI(chain=args.network)

    # --- Fetch all markets once ---
    console.print("[bold]Fetching markets from GMX API...[/bold]")
    all_markets = _fetch_all_markets(api)
    if not all_markets:
        console.print("[red]Error: No market data returned from GMX API[/red]")
        sys.exit(1)

    # --- 1. Markets snapshot (ALL markets: perp + swap-only + unlisted) ---
    console.print("\n[bold]Phase 1: Markets snapshot (OI, liquidity, rates)[/bold]")
    markets_df = collect_markets_snapshot(all_markets, date_str)

    markets_path = snapshots_dir / f"{date_str}.parquet"
    markets_path.parent.mkdir(parents=True, exist_ok=True)
    markets_df.to_parquet(markets_path, index=False)
    console.print(f"  Saved → {markets_path}\n")

    # --- 2. OHLCV daily candles (CCXT feather format) ---
    console.print("[bold]Phase 2: Daily OHLCV candles (CCXT feather)[/bold]")
    candle_count, failed_symbols = collect_and_save_ohlcv(
        api, all_markets, date_str, futures_dir
    )
    console.print()

    # --- 3. Generate report ---
    console.print("[bold]Phase 3: Data report[/bold]")
    generate_report(
        date_str=date_str,
        markets_df=markets_df,
        candle_count=candle_count,
        failed_symbols=failed_symbols,
        futures_dir=futures_dir,
        snapshots_dir=snapshots_dir,
        report_path=report_path,
    )
    console.print()

    # --- Summary ---
    console.print("[bold]Summary[/bold]")
    console.print(f"  Date:      {date_str}")
    console.print(f"  Markets:   {len(markets_df)} (all)")
    console.print(f"  Candles:   {candle_count}")
    console.print("[green]Done.[/green]")


if __name__ == "__main__":
    main()
