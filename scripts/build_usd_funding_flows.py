#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "polars>=0.20.0",
#     "rich>=13.0",
#     "pyarrow>=14.0",
# ]
# ///
"""
GMX V2 USD Funding Flow Builder (Dune-Equivalent Methodology)
=============================================================
Joins ``FundingFeeAmountPerSizeUpdated`` events with
``ClaimableFundingAmountPerSizeUpdated`` events and oracle prices to produce
annualised USD flow data per event.

This mirrors the Dune Analytics approach:

- ``delta_usd = delta * oracle_max_price``
- ``annual_rate_usd = (delta_usd / seconds_between_events) * 86400 * 365``
- Single-token markets: ``2x`` (funding_fee) / ``4x`` (claimable) multiplier
- Direction from paying/receiving side join

**Inputs** (must exist before running):

1. ``data/funding/{network}/raw/fee_per_size/{SYMBOL}/data.parquet`` — paying side deltas
2. ``data/funding/{network}/raw/claimable_fee_per_size/{SYMBOL}/data.parquet`` — receiving side
3. ``data/funding/{network}/raw/oracle_prices/data.parquet`` — per-event oracle prices

**Output**: ``data/funding/{network}/usd_flows/{SYMBOL}/events.parquet``

USAGE
-----
    poetry run python scripts/build_usd_funding_flows.py [OPTIONS]

EXAMPLES
--------
    # Build USD flows for all markets
    poetry run python scripts/build_usd_funding_flows.py

    # Single market
    poetry run python scripts/build_usd_funding_flows.py --market ETH/USD
================================================================================
"""

import argparse
import sys
from datetime import UTC, datetime
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from gmx_historical_data.atomic_parquet import atomic_write_parquet
from gmx_historical_data.market_registry import fetch_markets

try:
    import polars as pl
except ImportError:
    print("ERROR: polars is required. Install with: pip install polars")
    sys.exit(1)

console = Console()

# GMX V2 genesis timestamp — filter out events before this
GMX_V2_GENESIS_TIMESTAMP = 1690848000  # 2023-08-01 00:00:00 UTC


# =============================================================================
# SINGLE-TOKEN MARKET DETECTION
# =============================================================================


def build_single_token_set(markets: dict[str, dict]) -> set[str]:
    """Return set of market addresses where longToken == shortToken.

    Single-token markets produce duplicate events that Dune corrects with
    multipliers: 2x for funding_fee deltas, 4x for claimable deltas.

    :param markets: Market registry from :func:`fetch_markets`.
    :returns: Set of lowercase market addresses that are single-token markets.
    """
    result = set()
    for addr, info in markets.items():
        long_sym = info.get("longTokenSymbol", "")
        short_sym = info.get("shortTokenSymbol", "")
        if long_sym and short_sym and long_sym == short_sym:
            result.add(addr.lower())
    return result


# =============================================================================
# DATA LOADING & DEDUP
# =============================================================================


def load_and_dedup(raw_dir: Path, event_type: str) -> pl.DataFrame:
    """Load raw events and deduplicate per (market, collateral_token, is_long, tx_hash).

    Keeps the latest event per group (by block_number DESC, log_index DESC),
    matching Dune's ``ROW_NUMBER`` dedup pattern.

    :param raw_dir: Directory containing per-symbol ``data.parquet`` files.
    :param event_type: Label for logging (``'funding_fee'`` or ``'claimable'``).
    :returns: Deduplicated polars DataFrame.
    """
    if not raw_dir.exists():
        console.print(f"  [yellow]Directory not found: {raw_dir}[/yellow]")
        return pl.DataFrame()

    # Use scan_parquet to avoid loading all per-symbol files into memory at once
    parquet_files = sorted(raw_dir.glob("*/data.parquet"))
    if not parquet_files:
        console.print(f"  [yellow]No {event_type} data found in {raw_dir}[/yellow]")
        return pl.DataFrame()

    combined = pl.concat([pl.scan_parquet(f) for f in parquet_files]).collect()

    # Dedup: keep latest per (market, collateral_token, is_long, transaction_hash)
    combined = combined.sort(["block_number", "log_index"], descending=True)
    combined = combined.unique(
        subset=["market", "collateral_token", "is_long", "transaction_hash"],
        keep="first",
    )

    console.print(f"  Loaded {len(combined):,} {event_type} events (deduped)")
    return combined


# =============================================================================
# ORACLE PRICE JOIN
# =============================================================================


def join_oracle_prices(
    df: pl.DataFrame,
    oracle_df: pl.DataFrame,
) -> pl.DataFrame:
    """Join events with oracle prices on (transaction_hash, collateral_token=token).

    Computes ``delta_usd = delta * max_price`` (both converted to Float64).

    :param df: Funding/claimable events with ``transaction_hash``,
        ``collateral_token``, ``delta``.
    :param oracle_df: Oracle price events with ``transaction_hash``,
        ``token``, ``max_price``.
    :returns: DataFrame with ``delta_usd`` column added.
    """
    if df.is_empty() or oracle_df.is_empty():
        return df.with_columns(pl.lit(None).cast(pl.Float64).alias("delta_usd"))

    # Prepare oracle: dedup to one price per (tx_hash, token), keep latest
    oracle = (
        oracle_df.sort("log_index", descending=True)
        .unique(subset=["transaction_hash", "token"], keep="first")
        .select(
            [
                "transaction_hash",
                pl.col("token").alias("collateral_token"),
                pl.col("max_price").cast(pl.Float64).alias("oracle_max_price"),
            ]
        )
    )

    # Lazy join to let Polars optimize the query plan and reduce peak memory
    joined = (
        df.lazy()
        .join(
            oracle.lazy(),
            on=["transaction_hash", "collateral_token"],
            how="left",
        )
        .collect()
    )

    # Compute delta_usd.  Both delta and oracle_max_price are GMX 30-decimal
    # fixed-point integers stored as strings.  Casting to Float64 before
    # multiplying can produce intermediates up to ~1e63 which is within Float64
    # range (~1.8e308) but near its ~15-digit significand limit.  This is
    # acceptable for annualised-rate comparisons (relative values).
    joined = joined.with_columns(
        (pl.col("delta").cast(pl.Float64) * pl.col("oracle_max_price")).alias("delta_usd")
    )

    return joined


# =============================================================================
# TIME-DIFF & ANNUALISATION
# =============================================================================


def compute_annual_rates(
    df: pl.DataFrame,
    single_token_markets: set[str],
    multiplier: int,
) -> pl.DataFrame:
    """Compute annualised USD rate per event using Dune methodology.

    For each partition of ``(market, collateral_token, is_long)``, computes the
    time difference between consecutive events and annualises the USD delta:
    ``annual_rate_usd = (delta_usd / seconds_diff) * 86400 * 365``

    Single-token markets get a correction multiplier applied.

    :param df: Events with ``delta_usd``, ``block_timestamp``, partitioned by
        ``(market, collateral_token, is_long)``.
    :param single_token_markets: Set of market addresses that are single-token.
    :param multiplier: Base multiplier for single-token correction
        (2 for funding_fee, 4 for claimable).
    :returns: DataFrame with ``annual_rate_usd`` column.
    """
    if df.is_empty():
        return df.with_columns(pl.lit(None).cast(pl.Float64).alias("annual_rate_usd"))

    df = df.sort(["market", "collateral_token", "is_long", "block_timestamp"])

    # Compute seconds_diff using lag
    df = df.with_columns(
        pl.col("block_timestamp")
        .shift(1)
        .over(["market", "collateral_token", "is_long"])
        .alias("prev_timestamp")
    )
    df = df.with_columns(
        (pl.col("block_timestamp") - pl.col("prev_timestamp")).alias("seconds_diff")
    )

    # Filter: need previous timestamp and post-genesis
    df = df.filter(
        pl.col("prev_timestamp").is_not_null()
        & (pl.col("block_timestamp") >= GMX_V2_GENESIS_TIMESTAMP)
    )

    # Filter out zero seconds_diff (same-block events — indeterminate time delta)
    df = df.filter(pl.col("seconds_diff") > 0)

    # Annualise: (delta_usd / seconds_diff) * 86400 * 365
    df = df.with_columns(
        (pl.col("delta_usd") / pl.col("seconds_diff") * 86400 * 365).alias("annual_rate_usd_raw")
    )

    # Single-token market multiplier
    df = df.with_columns(
        pl.when(pl.col("market").is_in(list(single_token_markets)))
        .then(pl.col("annual_rate_usd_raw") * multiplier)
        .otherwise(pl.col("annual_rate_usd_raw"))
        .alias("annual_rate_usd")
    )

    return df


# =============================================================================
# JOIN PAYING + RECEIVING & AGGREGATE
# =============================================================================


def build_usd_flows(
    funding_fee_df: pl.DataFrame,
    claimable_df: pl.DataFrame,
) -> pl.DataFrame:
    """Join paying and receiving sides, aggregate per block.

    Mirrors Dune's ``joined_data`` and ``agg_data_hash`` CTEs.

    :param funding_fee_df: Funding fee events with ``annual_rate_usd``.
    :param claimable_df: Claimable events with ``annual_rate_usd``.
    :returns: Aggregated USD flow DataFrame.
    """
    if funding_fee_df.is_empty() or claimable_df.is_empty():
        console.print("  [yellow]One or both sides empty — cannot build flows[/yellow]")
        return pl.DataFrame()

    # Rename claimable columns to avoid collision
    claimable_renamed = claimable_df.select(
        [
            "transaction_hash",
            "market",
            "collateral_token",
            pl.col("is_long").alias("is_long_cf"),
            pl.col("annual_rate_usd").alias("annual_rate_usd_cf"),
        ]
    )

    # Inner join on (transaction_hash, market, collateral_token)
    joined = funding_fee_df.join(
        claimable_renamed,
        on=["transaction_hash", "market", "collateral_token"],
        how="inner",
    )

    if joined.is_empty():
        console.print("  [yellow]No matching events between paying/receiving sides[/yellow]")
        return pl.DataFrame()

    # Aggregate per (block_timestamp, market, is_long, is_long_cf)
    agg = joined.group_by(
        [
            "block_timestamp",
            "block_number",
            "market",
            "symbol",
            "is_long",
            "is_long_cf",
        ]
    ).agg(
        [
            pl.col("annual_rate_usd").mean().alias("funding_rate_ff"),
            pl.col("annual_rate_usd_cf").mean().alias("funding_rate_cf"),
        ]
    )

    # Compute long/short funding rates (Dune logic)
    agg = agg.with_columns(
        [
            pl.when(pl.col("is_long") & ~pl.col("is_long_cf"))
            .then(pl.col("funding_rate_ff"))
            .otherwise(-pl.col("funding_rate_cf"))
            .alias("long_funding_rate"),
            pl.when(~pl.col("is_long") & pl.col("is_long_cf"))
            .then(pl.col("funding_rate_ff"))
            .otherwise(-pl.col("funding_rate_cf"))
            .alias("short_funding_rate"),
        ]
    )

    return agg.sort("block_timestamp", descending=True)


# =============================================================================
# SUMMARY
# =============================================================================


def print_summary(flows_by_symbol: dict[str, pl.DataFrame]) -> None:
    """Print per-symbol summary of USD flows.

    :param flows_by_symbol: Mapping of ``{symbol: flow_df}``.
    """
    table = Table(title="USD Funding Flows Summary", show_lines=False)
    table.add_column("Symbol", style="cyan")
    table.add_column("Events", justify="right")
    table.add_column("Time Range", justify="center")

    for symbol in sorted(flows_by_symbol.keys()):
        df = flows_by_symbol[symbol]
        if df.is_empty():
            continue

        timestamps = df["block_timestamp"]
        first = datetime.fromtimestamp(timestamps.min(), tz=UTC).strftime("%Y-%m-%d")
        last = datetime.fromtimestamp(timestamps.max(), tz=UTC).strftime("%Y-%m-%d")

        table.add_row(
            symbol,
            f"{len(df):,}",
            f"{first} to {last}",
        )

    console.print()
    console.print(table)


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    """CLI entry point for USD funding flow builder."""
    parser = argparse.ArgumentParser(
        description="Build USD-denominated funding flows (Dune-equivalent methodology)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Build USD flows for all markets
  poetry run python scripts/build_usd_funding_flows.py

  # Single market
  poetry run python scripts/build_usd_funding_flows.py --market ETH/USD
        """,
    )
    parser.add_argument(
        "--network",
        choices=["arbitrum", "avalanche"],
        default="arbitrum",
        help="Network (default: arbitrum)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data/funding",
        help="Base output directory (default: ./data/funding)",
    )
    parser.add_argument(
        "--market",
        type=str,
        default=None,
        help="Filter by market symbol (e.g., 'ETH/USD')",
    )
    parser.add_argument(
        "--refresh-markets",
        action="store_true",
        help="Force re-fetch of GMX market registry",
    )
    args = parser.parse_args()

    network_dir = Path(args.output_dir) / args.network

    header_lines = [
        f"Network:    [cyan]{args.network}[/cyan]",
        f"Output dir: [cyan]{network_dir}[/cyan]",
    ]
    if args.market:
        header_lines.append(f"Market:     [cyan]{args.market}[/cyan]")

    console.print(
        Panel(
            "\n".join(header_lines),
            title="GMX V2 USD Funding Flow Builder",
            subtitle="Dune-Equivalent Methodology",
            border_style="blue",
        )
    )

    # Load market registry for single-token detection
    with console.status("Fetching GMX market registry..."):
        markets = fetch_markets(args.network, force_refresh=args.refresh_markets)
    single_token_mkts = build_single_token_set(markets)
    console.print(f"  Markets loaded: [cyan]{len(markets):,}[/cyan]")
    if single_token_mkts:
        console.print(f"  Single-token markets: [cyan]{len(single_token_mkts)}[/cyan]")

    # Load raw data
    console.print("\n[bold]Loading raw event data...[/bold]")
    ff_raw = load_and_dedup(network_dir / "raw" / "fee_per_size", "funding_fee")
    cf_raw = load_and_dedup(network_dir / "raw" / "claimable_fee_per_size", "claimable")

    oracle_path = network_dir / "raw" / "oracle_prices" / "data.parquet"
    if not oracle_path.exists():
        console.print(f"  [red]Oracle price data not found: {oracle_path}[/red]")
        console.print("  Run extract_oracle_prices_raw.py first.")
        sys.exit(1)
    oracle_raw = pl.read_parquet(oracle_path)
    console.print(f"  Loaded {len(oracle_raw):,} oracle price events")

    if ff_raw.is_empty() or cf_raw.is_empty():
        console.print("\n[red]Missing required data. Run extraction scripts first.[/red]")
        sys.exit(1)

    # Join oracle prices
    console.print("\n[bold]Joining oracle prices...[/bold]")
    ff_priced = join_oracle_prices(ff_raw, oracle_raw)
    cf_priced = join_oracle_prices(cf_raw, oracle_raw)

    # Compute annual rates
    console.print("\n[bold]Computing annualised rates...[/bold]")
    ff_annual = compute_annual_rates(ff_priced, single_token_mkts, multiplier=2)
    cf_annual = compute_annual_rates(cf_priced, single_token_mkts, multiplier=4)

    # Build USD flows
    console.print("\n[bold]Building USD flows...[/bold]")
    flows = build_usd_flows(ff_annual, cf_annual)

    if flows.is_empty():
        console.print("\n[yellow]No USD flows produced.[/yellow]")
        sys.exit(0)

    # Apply market filter if specified
    if args.market:
        filter_sym = args.market.split("/")[0].strip()
        flows = flows.filter(pl.col("symbol") == filter_sym)
        if flows.is_empty():
            console.print(f"\n[yellow]No flows for market {args.market}[/yellow]")
            sys.exit(0)

    # Save per symbol
    console.print("\n[bold]Saving USD flows...[/bold]")
    flows_by_symbol: dict[str, pl.DataFrame] = {}
    for symbol in flows["symbol"].unique().sort().to_list():
        sym_df = flows.filter(pl.col("symbol") == symbol)
        out_dir = network_dir / "usd_flows" / symbol
        out_dir.mkdir(parents=True, exist_ok=True)
        atomic_write_parquet(sym_df, out_dir / "events.parquet")
        console.print(
            f"  [cyan]{symbol}[/cyan]: {len(sym_df):,} events -> "
            f"[green]{out_dir / 'events.parquet'}[/green]"
        )
        flows_by_symbol[symbol] = sym_df

    print_summary(flows_by_symbol)
    console.print(f"\n  [green]USD flows saved for {len(flows_by_symbol)} symbols[/green]")


if __name__ == "__main__":
    main()
