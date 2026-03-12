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
GMX V2 Funding Rate Cross-Validator
====================================
Compares our direct ``fundingFactorPerSecond``-derived rates with USD flow data
(Dune-equivalent) to flag discrepancies and generate confidence scores.

**Inputs** (must exist before running):

1. ``data/funding/{network}/rates/{SYMBOL}/1h.parquet`` — our unified hourly rates
2. ``data/funding/{network}/usd_flows/{SYMBOL}/events.parquet`` — USD flow events

**Output**: ``data/funding/{network}/validation/{SYMBOL}/1h_comparison.parquet``

USAGE
-----
    poetry run python scripts/validate_funding_rates.py [OPTIONS]

EXAMPLES
--------
    # Validate all markets
    poetry run python scripts/validate_funding_rates.py

    # Single market
    poetry run python scripts/validate_funding_rates.py --market ETH/USD
================================================================================
"""

import argparse
import sys
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.table import Table

try:
    import polars as pl
except ImportError:
    print("ERROR: polars is required. Install with: pip install polars")
    sys.exit(1)

console = Console()


# =============================================================================
# HOURLY AGGREGATION
# =============================================================================


def hourly_aggregate_usd_flows(flows: pl.DataFrame) -> pl.DataFrame:
    """Aggregate per-event USD flows to hourly implied rates.

    :param flows: Per-event USD flow data from ``build_usd_funding_flows.py``.
    :returns: Hourly aggregated DataFrame with ``timestamp`` and implied rate columns.
    """
    if flows.is_empty():
        return pl.DataFrame()

    flows = flows.with_columns((pl.col("block_timestamp") // 3600 * 3600).alias("hour_ts"))

    hourly = flows.group_by("hour_ts").agg(
        [
            pl.col("long_funding_rate").mean().alias("implied_long_annual"),
            pl.col("short_funding_rate").mean().alias("implied_short_annual"),
            pl.len().alias("event_count"),
        ]
    )

    return hourly.rename({"hour_ts": "timestamp"}).sort("timestamp")


# =============================================================================
# RATE COMPARISON
# =============================================================================


def compare_rates(
    direct: pl.DataFrame,
    implied: pl.DataFrame,
) -> pl.DataFrame:
    """Join direct rates with USD-flow implied rates and compute discrepancy.

    :param direct: Our unified ``1h.parquet`` with ``timestamp``,
        ``funding_rate_annualized``, ``longs_pay_shorts``.
    :param implied: Hourly aggregated USD flows with ``timestamp``,
        ``implied_long_annual``, ``implied_short_annual``.
    :returns: Comparison DataFrame with discrepancy metrics.
    """
    if direct.is_empty() or implied.is_empty():
        return pl.DataFrame()

    # Align timestamp types — direct may be Datetime, implied is Int64
    if direct["timestamp"].dtype != pl.Int64:
        direct = direct.with_columns(pl.col("timestamp").dt.epoch("s").alias("ts_epoch"))
    else:
        direct = direct.with_columns(pl.col("timestamp").alias("ts_epoch"))

    joined = direct.join(implied, left_on="ts_epoch", right_on="timestamp", how="left")

    # Compute signed annual rate from our data
    joined = joined.with_columns(
        pl.when(pl.col("longs_pay_shorts"))
        .then(pl.col("funding_rate_annualized"))
        .otherwise(-pl.col("funding_rate_annualized"))
        .alias("direct_long_annual")
    )

    # Discrepancy.
    # Direction match: when direct rate is zero any implied direction counts as
    # a match (no meaningful direction to compare).  When implied_long_annual is
    # null (no USD flow data for that hour) the result is null — polars .sum()
    # and .mean() on booleans correctly ignore nulls, and upstream filtering on
    # implied_long_annual.is_not_null() excludes these rows from summaries.
    joined = joined.with_columns(
        [
            (pl.col("direct_long_annual") - pl.col("implied_long_annual")).abs().alias("rate_diff"),
            (
                pl.when(pl.col("direct_long_annual") == 0)
                .then(True)
                .when(pl.col("direct_long_annual") > 0)
                .then(pl.col("implied_long_annual") > 0)
                .otherwise(pl.col("implied_long_annual") <= 0)
            ).alias("direction_match"),
        ]
    )

    return joined


# =============================================================================
# SUMMARY
# =============================================================================


def print_validation_summary(results: dict[str, pl.DataFrame]) -> None:
    """Print per-symbol validation summary as a Rich table.

    :param results: Mapping of ``{symbol: comparison_df}``.
    """
    table = Table(title="Funding Rate Cross-Validation", show_lines=False)
    table.add_column("Symbol", style="cyan")
    table.add_column("Hours Compared", justify="right")
    table.add_column("Direction Match %", justify="right")
    table.add_column("Avg Rate Diff (annual)", justify="right")
    table.add_column("Max Rate Diff (annual)", justify="right")

    total_compared = 0
    total_matched = 0

    for symbol, df in sorted(results.items()):
        matched = df.filter(pl.col("implied_long_annual").is_not_null())
        if len(matched) == 0:
            table.add_row(symbol, "0", "N/A", "N/A", "N/A")
            continue

        dir_pct = matched["direction_match"].mean() * 100
        avg_diff = matched["rate_diff"].mean()
        max_diff = matched["rate_diff"].max()

        total_compared += len(matched)
        total_matched += matched["direction_match"].sum()

        table.add_row(
            symbol,
            f"{len(matched):,}",
            f"{dir_pct:.1f}%",
            f"{avg_diff:.6f}",
            f"{max_diff:.6f}",
        )

    console.print()
    console.print(table)

    if total_compared > 0:
        overall_pct = total_matched / total_compared * 100
        console.print(
            f"\n  Overall: [cyan]{total_compared:,}[/cyan] hours compared, "
            f"[{'green' if overall_pct > 90 else 'yellow'}]{overall_pct:.1f}%[/] direction match"
        )


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    """CLI entry point for funding rate validation."""
    parser = argparse.ArgumentParser(
        description="Cross-validate funding rates vs USD flows",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Validate all markets
  poetry run python scripts/validate_funding_rates.py

  # Single market
  poetry run python scripts/validate_funding_rates.py --market ETH/USD
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
    args = parser.parse_args()

    network_dir = Path(args.output_dir) / args.network
    rates_dir = network_dir / "rates"
    flows_dir = network_dir / "usd_flows"
    validation_dir = network_dir / "validation"

    console.print(
        Panel(
            f"Network:        [cyan]{args.network}[/cyan]\n"
            f"Rates dir:      [cyan]{rates_dir}[/cyan]\n"
            f"USD flows dir:  [cyan]{flows_dir}[/cyan]\n"
            f"Validation dir: [cyan]{validation_dir}[/cyan]",
            title="GMX V2 Funding Rate Cross-Validator",
            subtitle="Direct Rate vs USD Flow Implied Rate",
            border_style="blue",
        )
    )

    if not rates_dir.exists():
        console.print(f"\n[red]Rates directory not found: {rates_dir}[/red]")
        console.print("  Run the unified funding extraction pipeline first.")
        sys.exit(1)

    if not flows_dir.exists():
        console.print(f"\n[red]USD flows directory not found: {flows_dir}[/red]")
        console.print("  Run with --include-usd-flows first.")
        sys.exit(1)

    symbols = sorted(d.name for d in rates_dir.iterdir() if d.is_dir())

    if args.market:
        filter_sym = args.market.split("/")[0].strip()
        symbols = [s for s in symbols if s == filter_sym]

    if not symbols:
        console.print("\n[yellow]No symbols to validate.[/yellow]")
        sys.exit(0)

    console.print(f"\n  Symbols to validate: [cyan]{', '.join(symbols)}[/cyan]")

    results: dict[str, pl.DataFrame] = {}

    for symbol in symbols:
        rate_path = rates_dir / symbol / "1h.parquet"
        flow_path = flows_dir / symbol / "events.parquet"

        if not rate_path.exists():
            console.print(f"  [yellow]{symbol}: no rate data, skipping[/yellow]")
            continue
        if not flow_path.exists():
            console.print(f"  [yellow]{symbol}: no USD flow data, skipping[/yellow]")
            continue

        direct = pl.read_parquet(rate_path)
        flows = pl.read_parquet(flow_path)

        implied = hourly_aggregate_usd_flows(flows)
        comparison = compare_rates(direct, implied)

        if comparison.is_empty():
            console.print(f"  [yellow]{symbol}: no overlapping hours[/yellow]")
            continue

        # Save
        out_dir = validation_dir / symbol
        out_dir.mkdir(parents=True, exist_ok=True)
        comparison.write_parquet(out_dir / "1h_comparison.parquet")
        results[symbol] = comparison
        console.print(
            f"  [cyan]{symbol}[/cyan]: {len(comparison):,} hours -> "
            f"[green]{out_dir / '1h_comparison.parquet'}[/green]"
        )

    print_validation_summary(results)
    console.print(f"\n  Validation data saved to [green]{validation_dir}[/green]")


if __name__ == "__main__":
    main()
