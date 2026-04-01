#!/usr/bin/env python3
"""Fetch and display GMX 24h trading volume from Subsquid.

Usage::

    poetry run python scripts/fetch_volume.py
    poetry run python scripts/fetch_volume.py --chain avalanche
    poetry run python scripts/fetch_volume.py --history 7

Note: Per-market historical volume is NOT available in the Subsquid schema.
Only current 24h per-market and aggregate daily history are supported.
"""

import argparse
import sys
from datetime import UTC, datetime
from decimal import Decimal

from rich.console import Console
from rich.table import Table

from gmx_historical_data.market_registry import fetch_markets
from gmx_historical_data.subsquid_volume import fetch_daily_volumes, fetch_volume_history

console = Console()


def fmt_usd(val: Decimal) -> str:
    """Format a Decimal USD value with commas and dollar sign."""
    return f"${val:,.0f}"


def _build_addr_to_symbol(markets: dict[str, dict]) -> dict[str, str]:
    """Build checksummed market address -> symbol mapping."""
    from eth_utils import to_checksum_address

    mapping = {}
    for addr_lower, info in markets.items():
        try:
            checksummed = to_checksum_address(addr_lower)
            mapping[checksummed] = info["symbol"]
        except Exception:
            continue
    return mapping


def show_daily_volumes(chain: str) -> None:
    """Fetch and print all per-market 24h volumes with symbol names."""
    with console.status("Fetching market registry..."):
        markets = fetch_markets(chain=chain)
    addr_to_sym = _build_addr_to_symbol(markets)

    with console.status("Fetching 24h volume data..."):
        volumes = fetch_daily_volumes(chain=chain)

    if not volumes:
        console.print("[red]No volume data returned.[/red]")
        return

    sorted_vols = sorted(volumes.items(), key=lambda x: x[1], reverse=True)
    total = sum(volumes.values())
    nonzero = sum(1 for _, v in sorted_vols if v > 0)

    table = Table(
        title=f"GMX 24h Volume — {chain.title()}",
        caption=(
            f"{nonzero} markets with volume / {len(volumes)} total  |  "
            f"Total: [bold green]{fmt_usd(total)}[/bold green]"
        ),
    )
    table.add_column("#", justify="right", style="dim", width=4)
    table.add_column("Market", style="cyan")
    table.add_column("Volume (USD)", justify="right", style="green", width=18)
    table.add_column("Share", justify="right", style="yellow", width=7)

    for i, (addr, vol) in enumerate(sorted_vols, 1):
        sym = addr_to_sym.get(addr, addr[:10])
        share = f"{vol / total * 100:.1f}%" if total > 0 and vol > 0 else "—"
        style = "bold" if i <= 3 else None
        table.add_row(str(i), sym, fmt_usd(vol), share, style=style)

    console.print()
    console.print(table)
    console.print()


def show_volume_history(chain: str, days: int) -> None:
    """Fetch and print historical daily aggregate volume."""
    with console.status(f"Fetching {days}-day volume history..."):
        history = fetch_volume_history(days=days, chain=chain)

    if not history:
        console.print("[red]No history data returned.[/red]")
        return

    table = Table(
        title=f"GMX Daily Volume History — last {days} days ({chain.title()})"
    )
    table.add_column("Date", style="cyan", width=12)
    table.add_column("Total", justify="right", style="bold green", width=18)
    table.add_column("Margin", justify="right", style="green", width=18)
    table.add_column("Swap", justify="right", style="yellow", width=18)

    for entry in history:
        dt = datetime.fromtimestamp(entry["timestamp"], tz=UTC)
        table.add_row(
            dt.strftime("%Y-%m-%d"),
            fmt_usd(entry["volume_usd"]),
            fmt_usd(entry["margin_volume_usd"]),
            fmt_usd(entry["swap_volume_usd"]),
        )

    total = sum(e["volume_usd"] for e in history)
    avg = total / len(history) if history else Decimal(0)
    table.add_section()
    table.add_row("Total", f"[bold]{fmt_usd(total)}[/bold]", "", "")
    table.add_row("Avg/day", fmt_usd(avg), "", "", style="dim")

    console.print()
    console.print(table)
    console.print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch GMX trading volume from Subsquid"
    )
    parser.add_argument(
        "--chain", default="arbitrum", choices=["arbitrum", "avalanche"]
    )
    parser.add_argument(
        "--history",
        type=int,
        metavar="DAYS",
        help="Show daily aggregate volume history for N days",
    )
    args = parser.parse_args()

    try:
        show_daily_volumes(args.chain)
        if args.history:
            show_volume_history(args.chain, args.history)
    except Exception as e:
        console.print(f"\n[red bold]Error:[/red bold] {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
