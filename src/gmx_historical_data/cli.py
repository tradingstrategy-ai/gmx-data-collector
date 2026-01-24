"""Command-line interface for GMX historical data collection."""

import sys
import os
import asyncio
import datetime
from pathlib import Path
from typing import Optional
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
from web3 import Web3

console = Console()

from gmx_historical_data.config import CollectionConfig, TIMEFRAMES
from gmx_historical_data.chainlink_feeds import get_feed_address, get_all_symbols
from gmx_historical_data.aggregator_discovery import AggregatorDiscovery
from gmx_historical_data.hypersync_collector import HyperSyncCollector
from gmx_historical_data.storage import ParquetStorage
from gmx_historical_data.checkpoint import CheckpointManager
from gmx_historical_data.resampler import OHLCVResampler
from gmx_historical_data.gmx_api_integration import (
    GMXDataFetcher,
    combine_gmx_and_chainlink_data,
    map_timeframe_to_gmx_period,
)


class DataCollector:
    """Main data collection orchestrator.

    :param config: Collection configuration
    :param use_gmx_api: If True, fetch latest data from GMX API and historical from Chainlink
    """

    def __init__(self, config: CollectionConfig, use_gmx_api: bool = True) -> None:
        """Initialize data collector.

        :param config: Collection configuration
        :param use_gmx_api: If True, fetch latest data from GMX API
        """
        self.config = config
        self.use_gmx_api = use_gmx_api
        self.web3 = Web3(Web3.HTTPProvider(config.rpc_url))
        self.hypersync = HyperSyncCollector(
            config.hypersync_endpoint,
            config.hypersync_api_token,
        )
        self.storage = ParquetStorage(config.output_dir)
        self.checkpoint_mgr = CheckpointManager(config.checkpoints_dir)
        self.resampler = OHLCVResampler(decimals=8)

        # Initialize GMX API fetcher if enabled
        if use_gmx_api:
            self.gmx_fetcher = GMXDataFetcher(chain="arbitrum")
        else:
            self.gmx_fetcher = None

        # Ensure directories exist
        config.ensure_directories()

    async def collect_symbol(
        self,
        symbol: str,
        full: bool = False,
    ) -> None:
        """Collect data for a single symbol.

        :param symbol: Token symbol (e.g., 'ETH')
        :param full: If True, collect from genesis; if False, resume from checkpoint
        """
        console.print()
        console.print(Panel(f"[bold cyan]Collecting data for {symbol}[/bold cyan]", box=box.ROUNDED))

        # Get Chainlink proxy address
        try:
            proxy_address = get_feed_address(symbol)
            console.print(f"  [dim]Chainlink proxy:[/dim] [yellow]{proxy_address}[/yellow]")
        except KeyError as e:
            console.print(f"[red]✗ Error: {e}[/red]")
            return

        # Discover aggregator address
        discovery = AggregatorDiscovery(self.web3)
        try:
            aggregator_info = discovery.get_aggregator_info(proxy_address)
            aggregator_address = aggregator_info["current_aggregator"]
            console.print(f"  [dim]Current aggregator:[/dim] [yellow]{aggregator_address}[/yellow]")
            console.print(f"  [dim]Phase ID:[/dim] {aggregator_info['current_phase']}")
        except Exception as e:
            error_msg = f"Chainlink aggregator discovery failed - feed may not be active on Arbitrum"
            console.print(f"[red]✗ {error_msg}[/red]")
            raise ValueError(error_msg) from e

        # Determine start block
        if full:
            start_block = self.config.start_block or 0
            console.print(f"  [dim]Mode:[/dim] Full collection from block [cyan]{start_block}[/cyan]")
        else:
            start_block = self.checkpoint_mgr.get_resume_block(symbol, default=0)
            console.print(f"  [dim]Mode:[/dim] Resuming from block [cyan]{start_block}[/cyan]")

        # Collect events
        console.print(f"\n[bold]Querying HyperSync...[/bold]")
        try:
            events, stats = await self.hypersync.collect_all_events(
                aggregator_addresses=[aggregator_address],
                start_block=start_block,
                end_block=self.config.end_block,
                auto_detect_start=False,  # HyperSync is efficient from block 0
            )
        except Exception as e:
            console.print(f"[red]Error collecting events: {e}[/red]")
            return

        if not events:
            console.print("[yellow]No events found.[/yellow]")
            return

        # Show first oracle update info
        first_event = min(events, key=lambda e: e.block_number)
        first_date = datetime.datetime.fromtimestamp(first_event.timestamp, tz=datetime.timezone.utc)

        # Create statistics table
        table = Table(title="Collection Statistics", box=box.ROUNDED, show_header=False)
        table.add_column("Metric", style="dim")
        table.add_column("Value", style="cyan")

        table.add_row("First oracle update", f"{first_date.strftime('%Y-%m-%d %H:%M:%S UTC')} (block {first_event.block_number})")
        table.add_row("Total events", f"{stats.total_events:,}")
        table.add_row("Blocks scanned", f"{stats.blocks_scanned:,}")
        table.add_row("Block range", f"{stats.start_block:,} → {stats.end_block:,}")
        table.add_row("Aggregators found", str(stats.aggregators_found))

        console.print()
        console.print(table)

        # Save raw events
        console.print("\n[bold]Saving raw events...[/bold]")
        if full:
            output_path = self.storage.save_raw_events(events, symbol, partition_id=0)
        else:
            output_path = self.storage.append_raw_events(events, symbol)
        console.print(f"  [green]✓[/green] Saved to: [dim]{output_path}[/dim]")

        # Update checkpoint
        last_event = max(events, key=lambda e: e.block_number)
        checkpoint = self.checkpoint_mgr.update_checkpoint(
            symbol=symbol,
            last_block=last_event.block_number,
            last_timestamp=last_event.timestamp,
            events_added=len(events),
        )
        console.print(f"  [green]✓[/green] Checkpoint updated: [cyan]{checkpoint.total_events:,}[/cyan] total events")

        # Resample to OHLCV
        console.print("\n[bold]Resampling to OHLCV candles...[/bold]")
        raw_df = self.storage.read_raw_events(symbol)
        chainlink_candles = self.resampler.resample_all_timeframes(raw_df, symbol)

        # Fetch GMX data if enabled
        gmx_candles = {}
        if self.use_gmx_api and self.gmx_fetcher:
            console.print("\n[bold]Fetching latest data from GMX API...[/bold]")
            for timeframe in TIMEFRAMES:
                gmx_period = map_timeframe_to_gmx_period(timeframe)
                gmx_df = self.gmx_fetcher.fetch_gmx_candles(symbol, period=gmx_period)

                if not gmx_df.empty:
                    earliest, latest = gmx_df["timestamp"].min(), gmx_df["timestamp"].max()
                    console.print(f"  [green]✓[/green] {timeframe}: [cyan]{len(gmx_df):,}[/cyan] candles from GMX [dim]({earliest} to {latest})[/dim]")
                    gmx_candles[timeframe] = gmx_df
                else:
                    console.print(f"  [yellow]○[/yellow] {timeframe}: No GMX data available")

        # Combine GMX and Chainlink data
        console.print("\n[bold]Saving combined candles...[/bold]")
        for timeframe in TIMEFRAMES:
            chainlink_df = chainlink_candles.get(timeframe)
            gmx_df = gmx_candles.get(timeframe)

            # Combine if both sources have data
            if chainlink_df is not None and gmx_df is not None and not gmx_df.empty:
                combined_df = combine_gmx_and_chainlink_data(gmx_df, chainlink_df)
                candle_path = self.storage.save_candles(combined_df, timeframe, symbol)
                earliest, latest = combined_df["timestamp"].min(), combined_df["timestamp"].max()
                console.print(f"  [green]✓[/green] {timeframe}: [cyan]{len(combined_df):,}[/cyan] total candles [dim]({earliest} to {latest})[/dim]")
            elif chainlink_df is not None:
                # Only Chainlink data
                candle_path = self.storage.save_candles(chainlink_df, timeframe, symbol)
                earliest, latest = chainlink_df["timestamp"].min(), chainlink_df["timestamp"].max()
                console.print(f"  [green]✓[/green] {timeframe}: [cyan]{len(chainlink_df):,}[/cyan] candles [dim]({earliest} to {latest})[/dim]")
            elif gmx_df is not None and not gmx_df.empty:
                # Only GMX data
                candle_path = self.storage.save_candles(gmx_df, timeframe, symbol)
                earliest, latest = gmx_df["timestamp"].min(), gmx_df["timestamp"].max()
                console.print(f"  [green]✓[/green] {timeframe}: [cyan]{len(gmx_df):,}[/cyan] candles [dim]({earliest} to {latest})[/dim]")

        console.print(f"\n[bold green]✓ Collection complete for {symbol}[/bold green]")

    async def collect_all_symbols(self, full: bool = False) -> None:
        """Collect data for all supported symbols.

        :param full: If True, collect from genesis; if False, resume from checkpoints
        """
        symbols = get_all_symbols()
        total = len(symbols)
        successful = 0
        failed = 0
        failed_symbols = []

        console.print(f"\n[bold]Collecting data for [cyan]{total}[/cyan] symbols...[/bold]")

        for i, symbol in enumerate(symbols, 1):
            console.print(f"\n[bold blue][{i}/{total}][/bold blue] Processing {symbol}...")
            try:
                await self.collect_symbol(symbol, full=full)
                successful += 1
            except Exception as e:
                console.print(f"[red]✗ Error processing {symbol}: {e}[/red]")
                failed += 1
                failed_symbols.append(symbol)
                continue

        # Create summary table
        summary_table = Table(title="Collection Summary", box=box.ROUNDED, show_header=False)
        summary_table.add_column("Status", style="bold")
        summary_table.add_column("Count", justify="right")

        summary_table.add_row("[green]✓ Successful[/green]", f"[green]{successful}/{total}[/green]")
        summary_table.add_row("[red]✗ Failed[/red]", f"[red]{failed}/{total}[/red]")
        if failed_symbols:
            summary_table.add_row("[yellow]Failed symbols[/yellow]", f"[yellow]{', '.join(failed_symbols)}[/yellow]")

        console.print()
        console.print(summary_table)


def cli(
    full: bool = typer.Option(
        False,
        "--full",
        help="Collect full historical data from genesis",
    ),
    update: bool = typer.Option(
        False,
        "--update",
        help="Incremental update from last checkpoint",
    ),
    symbol: Optional[str] = typer.Option(
        None,
        "--symbol",
        help="Specific token symbol to collect (e.g., ETH, BTC)",
    ),
    output_dir: Path = typer.Option(
        Path("./data"),
        "--output-dir",
        help="Output directory for data",
    ),
    rpc_url: Optional[str] = typer.Option(
        None,
        "--rpc-url",
        envvar="JSON_RPC_ARBITRUM",
        help="Arbitrum RPC URL (or set JSON_RPC_ARBITRUM env var)",
    ),
    hypersync_token: Optional[str] = typer.Option(
        None,
        "--hypersync-token",
        envvar="HYPERSYNC_API_TOKEN",
        help="HyperSync API token (or set HYPERSYNC_API_TOKEN env var)",
    ),
    start_block: Optional[int] = typer.Option(
        None,
        "--start-block",
        help="Starting block number (default: 0)",
    ),
    end_block: Optional[int] = typer.Option(
        None,
        "--end-block",
        help="Ending block number (default: latest)",
    ),
    use_gmx_api: bool = typer.Option(
        True,
        "--use-gmx-api/--no-gmx-api",
        help="Fetch latest data from GMX API",
    ),
) -> None:
    """Collect GMX historical price data.

    Examples:
        # Full historical collection for ETH
        gmx_historical_data --full --symbol ETH --output-dir ./data

        # Incremental update for all symbols
        gmx_historical_data --update --output-dir ./data

        # Full collection for all symbols
        gmx_historical_data --full --output-dir ./data
    """
    # Validate arguments
    if not full and not update:
        console.print("[red]Error: Must specify either --full or --update[/red]")
        raise typer.Exit(1)

    # Validate RPC URL
    if not rpc_url:
        console.print(
            "[red]Error: RPC URL required. Set JSON_RPC_ARBITRUM environment variable "
            "or use --rpc-url argument[/red]"
        )
        raise typer.Exit(1)

    # Validate HyperSync API token
    if not hypersync_token:
        warning_panel = Panel(
            "[bold red]HyperSync API token not set![/bold red]\n\n"
            "HyperSync requires an API token to access historical data.\n"
            "Without it, you'll get 403 Forbidden errors.\n\n"
            "[bold]To fix this:[/bold]\n"
            "  1. Get a free API token from: [cyan]https://envio.dev/[/cyan]\n"
            "  2. Set the environment variable:\n"
            "     [yellow]export HYPERSYNC_API_TOKEN=\"your_token_here\"[/yellow]\n"
            "  3. Or use --hypersync-token argument\n\n"
            "[dim]If you only want GMX API data (last ~6 months), you can skip\n"
            "Chainlink historical data collection.[/dim]",
            title="⚠️  Warning",
            box=box.HEAVY,
        )
        console.print()
        console.print(warning_panel)
        console.print()
        raise typer.Exit(1)

    # Create configuration
    config = CollectionConfig(
        output_dir=output_dir,
        rpc_url=rpc_url,
        hypersync_api_token=hypersync_token,
        start_block=start_block,
        end_block=end_block,
    )

    # Create collector
    collector = DataCollector(config, use_gmx_api=use_gmx_api)

    # Run collection
    try:
        if symbol:
            asyncio.run(collector.collect_symbol(symbol.upper(), full=full))
        else:
            asyncio.run(collector.collect_all_symbols(full=full))
    except KeyboardInterrupt:
        console.print("\n\n[yellow]Collection interrupted by user[/yellow]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"\n\n[red bold]Error: {e}[/red bold]")
        import traceback

        traceback.print_exc()
        raise typer.Exit(1)


def main() -> None:
    """Main CLI entry point."""
    typer.run(cli)


if __name__ == "__main__":
    main()
