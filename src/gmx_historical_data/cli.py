"""Command-line interface for GMX historical data collection."""

import asyncio
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
from gmx_historical_data.gmx_event_collector import GMXEventCollector
from gmx_historical_data.gmx_market_mapper import GMXMarketMapper
from gmx_historical_data.event_aggregator import aggregate_events_to_ohlcv
from gmx_historical_data.chainlink_feeds_complete import (
    get_feed_address_for_gmx_symbol,
    find_chainlink_symbol,
)
from gmx_historical_data.aggregator_discovery import AggregatorDiscovery
from gmx_historical_data.hypersync_collector import HyperSyncCollector
from gmx_historical_data.chainlink_rpc_collector import ChainlinkRPCCollector
from gmx_historical_data.storage import ParquetStorage
from gmx_historical_data.checkpoint import CheckpointManager
from gmx_historical_data.resampler import OHLCVResampler
from gmx_historical_data.event_decoder import AnswerUpdatedEvent
from gmx_historical_data.gmx_api_integration import (
    GMXDataFetcher,
    combine_gmx_and_chainlink_data,
    map_timeframe_to_gmx_period,
)
from gmx_historical_data.gmx_token_discovery import GMXTokenDiscovery
from gmx_historical_data.gap_analyzer import DataGapAnalyzer


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
        self.rpc_collector = ChainlinkRPCCollector(self.web3)
        self.storage = ParquetStorage(config.output_dir)
        self.checkpoint_mgr = CheckpointManager(config.checkpoints_dir)
        self.resampler = OHLCVResampler(decimals=8)

        # Initialize GMX API fetcher if enabled
        if use_gmx_api:
            self.gmx_fetcher = GMXDataFetcher(chain="arbitrum")
        else:
            self.gmx_fetcher = None

        # GMX token discovery
        self.gmx_discovery = GMXTokenDiscovery(chain="arbitrum")

        # Gap analyzer
        self.gap_analyzer = DataGapAnalyzer()

        # Ensure directories exist
        config.ensure_directories()

    async def collect_symbol(
        self,
        symbol: str,
        full: bool = False,
    ) -> None:
        """Collect data for a single symbol using GMX-first approach.

        :param symbol: Token symbol (e.g., 'ETH')
        :param full: If True, collect from genesis; if False, resume from checkpoint
        """
        console.print()
        console.print(
            Panel(
                f"[bold cyan]Collecting data for {symbol}[/bold cyan]", box=box.ROUNDED
            )
        )

        # Step 1: Fetch GMX data (all timeframes in parallel)
        console.print("\n[bold]Fetching latest data from GMX API...[/bold]")
        gmx_candles = {}

        if self.use_gmx_api and self.gmx_fetcher:
            # Create async tasks for parallel timeframe fetching
            async def fetch_timeframe(tf: str, timeout: float = 120.0):
                """Fetch GMX data for a single timeframe with timeout.

                :param tf: Timeframe string
                :param timeout: Timeout in seconds (default: 120s = 2min)
                """
                gmx_period = map_timeframe_to_gmx_period(tf)
                try:
                    df = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.gmx_fetcher.fetch_gmx_candles, symbol, gmx_period
                        ),
                        timeout=timeout,
                    )
                    return tf, df
                except asyncio.TimeoutError:
                    console.print(
                        f"  [yellow]⏱ {tf}: Timeout after {timeout}s[/yellow]"
                    )
                    import pandas as pd

                    return tf, pd.DataFrame()  # Return empty DataFrame on timeout

            # Execute all timeframe fetches in parallel
            timeframe_tasks = [fetch_timeframe(tf) for tf in TIMEFRAMES]
            results = await asyncio.gather(*timeframe_tasks, return_exceptions=True)

            # Process results
            for result in results:
                if isinstance(result, Exception):
                    console.print(f"  [red]✗ Error fetching timeframe: {result}[/red]")
                    continue

                timeframe, gmx_df = result
                if not gmx_df.empty:
                    earliest, latest = (
                        gmx_df["timestamp"].min(),
                        gmx_df["timestamp"].max(),
                    )
                    console.print(
                        f"  [green]✓[/green] {timeframe}: [cyan]{len(gmx_df):,}[/cyan] candles from GMX [dim]({earliest} to {latest})[/dim]"
                    )
                    gmx_candles[timeframe] = gmx_df
                else:
                    console.print(
                        f"  [yellow]○[/yellow] {timeframe}: No GMX data available"
                    )

        if not gmx_candles:
            console.print(f"[yellow]No GMX data available for {symbol}[/yellow]")
            return

        # Step 2: Find Chainlink feed (if exists)
        console.print(f"\n[bold]Checking for Chainlink feed...[/bold]")
        chainlink_symbol = find_chainlink_symbol(symbol)
        chainlink_feed_address = (
            get_feed_address_for_gmx_symbol(symbol) if chainlink_symbol else None
        )

        if chainlink_feed_address:
            console.print(
                f"  [green]✓[/green] Found Chainlink feed: [yellow]{chainlink_feed_address}[/yellow]"
            )
            console.print(f"  [dim]Mapped symbol:[/dim] {symbol} → {chainlink_symbol}")
        else:
            console.print(
                f"  [yellow]○[/yellow] No Chainlink feed found - using GMX data only"
            )

        # Step 3: Calculate gap and backfill with Chainlink
        chainlink_candles = {}

        if chainlink_feed_address:
            # Use 1h candles to determine the gap (representative)
            gmx_1h = gmx_candles.get("1h")

            if gmx_1h is not None:
                backfill_start, backfill_end = self.gap_analyzer.calculate_gap(
                    gmx_df=gmx_1h, chainlink_available=True
                )

                console.print(f"\n[bold]Analyzing data gap...[/bold]")
                if backfill_end is not None:
                    gmx_earliest = gmx_1h["timestamp"].min()
                    console.print(f"  [dim]GMX coverage starts:[/dim] {gmx_earliest}")
                    console.print(
                        f"  [dim]Backfill needed:[/dim] Genesis → {gmx_earliest}"
                    )
                else:
                    console.print(
                        f"  [green]✓[/green] No gap - GMX data covers full history"
                    )

                # Collect Chainlink data to fill the gap
                if backfill_start is not None:
                    console.print(f"\n[bold]Backfilling with Chainlink data...[/bold]")

                    # Discover aggregator address
                    discovery = AggregatorDiscovery(self.web3)
                    try:
                        aggregator_info = discovery.get_aggregator_info(
                            chainlink_feed_address
                        )
                        aggregator_address = aggregator_info["current_aggregator"]
                        console.print(
                            f"  [dim]Aggregator:[/dim] [yellow]{aggregator_address}[/yellow]"
                        )
                    except Exception as e:
                        console.print(f"[red]✗ Aggregator discovery failed: {e}[/red]")
                        chainlink_feed_address = None  # Disable Chainlink backfill

                    if chainlink_feed_address:
                        # Determine start block
                        if full:
                            start_block = self.config.start_block or 0
                        else:
                            start_block = self.checkpoint_mgr.get_resume_block(
                                symbol, default=0
                            )

                        # Convert backfill_end timestamp to block
                        end_block = None  # Will query up to backfill_end timestamp

                        # Collect Chainlink events (HyperSync with RPC fallback)
                        try:
                            console.print(f"  [dim]Trying HyperSync first...[/dim]")
                            events, stats = await self.hypersync.collect_all_events(
                                aggregator_addresses=[aggregator_address],
                                start_block=start_block,
                                end_block=end_block,
                                auto_detect_start=False,
                            )

                            if events:
                                # Filter events to only those before GMX coverage
                                events = [
                                    e for e in events if e.timestamp <= backfill_end
                                ]

                                console.print(
                                    f"  [green]✓[/green] Collected [cyan]{len(events):,}[/cyan] Chainlink events via HyperSync"
                                )

                                # Save raw events
                                if full:
                                    self.storage.save_raw_events(
                                        events, symbol, partition_id=0
                                    )
                                else:
                                    self.storage.append_raw_events(
                                        events, symbol
                                    )

                                # Resample to OHLCV
                                raw_df = self.storage.read_raw_events(symbol)
                                chainlink_candles = (
                                    self.resampler.resample_all_timeframes(
                                        raw_df, symbol
                                    )
                                )

                                console.print(
                                    f"  [green]✓[/green] Resampled to OHLCV candles"
                                )

                        except Exception as e:
                            console.print(
                                f"  [yellow]⚠ HyperSync failed: {e}[/yellow]"
                            )
                            console.print(
                                f"  [cyan]Falling back to RPC collection...[/cyan]"
                            )

                            # RPC Fallback
                            try:
                                # Collect rounds via RPC
                                rounds = self.rpc_collector.collect_historical_rounds(
                                    aggregator_address=aggregator_address,
                                    start_timestamp=None,  # Collect all available
                                    end_timestamp=int(backfill_end.timestamp()) if backfill_end else None,
                                    max_rounds=100000,  # Generous limit
                                )

                                if rounds:
                                    # Convert rounds to events
                                    events = []
                                    for round_data in rounds:
                                        event = AnswerUpdatedEvent(
                                            block_number=0,  # Not available from RPC
                                            block_timestamp=round_data.updated_at,
                                            transaction_hash="",  # Not available from RPC
                                            log_index=0,
                                            round_id=round_data.round_id,
                                            price=round_data.answer,
                                            timestamp=round_data.updated_at,
                                            symbol=symbol,
                                            aggregator_address=aggregator_address,
                                        )
                                        events.append(event)

                                    console.print(
                                        f"  [green]✓[/green] Collected [cyan]{len(events):,}[/cyan] Chainlink events via RPC"
                                    )

                                    # Save raw events
                                    if full:
                                        self.storage.save_raw_events(
                                            events, symbol, partition_id=0
                                        )
                                    else:
                                        self.storage.append_raw_events(
                                            events, symbol
                                        )

                                    # Resample to OHLCV
                                    raw_df = self.storage.read_raw_events(symbol)
                                    chainlink_candles = (
                                        self.resampler.resample_all_timeframes(
                                            raw_df, symbol
                                        )
                                    )

                                    console.print(
                                        f"  [green]✓[/green] Resampled to OHLCV candles"
                                    )
                                else:
                                    console.print(
                                        f"  [red]✗ RPC collection returned no data[/red]"
                                    )

                            except Exception as rpc_error:
                                console.print(
                                    f"  [red]✗ RPC fallback also failed: {rpc_error}[/red]"
                                )

        # Step 4: Combine and save
        console.print("\n[bold]Saving combined candles...[/bold]")
        for timeframe in TIMEFRAMES:
            chainlink_df = chainlink_candles.get(timeframe)
            gmx_df = gmx_candles.get(timeframe)

            # Combine if both sources have data
            if chainlink_df is not None and gmx_df is not None and not gmx_df.empty:
                combined_df = combine_gmx_and_chainlink_data(gmx_df, chainlink_df)
                self.storage.save_candles(combined_df, timeframe, symbol)
                earliest, latest = (
                    combined_df["timestamp"].min(),
                    combined_df["timestamp"].max(),
                )
                console.print(
                    f"  [green]✓[/green] {timeframe}: [cyan]{len(combined_df):,}[/cyan] total candles [dim]({earliest} to {latest})[/dim]"
                )
            elif chainlink_df is not None:
                # Only Chainlink data
                self.storage.save_candles(chainlink_df, timeframe, symbol)
                earliest, latest = (
                    chainlink_df["timestamp"].min(),
                    chainlink_df["timestamp"].max(),
                )
                console.print(
                    f"  [green]✓[/green] {timeframe}: [cyan]{len(chainlink_df):,}[/cyan] candles [dim]({earliest} to {latest})[/dim]"
                )
            elif gmx_df is not None and not gmx_df.empty:
                # Only GMX data
                self.storage.save_candles(gmx_df, timeframe, symbol)
                earliest, latest = gmx_df["timestamp"].min(), gmx_df["timestamp"].max()
                console.print(
                    f"  [green]✓[/green] {timeframe}: [cyan]{len(gmx_df):,}[/cyan] candles [dim]({earliest} to {latest})[/dim]"
                )

        console.print(f"\n[bold green]✓ Collection complete for {symbol}[/bold green]")

    async def collect_all_symbols(
        self,
        full: bool = False,
        concurrency: int = 1,
        use_events: bool = False,
    ) -> None:
        """Collect data for all supported symbols with parallel processing.

        :param full: If True, collect from genesis; if False, resume from checkpoints
        :param concurrency: Number of symbols to process concurrently (default: 1, use --concurrency for parallel)
        :param use_events: Use event-based collection instead of oracle-based
        """
        if use_events:
            # TODO: Implement event-based collection in Task 6
            # Will use GMXEventCollector, GMXMarketMapper, and aggregate_events_to_ohlcv
            raise NotImplementedError(
                "Event-based collection (--use-events) is not yet implemented. "
                "This feature is under development. Use oracle-based collection (default) for now."
            )

        # Discover all GMX tokens
        console.print(f"\n[bold]Discovering GMX tokens...[/bold]")
        symbols = self.gmx_discovery.get_supported_symbols()
        console.print(
            f"  [green]✓[/green] Found [cyan]{len(symbols)}[/cyan] GMX-supported tokens"
        )

        total = len(symbols)
        successful = 0
        failed = 0
        failed_symbols = []

        console.print(
            f"\n[bold]Collecting data for [cyan]{total}[/cyan] symbols (concurrency: {concurrency})...[/bold]"
        )

        # Process symbols in batches for controlled parallelism
        for batch_start in range(0, len(symbols), concurrency):
            batch_end = min(batch_start + concurrency, len(symbols))
            batch = symbols[batch_start:batch_end]

            console.print(
                f"\n[bold cyan]Batch {batch_start // concurrency + 1}: Processing {len(batch)} symbols ({batch_start + 1}-{batch_end}/{total})[/bold cyan]"
            )

            # Create tasks for parallel execution
            tasks = []
            for symbol in batch:
                tasks.append(self.collect_symbol(symbol, full=full))

            # Execute batch in parallel, capturing exceptions
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results
            for symbol, result in zip(batch, results):
                if isinstance(result, Exception):
                    console.print(f"  [red]✗ {symbol}: {result}[/red]")
                    failed += 1
                    failed_symbols.append(symbol)
                else:
                    console.print(f"  [green]✓ {symbol}: Success[/green]")
                    successful += 1

        # Create summary table
        summary_table = Table(
            title="Collection Summary", box=box.ROUNDED, show_header=False
        )
        summary_table.add_column("Status", style="bold")
        summary_table.add_column("Count", justify="right")

        summary_table.add_row(
            "[green]✓ Successful[/green]", f"[green]{successful}/{total}[/green]"
        )
        summary_table.add_row("[red]✗ Failed[/red]", f"[red]{failed}/{total}[/red]")
        if failed_symbols:
            summary_table.add_row(
                "[yellow]Failed symbols[/yellow]",
                f"[yellow]{', '.join(failed_symbols)}[/yellow]",
            )

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
        help="HyperSync API token(s) - comma-separated for multiple tokens (or set HYPERSYNC_API_TOKEN env var)",
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
    use_events: bool = typer.Option(
        False,
        "--use-events",
        help="Use event-based collection (index GMX position events) instead of oracle-based",
    ),
    concurrency: int = typer.Option(
        1,
        "--concurrency",
        help="Number of symbols to process in parallel (default: 1 for sequential, set higher for parallel)",
        min=1,
        max=50,
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
            '     [yellow]export HYPERSYNC_API_TOKEN="your_token_here"[/yellow]\n'
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
            asyncio.run(
                collector.collect_all_symbols(
                    full=full,
                    concurrency=concurrency,
                    use_events=use_events,
                )
            )
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
