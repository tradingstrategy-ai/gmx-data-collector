"""Command-line interface for GMX historical data collection."""

import asyncio
from collections import defaultdict
from pathlib import Path
import traceback
from typing import Optional
import pandas as pd
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich import box
from web3 import Web3

console = Console()

from gmx_historical_data.config import CollectionConfig, TIMEFRAMES, GMX_V2_GENESIS_BLOCK, EXCLUDED_SYMBOLS
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
from gmx_historical_data.daemon.config import get_gmx_markets_without_chainlink_feeds
from gmx_historical_data.daemon.gap_detector import AdaptiveGapDetector, GapStatus
from gmx_historical_data.daemon.data_loss_handler import DataLossHandler


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

        # Adaptive gap detection for sliding window awareness
        if use_gmx_api and self.gmx_fetcher:
            self.adaptive_gap_detector = AdaptiveGapDetector(
                storage=self.storage,
                gmx_fetcher=self.gmx_fetcher,
            )
            self.data_loss_handler = DataLossHandler()
        else:
            self.adaptive_gap_detector = None
            self.data_loss_handler = None

        # Ensure directories exist
        config.ensure_directories()

    def check_and_report_data_loss(self, symbol: str) -> dict[str, any]:
        """Check for data loss across all timeframes for a symbol.

        Uses adaptive gap detection to identify if GMX API's sliding window
        has moved past our stored data, resulting in permanent data loss.

        :param symbol: Token symbol (e.g., 'ETH')
        :return: Dictionary with data loss info per timeframe
        """
        from gmx_historical_data.config import TIMEFRAMES

        if not self.adaptive_gap_detector:
            return {}

        results = {}
        for timeframe in TIMEFRAMES:
            try:
                gap_result = self.adaptive_gap_detector.detect_gap_adaptive(
                    symbol, timeframe
                )
                results[timeframe] = {
                    "status": gap_result.status.value,
                    "needs_fetch": gap_result.needs_fetch,
                    "has_data_loss": gap_result.has_data_loss,
                    "our_latest": gap_result.our_latest,
                    "api_earliest": gap_result.api_earliest,
                    "api_latest": gap_result.api_latest,
                    "lost_candles": gap_result.lost_candles_estimate,
                    "lost_timespan": gap_result.lost_timespan,
                }

                # Handle data loss
                if gap_result.has_data_loss and self.data_loss_handler:
                    event = self.data_loss_handler.handle_gap_result(
                        symbol, timeframe, gap_result
                    )
                    if event:
                        console.print(
                            f"  [red bold]DATA LOSS[/red bold] {timeframe}: "
                            f"~{gap_result.lost_candles_estimate} candles lost "
                            f"({gap_result.lost_timespan})"
                        )

            except Exception as e:
                console.print(f"  [yellow]Warning: Could not check {timeframe}: {e}[/yellow]")
                results[timeframe] = {"error": str(e)}

        return results

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

        # Step 0: Check for data loss (adaptive gap detection)
        if self.adaptive_gap_detector:
            console.print("\n[bold]Checking for data gaps (sliding window awareness)...[/bold]")
            gap_info = self.check_and_report_data_loss(symbol)

            # Summarize gap status
            data_loss_count = sum(1 for tf, info in gap_info.items() if info.get("has_data_loss"))
            no_gap_count = sum(1 for tf, info in gap_info.items() if info.get("status") == "no_gap")

            if data_loss_count > 0:
                console.print(f"  [red]⚠ Data loss detected in {data_loss_count} timeframe(s)[/red]")
            if no_gap_count > 0:
                console.print(f"  [green]✓ {no_gap_count} timeframe(s) are up to date[/green]")
            if no_gap_count < len(gap_info) - data_loss_count:
                needs_update = len(gap_info) - no_gap_count - data_loss_count
                console.print(f"  [cyan]→ {needs_update} timeframe(s) need incremental update[/cyan]")

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
                    gmx_df=gmx_1h,
                    chainlink_available=True,
                    gmx_v2_genesis_block=GMX_V2_GENESIS_BLOCK,
                )

                console.print(f"\n[bold]Analyzing data gap...[/bold]")
                if backfill_end is not None:
                    gmx_earliest = gmx_1h["timestamp"].min()
                    console.print(f"  [dim]GMX V2 genesis:[/dim] Block {backfill_start:,}")
                    console.print(f"  [dim]GMX coverage starts:[/dim] {gmx_earliest}")

                    # Calculate time window
                    time_delta = (gmx_earliest - pd.Timestamp('2023-08-07', tz='UTC')).total_seconds()
                    days = time_delta / 86400
                    console.print(f"  [dim]Backfill window:[/dim] Block {backfill_start:,} → {gmx_earliest} (~{days:.0f} days)")
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
                        console.print(f"[cyan]  Falling back to oracle events for historical data...[/cyan]")
                        chainlink_feed_address = None  # Disable Chainlink backfill

                        # Try oracle event fallback for this symbol
                        try:
                            await self._collect_symbol_via_oracle_fallback(symbol)
                        except Exception as fallback_e:
                            console.print(f"[yellow]  Oracle fallback also failed: {fallback_e}[/yellow]")

                    if chainlink_feed_address:
                        # Determine start block
                        if full:
                            start_block = backfill_start or GMX_V2_GENESIS_BLOCK
                        else:
                            start_block = self.checkpoint_mgr.get_resume_block(
                                symbol,
                                default=backfill_start or GMX_V2_GENESIS_BLOCK
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

    async def _collect_symbol_via_oracle_fallback(
        self,
        symbol: str,
        start_block: int | None = None,
        end_block: int | None = None,
    ) -> None:
        """Fallback collection for a single symbol via oracle events.

        Used when GMX API or Chainlink collection fails.

        :param symbol: Token symbol
        :param start_block: Starting block (default: GMX_V2_GENESIS_BLOCK)
        :param end_block: Ending block (default: latest)
        """
        from gmx_historical_data.oracle_price_collector import OraclePriceCollector
        from gmx_historical_data.gmx_token_mapper import GMXTokenMapper
        from gmx_historical_data.oracle_event_aggregator import aggregate_oracle_events_to_ohlcv

        # Initialize oracle collector (pure HyperSync, no RPC needed)
        oracle_collector = OraclePriceCollector(
            hypersync_endpoint=self.config.hypersync_endpoint,
            api_token=self.config.hypersync_api_token,
        )

        # Initialize token mapper
        token_mapper = GMXTokenMapper(self.web3)

        # Get token address for the symbol
        try:
            token_mapping = token_mapper.get_all_token_mapping()
        except Exception as e:
            console.print(f"  [red]Failed to get token mapping: {e}[/red]")
            return

        # Find the token address for this symbol
        symbol_upper = symbol.upper()
        token_address = None
        for addr, sym in token_mapping.items():
            if sym.upper() == symbol_upper:
                token_address = addr
                break

        if not token_address:
            console.print(f"  [yellow]No token address found for {symbol} in oracle mapping[/yellow]")
            return

        # Get token decimals for price conversion
        token_decimals = token_mapper.get_decimals_for_symbol(symbol_upper) or 18
        console.print(f"  [dim]Found token address:[/dim] {token_address} ({token_decimals} decimals)")

        # Determine block range
        start = start_block or GMX_V2_GENESIS_BLOCK

        # Collect oracle events for this token
        try:
            events = await oracle_collector.collect_oracle_events(
                start_block=start,
                end_block=end_block,
                token_addresses=[token_address],
            )
        except Exception as e:
            console.print(f"  [red]Failed to collect oracle events: {e}[/red]")
            return

        if not events:
            console.print(f"  [yellow]No oracle events found for {symbol}[/yellow]")
            return

        console.print(f"  [green]✓[/green] Collected [cyan]{len(events):,}[/cyan] oracle events")

        # Aggregate to OHLCV
        for timeframe in TIMEFRAMES:
            try:
                ohlcv = aggregate_oracle_events_to_ohlcv(
                    events, timeframe, symbol, token_decimals=token_decimals
                )

                if ohlcv.empty:
                    continue

                # Merge with existing data
                existing = self.storage.read_candles(timeframe, symbol)
                if not existing.empty:
                    merged = pd.concat([existing, ohlcv], ignore_index=True)
                    merged = merged.sort_values("timestamp").drop_duplicates(
                        subset=["timestamp"], keep="last"
                    )
                else:
                    merged = ohlcv

                self.storage.save_candles(merged, timeframe, symbol)
                console.print(f"  [green]✓[/green] {timeframe}: {len(ohlcv):,} candles via oracle fallback")

            except Exception as e:
                console.print(f"  [red]✗ {timeframe} oracle aggregation failed: {e}[/red]")

    async def collect_all_symbols(
        self,
        full: bool = False,
        concurrency: int = 1,
        use_events: bool = False,
    ) -> None:
        """Collect data for all supported symbols with parallel processing.

        :param full: If True, collect from genesis; if False, resume from checkpoints
        :param concurrency: Number of symbols to process concurrently (default: 1, use --concurrency for parallel). Note: Ignored in event mode.
        :param use_events: Use event-based collection (batch all markets) instead of oracle-based
        """
        if use_events:
            # Event-based collection mode
            console.print("[cyan]Using event-based collection (indexing position events)[/cyan]\n")

            # Initialize event collector
            event_collector = GMXEventCollector(
                hypersync_endpoint=self.config.hypersync_endpoint,
                rpc_url=self.config.rpc_url,
                api_token=self.config.hypersync_api_token,
            )

            # Initialize market mapper
            mapper = GMXMarketMapper(self.web3)
            market_mapping = mapper.get_market_symbol_mapping()

            # Invert mapping: market address -> symbol
            address_to_symbol = {addr.lower(): sym for addr, sym in market_mapping.items()}

            console.print(f"[dim]Mapped {len(address_to_symbol)} market addresses to symbols[/dim]\n")

            # Determine block range
            start_block = self.config.start_block or 0
            end_block = self.config.end_block

            console.print(f"Collecting ALL position events from block {start_block} to {end_block or 'latest'}...")

            # Collect ALL events at once (much faster than per-market)
            all_events = await event_collector.collect_position_events(
                start_block=start_block,
                end_block=end_block,
            )

            console.print(f"[green]✓[/green] Collected {len(all_events)} total events\n")

            # Group events by market address to aggregate per-symbol OHLCV data
            events_by_market = defaultdict(list)

            for event in all_events:
                market_address = event.market.lower()
                if market_address in address_to_symbol:
                    events_by_market[market_address].append(event)

            total = len(events_by_market)
            successful = 0
            failed = 0
            failed_symbols = []

            # Process each market
            for market_address, market_events in events_by_market.items():
                symbol = address_to_symbol[market_address]

                # Skip excluded symbols
                if symbol in EXCLUDED_SYMBOLS:
                    console.print(f"\n[dim]Skipping {symbol} (excluded)[/dim]")
                    continue

                console.print(f"\n[bold cyan]Processing {symbol}[/bold cyan]")
                console.print(f"  Events: {len(market_events)}")

                if len(market_events) == 0:
                    continue

                try:
                    # Save raw events
                    events_path = self.storage.save_position_events(
                        market_events,
                        symbol,
                        partition_id=0,
                    )
                    console.print(f"  [green]✓[/green] Saved raw events")

                    # Generate OHLCV for each timeframe
                    for timeframe in TIMEFRAMES:
                        try:
                            ohlcv = aggregate_events_to_ohlcv(
                                events=market_events,
                                timeframe=timeframe,
                                symbol=symbol,
                            )

                            if len(ohlcv) == 0:
                                continue

                            self.storage.save_candles(ohlcv, timeframe, symbol)

                            console.print(f"  [green]✓[/green] {timeframe}: {len(ohlcv)} candles")

                        except Exception as e:
                            console.print(f"  [red]✗ {timeframe} failed: {e}[/red]")

                    console.print(f"[green]✓ Complete for {symbol}[/green]")
                    successful += 1

                except Exception as e:
                    console.print(f"  [red]✗ Failed to save events: {e}[/red]")
                    failed += 1
                    failed_symbols.append(symbol)
                    continue

            console.print(f"\n[bold green]Event-based collection complete![/bold green]")

        else:
            # Existing oracle-based collection mode
            # Discover all GMX tokens
            console.print(f"\n[bold]Discovering GMX tokens...[/bold]")
            all_symbols = self.gmx_discovery.get_supported_symbols()

            # Filter out excluded symbols
            symbols = [s for s in all_symbols if s not in EXCLUDED_SYMBOLS]
            excluded_count = len(all_symbols) - len(symbols)

            console.print(
                f"  [green]✓[/green] Found [cyan]{len(symbols)}[/cyan] GMX-supported tokens"
            )
            if excluded_count > 0:
                console.print(
                    f"  [dim]Excluded {excluded_count} deprecated/problematic symbol(s)[/dim]"
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

    async def collect_non_chainlink_markets(
        self,
        start_block: int | None = None,
        end_block: int | None = None,
    ) -> None:
        """Collect data for non-Chainlink markets via OraclePriceUpdate events.

        Uses GMX oracle events from EventEmitter to build OHLCV candles for
        the 84 markets that don't have Chainlink price feeds.

        :param start_block: Starting block (default: GMX_V2_GENESIS_BLOCK)
        :param end_block: Ending block (default: latest)
        """
        from gmx_historical_data.oracle_price_collector import OraclePriceCollector
        from gmx_historical_data.gmx_token_mapper import GMXTokenMapper
        from gmx_historical_data.oracle_event_aggregator import aggregate_oracle_events_to_ohlcv

        console.print(Panel(
            "[bold magenta]Non-Chainlink Market Collection[/bold magenta]\n\n"
            "Collecting OHLCV data from OraclePriceUpdate events for markets\n"
            "without Chainlink price feeds.",
            box=box.ROUNDED,
        ))

        # Initialize oracle collector (pure HyperSync, no RPC needed)
        console.print("\n[bold]Initializing oracle price collector...[/bold]")
        oracle_collector = OraclePriceCollector(
            hypersync_endpoint=self.config.hypersync_endpoint,
            api_token=self.config.hypersync_api_token,
        )

        # Initialize token mapper
        token_mapper = GMXTokenMapper(self.web3)

        # Get non-Chainlink token mapping
        try:
            token_mapping = token_mapper.get_non_chainlink_tokens()
        except Exception as e:
            console.print(f"[red]Failed to get token mapping: {e}[/red]")
            return

        if not token_mapping:
            console.print("[yellow]No non-Chainlink tokens found[/yellow]")
            return

        console.print(f"[green]✓[/green] Found [cyan]{len(token_mapping)}[/cyan] non-Chainlink markets")

        # Get token decimals for price conversion
        try:
            token_decimals_map = token_mapper.get_token_decimals()
        except Exception as e:
            console.print(f"[yellow]Warning: Could not get token decimals: {e}. Using default 18.[/yellow]")
            token_decimals_map = {}

        # List markets
        market_list = ", ".join(sorted(set(token_mapping.values())))
        console.print(f"[dim]Markets: {market_list}[/dim]\n")

        # Determine block range
        start = start_block or GMX_V2_GENESIS_BLOCK
        console.print(f"[bold]Collecting oracle events...[/bold]")
        console.print(f"  [dim]Start block:[/dim] {start:,}")
        console.print(f"  [dim]End block:[/dim] {end_block or 'latest'}")

        # Collect ALL oracle events at once (efficient batch query)
        token_addresses = list(token_mapping.keys())
        try:
            events = await oracle_collector.collect_oracle_events(
                start_block=start,
                end_block=end_block,
                token_addresses=token_addresses,
            )
        except Exception as e:
            console.print(f"[red]Failed to collect oracle events: {e}[/red]")
            return

        if not events:
            console.print("[yellow]No oracle events found[/yellow]")
            return

        console.print(f"[green]✓[/green] Collected [cyan]{len(events):,}[/cyan] oracle events")

        # Group events by token
        events_by_token: dict[str, list] = defaultdict(list)
        for event in events:
            events_by_token[event.token.lower()].append(event)

        console.print(f"[dim]Events span {len(events_by_token)} unique tokens[/dim]\n")

        # Aggregate to OHLCV per symbol
        total = len(events_by_token)
        successful = 0
        failed = 0
        failed_symbols = []

        console.print("[bold]Aggregating to OHLCV candles...[/bold]")

        for token_addr, token_events in events_by_token.items():
            symbol = token_mapping.get(token_addr)
            if not symbol:
                continue

            # Skip excluded symbols
            if symbol in EXCLUDED_SYMBOLS:
                console.print(f"[dim]Skipping {symbol} (excluded)[/dim]")
                continue

            # Get decimals for this token (default 18 if not found)
            decimals = token_decimals_map.get(symbol, 18)
            console.print(f"\n[cyan]{symbol}[/cyan]: {len(token_events):,} events ({decimals} decimals)")

            try:
                for timeframe in TIMEFRAMES:
                    ohlcv = aggregate_oracle_events_to_ohlcv(
                        token_events, timeframe, symbol, token_decimals=decimals
                    )

                    if ohlcv.empty:
                        continue

                    # Merge with existing data
                    existing = self.storage.read_candles(timeframe, symbol)
                    if not existing.empty:
                        merged = pd.concat([existing, ohlcv], ignore_index=True)
                        merged = merged.sort_values("timestamp").drop_duplicates(
                            subset=["timestamp"], keep="last"
                        )
                    else:
                        merged = ohlcv

                    self.storage.save_candles(merged, timeframe, symbol)
                    console.print(f"  [green]✓[/green] {timeframe}: {len(ohlcv):,} candles")

                successful += 1

            except Exception as e:
                console.print(f"  [red]✗ Error: {e}[/red]")
                failed += 1
                failed_symbols.append(symbol)

        # Summary
        summary_table = Table(
            title="Non-Chainlink Collection Summary", box=box.ROUNDED, show_header=False
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
    collect_non_chainlink: bool = typer.Option(
        True,
        "--collect-non-chainlink/--no-collect-non-chainlink",
        help="Collect data for markets without Chainlink feeds using OraclePriceUpdate events (enabled by default)",
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

    By default, collects BOTH Chainlink markets (34) and non-Chainlink markets (84)
    for a total of 118 GMX V2 markets on Arbitrum.

    DATA SOURCES:
      • Chainlink Markets: GMX API (last ~6 months) + Chainlink HyperSync backfill
      • Non-Chainlink Markets: OraclePriceUpdate events via HyperSync + eth_defi

    TIMEFRAMES COLLECTED:
      1min, 5min, 15min, 1h, 4h, 1D

    OUTPUT STRUCTURE:
      data/
      └── candles/
          ├── ETH/
          │   ├── 1min.parquet
          │   ├── 5min.parquet
          │   ├── 15min.parquet
          │   ├── 1h.parquet
          │   ├── 4h.parquet
          │   └── 1D.parquet
          ├── BTC/
          │   └── ...
          └── SUI/
              └── ...

    EXAMPLES:

      # Full historical collection for all 118 markets
      export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
      export HYPERSYNC_API_TOKEN="YOUR_TOKEN"
      gmx_historical_data collect --full --output-dir ./data

      # Incremental update (faster, only new data since last collection)
      gmx_historical_data collect --update --output-dir ./data

      # Single symbol - Chainlink market (uses GMX API + Chainlink backfill)
      gmx_historical_data collect --full --symbol ETH --output-dir ./data

      # Single symbol - Non-Chainlink market (uses oracle events)
      gmx_historical_data collect --full --symbol SUI --output-dir ./data

      # Collect only Chainlink markets (skip 84 non-Chainlink markets)
      gmx_historical_data collect --full --no-collect-non-chainlink --output-dir ./data

      # Parallel collection (faster but uses more resources)
      gmx_historical_data collect --update --concurrency 4 --output-dir ./data

    VERIFICATION:
      After collection, verify data quality:
      gmx_historical_data verify --output-dir ./data
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
            # Single symbol collection
            symbol_upper = symbol.upper()
            if symbol_upper in EXCLUDED_SYMBOLS:
                console.print(f"[yellow]Warning: {symbol_upper} is excluded (deprecated/problematic)[/yellow]")
                console.print(f"[dim]Skipping collection for {symbol_upper}[/dim]")
                raise typer.Exit(0)

            # Check if symbol is non-Chainlink
            non_chainlink_symbols = get_gmx_markets_without_chainlink_feeds()
            if symbol_upper in non_chainlink_symbols:
                # Non-Chainlink symbol - use oracle events
                console.print(f"[cyan]{symbol_upper} is a non-Chainlink market - using oracle events[/cyan]")
                asyncio.run(
                    collector.collect_non_chainlink_markets(
                        start_block=start_block,
                        end_block=end_block,
                    )
                )
            elif use_events:
                console.print("[red]Error: Single symbol collection with --use-events is not supported. Use collect_all_symbols instead.[/red]")
                raise typer.Exit(1)
            else:
                asyncio.run(collector.collect_symbol(symbol_upper, full=full))
        else:
            # Collect all symbols
            # Step 1: Collect Chainlink markets via GMX API
            asyncio.run(
                collector.collect_all_symbols(
                    full=full,
                    concurrency=concurrency,
                    use_events=use_events,
                )
            )

            # Step 2: Collect non-Chainlink markets via oracle events (if enabled)
            if collect_non_chainlink:
                console.print("\n")
                asyncio.run(
                    collector.collect_non_chainlink_markets(
                        start_block=start_block,
                        end_block=end_block,
                    )
                )
    except KeyboardInterrupt:
        console.print("\n\n[yellow]Collection interrupted by user[/yellow]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"\n\n[red bold]Error: {e}[/red bold]")
        traceback.print_exc()
        raise typer.Exit(1)


def verify_command(
    symbol: Optional[str] = typer.Option(None, "--symbol", help="Symbol to verify (omit for all)"),
    output_dir: Path = typer.Option(Path("./data"), "--output-dir", help="Data directory"),
    timeframe: str = typer.Option("1h", "--timeframe", help="Timeframe to verify"),
) -> None:
    """Verify collected data quality.

    Checks for data gaps, coverage, and quality metrics for collected OHLCV data.

    METRICS REPORTED:
      • Coverage Start/End: Time range of collected data
      • Candle Count: Total number of candles
      • Gaps: Missing data periods
      • Quality Score: 0-100 score based on completeness

    EXAMPLES:

      # Verify all symbols (1h timeframe by default)
      gmx_historical_data verify --output-dir ./data

      # Verify specific symbol
      gmx_historical_data verify --symbol ETH --output-dir ./data

      # Verify different timeframe
      gmx_historical_data verify --timeframe 5min --output-dir ./data

    PYTHON VERIFICATION:
      # Quick check of Parquet files
      python -c "
      import pandas as pd
      from pathlib import Path

      data_dir = Path('./data/candles')
      for symbol_dir in sorted(data_dir.iterdir())[:10]:
          if symbol_dir.is_dir():
              for tf_file in symbol_dir.glob('*.parquet'):
                  df = pd.read_parquet(tf_file)
                  print(f'{symbol_dir.name}/{tf_file.stem}: {len(df)} candles')
      "
    """
    from gmx_historical_data.data_verifier import DataVerifier

    storage = ParquetStorage(output_dir)
    verifier = DataVerifier(storage)

    console.print("\n[bold cyan]Data Verification Report[/bold cyan]\n")

    # Get symbols to verify
    if symbol:
        symbols = [symbol.upper()]
    else:
        # Discover all collected symbols
        candles_dir = storage.candles_dir
        if not candles_dir.exists():
            console.print("[yellow]No data found in output directory[/yellow]")
            return
        symbols = [d.name for d in candles_dir.iterdir() if d.is_dir()]

    if not symbols:
        console.print("[yellow]No symbols found[/yellow]")
        return

    # Create verification table
    table = Table(show_header=True, header_style="bold magenta", box=box.ROUNDED)
    table.add_column("Symbol", style="cyan", width=8)
    table.add_column("Coverage Start", width=20)
    table.add_column("Coverage End", width=20)
    table.add_column("Candles", justify="right", width=10)
    table.add_column("Gaps", justify="right", width=8)
    table.add_column("Quality", justify="right", width=10)

    for sym in sorted(symbols):
        report = verifier.verify_symbol(sym, timeframe)

        if report.coverage_start is None:
            table.add_row(
                sym,
                "[dim]No data[/dim]",
                "[dim]No data[/dim]",
                "0",
                "N/A",
                "[dim]N/A[/dim]",
            )
            continue

        gaps_str = (
            f"[yellow]{len(report.gaps_detected)}[/yellow]"
            if report.gaps_detected
            else "[green]0[/green]"
        )

        quality_color = (
            "green"
            if report.quality_score > 80
            else "yellow"
            if report.quality_score > 60
            else "red"
        )
        quality_str = f"[{quality_color}]{report.quality_score:.0f}/100[/{quality_color}]"

        table.add_row(
            sym,
            str(report.coverage_start),
            str(report.coverage_end),
            f"{report.total_candles:,}",
            gaps_str,
            quality_str,
        )

    console.print(table)
    console.print()


def debug_oracle_command(
    symbol: Optional[str] = typer.Option(
        None,
        "--symbol",
        help="Specific token symbol to debug (e.g., SUI, TAO). If not provided, lists all non-Chainlink markets.",
    ),
    rpc_url: Optional[str] = typer.Option(
        None,
        "--rpc-url",
        envvar="JSON_RPC_ARBITRUM",
        help="Arbitrum RPC URL",
    ),
    hypersync_token: Optional[str] = typer.Option(
        None,
        "--hypersync-token",
        envvar="HYPERSYNC_API_TOKEN",
        help="HyperSync API token",
    ),
    limit: int = typer.Option(
        100,
        "--limit",
        help="Maximum number of events to show",
    ),
    show_raw: bool = typer.Option(
        False,
        "--show-raw",
        help="Show raw event data for debugging parsing issues",
    ),
    concurrency: int = typer.Option(
        4,
        "--concurrency",
        help="Number of parallel workers for block scanning (1-8)",
    ),
) -> None:
    """Debug oracle events for non-Chainlink tokens.

    Use this command to:
      • List all GMX markets and their Chainlink status
      • Verify OraclePriceUpdate events are being emitted for a token
      • Debug collection issues for specific tokens
      • Inspect raw event data for parsing issues

    Automatically fetches ALL historical events from GMX V2 genesis block
    (block 120,000,000 on Arbitrum).

    MARKET CATEGORIES:
      • Chainlink Markets (34): Have public price feeds, use GMX API
      • Non-Chainlink Markets (84): Use OraclePriceUpdate events

    EXAMPLES:

      # List all markets and their data source
      gmx_historical_data debug-oracle

      # Debug oracle events for a non-Chainlink token
      gmx_historical_data debug-oracle --symbol SUI

      # Show raw event data for debugging parsing issues
      gmx_historical_data debug-oracle --symbol TAO --show-raw --limit 10

      # Use more parallel workers for faster scanning
      gmx_historical_data debug-oracle --symbol HYPE --concurrency 8

      # Limit output for quick check
      gmx_historical_data debug-oracle --symbol VIRTUAL --limit 50

    OUTPUT INCLUDES:
      • Token address and decimals
      • Market type (Chainlink vs Non-Chainlink)
      • Sample price events with timestamps
      • Price summary (range, latest, block range, time range)

    TROUBLESHOOTING:
      If no events found:
        1. Verify the token exists in GMX markets
        2. Check if token is a Chainlink market (uses different data source)
        3. Try increasing --concurrency for faster scanning
    """
    import asyncio
    from web3 import Web3
    from gmx_historical_data.gmx_token_mapper import GMXTokenMapper
    from gmx_historical_data.daemon.config import (
        get_gmx_markets_with_chainlink_feeds,
        get_gmx_markets_without_chainlink_feeds,
    )

    # Validate RPC URL
    if not rpc_url:
        console.print("[red]Error: RPC URL required. Set JSON_RPC_ARBITRUM env var or use --rpc-url[/red]")
        raise typer.Exit(1)

    web3 = Web3(Web3.HTTPProvider(rpc_url))

    # Initialize token mapper
    console.print("[bold]Initializing GMX token mapper...[/bold]")
    try:
        token_mapper = GMXTokenMapper(web3)
        all_tokens = token_mapper.get_all_token_mapping()
        non_chainlink_tokens = token_mapper.get_non_chainlink_tokens()
        chainlink_tokens = token_mapper.get_chainlink_tokens()
    except Exception as e:
        console.print(f"[red]Failed to initialize token mapper: {e}[/red]")
        raise typer.Exit(1)

    # If no symbol, just list markets
    if not symbol:
        console.print()
        console.print(Panel(
            f"[bold cyan]GMX Market Summary[/bold cyan]\n\n"
            f"Total markets: {len(all_tokens)}\n"
            f"Chainlink markets: {len(chainlink_tokens)}\n"
            f"Non-Chainlink markets: {len(non_chainlink_tokens)}",
            box=box.ROUNDED,
        ))

        # Show Chainlink markets
        chainlink_table = Table(title="Chainlink Markets (GMX API)", box=box.ROUNDED)
        chainlink_table.add_column("Symbol", style="green")
        chainlink_table.add_column("Token Address", style="dim")

        for addr, sym in sorted(chainlink_tokens.items(), key=lambda x: x[1]):
            chainlink_table.add_row(sym, addr)

        console.print(chainlink_table)
        console.print()

        # Show non-Chainlink markets
        non_chainlink_table = Table(title="Non-Chainlink Markets (Oracle Events)", box=box.ROUNDED)
        non_chainlink_table.add_column("Symbol", style="magenta")
        non_chainlink_table.add_column("Token Address", style="dim")

        for addr, sym in sorted(non_chainlink_tokens.items(), key=lambda x: x[1]):
            non_chainlink_table.add_row(sym, addr)

        console.print(non_chainlink_table)
        return

    # Debug specific symbol
    symbol_upper = symbol.upper()
    console.print(f"\n[bold]Debugging oracle events for {symbol_upper}...[/bold]")

    # Find token address
    token_address = None
    is_chainlink = False

    for addr, sym in all_tokens.items():
        if sym.upper() == symbol_upper:
            token_address = addr
            is_chainlink = sym in [s for _, s in chainlink_tokens.items()]
            break

    if not token_address:
        console.print(f"[red]Token {symbol_upper} not found in GMX markets[/red]")
        raise typer.Exit(1)

    # Get token decimals for price conversion
    token_decimals = token_mapper.get_decimals_for_symbol(symbol_upper) or 18

    console.print(f"  [dim]Token address:[/dim] {token_address}")
    console.print(f"  [dim]Token decimals:[/dim] {token_decimals}")
    console.print(f"  [dim]Market type:[/dim] {'Chainlink' if is_chainlink else 'Non-Chainlink (Oracle Events)'}")

    if is_chainlink:
        console.print(f"\n[yellow]Note: {symbol_upper} is a Chainlink market.[/yellow]")
        console.print("[yellow]It uses GMX API for data, but may also have oracle events.[/yellow]")

    from gmx_historical_data.oracle_price_collector import OraclePriceCollector
    from hypersync import HypersyncClient, ClientConfig

    # Get current block via HyperSync (no RPC needed)
    console.print(f"\n[bold]Initializing HyperSync collector...[/bold]")
    try:
        config = ClientConfig(url="https://arbitrum.hypersync.xyz", bearer_token=hypersync_token)
        hs_client = HypersyncClient(config)
        current_block = asyncio.run(hs_client.get_height())
        console.print(f"  [dim]Current block:[/dim] {current_block:,}")
    except Exception as e:
        console.print(f"[red]Failed to get current block from HyperSync: {e}[/red]")
        raise typer.Exit(1)

    # Start from GMX V2 genesis block to get ALL historical events
    start_block = GMX_V2_GENESIS_BLOCK
    console.print(f"  [dim]Scanning from GMX V2 genesis:[/dim] block {start_block:,} to {current_block:,}")

    # Collect oracle events
    total_blocks = current_block - start_block
    console.print(f"\n[bold]Fetching oracle events from HyperSync...[/bold]")
    console.print(f"  [dim]Block range:[/dim] {total_blocks:,} blocks to scan")
    console.print(f"  [dim]Using {concurrency} parallel workers for faster collection[/dim]")
    console.print()

    # Pure HyperSync collector - no RPC needed for maximum speed
    oracle_collector = OraclePriceCollector(
        hypersync_endpoint="https://arbitrum.hypersync.xyz",
        api_token=hypersync_token,
    )

    # Progress tracking with Rich live display
    progress_status = {"message": "Starting..."}

    def progress_callback(msg: str):
        progress_status["message"] = msg
        # Also print to console for logging
        console.print(f"  [dim]{msg}[/dim]")

    async def fetch_events():
        return await oracle_collector.collect_oracle_events(
            start_block=start_block,
            end_block=current_block,
            token_addresses=[token_address],
            concurrency=concurrency,
            progress_callback=progress_callback,
        )

    try:
        events = asyncio.run(fetch_events())
    except Exception as e:
        console.print(f"[red]Failed to fetch oracle events: {e}[/red]")
        traceback.print_exc()
        raise typer.Exit(1)

    if not events:
        console.print(f"[yellow]No oracle events found for {symbol_upper} in the specified block range[/yellow]")
        console.print("[dim]Try using --start-block with an earlier block number[/dim]")
        return

    console.print(f"[green]✓[/green] Found [cyan]{len(events):,}[/cyan] oracle events")

    # Show raw event data if requested (for debugging)
    if show_raw and events:
        console.print("\n[bold]Raw Event Data (first event):[/bold]")
        first_event = events[0]
        console.print(f"  block_number: {first_event.block_number}")
        console.print(f"  block_timestamp: {first_event.block_timestamp}")
        console.print(f"  transaction_hash: {first_event.transaction_hash}")
        console.print(f"  log_index: {first_event.log_index}")
        console.print(f"  token: {first_event.token}")
        console.print(f"  provider: {first_event.provider}")
        console.print(f"  min_price (raw): {first_event.min_price}")
        console.print(f"  max_price (raw): {first_event.max_price}")
        console.print(f"  timestamp: {first_event.timestamp}")
        console.print()

    # Show sample events
    events_table = Table(title=f"Oracle Events for {symbol_upper} (showing up to {limit})", box=box.ROUNDED)
    events_table.add_column("Block", style="dim", justify="right")
    events_table.add_column("Timestamp", style="cyan")
    events_table.add_column("Min Price", justify="right")
    events_table.add_column("Max Price", justify="right")
    events_table.add_column("Mid Price (USD)", justify="right", style="green")

    from datetime import datetime
    from gmx_historical_data.oracle_event_aggregator import get_price_divisor

    # Calculate divisor based on token decimals
    price_divisor = get_price_divisor(token_decimals)

    for event in events[:limit]:
        timestamp = datetime.utcfromtimestamp(event.block_timestamp).strftime("%Y-%m-%d %H:%M:%S")
        min_price = event.min_price / price_divisor
        max_price = event.max_price / price_divisor
        mid_price = (min_price + max_price) / 2

        events_table.add_row(
            f"{event.block_number:,}",
            timestamp,
            f"${min_price:,.4f}",
            f"${max_price:,.4f}",
            f"${mid_price:,.4f}",
        )

    console.print()
    console.print(events_table)

    if len(events) > limit:
        console.print(f"\n[dim]... and {len(events) - limit} more events (use --limit to see more)[/dim]")

    # Show summary statistics
    console.print()
    min_prices = [e.min_price / price_divisor for e in events]
    max_prices = [e.max_price / price_divisor for e in events]
    mid_prices = [(e.min_price + e.max_price) / 2 / price_divisor for e in events]

    summary_table = Table(title="Price Summary", box=box.ROUNDED, show_header=False)
    summary_table.add_column("Metric", style="dim")
    summary_table.add_column("Value", style="cyan")

    summary_table.add_row("Events count", f"{len(events):,}")
    summary_table.add_row("Price range", f"${min(mid_prices):,.4f} - ${max(mid_prices):,.4f}")
    summary_table.add_row("Latest price", f"${mid_prices[-1]:,.4f}")
    summary_table.add_row("Block range", f"{events[0].block_number:,} - {events[-1].block_number:,}")

    first_ts = datetime.utcfromtimestamp(events[0].block_timestamp)
    last_ts = datetime.utcfromtimestamp(events[-1].block_timestamp)
    summary_table.add_row("Time range", f"{first_ts} - {last_ts}")

    console.print(summary_table)


# Create Typer app with comprehensive help
CLI_HELP = """
GMX Historical Data Collection CLI

Collects OHLCV price data for GMX V2 markets on Arbitrum.

MARKET TYPES:
  • Chainlink Markets (34): ETH, BTC, SOL, ARB, LINK, etc.
    - Data source: GMX API + Chainlink historical backfill
    - Collection: Uses GMXDataFetcher + HyperSync

  • Non-Chainlink Markets (84): SUI, HYPE, TAO, VIRTUAL, etc.
    - Data source: OraclePriceUpdate events from GMX EventEmitter
    - Collection: Uses OraclePriceCollector via HyperSync

ENVIRONMENT VARIABLES:
  JSON_RPC_ARBITRUM     Arbitrum RPC URL (required)
  HYPERSYNC_API_TOKEN   HyperSync API token from envio.dev (required)

QUICK START:
  # Full historical collection for all 118 markets
  gmx_historical_data collect --full --output-dir ./data

  # Incremental update (faster, only new data)
  gmx_historical_data collect --update --output-dir ./data

  # Single symbol collection
  gmx_historical_data collect --full --symbol ETH --output-dir ./data

  # Verify data quality
  gmx_historical_data verify --output-dir ./data

  # Debug oracle events for non-Chainlink tokens
  gmx_historical_data debug-oracle --symbol SUI --show-raw

DAEMON (Continuous Collection):
  gmx-periodic-collector

For more details on each command, use --help:
  gmx_historical_data collect --help
  gmx_historical_data verify --help
  gmx_historical_data debug-oracle --help
"""

app = typer.Typer(help=CLI_HELP, rich_markup_mode="rich")

# Register commands
app.command(name="collect")(cli)
app.command(name="verify")(verify_command)
app.command(name="debug-oracle")(debug_oracle_command)


def main() -> None:
    """Main CLI entry point."""
    app()


if __name__ == "__main__":
    main()
