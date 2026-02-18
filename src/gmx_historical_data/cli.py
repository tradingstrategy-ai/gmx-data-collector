"""Command-line interface for GMX historical data collection."""

import asyncio
from collections import defaultdict
from datetime import datetime
import logging
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

logger = logging.getLogger(__name__)

console = Console()

from gmx_historical_data.config import (
    CollectionConfig,
    FetchMode,
    TIMEFRAMES,
    GMX_V2_GENESIS_BLOCK,
    is_excluded_symbol,
)
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
from gmx_historical_data.daemon.config import (
    get_gmx_markets_with_chainlink_feeds,
    get_gmx_markets_without_chainlink_feeds,
)
from gmx_historical_data.daemon.gap_detector import AdaptiveGapDetector
from gmx_historical_data.daemon.data_loss_handler import DataLossHandler
from gmx_historical_data.fetch_boundary_calculator import FetchBoundaryCalculator


class DataCollector:
    """Main data collection orchestrator.

    :param config: Collection configuration
    :param use_gmx_api: If True, fetch latest data from GMX API and historical from Chainlink
    :param chainlink_concurrency: Number of concurrent workers for Chainlink RPC batch requests
    """

    def __init__(
        self,
        config: CollectionConfig,
        use_gmx_api: bool = True,
        chainlink_concurrency: int = 4,
        use_hypersync: bool = True,
    ) -> None:
        """Initialize data collector.

        :param config: Collection configuration
        :param use_gmx_api: If True, fetch latest data from GMX API
        :param chainlink_concurrency: Concurrent workers for Chainlink RPC batches
        :param use_hypersync: If True, initialize HyperSync for oracle events (required for non-Chainlink symbols)
        """
        self.config = config
        self.use_gmx_api = use_gmx_api
        self.chainlink_concurrency = chainlink_concurrency

        # Check if RPC URL contains multiple providers (space-separated)
        rpc_urls = config.rpc_url.strip().split()
        if len(rpc_urls) > 1:
            # Multiple RPC URLs - use multi-provider
            console.print(f"[green]Using {len(rpc_urls)} RPC provider(s) with automatic failover[/green]")
            self.rpc_collector = ChainlinkRPCCollector(rpc_config=config.rpc_url)
            # Create Web3 with first URL for basic operations
            self.web3 = Web3(Web3.HTTPProvider(rpc_urls[0]))
        else:
            # Single RPC URL - use simple provider
            console.print("[yellow]Using single RPC provider (no automatic failover)[/yellow]")
            self.web3 = Web3(Web3.HTTPProvider(config.rpc_url))
            self.rpc_collector = ChainlinkRPCCollector(self.web3)

        # Only initialize HyperSync if needed (for non-Chainlink symbols or oracle events)
        if use_hypersync:
            self.hypersync = HyperSyncCollector(
                config.hypersync_endpoint,
                config.hypersync_api_token,
            )
        else:
            self.hypersync = None
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

        # Fetch boundary calculator for smart incremental updates
        if use_gmx_api:
            self.boundary_calculator = FetchBoundaryCalculator(
                storage=self.storage,
                adaptive_gap_detector=self.adaptive_gap_detector,
                gap_analyzer=self.gap_analyzer,
            )
        else:
            self.boundary_calculator = None

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
                console.print(
                    f"  [yellow]Warning: Could not check {timeframe}: {e}[/yellow]"
                )
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
            console.print(
                "\n[bold]Checking for data gaps (sliding window awareness)...[/bold]"
            )
            gap_info = self.check_and_report_data_loss(symbol)

            # Summarize gap status
            data_loss_count = sum(
                1 for tf, info in gap_info.items() if info.get("has_data_loss")
            )
            no_gap_count = sum(
                1 for tf, info in gap_info.items() if info.get("status") == "no_gap"
            )

            if data_loss_count > 0:
                console.print(
                    f"  [red]⚠ Data loss detected in {data_loss_count} timeframe(s)[/red]"
                )
            if no_gap_count > 0:
                console.print(
                    f"  [green]✓ {no_gap_count} timeframe(s) are up to date[/green]"
                )
            if no_gap_count < len(gap_info) - data_loss_count:
                needs_update = len(gap_info) - no_gap_count - data_loss_count
                console.print(
                    f"  [cyan]→ {needs_update} timeframe(s) need incremental update[/cyan]"
                )

        # Step 0.5: Determine collection mode and calculate fetch boundaries
        fetch_mode = FetchMode.FULL if full else FetchMode.INCREMENTAL
        console.print(f"\n[bold]Collection mode: {fetch_mode.value}[/bold]")

        # Check Chainlink availability early for boundary calculation
        chainlink_symbol = find_chainlink_symbol(symbol)
        chainlink_available = get_feed_address_for_gmx_symbol(symbol) is not None

        # Step 1: Fetch GMX data with boundary-aware logic
        console.print("\n[bold]Fetching data from GMX API...[/bold]")
        gmx_candles = {}
        fetch_boundaries_by_tf = {}

        if self.use_gmx_api and self.gmx_fetcher and self.boundary_calculator:
            # Calculate boundaries for each timeframe
            for tf in TIMEFRAMES:
                boundaries = self.boundary_calculator.calculate_boundaries(
                    symbol=symbol,
                    timeframe=tf,
                    mode=fetch_mode,
                    chainlink_available=chainlink_available,
                    gmx_earliest=None,  # Will be determined from GMX API response
                )
                fetch_boundaries_by_tf[tf] = boundaries

                if not boundaries.gmx_api_needed:
                    console.print(
                        f"  [green]✓[/green] {tf}: Data is current, skipping GMX API fetch"
                    )
                    # Load existing data from storage
                    existing_df = self.storage.read_candles(tf, symbol)
                    if not existing_df.empty:
                        gmx_candles[tf] = existing_df
                else:
                    mode_label = "full" if boundaries.mode == FetchMode.FULL else "incremental"
                    console.print(
                        f"  [cyan]→[/cyan] {tf}: Fetching from GMX API ({mode_label})"
                    )

            # Fetch only timeframes that need updates
            async def fetch_timeframe(tf: str, timeout: float = 120.0):
                """Fetch GMX data for a single timeframe with timeout.

                :param tf: Timeframe string
                :param timeout: Timeout in seconds (default: 120s = 2min)
                """
                boundaries = fetch_boundaries_by_tf[tf]
                if not boundaries.gmx_api_needed:
                    return tf, None  # Skip

                gmx_period = map_timeframe_to_gmx_period(tf)
                try:
                    df = await asyncio.wait_for(
                        asyncio.to_thread(
                            self.gmx_fetcher.fetch_gmx_candles, symbol, gmx_period
                        ),
                        timeout=timeout,
                    )

                    # Filter to boundary range if incremental
                    if boundaries.mode == FetchMode.INCREMENTAL and boundaries.gmx_api_start:
                        df = df[df["timestamp"] >= boundaries.gmx_api_start]

                    return tf, df
                except asyncio.TimeoutError:
                    console.print(
                        f"  [yellow]⏱ {tf}: Timeout after {timeout}s[/yellow]"
                    )
                    return tf, pd.DataFrame()  # Return empty DataFrame on timeout

            # Execute fetches in parallel (only for timeframes that need updates)
            timeframe_tasks = [
                fetch_timeframe(tf)
                for tf in TIMEFRAMES
                if fetch_boundaries_by_tf[tf].gmx_api_needed
            ]

            if timeframe_tasks:
                results = await asyncio.gather(*timeframe_tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        console.print(f"  [red]✗ Error fetching timeframe: {result}[/red]")
                        continue

                    timeframe, gmx_df = result
                    if gmx_df is not None and not gmx_df.empty:
                        earliest, latest = (
                            gmx_df["timestamp"].min(),
                            gmx_df["timestamp"].max(),
                        )
                        console.print(
                            f"  [green]✓[/green] {timeframe}: [cyan]{len(gmx_df):,}[/cyan] candles from GMX [dim]({earliest} to {latest})[/dim]"
                        )
                        gmx_candles[timeframe] = gmx_df
        elif self.use_gmx_api and self.gmx_fetcher:
            # Fallback to old behavior if boundary calculator not available
            async def fetch_timeframe(tf: str, timeout: float = 120.0):
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
                    return tf, pd.DataFrame()

            timeframe_tasks = [fetch_timeframe(tf) for tf in TIMEFRAMES]
            results = await asyncio.gather(*timeframe_tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    console.print(f"  [red]✗ Error fetching timeframe: {result}[/red]")
                    continue

                timeframe, gmx_df = result
                if not gmx_df.empty:
                    gmx_candles[timeframe] = gmx_df

        if not gmx_candles:
            console.print(f"[yellow]No GMX data available for {symbol}[/yellow]")
            return

        # Step 2: Find Chainlink feed (already determined earlier)
        chainlink_feed_address = (
            get_feed_address_for_gmx_symbol(symbol) if chainlink_available else None
        )

        if chainlink_feed_address:
            console.print("\n[bold]Checking for Chainlink feed...[/bold]")
            console.print(
                f"  [green]✓[/green] Found Chainlink feed: [yellow]{chainlink_feed_address}[/yellow]"
            )
            console.print(f"  [dim]Mapped symbol:[/dim] {symbol} → {chainlink_symbol}")
        else:
            console.print(
                "\n[bold]No Chainlink feed found[/bold] - will use oracle events for historical data"
            )

        # Step 3: Backfill with Chainlink based on calculated boundaries
        chainlink_candles = {}

        if chainlink_feed_address:
            # Use 1h timeframe boundaries (representative)
            boundaries_1h = fetch_boundaries_by_tf.get("1h")

            if boundaries_1h and boundaries_1h.chainlink_needed:
                console.print("\n[bold]Backfilling with Chainlink data...[/bold]")
                console.print(
                    f"  [dim]Mode:[/dim] {boundaries_1h.mode.value}"
                )
                if boundaries_1h.chainlink_end_timestamp:
                    console.print(
                        f"  [dim]Fetch range:[/dim] all historical to timestamp {boundaries_1h.chainlink_end_timestamp}"
                    )
                else:
                    console.print(
                        f"  [dim]Fetch range:[/dim] all available historical data"
                    )

                # Use feed proxy address directly (NOT underlying aggregator!)
                # The aggregator blocks contract-to-contract calls with "No access"
                try:
                    console.print(
                        f"  [dim]Feed proxy:[/dim] [yellow]{chainlink_feed_address}[/yellow]"
                    )

                    # Collect with boundary-aware timestamps using feed proxy
                    rounds = self.rpc_collector.collect_historical_rounds(
                        feed_address=chainlink_feed_address,  # Use feed proxy, NOT aggregator!
                        start_timestamp=boundaries_1h.chainlink_start_timestamp,  # None = fetch all
                        end_timestamp=boundaries_1h.chainlink_end_timestamp,      # Use calculated boundary
                        max_rounds=1000000,
                        batch_size=1500,  # Safe for most RPC providers; auto-reduces on 413
                        concurrency=self.chainlink_concurrency,
                    )

                    if rounds:
                        # Convert rounds to events for storage compatibility
                        events = []
                        for round_data in rounds:
                            event = AnswerUpdatedEvent(
                                block_number=0,
                                block_timestamp=round_data.updated_at,
                                transaction_hash="",
                                log_index=0,
                                aggregator_address=chainlink_feed_address,  # Use feed address
                                price=round_data.answer,
                                round_id=round_data.round_id,
                                timestamp=round_data.updated_at,
                            )
                            events.append(event)

                        console.print(
                            f"  [green]✓[/green] Collected [cyan]{len(events):,}[/cyan] Chainlink rounds via RPC"
                        )

                        # Save raw events
                        if full:
                            self.storage.save_raw_events(
                                events, symbol, partition_id=0
                            )
                        else:
                            self.storage.append_raw_events(events, symbol)

                        # Resample to OHLCV
                        raw_df = self.storage.read_raw_events(symbol)
                        chainlink_candles = (
                            self.resampler.resample_all_timeframes(
                                raw_df, symbol
                            )
                        )

                        console.print(
                            "  [green]✓[/green] Resampled to OHLCV candles"
                        )
                    else:
                        console.print(
                            "  [yellow]⚠ RPC collection returned no data[/yellow]"
                        )

                except Exception as e:
                    console.print(f"[red]✗ Chainlink backfill failed: {e}[/red]")
                    chainlink_feed_address = None  # Disable Chainlink backfill

                    # Try oracle event fallback for this symbol (requires HyperSync)
                    if self.hypersync is None:
                        console.print(
                            "[yellow]  Oracle events unavailable (HyperSync not initialized)[/yellow]"
                        )
                    else:
                        console.print(
                            "[cyan]  Falling back to oracle events for historical data...[/cyan]"
                        )
                        try:
                            await self._collect_symbol_via_oracle_fallback(symbol)
                            # Read back from storage into chainlink_candles
                            # so the combining step can merge historical + GMX data
                            for tf in TIMEFRAMES:
                                stored_df = self.storage.read_candles(tf, symbol)
                                if not stored_df.empty:
                                    chainlink_candles[tf] = stored_df
                                    console.print(
                                        f"  [green]✓[/green] {tf}: Loaded {len(stored_df):,} historical candles from storage"
                                    )
                        except Exception as fallback_e:
                            console.print(
                                f"[yellow]  Oracle fallback also failed: {fallback_e}[/yellow]"
                            )
            else:
                console.print(
                    "\n[green]✓[/green] Chainlink backfill not needed - data is complete"
                )
        else:
            # No Chainlink feed - use oracle events for historical data (requires HyperSync)
            if self.hypersync is None:
                console.print(
                    "\n[yellow]⚠ No Chainlink feed found and HyperSync not initialized[/yellow]"
                )
                console.print(
                    "[dim]This symbol requires oracle events, which need HyperSync[/dim]"
                )
            else:
                console.print("\n[bold]Backfilling with oracle events...[/bold]")
                try:
                    await self._collect_symbol_via_oracle_fallback(symbol)
                    # Read back from storage into chainlink_candles
                    # so the combining step can merge historical + GMX data
                    for tf in TIMEFRAMES:
                        stored_df = self.storage.read_candles(tf, symbol)
                        if not stored_df.empty:
                            chainlink_candles[tf] = stored_df
                            console.print(
                                f"  [green]✓[/green] {tf}: Loaded {len(stored_df):,} historical candles from storage"
                            )
                except Exception as fallback_e:
                    console.print(
                        f"[yellow]  Oracle fallback failed: {fallback_e}[/yellow]"
                    )

        # Step 4: Merge and save (incremental mode merges with existing data)
        console.print("\n[bold]Saving candles...[/bold]")

        for timeframe in TIMEFRAMES:
            boundaries = fetch_boundaries_by_tf.get(timeframe) if fetch_boundaries_by_tf else None

            if boundaries and boundaries.mode == FetchMode.NO_FETCH:
                console.print(f"  [green]✓[/green] {timeframe}: Already up to date, skipped")
                continue

            chainlink_df = chainlink_candles.get(timeframe)
            gmx_df = gmx_candles.get(timeframe)

            # Combine sources
            if chainlink_df is not None and gmx_df is not None and not gmx_df.empty:
                new_df = combine_gmx_and_chainlink_data(gmx_df, chainlink_df)
            elif chainlink_df is not None:
                new_df = chainlink_df
            elif gmx_df is not None and not gmx_df.empty:
                new_df = gmx_df
            else:
                continue

            # Merge with existing data if incremental mode
            if boundaries and boundaries.mode == FetchMode.INCREMENTAL:
                existing_df = self.storage.read_candles(timeframe, symbol)
                if not existing_df.empty:
                    # Concatenate and deduplicate
                    combined = pd.concat([existing_df, new_df], ignore_index=True)
                    combined = combined.drop_duplicates(subset=["timestamp"], keep="last")
                    combined = combined.sort_values("timestamp").reset_index(drop=True)
                    added_count = len(combined) - len(existing_df)
                    new_df = combined
                    console.print(
                        f"  [cyan]→[/cyan] {timeframe}: Merged {len(new_df):,} total candles "
                        f"(added {added_count} new)"
                    )

            # Save to storage
            self.storage.save_candles(new_df, timeframe, symbol)
            earliest, latest = new_df["timestamp"].min(), new_df["timestamp"].max()

            if not boundaries or boundaries.mode != FetchMode.INCREMENTAL:
                console.print(
                    f"  [green]✓[/green] {timeframe}: {len(new_df):,} candles saved "
                    f"[dim]({earliest} to {latest})[/dim]"
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
        from gmx_historical_data.oracle_event_aggregator import (
            aggregate_oracle_events_to_ohlcv,
        )

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
            console.print(
                f"  [yellow]No token address found for {symbol} in oracle mapping[/yellow]"
            )
            return

        # Get token decimals for price conversion
        token_decimals = token_mapper.get_decimals_for_symbol(symbol_upper) or 18
        console.print(
            f"  [dim]Found token address:[/dim] {token_address} ({token_decimals} decimals)"
        )

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

        console.print(
            f"  [green]✓[/green] Collected [cyan]{len(events):,}[/cyan] oracle events"
        )

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
                console.print(
                    f"  [green]✓[/green] {timeframe}: {len(ohlcv):,} candles via oracle fallback"
                )

            except Exception as e:
                console.print(
                    f"  [red]✗ {timeframe} oracle aggregation failed: {e}[/red]"
                )

    async def collect_all_symbols(
        self,
        full: bool = False,
        concurrency: int = 1,
        use_events: bool = False,
        chainlink_only: bool = False,
    ) -> None:
        """Collect data for all supported symbols with parallel processing.

        :param full: If True, collect from genesis; if False, resume from checkpoints
        :param concurrency: Number of symbols to process concurrently (default: 1, use --concurrency for parallel). Note: Ignored in event mode.
        :param use_events: Use event-based collection (batch all markets) instead of oracle-based
        :param chainlink_only: If True, only collect markets with Chainlink feeds
        """
        if use_events:
            # Event-based collection mode requires HyperSync
            if self.hypersync is None:
                console.print(
                    "[red]✗ HyperSync not initialized - cannot use event-based collection[/red]"
                )
                console.print(
                    "[dim]Event-based collection requires HyperSync API token.[/dim]"
                )
                return

            console.print(
                "[cyan]Using event-based collection (indexing position events)[/cyan]\n"
            )

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
            address_to_symbol = {
                addr.lower(): sym for addr, sym in market_mapping.items()
            }

            console.print(
                f"[dim]Mapped {len(address_to_symbol)} market addresses to symbols[/dim]\n"
            )

            # Determine block range
            start_block = self.config.start_block or 0
            end_block = self.config.end_block

            console.print(
                f"Collecting ALL position events from block {start_block} to {end_block or 'latest'}..."
            )

            # Collect ALL events at once (much faster than per-market)
            all_events = await event_collector.collect_position_events(
                start_block=start_block,
                end_block=end_block,
            )

            console.print(
                f"[green]✓[/green] Collected {len(all_events)} total events\n"
            )

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

            # Filter to Chainlink-only symbols if requested
            chainlink_upper_set = None
            if chainlink_only:
                chainlink_upper_set = {s.upper() for s in get_gmx_markets_with_chainlink_feeds()}
                skipped = sum(
                    1 for addr in events_by_market
                    if address_to_symbol.get(addr, "").upper() not in chainlink_upper_set
                )
                if skipped > 0:
                    console.print(
                        f"[dim]Skipping {skipped} non-Chainlink market(s) (--chainlink-only)[/dim]"
                    )

            # Process each market
            for market_address, market_events in events_by_market.items():
                symbol = address_to_symbol[market_address]

                # Skip excluded symbols (case-insensitive)
                if is_excluded_symbol(symbol):
                    console.print(f"\n[dim]Skipping {symbol} (excluded)[/dim]")
                    continue

                # Skip non-Chainlink symbols if --chainlink-only
                if chainlink_upper_set is not None and symbol.upper() not in chainlink_upper_set:
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
                    console.print("  [green]✓[/green] Saved raw events")

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

                            console.print(
                                f"  [green]✓[/green] {timeframe}: {len(ohlcv)} candles"
                            )

                        except Exception as e:
                            console.print(f"  [red]✗ {timeframe} failed: {e}[/red]")

                    console.print(f"[green]✓ Complete for {symbol}[/green]")
                    successful += 1

                except Exception as e:
                    console.print(f"  [red]✗ Failed to save events: {e}[/red]")
                    failed += 1
                    failed_symbols.append(symbol)
                    continue

            console.print("\n[bold green]Event-based collection complete![/bold green]")

        else:
            # Existing oracle-based collection mode
            # Discover GMX tokens
            if chainlink_only:
                console.print("\n[bold]Loading Chainlink-feed markets...[/bold]")
                all_symbols = get_gmx_markets_with_chainlink_feeds()
            else:
                console.print("\n[bold]Discovering GMX tokens...[/bold]")
                all_symbols = self.gmx_discovery.get_supported_symbols()

            # Filter out excluded symbols (case-insensitive)
            symbols = [s for s in all_symbols if not is_excluded_symbol(s)]
            excluded_count = len(all_symbols) - len(symbols)

            if chainlink_only:
                console.print(
                    f"  [green]✓[/green] Found [cyan]{len(symbols)}[/cyan] Chainlink-feed markets"
                )
            else:
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
        symbols: list[str] | None = None,
    ) -> None:
        """Collect data for non-Chainlink markets via GMX API + OraclePriceUpdate events.

        Uses GMX API for recent data (~6 months) and backfills historical data
        with OraclePriceUpdate events from GMX EventEmitter.

        :param start_block: Starting block (default: GMX_V2_GENESIS_BLOCK)
        :param end_block: Ending block (default: latest)
        :param symbols: List of specific symbols to collect (None = all non-Chainlink)
        """
        # Check HyperSync availability (required for oracle events)
        if self.hypersync is None:
            console.print(
                "[red]✗ HyperSync not initialized - cannot collect non-Chainlink markets[/red]"
            )
            console.print(
                "[dim]Non-Chainlink markets require oracle events via HyperSync.[/dim]"
            )
            console.print(
                "[dim]Use --all-markets (default) or set HYPERSYNC_API_TOKEN.[/dim]"
            )
            return

        from gmx_historical_data.oracle_price_collector import OraclePriceCollector
        from gmx_historical_data.gmx_token_mapper import GMXTokenMapper
        from gmx_historical_data.oracle_event_aggregator import (
            aggregate_oracle_events_to_ohlcv,
        )
        from gmx_historical_data.hypersync_key_rotator import HyperSyncKeyRotator
        from gmx_historical_data.block_timestamp_cache import BlockTimestampCache
        from gmx_historical_data.data_coverage_analyzer import DataCoverageAnalyzer

        console.print(
            Panel(
                "[bold magenta]Non-Chainlink Market Collection (Incremental)[/bold magenta]\n\n"
                "Collecting OHLCV data using:\n"
                "  • GMX API: Recent data (~6 months)\n"
                "  • OraclePriceUpdate events: Historical backfill (incremental)",
                box=box.ROUNDED,
            )
        )

        # Initialize oracle collector (pure HyperSync, no RPC needed)
        console.print("\n[bold]Initializing collectors...[/bold]")

        # Initialize key rotator if multiple keys provided
        key_rotator = None
        if self.config.hypersync_api_token and ' ' in self.config.hypersync_api_token:
            key_rotator = HyperSyncKeyRotator(self.config.hypersync_api_token)
            console.print(
                f"[green]✓[/green] Initialized HyperSync key rotation "
                f"with {key_rotator.total_keys} key(s)"
            )

        # Initialize block-timestamp cache
        console.print("\n[bold]Initializing block-timestamp cache...[/bold]")
        cache_path = self.config.output_dir / ".cache" / "block_timestamps.parquet"
        block_cache = BlockTimestampCache(cache_path, self.web3)

        # Initialize coverage analyzer
        coverage_analyzer = DataCoverageAnalyzer(self.config.output_dir)

        oracle_collector = OraclePriceCollector(
            hypersync_endpoint=self.config.hypersync_endpoint,
            api_token=self.config.hypersync_api_token,
            key_rotator=key_rotator,
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

        # Get unique symbols (filter excluded, case-insensitive)
        all_non_chainlink_symbols = sorted(set(token_mapping.values()))
        all_non_chainlink_symbols = [
            s for s in all_non_chainlink_symbols if not is_excluded_symbol(s)
        ]

        # Filter to requested symbols if specified
        if symbols:
            requested_upper = [s.upper() for s in symbols]
            # Only include symbols that are both requested AND non-Chainlink
            filtered_symbols = [
                s for s in all_non_chainlink_symbols if s.upper() in requested_upper
            ]
            if not filtered_symbols:
                console.print(
                    f"[yellow]None of the requested symbols ({', '.join(symbols)}) "
                    f"are non-Chainlink markets[/yellow]"
                )
                return
            symbols_to_collect = filtered_symbols
            # Filter token_mapping to only include tokens for requested symbols
            token_mapping = {
                addr: sym
                for addr, sym in token_mapping.items()
                if sym.upper() in requested_upper
            }
            console.print(
                f"[green]✓[/green] Collecting [cyan]{len(symbols_to_collect)}[/cyan] "
                f"non-Chainlink market(s): {', '.join(symbols_to_collect)}"
            )
        else:
            symbols_to_collect = all_non_chainlink_symbols
            console.print(
                f"[green]✓[/green] Found [cyan]{len(symbols_to_collect)}[/cyan] non-Chainlink markets"
            )

        # Get token decimals for price conversion
        try:
            token_decimals_map = token_mapper.get_token_decimals()
        except Exception as e:
            console.print(
                f"[yellow]Warning: Could not get token decimals: {e}. Using default 18.[/yellow]"
            )
            token_decimals_map = {}

        # Step 1: Fetch GMX API data for all symbols
        console.print("\n[bold]Step 1: Fetching recent data from GMX API...[/bold]")
        gmx_data_by_symbol: dict[str, dict[str, pd.DataFrame]] = {}

        if self.use_gmx_api and self.gmx_fetcher:
            for symbol in symbols_to_collect:
                console.print(f"\n[cyan]{symbol}[/cyan]")
                gmx_candles = {}

                async def fetch_timeframe(tf: str, sym: str, timeout: float = 120.0):
                    """Fetch GMX data for a single timeframe."""
                    gmx_period = map_timeframe_to_gmx_period(tf)
                    try:
                        df = await asyncio.wait_for(
                            asyncio.to_thread(
                                self.gmx_fetcher.fetch_gmx_candles, sym, gmx_period
                            ),
                            timeout=timeout,
                        )
                        return tf, df
                    except asyncio.TimeoutError:
                        return tf, pd.DataFrame()
                    except Exception:
                        return tf, pd.DataFrame()

                # Fetch all timeframes in parallel
                tasks = [fetch_timeframe(tf, symbol) for tf in TIMEFRAMES]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, Exception):
                        continue
                    timeframe, gmx_df = result
                    if not gmx_df.empty:
                        gmx_candles[timeframe] = gmx_df
                        console.print(
                            f"  [green]✓[/green] {timeframe}: {len(gmx_df):,} candles from GMX API"
                        )

                if gmx_candles:
                    gmx_data_by_symbol[symbol] = gmx_candles
                else:
                    console.print("  [yellow]○[/yellow] No GMX API data available")

        # Build reverse mapping: symbol -> token_addr (needed for Step 2)
        symbol_to_token = {}
        for addr, sym in token_mapping.items():
            symbol_to_token[sym] = addr

        # Step 2: Analyze coverage and collect oracle events per symbol (incremental)
        console.print(
            "\n[bold]Step 2: Analyzing existing coverage and collecting missing oracle events...[/bold]"
        )

        # Determine default block range
        default_start = start_block or GMX_V2_GENESIS_BLOCK
        default_end = end_block  # None = latest

        # Process each symbol individually for incremental collection
        oracle_events_by_symbol: dict[str, list] = {}

        for symbol in symbols_to_collect:
            console.print(f"\n[cyan]{symbol}[/cyan]")

            # Analyze existing coverage
            console.print("  [dim]Analyzing existing data coverage...[/dim]")
            coverage = coverage_analyzer.analyze_symbol_coverage(symbol)

            if coverage.has_data:
                console.print(
                    f"  [green]✓[/green] Found existing data covering "
                    f"{len(coverage.timeframe_coverage)} timeframe(s)"
                )
                for tf, tf_cov in coverage.timeframe_coverage.items():
                    earliest_dt = pd.to_datetime(tf_cov.earliest, unit='s', utc=True)
                    latest_dt = pd.to_datetime(tf_cov.latest, unit='s', utc=True)
                    console.print(
                        f"    {tf}: {tf_cov.candle_count:,} candles "
                        f"({earliest_dt.strftime('%Y-%m-%d')} to {latest_dt.strftime('%Y-%m-%d')})"
                    )
            else:
                console.print("  [yellow]○[/yellow] No existing data - full historical collection")

            # Calculate missing block range
            symbol_start, symbol_end = coverage_analyzer.get_missing_block_range(
                coverage,
                block_cache,
                genesis_block=default_start,
                safety_margin=1000,  # 1000 blocks overlap for safety
            )

            if symbol_start is None and symbol_end is None:
                console.print("  [green]✓[/green] Data already complete - no oracle events needed")
                oracle_events_by_symbol[symbol] = []
                continue

            # Display range to fetch
            if symbol_end is None:
                console.print(
                    f"  [dim]Fetching oracle events:[/dim] blocks {symbol_start:,} to latest"
                )
            else:
                blocks_to_fetch = symbol_end - symbol_start
                console.print(
                    f"  [dim]Fetching oracle events:[/dim] blocks {symbol_start:,} to {symbol_end:,} "
                    f"({blocks_to_fetch:,} blocks)"
                )

            # Get token address for this symbol
            token_addr = symbol_to_token.get(symbol, "").lower()
            if not token_addr:
                console.print("  [red]✗[/red] Token address not found")
                oracle_events_by_symbol[symbol] = []
                continue

            # Collect oracle events for this symbol's range
            try:
                events = await oracle_collector.collect_oracle_events(
                    start_block=symbol_start,
                    end_block=symbol_end,
                    token_addresses=[token_addr],  # Only this token
                    concurrency=4,
                )

                oracle_events_by_symbol[symbol] = events

                if events:
                    console.print(
                        f"  [green]✓[/green] Collected {len(events):,} oracle events"
                    )
                else:
                    console.print("  [yellow]○[/yellow] No oracle events found in range")

            except Exception as e:
                console.print(f"  [red]✗[/red] Failed to collect oracle events: {e}")
                logger.error(f"Oracle collection failed for {symbol}: {e}")
                traceback.print_exc()
                oracle_events_by_symbol[symbol] = []

        # Step 3: Combine GMX API + Oracle events per symbol
        console.print("\n[bold]Step 3: Combining GMX API + Oracle data...[/bold]")

        total = len(symbols_to_collect)
        successful = 0
        failed = 0
        failed_symbols = []

        for symbol in symbols_to_collect:
            console.print(f"\n[cyan]{symbol}[/cyan]")

            # Get GMX API data
            gmx_candles = gmx_data_by_symbol.get(symbol, {})

            # Get oracle events for this symbol (from incremental collection)
            token_events = oracle_events_by_symbol.get(symbol, [])

            # Get decimals for this token
            decimals = token_decimals_map.get(symbol, 18)

            if not gmx_candles and not token_events:
                console.print(
                    "  [yellow]○[/yellow] No data available (GMX API or oracle events)"
                )
                failed += 1
                failed_symbols.append(symbol)
                continue

            try:
                for timeframe in TIMEFRAMES:
                    gmx_df = gmx_candles.get(timeframe)
                    oracle_df = None

                    # Aggregate oracle events to OHLCV
                    if token_events:
                        oracle_df = aggregate_oracle_events_to_ohlcv(
                            token_events, timeframe, symbol, token_decimals=decimals
                        )
                        if oracle_df.empty:
                            oracle_df = None

                    # Combine: Oracle (historical) + GMX (recent)
                    if gmx_df is not None and oracle_df is not None:
                        # Filter oracle data to only before GMX coverage
                        gmx_earliest = gmx_df["timestamp"].min()
                        oracle_df_filtered = oracle_df[
                            oracle_df["timestamp"] < gmx_earliest
                        ]

                        if not oracle_df_filtered.empty:
                            combined = pd.concat(
                                [oracle_df_filtered, gmx_df], ignore_index=True
                            )
                            combined = combined.sort_values(
                                "timestamp"
                            ).drop_duplicates(subset=["timestamp"], keep="last")
                            self.storage.save_candles(combined, timeframe, symbol)
                            console.print(
                                f"  [green]✓[/green] {timeframe}: {len(combined):,} candles "
                                f"(oracle: {len(oracle_df_filtered):,} + GMX: {len(gmx_df):,})"
                            )
                        else:
                            # GMX covers everything
                            self.storage.save_candles(gmx_df, timeframe, symbol)
                            console.print(
                                f"  [green]✓[/green] {timeframe}: {len(gmx_df):,} candles (GMX API)"
                            )
                    elif gmx_df is not None:
                        # Only GMX data
                        self.storage.save_candles(gmx_df, timeframe, symbol)
                        console.print(
                            f"  [green]✓[/green] {timeframe}: {len(gmx_df):,} candles (GMX API)"
                        )
                    elif oracle_df is not None:
                        # Only oracle data
                        self.storage.save_candles(oracle_df, timeframe, symbol)
                        console.print(
                            f"  [green]✓[/green] {timeframe}: {len(oracle_df):,} candles (oracle events)"
                        )

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
                f"[yellow]{', '.join(failed_symbols[:10])}{'...' if len(failed_symbols) > 10 else ''}[/yellow]",
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
        help="Token symbol(s) to collect, comma-separated (e.g., ETH,BTC,SUI)",
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
        help="HyperSync API token(s) - space-separated for multiple tokens (or set HYPERSYNC_API_TOKEN env var)",
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
    chainlink_only: bool = typer.Option(
        False,
        "--chainlink-only/--all-markets",
        help="Collect only Chainlink markets (skip 84 non-Chainlink markets)",
    ),
    concurrency: int = typer.Option(
        2,
        "--concurrency",
        help="Parallelism level: symbols processed concurrently + RPC batch workers (default: 2)",
        min=1,
        max=50,
    ),
    default_mode: bool = typer.Option(
        False,
        "--default",
        help="Recommended: GMX API + Chainlink historical backfill for all tokens",
    ),
    log_file: Optional[str] = typer.Option(
        None,
        "--log-file",
        help="Path to log file. If not specified, logs to ./logs/gmx-YYYY-MM-DD-HH-MM-SS.log",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        help="Suppress console output when logging to file (file-only mode)",
    ),
) -> None:
    """Collect GMX historical price data.

    By default, collects BOTH Chainlink markets (34) and non-Chainlink markets (84)
    for a total of 118 GMX V2 markets on Arbitrum.

    DATA SOURCES:
      • Chainlink Markets: GMX API (last ~6 months) + Chainlink HyperSync backfill
      • Non-Chainlink Markets: OraclePriceUpdate events via HyperSync + eth_defi

    TIMEFRAMES COLLECTED:
      1min, 5min, 15min, 1h, 4h, 1d

    INCREMENTAL COLLECTION:
      • Checks existing data coverage before fetching
      • Only fetches missing oracle events (saves bandwidth and time)
      • Supports HyperSync API key rotation (space-separated keys)
      • Uses block-timestamp cache for fast block-to-timestamp lookups
      • Automatically detects gaps and fetches only what's needed

    OUTPUT STRUCTURE:
      data/
      └── candles/
          ├── ETH/
          │   ├── 1m.parquet
          │   ├── 5m.parquet
          │   ├── 15m.parquet
          │   ├── 1h.parquet
          │   ├── 4h.parquet
          │   └── 1d.parquet
          ├── BTC/
          │   └── ...
          └── SUI/
              └── ...

    EXAMPLES:

      # Recommended: Use --default for simplest full collection
      gmx_historical_data collect --default

      # Collect specific tokens (comma-separated)
      gmx_historical_data collect --default --symbol ETH,BTC,SUI

      # Full historical collection for all 118 markets
      export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
      export HYPERSYNC_API_TOKEN="YOUR_TOKEN"
      gmx_historical_data collect --full --output-dir ./data

      # Incremental update (faster, only new data since last collection)
      gmx_historical_data collect --update --output-dir ./data

      # Chainlink + non-Chainlink mix (auto-detected)
      gmx_historical_data collect --full --symbol ETH,SUI --output-dir ./data

      # Single non-Chainlink market
      gmx_historical_data collect --full --symbol SUI --output-dir ./data

      # Collect only Chainlink markets (skip 84 non-Chainlink markets)
      gmx_historical_data collect --full --chainlink-only --output-dir ./data

      # Fast parallel collection (10 symbols + RPC batches concurrently)
      gmx_historical_data collect --full --concurrency 10 --output-dir ./data

    VERIFICATION:
      After collection, verify data quality:
      gmx_historical_data verify --output-dir ./data
    """
    # Setup logging first, before any output
    from gmx_historical_data.log_capture import LogCapture

    # Determine log file path
    if log_file:
        log_path = Path(log_file)
    else:
        # Default: ./logs/gmx-YYYY-MM-DD-HH-MM-SS.log
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        log_path = Path(f"./logs/gmx-{timestamp}.log")

    # Wrap entire execution in log capture context
    with LogCapture(log_path, quiet=quiet):
        _cli_impl(
            full=full,
            update=update,
            symbol=symbol,
            output_dir=output_dir,
            rpc_url=rpc_url,
            hypersync_token=hypersync_token,
            start_block=start_block,
            end_block=end_block,
            use_gmx_api=use_gmx_api,
            use_events=use_events,
            chainlink_only=chainlink_only,
            concurrency=concurrency,
            default_mode=default_mode,
        )


def _cli_impl(
    full: bool,
    update: bool,
    symbol: Optional[str],
    output_dir: Path,
    rpc_url: Optional[str],
    hypersync_token: Optional[str],
    start_block: Optional[int],
    end_block: Optional[int],
    use_gmx_api: bool,
    use_events: bool,
    chainlink_only: bool,
    concurrency: int,
    default_mode: bool,
) -> None:
    """Internal implementation of CLI logic."""
    # Validate --default flag
    if default_mode:
        if use_events:
            console.print(
                "[red]Error: --default and --use-events are mutually exclusive[/red]"
            )
            console.print(
                "[dim]--default uses oracle-based collection (GMX API + Chainlink)[/dim]"
            )
            raise typer.Exit(1)
        # --default implies --full and enables GMX API
        full = True
        use_gmx_api = True

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

    # Determine if HyperSync is needed
    # HyperSync is required for:
    # 1. Non-Chainlink market collection (oracle events)
    # 2. Event-based collection mode (--use-events)
    use_hypersync = (not chainlink_only) or use_events

    # Validate HyperSync API token (only if needed)
    if use_hypersync and not hypersync_token:
        warning_panel = Panel(
            "[bold red]HyperSync API token not set![/bold red]\n\n"
            "HyperSync requires an API token to access historical data.\n"
            "Without it, you'll get 403 Forbidden errors.\n\n"
            "[bold]To fix this:[/bold]\n"
            "  1. Get a free API token from: [cyan]https://envio.dev/[/cyan]\n"
            "  2. Set the environment variable:\n"
            '     [yellow]export HYPERSYNC_API_TOKEN="your_token_here"[/yellow]\n'
            "  3. Or use --hypersync-token argument\n\n"
            "[bold]Alternatively:[/bold]\n"
            "  • Use --chainlink-only to skip oracle events (Chainlink symbols only)\n"
            "  • This only requires RPC for Chainlink historical data",
            title="⚠️  Warning",
            box=box.HEAVY,
        )
        console.print()
        console.print(warning_panel)
        console.print()
        raise typer.Exit(1)

    # Parse HyperSync API token(s) - space-separated for multiple keys
    hypersync_tokens = None
    if hypersync_token:
        # Support space-separated tokens for rotation
        hypersync_tokens = hypersync_token.strip()

    # Create configuration
    config = CollectionConfig(
        output_dir=output_dir,
        rpc_url=rpc_url,
        hypersync_api_token=hypersync_tokens,  # Now supports space-separated
        start_block=start_block,
        end_block=end_block,
    )

    # Create collector
    # Auto-derive chainlink RPC concurrency: use concurrency but cap at 3 to avoid RPC rate limits
    chainlink_concurrency = min(concurrency, 3)
    collector = DataCollector(
        config,
        use_gmx_api=use_gmx_api,
        chainlink_concurrency=chainlink_concurrency,
        use_hypersync=use_hypersync,
    )

    # Run collection
    try:
        if symbol:
            # Parse comma-separated symbols
            symbols_list = [s.strip().upper() for s in symbol.split(",") if s.strip()]

            # Filter out excluded symbols
            valid_symbols = []
            for sym in symbols_list:
                if is_excluded_symbol(sym):
                    console.print(
                        f"[yellow]Warning: {sym} is excluded (deprecated/problematic) - skipping[/yellow]"
                    )
                else:
                    valid_symbols.append(sym)

            if not valid_symbols:
                console.print("[red]No valid symbols to collect[/red]")
                raise typer.Exit(0)

            # Separate Chainlink and non-Chainlink symbols
            non_chainlink_set = get_gmx_markets_without_chainlink_feeds()
            chainlink_symbols = [s for s in valid_symbols if s not in non_chainlink_set]
            non_chainlink_symbols = [s for s in valid_symbols if s in non_chainlink_set]

            # Collect Chainlink symbols
            for sym in chainlink_symbols:
                if use_events:
                    console.print(
                        "[red]Error: Single symbol collection with --use-events is not supported. "
                        "Use collect_all_symbols instead.[/red]"
                    )
                    raise typer.Exit(1)
                asyncio.run(collector.collect_symbol(sym, full=full))

            # Collect non-Chainlink symbols (if not --chainlink-only)
            if non_chainlink_symbols and not chainlink_only:
                console.print(
                    f"[cyan]Non-Chainlink market(s): {', '.join(non_chainlink_symbols)} "
                    f"- using oracle events[/cyan]"
                )
                asyncio.run(
                    collector.collect_non_chainlink_markets(
                        start_block=start_block,
                        end_block=end_block,
                        symbols=non_chainlink_symbols,
                    )
                )
            elif non_chainlink_symbols and chainlink_only:
                console.print(
                    f"[yellow]Skipping non-Chainlink market(s): {', '.join(non_chainlink_symbols)} "
                    f"(--chainlink-only is set)[/yellow]"
                )
        else:
            # Step 1: Collect markets (filtered by --chainlink-only if set)
            asyncio.run(
                collector.collect_all_symbols(
                    full=full,
                    concurrency=concurrency,
                    use_events=use_events,
                    chainlink_only=chainlink_only,
                )
            )

            # Step 2: Collect non-Chainlink markets via oracle events (if --all-markets)
            if not chainlink_only:
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
    symbol: Optional[str] = typer.Option(
        None, "--symbol", help="Symbol to verify (omit for all)"
    ),
    output_dir: Path = typer.Option(
        Path("./data"), "--output-dir", help="Data directory"
    ),
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
        quality_str = (
            f"[{quality_color}]{report.quality_score:.0f}/100[/{quality_color}]"
        )

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

    # Validate RPC URL
    if not rpc_url:
        console.print(
            "[red]Error: RPC URL required. Set JSON_RPC_ARBITRUM env var or use --rpc-url[/red]"
        )
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
        console.print(
            Panel(
                f"[bold cyan]GMX Market Summary[/bold cyan]\n\n"
                f"Total markets: {len(all_tokens)}\n"
                f"Chainlink markets: {len(chainlink_tokens)}\n"
                f"Non-Chainlink markets: {len(non_chainlink_tokens)}",
                box=box.ROUNDED,
            )
        )

        # Show Chainlink markets
        chainlink_table = Table(title="Chainlink Markets (GMX API)", box=box.ROUNDED)
        chainlink_table.add_column("Symbol", style="green")
        chainlink_table.add_column("Token Address", style="dim")

        for addr, sym in sorted(chainlink_tokens.items(), key=lambda x: x[1]):
            chainlink_table.add_row(sym, addr)

        console.print(chainlink_table)
        console.print()

        # Show non-Chainlink markets
        non_chainlink_table = Table(
            title="Non-Chainlink Markets (Oracle Events)", box=box.ROUNDED
        )
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
    console.print(
        f"  [dim]Market type:[/dim] {'Chainlink' if is_chainlink else 'Non-Chainlink (Oracle Events)'}"
    )

    if is_chainlink:
        console.print(f"\n[yellow]Note: {symbol_upper} is a Chainlink market.[/yellow]")
        console.print(
            "[yellow]It uses GMX API for data, but may also have oracle events.[/yellow]"
        )

    from gmx_historical_data.oracle_price_collector import OraclePriceCollector
    from hypersync import HypersyncClient, ClientConfig

    # Get current block via HyperSync (no RPC needed)
    console.print("\n[bold]Initializing HyperSync collector...[/bold]")
    try:
        config = ClientConfig(
            url="https://arbitrum.hypersync.xyz", bearer_token=hypersync_token
        )
        hs_client = HypersyncClient(config)
        current_block = asyncio.run(hs_client.get_height())
        console.print(f"  [dim]Current block:[/dim] {current_block:,}")
    except Exception as e:
        console.print(f"[red]Failed to get current block from HyperSync: {e}[/red]")
        raise typer.Exit(1)

    # Start from GMX V2 genesis block to get ALL historical events
    start_block = GMX_V2_GENESIS_BLOCK
    console.print(
        f"  [dim]Scanning from GMX V2 genesis:[/dim] block {start_block:,} to {current_block:,}"
    )

    # Collect oracle events
    total_blocks = current_block - start_block
    console.print("\n[bold]Fetching oracle events from HyperSync...[/bold]")
    console.print(f"  [dim]Block range:[/dim] {total_blocks:,} blocks to scan")
    console.print(
        f"  [dim]Using {concurrency} parallel workers for faster collection[/dim]"
    )
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
        console.print(
            f"[yellow]No oracle events found for {symbol_upper} in the specified block range[/yellow]"
        )
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
    events_table = Table(
        title=f"Oracle Events for {symbol_upper} (showing up to {limit})",
        box=box.ROUNDED,
    )
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
        timestamp = datetime.utcfromtimestamp(event.block_timestamp).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
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
        console.print(
            f"\n[dim]... and {len(events) - limit} more events (use --limit to see more)[/dim]"
        )

    # Show summary statistics
    console.print()
    min_prices = [e.min_price / price_divisor for e in events]
    max_prices = [e.max_price / price_divisor for e in events]
    mid_prices = [(e.min_price + e.max_price) / 2 / price_divisor for e in events]

    summary_table = Table(title="Price Summary", box=box.ROUNDED, show_header=False)
    summary_table.add_column("Metric", style="dim")
    summary_table.add_column("Value", style="cyan")

    summary_table.add_row("Events count", f"{len(events):,}")
    summary_table.add_row(
        "Price range", f"${min(mid_prices):,.4f} - ${max(mid_prices):,.4f}"
    )
    summary_table.add_row("Latest price", f"${mid_prices[-1]:,.4f}")
    summary_table.add_row(
        "Block range", f"{events[0].block_number:,} - {events[-1].block_number:,}"
    )

    first_ts = datetime.utcfromtimestamp(events[0].block_timestamp)
    last_ts = datetime.utcfromtimestamp(events[-1].block_timestamp)
    summary_table.add_row("Time range", f"{first_ts} - {last_ts}")

    console.print(summary_table)


def export_freqtrade_command(
    data_dir: Path = typer.Option(
        Path("./data"),
        "--data-dir",
        help="Source GMX data directory",
    ),
    output_dir: Path = typer.Option(
        Path("./freqtrade_data"),
        "--output-dir",
        help="Output directory for Freqtrade files",
    ),
    symbol: Optional[list[str]] = typer.Option(
        None,
        "--symbol",
        help="Specific symbols to export (can be repeated)",
    ),
    timeframe: Optional[list[str]] = typer.Option(
        None,
        "--timeframe",
        help="Specific timeframes to export (can be repeated)",
    ),
    output_format: str = typer.Option(
        "feather",
        "--format",
        help="Output format (feather or parquet)",
    ),
) -> None:
    """Export GMX data to Freqtrade-compatible format.

    Converts collected OHLCV candles AND funding rate data to Freqtrade's
    expected feather format.  For each symbol/timeframe, up to three files
    are generated:

    - OHLCV candles (``*-futures.feather``)
    - Funding rate  (``*-funding_rate.feather``) — rate in ``open`` column
    - Mark price    (``*-mark.feather``)         — OHLCV used as proxy

    OUTPUT STRUCTURE:
      freqtrade_data/
      └── gmx/
          └── futures/
              ├── ETH_USDC_USDC-1h-futures.feather
              ├── ETH_USDC_USDC-1h-funding_rate.feather
              ├── ETH_USDC_USDC-1h-mark.feather
              ├── BTC_USDC_USDC-1h-futures.feather
              └── ...

    FUNDING DATA:
      Funding rate parquet files are read from:
        {data_dir}/funding/arbitrum/rates/{SYMBOL}/{timeframe}.parquet

      These are generated by ``gmx_historical_data collect-funding``.

    FREQTRADE USAGE:
      Configure freqtrade to use the exported data::

        {
          "datadir": "freqtrade_data/gmx/futures",
          "exchange": {"name": "gmx"},
          "pairs": ["ETH/USDC:USDC", "BTC/USDC:USDC"],
          ...
        }

    EXAMPLES:

      # Export all symbols and timeframes (OHLCV + funding + mark)
      gmx_historical_data export-freqtrade --data-dir ./data

      # Export specific symbols
      gmx_historical_data export-freqtrade --symbol ETH --symbol BTC

      # Export specific timeframes
      gmx_historical_data export-freqtrade --timeframe 1h --timeframe 4h

      # Export to parquet format instead of feather
      gmx_historical_data export-freqtrade --format parquet
    """
    from gmx_historical_data.freqtrade_exporter import FreqtradeExporter

    console.print(
        Panel(
            "[bold cyan]Freqtrade Export[/bold cyan]\n\n"
            f"Source: {data_dir}\n"
            f"Output: {output_dir}\n"
            f"Format: {output_format}",
            box=box.ROUNDED,
        )
    )

    # Validate data directory exists
    if not data_dir.exists():
        console.print(f"[red]Error: Data directory not found: {data_dir}[/red]")
        raise typer.Exit(1)

    # Initialize exporter
    exporter = FreqtradeExporter(data_dir, output_dir)

    # Check available data (candles + funding)
    candle_symbols = exporter.storage.list_symbols()
    funding_symbols = exporter.list_funding_symbols()
    if not candle_symbols and not funding_symbols:
        console.print(f"[yellow]No candle or funding data found in {data_dir}[/yellow]")
        console.print("[dim]Run 'gmx_historical_data collect --default' first[/dim]")
        raise typer.Exit(1)

    console.print(
        f"\n[green]✓[/green] Found [cyan]{len(candle_symbols)}[/cyan] symbols with candle data"
    )
    console.print(
        f"[green]✓[/green] Found [cyan]{len(funding_symbols)}[/cyan] symbols with funding data"
    )

    # Filter symbols if specified
    symbols_to_export = list(symbol) if symbol else None
    timeframes_to_export = list(timeframe) if timeframe else None

    if symbols_to_export:
        console.print(f"  [dim]Filtering to: {', '.join(symbols_to_export)}[/dim]")
    if timeframes_to_export:
        console.print(f"  [dim]Timeframes: {', '.join(timeframes_to_export)}[/dim]")

    # Run export
    console.print("\n[bold]Exporting to Freqtrade format...[/bold]")

    try:
        results = exporter.export(
            symbols=symbols_to_export,
            timeframes=timeframes_to_export,
            output_format=output_format,
        )
    except Exception as e:
        console.print(f"[red]Export failed: {e}[/red]")
        raise typer.Exit(1)

    # Summary
    total_files = sum(r["files"] for r in results.values())
    total_candles = sum(r["candles"] for r in results.values())
    total_ohlcv = sum(r.get("ohlcv_files", r["files"]) for r in results.values())
    total_funding = sum(r.get("funding_files", 0) for r in results.values())
    total_mark = sum(r.get("mark_files", 0) for r in results.values())

    summary_table = Table(title="Export Summary", box=box.ROUNDED)
    summary_table.add_column("Symbol", style="cyan")
    summary_table.add_column("OHLCV", justify="right")
    summary_table.add_column("Funding", justify="right")
    summary_table.add_column("Mark", justify="right")
    summary_table.add_column("Candles", justify="right")

    for sym, stats in sorted(results.items()):
        summary_table.add_row(
            sym,
            str(stats.get("ohlcv_files", stats["files"])),
            str(stats.get("funding_files", 0)),
            str(stats.get("mark_files", 0)),
            f"{stats['candles']:,}",
        )

    summary_table.add_row(
        "[bold]Total[/bold]",
        f"[bold]{total_ohlcv}[/bold]",
        f"[bold]{total_funding}[/bold]",
        f"[bold]{total_mark}[/bold]",
        f"[bold]{total_candles:,}[/bold]",
    )

    console.print()
    console.print(summary_table)
    console.print()
    console.print(f"[green]✓[/green] Exported to: [cyan]{output_dir / 'gmx'}[/cyan]")


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
app.command(name="export-freqtrade")(export_freqtrade_command)


def main() -> None:
    """Main CLI entry point."""
    app()


if __name__ == "__main__":
    main()
