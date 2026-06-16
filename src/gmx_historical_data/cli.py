"""Command-line interface for GMX historical data collection."""

import asyncio
import gc
import logging
import traceback
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import polars as pl
import typer
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from web3 import Web3

from gmx_historical_data.cex_gap_fill import fill_gaps_from_cex
from gmx_historical_data.chainlink_feeds_complete import (
    find_chainlink_symbol,
    get_feed_address_for_gmx_symbol,
)
from gmx_historical_data.chainlink_rpc_collector import ChainlinkRPCCollector
from gmx_historical_data.checkpoint import Checkpoint, CheckpointManager
from gmx_historical_data.config import (
    GMX_V2_GENESIS_BLOCK,
    TIMEFRAMES,
    CollectionConfig,
    FetchMode,
    is_excluded_symbol,
)
from gmx_historical_data.daemon.config import (
    get_gmx_markets_with_chainlink_feeds,
    get_gmx_markets_without_chainlink_feeds,
)
from gmx_historical_data.daemon.data_loss_handler import DataLossHandler
from gmx_historical_data.daemon.gap_detector import AdaptiveGapDetector
from gmx_historical_data.event_aggregator import aggregate_events_to_ohlcv
from gmx_historical_data.event_decoder import AnswerUpdatedEvent
from gmx_historical_data.fetch_boundary_calculator import FetchBoundaryCalculator
from gmx_historical_data.gap_analyzer import DataGapAnalyzer
from gmx_historical_data.gmx_api_integration import (
    GMXDataFetcher,
    combine_gmx_and_chainlink_data,
    map_timeframe_to_gmx_period,
)
from gmx_historical_data.gmx_event_collector import GMXEventCollector
from gmx_historical_data.gmx_token_discovery import GMXTokenDiscovery
from gmx_historical_data.hypersync_collector import HyperSyncCollector
from gmx_historical_data.market_registry import fetch_markets
from gmx_historical_data.quickstart import (
    DEFAULT_RELEASE_TAG,
    print_coverage_summary,
    seed_from_release,
)
from gmx_historical_data.resampler import OHLCVResampler
from gmx_historical_data.storage import ParquetStorage

logger = logging.getLogger(__name__)

console = Console()


def _filter_and_categorize_symbols(
    symbols: list[str],
    chainlink_only: bool = False,
) -> tuple[list[str], list[str], int]:
    """Filter excluded symbols and categorize into Chainlink vs non-Chainlink.

    :param symbols: Raw symbol list to filter.
    :param chainlink_only: If True, non-Chainlink list is returned empty.
    :returns: Tuple of (chainlink_symbols, non_chainlink_symbols, excluded_count).
    """
    non_chainlink_set = set(get_gmx_markets_without_chainlink_feeds())
    chainlink = []
    non_chainlink = []
    excluded = 0

    for s in symbols:
        if is_excluded_symbol(s):
            excluded += 1
            continue
        if s in non_chainlink_set:
            if not chainlink_only:
                non_chainlink.append(s)
        else:
            chainlink.append(s)

    return chainlink, non_chainlink, excluded


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
            console.print(
                f"[green]Using {len(rpc_urls)} RPC provider(s) with automatic failover[/green]"
            )
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
                gap_result = self.adaptive_gap_detector.detect_gap_adaptive(symbol, timeframe)
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
                    event = self.data_loss_handler.handle_gap_result(symbol, timeframe, gap_result)
                    if event:
                        short_tf = timeframe in ("1min", "5min")
                        if short_tf:
                            # Short-TF loss is expected with daily collection (GMX API
                            # window: 1min≈5h, 5min≈34d). CEX fill recovers these gaps.
                            console.print(
                                f"  [yellow]DATA LOSS[/yellow] {timeframe}: "
                                f"~{gap_result.lost_candles_estimate} candles "
                                f"({gap_result.lost_timespan}) — recoverable via CEX fill"
                            )
                        else:
                            console.print(
                                f"  [red bold]DATA LOSS[/red bold] {timeframe}: "
                                f"~{gap_result.lost_candles_estimate} candles lost "
                                f"({gap_result.lost_timespan})"
                            )

            except Exception as e:
                console.print(f"  [yellow]Warning: Could not check {timeframe}: {e}[/yellow]")
                results[timeframe] = {"error": str(e)}

        return results

    async def _fetch_gmx_candles_for_timeframes(
        self,
        symbol: str,
        timeframes: list[str] | None = None,
        boundaries_by_tf: dict | None = None,
        timeout: float = 120.0,
    ) -> dict[str, pd.DataFrame]:
        """Fetch GMX API candles for multiple timeframes in parallel.

        :param symbol: Token symbol (e.g., 'ETH')
        :param timeframes: List of timeframe strings (default: all TIMEFRAMES)
        :param boundaries_by_tf: Optional boundary dict; if a timeframe's
            ``gmx_api_needed`` is False, it is skipped. If the boundary mode
            is INCREMENTAL, results are filtered to ``gmx_api_start``.
        :param timeout: Per-timeframe timeout in seconds.
        :returns: Dict mapping timeframe -> DataFrame of candles.
        """
        if not self.gmx_fetcher:
            return {}

        tfs = timeframes or TIMEFRAMES

        async def _fetch_one(tf: str) -> tuple[str, pd.DataFrame]:
            # Skip if boundary says not needed
            if boundaries_by_tf:
                bounds = boundaries_by_tf.get(tf)
                if bounds and not bounds.gmx_api_needed:
                    return tf, pd.DataFrame()

            gmx_period = map_timeframe_to_gmx_period(tf)
            try:
                df = await asyncio.wait_for(
                    asyncio.to_thread(self.gmx_fetcher.fetch_gmx_candles, symbol, gmx_period),
                    timeout=timeout,
                )

                # Apply boundary filter for incremental mode
                if boundaries_by_tf:
                    bounds = boundaries_by_tf.get(tf)
                    if bounds and bounds.mode == FetchMode.INCREMENTAL and bounds.gmx_api_start:
                        df = df[df["timestamp"] >= bounds.gmx_api_start]

                return tf, df
            except TimeoutError:
                console.print(f"  [yellow]⏱ {tf}: Timeout after {timeout}s[/yellow]")
                return tf, pd.DataFrame()
            except Exception as e:
                console.print(f"  [yellow]⚠ {tf}: {e}[/yellow]")
                return tf, pd.DataFrame()

        tasks = [_fetch_one(tf) for tf in tfs]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        candles: dict[str, pd.DataFrame] = {}
        for result in results:
            if isinstance(result, Exception):
                console.print(f"  [red]✗ Error fetching timeframe: {result}[/red]")
                continue
            tf, df = result
            if not df.empty:
                candles[tf] = df
        return candles

    def _merge_and_save_candles(
        self,
        symbol: str,
        timeframe: str,
        *dataframes: pd.DataFrame | None,
        merge_with_existing: bool = False,
        force: bool = False,
    ) -> int:
        """Combine, deduplicate, and save candle DataFrames for one timeframe.

        :param symbol: Token symbol.
        :param timeframe: Timeframe string (e.g., '1h').
        :param dataframes: One or more DataFrames to combine (None values ignored).
        :param merge_with_existing: If True, load existing candles from storage
            and merge with them (for incremental mode). Ignored when ``force``
            is set.
        :param force: If True, do not load/merge existing candles and overwrite
            the stored file (``save_candles(overwrite=True)``). Intentionally
            bypasses the merge/history-preservation path.
        :returns: Number of candles saved, or 0 if nothing to save.
        """
        # Collect non-empty DataFrames
        dfs = [df for df in dataframes if df is not None and not df.empty]
        if not dfs:
            return 0

        # Merge with existing storage if incremental (never under force, which
        # intentionally re-writes the file from the freshly fetched data).
        if merge_with_existing and not force:
            existing = self.storage.read_candles(timeframe, symbol)
            if not existing.empty:
                dfs = [existing, *dfs]

        # Convert one at a time, releasing each pandas DataFrame to bound memory
        pl_frames = []
        for i in range(len(dfs)):
            pl_frames.append(
                pl.from_pandas(dfs[i]).with_columns(pl.col("timestamp").dt.cast_time_unit("us"))
            )
            dfs[i] = None  # Release pandas reference
        combined = pl.concat(pl_frames)
        del pl_frames
        combined = combined.unique(subset=["timestamp"], keep="last", maintain_order=False)
        combined = combined.sort("timestamp")

        self.storage.save_candles(combined.to_pandas(), timeframe, symbol, overwrite=force)
        count = len(combined)
        del combined
        gc.collect()
        return count

    def _save_symbol_checkpoint(self, symbol: str) -> None:
        """Save a completion checkpoint for a symbol after successful collection.

        :param symbol: Token symbol that was collected.
        """
        try:
            latest_timestamp = 0
            total_candles = 0
            for tf in TIMEFRAMES:
                stored_df = self.storage.read_candles(tf, symbol)
                if not stored_df.empty:
                    total_candles += len(stored_df)
                    ts_max = stored_df["timestamp"].max()
                    if hasattr(ts_max, "timestamp"):
                        ts_val = int(ts_max.timestamp())
                    else:
                        ts_val = int(ts_max)
                    latest_timestamp = max(latest_timestamp, ts_val)
            self.checkpoint_mgr.save_checkpoint(
                Checkpoint(
                    symbol=symbol,
                    last_block=0,
                    last_timestamp=latest_timestamp,
                    total_events=total_candles,
                    last_updated=datetime.utcnow().isoformat(),
                )
            )
        except Exception as e:
            console.print(
                f"  [yellow]Warning: Could not save checkpoint for {symbol}: {e}[/yellow]"
            )

    async def collect_symbol(
        self,
        symbol: str,
        full: bool = False,
        force: bool = False,
    ) -> None:
        """Collect data for a single symbol using GMX-first approach.

        :param symbol: Token symbol (e.g., 'ETH')
        :param full: If True, collect from genesis; if False, resume from checkpoint
        :param force: If True, re-fetch from genesis (full boundaries) and
            overwrite stored files instead of merging/appending.
        """
        console.print()
        console.print(
            Panel(f"[bold cyan]Collecting data for {symbol}[/bold cyan]", box=box.ROUNDED)
        )

        # Step 0: Check for data loss (adaptive gap detection)
        if self.adaptive_gap_detector:
            console.print("\n[bold]Checking for data gaps (sliding window awareness)...[/bold]")
            gap_info = self.check_and_report_data_loss(symbol)

            # Summarize gap status
            data_loss_count = sum(1 for tf, info in gap_info.items() if info.get("has_data_loss"))
            no_gap_count = sum(1 for tf, info in gap_info.items() if info.get("status") == "no_gap")

            if data_loss_count > 0:
                console.print(
                    f"  [red]⚠ Data loss detected in {data_loss_count} timeframe(s)[/red]"
                )
            if no_gap_count > 0:
                console.print(f"  [green]✓ {no_gap_count} timeframe(s) are up to date[/green]")
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
                    force=force,
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
                    console.print(f"  [cyan]→[/cyan] {tf}: Fetching from GMX API ({mode_label})")

        if self.use_gmx_api and self.gmx_fetcher:
            gmx_candles_fetched = await self._fetch_gmx_candles_for_timeframes(
                symbol,
                boundaries_by_tf=fetch_boundaries_by_tf or None,
            )
            # Log fetched timeframes
            for tf, df in gmx_candles_fetched.items():
                earliest, latest = df["timestamp"].min(), df["timestamp"].max()
                console.print(
                    f"  [green]✓[/green] {tf}: [cyan]{len(df):,}[/cyan] candles "
                    f"from GMX [dim]({earliest} to {latest})[/dim]"
                )
            gmx_candles.update(gmx_candles_fetched)

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
                console.print(f"  [dim]Mode:[/dim] {boundaries_1h.mode.value}")
                if boundaries_1h.chainlink_end_timestamp:
                    console.print(
                        f"  [dim]Fetch range:[/dim] all historical to timestamp {boundaries_1h.chainlink_end_timestamp}"
                    )
                else:
                    console.print("  [dim]Fetch range:[/dim] all available historical data")

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
                        end_timestamp=boundaries_1h.chainlink_end_timestamp,  # Use calculated boundary
                        max_rounds=2000000,
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

                        # Save raw events. Under ``force`` (or full mode) the
                        # stored events are overwritten via save_raw_events;
                        # otherwise new events are appended.
                        if full or force:
                            self.storage.save_raw_events(events, symbol, partition_id=0)
                        else:
                            self.storage.append_raw_events(events, symbol)

                        # Resample to OHLCV
                        raw_df = self.storage.read_raw_events(symbol)
                        chainlink_candles = self.resampler.resample_all_timeframes(raw_df, symbol)

                        console.print("  [green]✓[/green] Resampled to OHLCV candles")
                    else:
                        console.print("  [yellow]⚠ RPC collection returned no data[/yellow]")

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
                            await self._collect_symbol_via_oracle_fallback(symbol, force=force)
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
                console.print("\n[green]✓[/green] Chainlink backfill not needed - data is complete")
        else:
            # No Chainlink feed - use oracle events for historical data (requires HyperSync)
            if self.hypersync is None:
                console.print(
                    "\n[yellow]⚠ No Chainlink feed found and HyperSync not initialized[/yellow]"
                )
                console.print("[dim]This symbol requires oracle events, which need HyperSync[/dim]")
            else:
                # Gate the oracle backfill exactly like the Chainlink one: only
                # fetch older history when it is genuinely missing (no stored data,
                # or our earliest stored candle is newer than the GMX window start),
                # or when forced. Otherwise we would re-walk oracle events we
                # already have. --force re-backfills from genesis and overwrites.
                existing_1h = self.storage.read_candles("1h", symbol)
                gmx_1h = gmx_candles.get("1h")
                _, hole_end = self.gap_analyzer._calculate_incremental_gap(
                    gmx_1h if gmx_1h is not None else pd.DataFrame(),
                    existing_1h if not existing_1h.empty else None,
                )
                oracle_needed = force or existing_1h.empty or hole_end is not None

                if not oracle_needed:
                    console.print(
                        "\n[green]✓[/green] Oracle backfill not needed - data is complete"
                    )
                else:
                    console.print("\n[bold]Backfilling with oracle events...[/bold]")
                    try:
                        await self._collect_symbol_via_oracle_fallback(symbol, force=force)
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
                        console.print(f"[yellow]  Oracle fallback failed: {fallback_e}[/yellow]")

        # Step 4: Merge and save (incremental mode merges with existing data)
        console.print("\n[bold]Saving candles...[/bold]")

        for timeframe in TIMEFRAMES:
            boundaries = fetch_boundaries_by_tf.get(timeframe) if fetch_boundaries_by_tf else None

            if boundaries and boundaries.mode == FetchMode.NO_FETCH:
                console.print(f"  [green]✓[/green] {timeframe}: Already up to date, skipped")
                continue

            chainlink_df = chainlink_candles.get(timeframe)
            gmx_df = gmx_candles.get(timeframe)

            # If both sources exist, combine them using the dedicated combiner
            if chainlink_df is not None and gmx_df is not None and not gmx_df.empty:
                merged_df = combine_gmx_and_chainlink_data(gmx_df, chainlink_df)
            else:
                merged_df = chainlink_df if chainlink_df is not None else gmx_df

            is_incremental = boundaries and boundaries.mode == FetchMode.INCREMENTAL
            count = self._merge_and_save_candles(
                symbol,
                timeframe,
                merged_df,
                merge_with_existing=is_incremental,
                force=force,
            )

            if count > 0:
                stored = self.storage.read_candles(timeframe, symbol)
                earliest, latest = stored["timestamp"].min(), stored["timestamp"].max()
                console.print(
                    f"  [green]✓[/green] {timeframe}: {count:,} candles saved "
                    f"[dim]({earliest} to {latest})[/dim]"
                )

        # Save checkpoint only for Chainlink-backed symbols.
        # Non-Chainlink symbols need oracle event historical backfill via
        # collect_non_chainlink_markets(), which skips any symbol that already
        # has a checkpoint. Saving a checkpoint here for non-Chainlink symbols
        # would cause collect_non_chainlink_markets() to skip them, leaving
        # those symbols with only GMX API recent data (no full historical data).
        if chainlink_available:
            self._save_symbol_checkpoint(symbol)

        console.print(f"\n[bold green]✓ Collection complete for {symbol}[/bold green]")

    async def _collect_symbol_via_oracle_fallback(
        self,
        symbol: str,
        start_block: int | None = None,
        end_block: int | None = None,
        force: bool = False,
    ) -> None:
        """Fallback collection for a single symbol via oracle events.

        Used when GMX API or Chainlink collection fails, and for the historical
        backfill of non-Chainlink symbols.

        :param symbol: Token symbol
        :param start_block: Starting block (default: GMX_V2_GENESIS_BLOCK)
        :param end_block: Ending block (default: latest)
        :param force: If True, overwrite stored candles instead of merging/appending
        """
        from gmx_historical_data.gmx_token_mapper import GMXTokenMapper
        from gmx_historical_data.oracle_event_aggregator import (
            aggregate_oracle_events_to_ohlcv,
        )
        from gmx_historical_data.oracle_price_collector import OraclePriceCollector

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

        console.print(f"  [green]✓[/green] Collected [cyan]{len(events):,}[/cyan] oracle events")

        # Aggregate to OHLCV
        for timeframe in TIMEFRAMES:
            try:
                ohlcv = aggregate_oracle_events_to_ohlcv(
                    events, timeframe, symbol, token_decimals=token_decimals
                )

                if ohlcv.empty:
                    continue

                # Append (merge) by default; --force overwrites stored history.
                self._merge_and_save_candles(
                    symbol,
                    timeframe,
                    ohlcv,
                    merge_with_existing=not force,
                    force=force,
                )
                console.print(
                    f"  [green]✓[/green] {timeframe}: {len(ohlcv):,} candles via oracle fallback"
                )

            except Exception as e:
                console.print(f"  [red]✗ {timeframe} oracle aggregation failed: {e}[/red]")

    async def collect_all_symbols(
        self,
        full: bool = False,
        concurrency: int = 1,
        use_events: bool = False,
        chainlink_only: bool = False,
        force: bool = False,
    ) -> None:
        """Collect data for all supported symbols with parallel processing.

        :param full: If True, collect from genesis; if False, resume from checkpoints
        :param concurrency: Number of symbols to process concurrently (default: 1, use --concurrency for parallel). Note: Ignored in event mode.
        :param use_events: Use event-based collection (batch all markets) instead of oracle-based
        :param chainlink_only: If True, only collect markets with Chainlink feeds
        :param force: If True, ignore checkpoints and re-collect all symbols
        """
        if use_events:
            # Event-based collection mode requires HyperSync
            if self.hypersync is None:
                console.print(
                    "[red]✗ HyperSync not initialized - cannot use event-based collection[/red]"
                )
                console.print("[dim]Event-based collection requires HyperSync API token.[/dim]")
                return

            console.print("[cyan]Using event-based collection (indexing position events)[/cyan]\n")

            # Initialize event collector
            event_collector = GMXEventCollector(
                hypersync_endpoint=self.config.hypersync_endpoint,
                rpc_url=self.config.rpc_url,
                api_token=self.config.hypersync_api_token,
            )

            # Fetch market registry (address -> symbol mapping)
            markets = fetch_markets("arbitrum")
            address_to_symbol = {addr: info["symbol"] for addr, info in markets.items()}

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

            # Filter to Chainlink-only symbols if requested
            chainlink_upper_set = None
            if chainlink_only:
                chainlink_upper_set = {s.upper() for s in get_gmx_markets_with_chainlink_feeds()}
                skipped = sum(
                    1
                    for addr in events_by_market
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
                    self.storage.save_position_events(
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

            chainlink_syms, non_chainlink_syms, excluded_count = _filter_and_categorize_symbols(
                all_symbols, chainlink_only
            )
            # For collect_all_symbols we process all non-excluded together
            symbols = chainlink_syms + non_chainlink_syms

            console.print(f"  [green]✓[/green] Found [cyan]{len(symbols)}[/cyan] markets")
            if excluded_count > 0:
                console.print(
                    f"  [dim]Excluded {excluded_count} deprecated/problematic symbol(s)[/dim]"
                )

            # Check existing checkpoints to skip already-completed symbols.
            # A checkpoint with total_events > 0 means the symbol was successfully
            # collected in a prior run. Skip it to enable resuming interrupted runs.
            # Use --force to ignore checkpoints and re-collect everything.
            skipped_symbols = []
            pending_symbols = []
            if force or not full:
                # --force, or incremental (--update): route every symbol through
                # collect_symbol so its gap detector checks existing coverage and
                # fetches only the missing slice (append). Only full-mode resume
                # skips already-collected symbols wholesale.
                pending_symbols = list(symbols)
            else:
                for s in symbols:
                    checkpoint = self.checkpoint_mgr.load_checkpoint(s)
                    if checkpoint and checkpoint.total_events > 0:
                        skipped_symbols.append(s)
                    else:
                        pending_symbols.append(s)

            if skipped_symbols:
                console.print(
                    f"  [green]✓[/green] Skipping [cyan]{len(skipped_symbols)}[/cyan] already-collected symbols "
                    f"(checkpoint exists)"
                )
                console.print(
                    f"  [dim]Skipped: {', '.join(skipped_symbols[:10])}"
                    + (
                        f"... (+{len(skipped_symbols) - 10} more)"
                        if len(skipped_symbols) > 10
                        else ""
                    )
                    + "[/dim]"
                )

            total = len(symbols)
            successful = len(skipped_symbols)
            failed = 0
            failed_symbols = []

            if not pending_symbols:
                console.print(
                    f"\n[bold green]All {total} symbols already collected. "
                    f"Use --full to force re-collection.[/bold green]"
                )
            else:
                console.print(
                    f"\n[bold]Collecting data for [cyan]{len(pending_symbols)}[/cyan] symbols "
                    f"({len(skipped_symbols)} skipped, concurrency: {concurrency})...[/bold]"
                )

            # Process pending symbols in batches for controlled parallelism
            for batch_start in range(0, len(pending_symbols), concurrency):
                batch_end = min(batch_start + concurrency, len(pending_symbols))
                batch = pending_symbols[batch_start:batch_end]

                console.print(
                    f"\n[bold cyan]Batch {batch_start // concurrency + 1}: Processing {len(batch)} symbols ({batch_start + 1}-{batch_end}/{len(pending_symbols)})[/bold cyan]"
                )

                # Create tasks for parallel execution
                tasks = []
                for symbol in batch:
                    tasks.append(self.collect_symbol(symbol, full=full, force=force))

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
        summary_table = Table(title="Collection Summary", box=box.ROUNDED, show_header=False)
        summary_table.add_column("Status", style="bold")
        summary_table.add_column("Count", justify="right")

        summary_table.add_row("[green]✓ Successful[/green]", f"[green]{successful}/{total}[/green]")
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
        concurrency: int = 1,
    ) -> None:
        """Collect data for non-Chainlink markets via GMX API + OraclePriceUpdate events.

        Uses GMX API for recent data (~6 months) and backfills historical data
        with OraclePriceUpdate events from GMX EventEmitter.

        :param start_block: Starting block (default: GMX_V2_GENESIS_BLOCK)
        :param end_block: Ending block (default: latest)
        :param symbols: List of specific symbols to collect (None = all non-Chainlink)
        :param concurrency: Number of symbols to process concurrently in Step 2 (default: 1)
        """
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")

        # Check HyperSync availability (required for oracle events)
        if self.hypersync is None:
            console.print(
                "[red]✗ HyperSync not initialized - cannot collect non-Chainlink markets[/red]"
            )
            console.print("[dim]Non-Chainlink markets require oracle events via HyperSync.[/dim]")
            console.print("[dim]Use --all-markets (default) or set HYPERSYNC_API_TOKEN.[/dim]")
            return

        from gmx_historical_data.block_timestamp_cache import BlockTimestampCache
        from gmx_historical_data.data_coverage_analyzer import DataCoverageAnalyzer
        from gmx_historical_data.gmx_token_mapper import GMXTokenMapper
        from gmx_historical_data.hypersync_key_rotator import HyperSyncKeyRotator
        from gmx_historical_data.oracle_event_aggregator import (
            build_oracle_price_dataframe,
            resample_oracle_price_dataframe,
        )
        from gmx_historical_data.oracle_price_collector import OraclePriceCollector

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
        if self.config.hypersync_api_token and (
            "," in self.config.hypersync_api_token or " " in self.config.hypersync_api_token
        ):
            key_rotator = HyperSyncKeyRotator(self.config.hypersync_api_token)
            console.print(
                f"[green]✓[/green] Initialized HyperSync key rotation "
                f"with {key_rotator.total_keys} key(s)"
            )

        # Initialize block-timestamp cache (auto-tuned workers from system resources)
        console.print("\n[bold]Initializing block-timestamp cache...[/bold]")
        cache_path = self.config.output_dir / ".cache" / "block_timestamps.parquet"
        try:
            from gmx_historical_data.resource_limiter import get_resource_limits

            _limits = get_resource_limits()
            _cache_workers = _limits["block_cache_workers"]
        except Exception:
            _cache_workers = 32
        block_cache = BlockTimestampCache(cache_path, self.web3, fetch_workers=_cache_workers)
        # Pre-build cache before parallel execution to avoid concurrent RPC calls
        block_cache._ensure_cache_loaded()

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
                addr: sym for addr, sym in token_mapping.items() if sym.upper() in requested_upper
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

        # Skip already-checkpointed symbols for resume
        if symbols_to_collect:
            pending = []
            skipped = []
            for s in symbols_to_collect:
                cp = self.checkpoint_mgr.load_checkpoint(s)
                if cp and cp.total_events > 0:
                    skipped.append(s)
                else:
                    pending.append(s)
            if skipped:
                console.print(
                    f"  [green]✓[/green] Skipping [cyan]{len(skipped)}[/cyan] "
                    f"already-collected non-Chainlink symbols"
                )
            symbols_to_collect = pending

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
            gmx_sem = asyncio.Semaphore(concurrency)

            async def _fetch_gmx_for_symbol(symbol: str) -> tuple[str, dict]:
                async with gmx_sem:
                    console.print(f"\n[cyan]{symbol}[/cyan]")
                    gmx_candles = await self._fetch_gmx_candles_for_timeframes(symbol)
                    if gmx_candles:
                        for tf, df in gmx_candles.items():
                            console.print(
                                f"  [green]✓[/green] {tf}: {len(df):,} candles from GMX API"
                            )
                    else:
                        console.print("  [yellow]○[/yellow] No GMX API data available")
                    return symbol, gmx_candles or {}

            gmx_results = await asyncio.gather(
                *[_fetch_gmx_for_symbol(s) for s in symbols_to_collect],
                return_exceptions=True,
            )
            for gmx_result in gmx_results:
                if isinstance(gmx_result, BaseException):
                    logger.error("GMX API fetch task failed: %s", gmx_result)
                else:
                    sym, candles = gmx_result
                    if candles:
                        gmx_data_by_symbol[sym] = candles

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
        oracle_events_by_symbol: dict[str, list] = {}

        sem = asyncio.Semaphore(concurrency)

        async def _collect_oracle_for_symbol(symbol: str) -> tuple[str, list]:
            async with sem:
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
                        earliest_dt = pd.to_datetime(tf_cov.earliest, unit="s", utc=True)
                        latest_dt = pd.to_datetime(tf_cov.latest, unit="s", utc=True)
                        console.print(
                            f"    {tf}: {tf_cov.candle_count:,} candles "
                            f"({earliest_dt.strftime('%Y-%m-%d')} to {latest_dt.strftime('%Y-%m-%d')})"
                        )
                else:
                    console.print(
                        "  [yellow]○[/yellow] No existing data - full historical collection"
                    )

                # Calculate missing block range
                symbol_start, symbol_end = coverage_analyzer.get_missing_block_range(
                    coverage,
                    block_cache,
                    genesis_block=default_start,
                    safety_margin=1000,  # 1000 blocks overlap for safety
                )

                if symbol_start is None and symbol_end is None:
                    console.print(
                        "  [green]✓[/green] Data already complete - no oracle events needed"
                    )
                    return symbol, []

                # Display range to fetch
                if symbol_end is None:
                    console.print(
                        f"  [dim]Fetching oracle events:[/dim] blocks {symbol_start:,} to latest"
                    )
                else:
                    blocks_to_fetch = symbol_end - symbol_start
                    console.print(
                        f"  [dim]Fetching oracle events:[/dim] blocks {symbol_start:,} to "
                        f"{symbol_end:,} ({blocks_to_fetch:,} blocks)"
                    )

                # Get token address for this symbol
                token_addr = symbol_to_token.get(symbol, "").lower()
                if not token_addr:
                    console.print("  [red]✗[/red] Token address not found")
                    return symbol, []

                # Collect oracle events for this symbol's range
                try:
                    events = await oracle_collector.collect_oracle_events(
                        start_block=symbol_start,
                        end_block=symbol_end,
                        token_addresses=[token_addr],  # Only this token
                        concurrency=4,
                    )

                    if events:
                        console.print(f"  [green]✓[/green] Collected {len(events):,} oracle events")
                    else:
                        console.print("  [yellow]○[/yellow] No oracle events found in range")

                    return symbol, events

                except Exception as e:
                    console.print(f"  [red]✗[/red] Failed to collect oracle events: {e}")
                    logger.error("Oracle collection failed for %s: %s", symbol, e)
                    traceback.print_exc()
                    return symbol, []

        # Run all symbols concurrently (bounded by semaphore)
        gather_results = await asyncio.gather(
            *[_collect_oracle_for_symbol(s) for s in symbols_to_collect],
            return_exceptions=True,
        )
        for gather_result in gather_results:
            if isinstance(gather_result, BaseException):
                logger.error("Oracle collection task failed: %s", gather_result)
            else:
                sym, events = gather_result
                oracle_events_by_symbol[sym] = events

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
                console.print("  [yellow]○[/yellow] No data available (GMX API or oracle events)")
                failed += 1
                failed_symbols.append(symbol)
                continue

            try:
                # Build price DataFrame once per symbol (expensive list comprehension + sort),
                # then resample cheaply per timeframe below — avoids 6x redundant reconstruction.
                oracle_price_df = (
                    build_oracle_price_dataframe(token_events, token_decimals=decimals)
                    if token_events
                    else None
                )

                for timeframe in TIMEFRAMES:
                    gmx_df = gmx_candles.get(timeframe)
                    oracle_df = None

                    if oracle_price_df is not None and not oracle_price_df.empty:
                        oracle_df = resample_oracle_price_dataframe(
                            oracle_price_df, timeframe, symbol
                        )
                        if oracle_df.empty:
                            oracle_df = None

                    # Filter oracle to before GMX coverage to avoid overlap
                    if gmx_df is not None and oracle_df is not None:
                        gmx_earliest = gmx_df["timestamp"].min()
                        oracle_df = oracle_df[oracle_df["timestamp"] < gmx_earliest]
                        if oracle_df.empty:
                            oracle_df = None

                    count = self._merge_and_save_candles(
                        symbol,
                        timeframe,
                        oracle_df,
                        gmx_df,
                        merge_with_existing=True,
                    )
                    if count > 0:
                        sources = []
                        if oracle_df is not None:
                            sources.append(f"oracle: {len(oracle_df):,}")
                        if gmx_df is not None:
                            sources.append(f"GMX: {len(gmx_df):,}")
                        console.print(
                            f"  [green]✓[/green] {timeframe}: {count:,} candles "
                            f"({' + '.join(sources) if sources else 'saved'})"
                        )

                # Save checkpoint for resume
                self._save_symbol_checkpoint(symbol)
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

        summary_table.add_row("[green]✓ Successful[/green]", f"[green]{successful}/{total}[/green]")
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
    symbol: str | None = typer.Option(
        None,
        "--symbol",
        help="Token symbol(s) to collect, comma-separated (e.g., ETH,BTC,SUI)",
    ),
    output_dir: Path = typer.Option(
        Path("./data"),
        "--output-dir",
        help="Output directory for data",
    ),
    rpc_url: str | None = typer.Option(
        None,
        "--rpc-url",
        envvar="JSON_RPC_ARBITRUM",
        help="Arbitrum RPC URL (or set JSON_RPC_ARBITRUM env var)",
    ),
    hypersync_token: str | None = typer.Option(
        None,
        "--hypersync-token",
        envvar="HYPERSYNC_API_TOKEN",
        help="HyperSync API token(s) - space-separated for multiple tokens (or set HYPERSYNC_API_TOKEN env var)",
    ),
    start_block: int | None = typer.Option(
        None,
        "--start-block",
        help="Starting block number (default: 0)",
    ),
    end_block: int | None = typer.Option(
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
    log_file: str | None = typer.Option(
        None,
        "--log-file",
        help="Path to log file. If not specified, logs to ./logs/gmx-YYYY-MM-DD-HH-MM-SS.log",
    ),
    quiet: bool = typer.Option(
        False,
        "--quiet",
        help="Suppress console output when logging to file (file-only mode)",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Ignore checkpoints and re-collect all symbols from scratch",
    ),
    nice: bool = typer.Option(
        False,
        "--nice",
        help="Lower process priority (nice +10) and auto-tune concurrency based on system resources",
    ),
    quickstart: bool = typer.Option(
        False,
        "-q",
        "--quickstart",
        help=(
            "Before collecting, shallow-clone the data/daily-collection branch "
            "and merge-copy its user_data/ tree into ./user_data/ (existing local "
            "files are never overwritten). The seed lands in ./user_data/, not in "
            "--output-dir, because the branch layout is CCXT/feather — distinct "
            "from the candles this CLI produces."
        ),
    ),
    seed_only: bool = typer.Option(
        False,
        "--seed-only",
        help="With --quickstart, seed and exit without running candle collection.",
    ),
    quickstart_ref: str = typer.Option(
        DEFAULT_RELEASE_TAG,
        "--quickstart-ref",
        help=f"Release tag to seed from (default: {DEFAULT_RELEASE_TAG} = most recent).",
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
    if quickstart:
        seed_dir = Path("./user_data")
        console.print("\n[bold]Quickstart: seeding from GitHub Release[/bold]")
        summary = seed_from_release(seed_dir, quickstart_ref, console)
        if "error" not in summary:
            console.print(
                f"  Copied {summary['copied']} new files "
                f"({summary['bytes'] / 1e6:.1f} MB), "
                f"skipped {summary['skipped']} existing."
            )
            print_coverage_summary(seed_dir, console)
        else:
            console.print("  [yellow]Proceeding without seed — collection will still run.[/yellow]")
        if seed_only:
            console.print("\n[green]--seed-only set, exiting.[/green]")
            return
        console.print()
    elif seed_only:
        console.print("[red]--seed-only requires --quickstart.[/red]")
        raise typer.Exit(code=2)

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
            force=force,
            nice=nice,
        )


def _cli_impl(
    full: bool,
    update: bool,
    symbol: str | None,
    output_dir: Path,
    rpc_url: str | None,
    hypersync_token: str | None,
    start_block: int | None,
    end_block: int | None,
    use_gmx_api: bool,
    use_events: bool,
    chainlink_only: bool,
    concurrency: int,
    default_mode: bool,
    force: bool = False,
    nice: bool = False,
) -> None:
    """Internal implementation of CLI logic."""
    if nice:
        from gmx_historical_data.resource_limiter import apply_nice, get_resource_limits

        apply_nice()
        resource_limits = get_resource_limits()

        # Auto-tune concurrency from system resources if user didn't override (default is 2)
        if concurrency == 2:
            concurrency = resource_limits["concurrency"]
            logger.info(f"Auto-tuned concurrency to {concurrency} based on system resources")

    # Validate --default flag
    if default_mode:
        if use_events:
            console.print("[red]Error: --default and --use-events are mutually exclusive[/red]")
            console.print("[dim]--default uses oracle-based collection (GMX API + Chainlink)[/dim]")
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
            # Parse comma-separated symbols and categorize
            symbols_list = [s.strip().upper() for s in symbol.split(",") if s.strip()]
            chainlink_symbols, non_chainlink_symbols, excluded_count = (
                _filter_and_categorize_symbols(symbols_list, chainlink_only)
            )
            if excluded_count > 0:
                console.print(
                    f"[yellow]Warning: {excluded_count} symbol(s) excluded "
                    f"(deprecated/problematic)[/yellow]"
                )
            if not chainlink_symbols and not non_chainlink_symbols:
                console.print("[red]No valid symbols to collect[/red]")
                raise typer.Exit(0)

            # Collect Chainlink symbols (skip already-checkpointed unless --full)
            for sym in chainlink_symbols:
                if use_events:
                    console.print(
                        "[red]Error: Single symbol collection with --use-events is not supported. "
                        "Use collect_all_symbols instead.[/red]"
                    )
                    raise typer.Exit(1)
                # Wholesale skip applies only to full-mode resume. In incremental
                # (--update) mode we always run collect_symbol so its gap detector
                # inspects existing coverage and fetches only the missing slice
                # (append), instead of treating a checkpoint as "done forever".
                if full and not force:
                    checkpoint = collector.checkpoint_mgr.load_checkpoint(sym)
                    if checkpoint and checkpoint.total_events > 0:
                        console.print(
                            f"  [green]✓[/green] {sym}: Already collected "
                            f"({checkpoint.total_events:,} candles) — skipping (full-mode resume)"
                        )
                        continue
                asyncio.run(collector.collect_symbol(sym, full=full, force=force))

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
                        concurrency=concurrency,
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
                    force=force,
                )
            )

            # Step 2: Collect non-Chainlink markets via oracle events (if --all-markets)
            if not chainlink_only:
                console.print("\n")
                asyncio.run(
                    collector.collect_non_chainlink_markets(
                        start_block=start_block,
                        end_block=end_block,
                        concurrency=concurrency,
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
    symbol: str | None = typer.Option(None, "--symbol", help="Symbol to verify (omit for all)"),
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
    symbol: str | None = typer.Option(
        None,
        "--symbol",
        help="Specific token symbol to debug (e.g., SUI, TAO). If not provided, lists all non-Chainlink markets.",
    ),
    rpc_url: str | None = typer.Option(
        None,
        "--rpc-url",
        envvar="JSON_RPC_ARBITRUM",
        help="Arbitrum RPC URL",
    ),
    hypersync_token: str | None = typer.Option(
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
    console.print(
        f"  [dim]Market type:[/dim] {'Chainlink' if is_chainlink else 'Non-Chainlink (Oracle Events)'}"
    )

    if is_chainlink:
        console.print(f"\n[yellow]Note: {symbol_upper} is a Chainlink market.[/yellow]")
        console.print("[yellow]It uses GMX API for data, but may also have oracle events.[/yellow]")

    from hypersync import ClientConfig, HypersyncClient

    from gmx_historical_data.oracle_price_collector import OraclePriceCollector

    # Get current block via HyperSync (no RPC needed)
    console.print("\n[bold]Initializing HyperSync collector...[/bold]")
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
    console.print(
        f"  [dim]Scanning from GMX V2 genesis:[/dim] block {start_block:,} to {current_block:,}"
    )

    # Collect oracle events
    total_blocks = current_block - start_block
    console.print("\n[bold]Fetching oracle events from HyperSync...[/bold]")
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
        console.print(
            f"\n[dim]... and {len(events) - limit} more events (use --limit to see more)[/dim]"
        )

    # Show summary statistics
    console.print()
    mid_prices = [(e.min_price + e.max_price) / 2 / price_divisor for e in events]

    summary_table = Table(title="Price Summary", box=box.ROUNDED, show_header=False)
    summary_table.add_column("Metric", style="dim")
    summary_table.add_column("Value", style="cyan")

    summary_table.add_row("Events count", f"{len(events):,}")
    summary_table.add_row("Price range", f"${min(mid_prices):,.4f} - ${max(mid_prices):,.4f}")
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
        Path("./user_data/data/gmx"),
        "--data-dir",
        help="Source GMX data directory (Freqtrade convention)",
    ),
    output_dir: Path = typer.Option(
        Path("./user_data/data"),
        "--output-dir",
        help="Output directory for Freqtrade files (writes gmx/futures/*.feather)",
    ),
    symbol: list[str] | None = typer.Option(
        None,
        "--symbol",
        help="Specific symbols to export (can be repeated)",
    ),
    timeframe: list[str] | None = typer.Option(
        None,
        "--timeframe",
        help="Specific timeframes to export (can be repeated)",
    ),
    output_format: str = typer.Option(
        "feather",
        "--format",
        help="Output format (feather or parquet)",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help=(
            "Backward-compat alias.  Still runs the history-preservation guard "
            "after the 2026-05-11 incident — use --unsafe-overwrite to actually "
            "discard old rows."
        ),
    ),
    unsafe_overwrite: bool = typer.Option(
        False,
        "--unsafe-overwrite",
        help=(
            "Replace existing feathers entirely, bypassing the history "
            "guard.  Only set this for schema migrations where you "
            "intentionally discard old data."
        ),
    ),
    delete_source: bool = typer.Option(
        False,
        "--delete-source",
        help=(
            "Delete the source candle parquet after writing the feather. "
            "Default is to keep the source — only set this for explicit "
            "parquet→feather migration workflows."
        ),
    ),
) -> None:
    """Export GMX data to Freqtrade-compatible format.

    Converts collected OHLCV candles AND funding rate data to Freqtrade's
    expected feather format.  For each symbol/timeframe, up to three files
    are generated:

    - OHLCV candles (``*-futures.feather``)
    - Funding rate  (``*-funding_rate.feather``) — rate in ``open`` column
    - Mark price    (``*-mark.feather``)         — OHLCV used as proxy
    - Index price   (``*-index.feather``)        — same as mark (GMX uses Chainlink as index)

    OUTPUT STRUCTURE:
      freqtrade_data/
      └── gmx/
          └── futures/
              ├── ETH_USDC_USDC-1h-futures.feather
              ├── ETH_USDC_USDC-1h-funding_rate.feather
              ├── ETH_USDC_USDC-1h-mark.feather
              ├── ETH_USDC_USDC-1h-index.feather
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
            overwrite=overwrite,
            unsafe_overwrite=unsafe_overwrite,
            keep_parquet=not delete_source,
        )
    except Exception as e:
        console.print(f"[red]Export failed: {e}[/red]")
        console.print("[red]Traceback:[/red]")
        console.print(traceback.format_exc())
        raise typer.Exit(1)

    # Summary
    total_candles = sum(r["candles"] for r in results.values())
    total_ohlcv = sum(r.get("ohlcv_files", r["files"]) for r in results.values())
    total_funding = sum(r.get("funding_files", 0) for r in results.values())
    total_mark = sum(r.get("mark_files", 0) for r in results.values())
    total_index = sum(r.get("index_files", 0) for r in results.values())

    summary_table = Table(title="Export Summary", box=box.ROUNDED)
    summary_table.add_column("Symbol", style="cyan")
    summary_table.add_column("OHLCV", justify="right")
    summary_table.add_column("Funding", justify="right")
    summary_table.add_column("Mark", justify="right")
    summary_table.add_column("Index", justify="right")
    summary_table.add_column("Candles", justify="right")

    for sym, stats in sorted(results.items()):
        summary_table.add_row(
            sym,
            str(stats.get("ohlcv_files", stats["files"])),
            str(stats.get("funding_files", 0)),
            str(stats.get("mark_files", 0)),
            str(stats.get("index_files", 0)),
            f"{stats['candles']:,}",
        )

    summary_table.add_row(
        "[bold]Total[/bold]",
        f"[bold]{total_ohlcv}[/bold]",
        f"[bold]{total_funding}[/bold]",
        f"[bold]{total_mark}[/bold]",
        f"[bold]{total_index}[/bold]",
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


def export_candles_command(
    data_dir: Path = typer.Option(
        Path("./user_data/data/gmx"),
        "--data-dir",
        help="Source GMX data directory (Freqtrade convention)",
    ),
    output_dir: Path = typer.Option(
        Path("./user_data/data"),
        "--output-dir",
        help="Output directory for Freqtrade files (writes gmx/futures/*.feather)",
    ),
    symbol: list[str] | None = typer.Option(
        None, "--symbol", help="Specific symbols to export (can be repeated)"
    ),
    timeframe: list[str] | None = typer.Option(
        None, "--timeframe", help="Specific timeframes to export (can be repeated)"
    ),
    output_format: str = typer.Option(
        "feather", "--format", help="Output format (feather or parquet)"
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help=(
            "Backward-compat alias.  Still runs the history-preservation guard "
            "after the 2026-05-11 incident — use --unsafe-overwrite to actually "
            "discard old rows."
        ),
    ),
    unsafe_overwrite: bool = typer.Option(
        False,
        "--unsafe-overwrite",
        help=(
            "Replace existing feathers entirely, bypassing the history "
            "guard.  Schema migrations only."
        ),
    ),
    delete_source: bool = typer.Option(
        False,
        "--delete-source",
        help=(
            "Delete the source candle parquet after writing the feather. Default keeps the source."
        ),
    ),
) -> None:
    """Export GMX OHLCV (candles + mark + index) feathers ONLY.

    Reads only from ``{data_dir}/candles/`` and writes ``-futures``, ``-mark``,
    and ``-index`` feathers.  Never touches funding files — use the
    ``export-funding`` command for that.
    """
    from gmx_historical_data.freqtrade_exporter import FreqtradeExporter

    if not data_dir.exists():
        console.print(f"[red]Error: Data directory not found: {data_dir}[/red]")
        raise typer.Exit(1)

    exporter = FreqtradeExporter(data_dir, output_dir)
    candle_symbols = exporter.storage.list_symbols()
    if not candle_symbols:
        console.print(f"[yellow]No candle data found in {data_dir}[/yellow]")
        raise typer.Exit(1)

    console.print(
        Panel(
            "[bold cyan]Freqtrade Export — Candles (OHLCV + mark + index)[/bold cyan]\n\n"
            f"Source: {data_dir}\n"
            f"Output: {output_dir}\n"
            f"Format: {output_format}",
            box=box.ROUNDED,
        )
    )
    try:
        results = exporter.export_candles(
            symbols=list(symbol) if symbol else None,
            timeframes=list(timeframe) if timeframe else None,
            output_format=output_format,
            overwrite=overwrite,
            unsafe_overwrite=unsafe_overwrite,
            keep_parquet=not delete_source,
        )
    except Exception as e:
        console.print(f"[red]Export failed: {e}[/red]")
        console.print(traceback.format_exc())
        raise typer.Exit(1) from e

    total_files = sum(r["files"] for r in results.values())
    total_candles = sum(r["candles"] for r in results.values())
    console.print(
        f"\n[green]✓[/green] {total_files} feather files written ({total_candles:,} candles) "
        f"to [cyan]{output_dir / 'gmx'}[/cyan]"
    )


def export_funding_command(
    data_dir: Path = typer.Option(
        Path("./user_data/data/gmx"),
        "--data-dir",
        help="Source GMX data directory (Freqtrade convention)",
    ),
    output_dir: Path = typer.Option(
        Path("./user_data/data"),
        "--output-dir",
        help="Output directory for Freqtrade files (writes gmx/futures/*.feather)",
    ),
    symbol: list[str] | None = typer.Option(
        None, "--symbol", help="Specific symbols to export (can be repeated)"
    ),
    timeframe: list[str] | None = typer.Option(
        None, "--timeframe", help="Specific timeframes to export (can be repeated)"
    ),
    output_format: str = typer.Option(
        "feather", "--format", help="Output format (feather or parquet)"
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help=(
            "Backward-compat alias.  Still runs the history-preservation guard "
            "after the 2026-05-11 incident."
        ),
    ),
    unsafe_overwrite: bool = typer.Option(
        False,
        "--unsafe-overwrite",
        help="Replace existing feathers entirely.  Schema migrations only.",
    ),
) -> None:
    """Export GMX funding-rate feathers ONLY.

    Reads only from ``{data_dir}/funding/`` and writes ``-funding_rate``
    feathers.  Never touches OHLCV files — use ``export-candles`` for those.
    The funding parquet source is owned by the unified-funding pipeline and
    is never deleted by this command.
    """
    from gmx_historical_data.freqtrade_exporter import FreqtradeExporter

    if not data_dir.exists():
        console.print(f"[red]Error: Data directory not found: {data_dir}[/red]")
        raise typer.Exit(1)

    exporter = FreqtradeExporter(data_dir, output_dir)
    funding_symbols = exporter.list_funding_symbols()
    if not funding_symbols:
        console.print(f"[yellow]No funding data found in {data_dir}[/yellow]")
        raise typer.Exit(1)

    console.print(
        Panel(
            "[bold cyan]Freqtrade Export — Funding rates[/bold cyan]\n\n"
            f"Source: {data_dir}\n"
            f"Output: {output_dir}\n"
            f"Format: {output_format}",
            box=box.ROUNDED,
        )
    )
    try:
        results = exporter.export_funding(
            symbols=list(symbol) if symbol else None,
            timeframes=list(timeframe) if timeframe else None,
            output_format=output_format,
            overwrite=overwrite,
            unsafe_overwrite=unsafe_overwrite,
        )
    except Exception as e:
        console.print(f"[red]Export failed: {e}[/red]")
        console.print(traceback.format_exc())
        raise typer.Exit(1) from e

    total = sum(r["funding_files"] for r in results.values())
    console.print(
        f"\n[green]✓[/green] {total} funding feathers written to [cyan]{output_dir / 'gmx'}[/cyan]"
    )


app = typer.Typer(help=CLI_HELP, rich_markup_mode="rich")

# Register commands
app.command(name="collect")(cli)
app.command(name="verify")(verify_command)
app.command(name="debug-oracle")(debug_oracle_command)
app.command(name="export-freqtrade")(export_freqtrade_command)
app.command(name="export-candles")(export_candles_command)
app.command(name="export-funding")(export_funding_command)


def fill_gaps_cex(
    data_dir: Path = typer.Option(Path("./user_data"), "--data-dir", help="Root data dir"),
    symbol: str = typer.Option("", "--symbol", help="Comma-separated whitelist; empty = all"),
    timeframe: str = typer.Option(
        "", "--timeframe", help="Comma-separated whitelist; empty = all six"
    ),
    gap_threshold: float = typer.Option(0.20, "--gap-threshold"),
    merge_gap_bars: int = typer.Option(2, "--merge-gap-bars"),
    cex_datadir: Path | None = typer.Option(None, "--cex-datadir"),
    exchanges: str = typer.Option("binance,bybit", "--exchanges"),
    routing_file: Path = typer.Option(Path("configs/cex_routing.json"), "--routing-file"),
    skip_download: bool = typer.Option(False, "--skip-download"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    log_dir: Path = typer.Option(Path("./logs"), "--log-dir"),
    network: str = typer.Option("arbitrum", "--network"),
) -> None:
    """Fill GMX OHLCV price gaps using Binance/Bybit via freqtrade download-data."""
    symbols = [s.strip() for s in symbol.split(",") if s.strip()] or None
    tfs = [t.strip() for t in timeframe.split(",") if t.strip()] or None
    exch = [e.strip() for e in exchanges.split(",") if e.strip()]
    fill_gaps_from_cex(
        data_dir=data_dir,
        symbols=symbols,
        timeframes=tfs,
        routing_file=routing_file,
        cex_datadir=cex_datadir,
        exchanges=exch,
        gap_threshold=gap_threshold,
        merge_gap_bars=merge_gap_bars,
        log_dir=log_dir,
        dry_run=dry_run,
        skip_download=skip_download,
        network=network,
    )


app.command(name="fill-gaps-cex")(fill_gaps_cex)


def main() -> None:
    """Main CLI entry point."""
    app()


if __name__ == "__main__":
    main()
