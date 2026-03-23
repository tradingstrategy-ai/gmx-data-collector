"""Periodic GMX data collection daemon.

Collects fresh GMX API data for all tokens at configurable intervals,
producing OHLCV candles suitable for backtesting.

Supports hybrid collection:
- Chainlink markets (34): Uses GMX API
- Non-Chainlink markets (84): Uses OraclePriceUpdate events from EventEmitter

USAGE:
    gmx-periodic-collector

ENVIRONMENT VARIABLES (required):
    JSON_RPC_ARBITRUM        Arbitrum RPC URL
    HYPERSYNC_API_TOKEN      HyperSync API token from envio.dev

ENVIRONMENT VARIABLES (optional):
    COLLECTION_INTERVAL_MINUTES  Collection interval (default: 60)
    OUTPUT_DIR                   Output directory (default: ./data)
    CHAINLINK_ONLY               Only collect Chainlink markets (default: true)
    ENABLE_ADAPTIVE_GAP_DETECTION  Detect API sliding window data loss (default: true)
    COLLECT_LIVE_FUNDING         Append live GMX funding rate after each cycle (default: false)
    LIVE_FUNDING_FEATHER_DIR     Directory for funding rate feather files (required when
                                 COLLECT_LIVE_FUNDING=true)
    TIMEFRAME_CONCURRENCY        Parallel timeframe fetches (default: 6)
    LOG_LEVEL                    Logging level (default: INFO)
    DRY_RUN                      Don't save data, just log (default: false)

DOCKER DEPLOYMENT:
    cd docker
    cp .env.daemon.example .env.daemon
    # Edit .env.daemon with your API keys
    docker-compose up -d
    docker logs -f gmx-collector

EXAMPLE:
    # Set required environment variables
    export JSON_RPC_ARBITRUM="https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY"
    export HYPERSYNC_API_TOKEN="YOUR_TOKEN"

    # Optional: configure collection
    export COLLECTION_INTERVAL_MINUTES=60
    export OUTPUT_DIR=./data
    export CHAINLINK_ONLY=false

    # Start daemon
    gmx-periodic-collector
"""

import asyncio
import gc
import logging
import signal
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta

import pandas as pd
import schedule
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
)
from rich.table import Table
from web3 import Web3

from gmx_historical_data.config import (
    GMX_V2_GENESIS_BLOCK,
    TIMEFRAMES,
    is_excluded_symbol,
)
from gmx_historical_data.daemon.config import (
    DaemonConfig,
    get_gmx_markets_with_chainlink_feeds,
)
from gmx_historical_data.daemon.data_loss_handler import DataLossHandler
from gmx_historical_data.daemon.gap_detector import (
    AdaptiveGapDetector,
    GapDetector,
)
from gmx_historical_data.daemon.health_monitor import HealthMonitor
from gmx_historical_data.gmx_api_integration import (
    GMXDataFetcher,
    map_timeframe_to_gmx_period,
)
from gmx_historical_data.gmx_token_discovery import GMXTokenDiscovery
from gmx_historical_data.storage import ParquetStorage

console = Console()
logger = logging.getLogger(__name__)


class GMXPeriodicCollector:
    """Periodic collection daemon for GMX OHLCV data.

    :param config: Daemon configuration
    """

    def __init__(self, config: DaemonConfig):
        """Initialize periodic collector.

        :param config: Daemon configuration
        """
        self.config = config
        self.running = False
        self.shutdown_requested = False

        # Initialize components
        self.storage = ParquetStorage(config.output_dir)
        self.gmx_fetcher = GMXDataFetcher(chain="arbitrum")
        self.gmx_discovery = GMXTokenDiscovery(chain="arbitrum")
        self.gap_detector = GapDetector(self.storage)
        self.health_monitor = HealthMonitor(log_metrics=True)

        # Initialize adaptive gap detection components
        self.adaptive_gap_detector = AdaptiveGapDetector(
            storage=self.storage,
            gmx_fetcher=self.gmx_fetcher,
        )
        self.data_loss_handler = DataLossHandler()

        # Initialize oracle collector for non-Chainlink markets if enabled
        self.oracle_collector = None
        self.token_mapper = None
        self._last_oracle_block = GMX_V2_GENESIS_BLOCK

        if not config.chainlink_only:
            from gmx_historical_data.gmx_token_mapper import GMXTokenMapper
            from gmx_historical_data.oracle_price_collector import OraclePriceCollector

            self.oracle_collector = OraclePriceCollector(
                hypersync_endpoint=config.hypersync_endpoint,
                api_token=config.hypersync_api_token,
            )
            web3 = Web3(Web3.HTTPProvider(config.rpc_url))
            self.token_mapper = GMXTokenMapper(web3)

        # Configure logging
        self._setup_logging()

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

    def _setup_logging(self) -> None:
        """Configure logging with rich handler for beautiful output."""
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format="%(message)s",
            datefmt="[%X]",
            handlers=[
                RichHandler(
                    console=console,
                    rich_tracebacks=True,
                    tracebacks_show_locals=True,
                    show_time=True,
                    show_path=False,
                )
            ],
        )

    def _handle_shutdown_signal(self, signum, frame) -> None:
        """Handle shutdown signals (SIGTERM, SIGINT).

        :param signum: Signal number
        :param frame: Current stack frame
        """
        signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        console.print(
            f"\n[yellow]Received {signal_name} - initiating graceful shutdown...[/yellow]"
        )
        logger.info(f"Shutdown signal received: {signal_name}")
        self.shutdown_requested = True

    def _discover_symbols(self) -> list[str]:
        """Discover GMX tokens to collect (only those with Chainlink feeds).

        By default, only collects markets with public Chainlink price feeds (34 markets).
        This ensures reliable OHLCV data quality.

        :return: List of token symbols (filtered by config and exclusions)
        """
        # Use configured symbols if specified
        if self.config.collection_symbols:
            symbols = self.config.collection_symbols
            console.print(f"[cyan]Using configured symbols: {len(symbols)} tokens[/cyan]")
        else:
            # Use only markets with Chainlink feeds (34 markets)
            symbols = get_gmx_markets_with_chainlink_feeds()

            # Filter out excluded symbols (case-insensitive for global, direct check for config)
            symbols = [s for s in symbols if not is_excluded_symbol(s)]
            symbols = [
                s
                for s in symbols
                if s.upper() not in {e.upper() for e in self.config.excluded_symbols}
            ]

            console.print(
                f"[cyan]Collecting {len(symbols)} GMX markets with Chainlink feeds[/cyan]"
            )
            console.print("[dim]  (84 additional markets excluded - no Chainlink feeds)[/dim]")

        return symbols

    def _collect_symbol_timeframe(
        self,
        symbol: str,
        timeframe: str,
    ) -> tuple[str, str, int, Exception | None]:
        """Collect data for a single symbol/timeframe combination.

        Uses adaptive gap detection when enabled to detect permanent data loss
        from GMX API's sliding window.

        :param symbol: Token symbol
        :param timeframe: Timeframe string
        :return: Tuple of (symbol, timeframe, candles_added, error)
        """
        try:
            # Use adaptive gap detection if enabled
            if self.config.enable_adaptive_gap_detection:
                gap_result = self.adaptive_gap_detector.detect_gap_adaptive(symbol, timeframe)

                # Handle data loss scenario
                if gap_result.has_data_loss:
                    event = self.data_loss_handler.handle_gap_result(symbol, timeframe, gap_result)
                    if event:
                        self.health_monitor.record_data_loss(event)
                        logger.critical(
                            f"DATA LOSS: {symbol} {timeframe} lost "
                            f"~{gap_result.lost_candles_estimate} candles "
                            f"({gap_result.lost_timespan})"
                        )

                # Skip if no gap
                if not gap_result.needs_fetch:
                    return symbol, timeframe, 0, None

                fetch_start = gap_result.fetch_start
            else:
                # Fall back to simple gap detection
                fetch_start, fetch_end = self.gap_detector.detect_gap(symbol, timeframe)

                # No gap - skip collection
                if fetch_start is None and fetch_end is None:
                    return symbol, timeframe, 0, None

            # Fetch GMX API data (map timeframe to GMX period format: 1min->1m, 1D->1d)
            gmx_period = map_timeframe_to_gmx_period(timeframe)
            gmx_df = self.gmx_fetcher.fetch_gmx_candles(
                symbol=symbol,
                period=gmx_period,
                limit=10000,
            )

            if gmx_df.empty:
                return symbol, timeframe, 0, None

            # Filter to gap period if fetch_start is specified
            if fetch_start is not None:
                gmx_df = gmx_df[gmx_df["timestamp"] >= fetch_start]

            if gmx_df.empty:
                return symbol, timeframe, 0, None

            # Read existing data
            existing_df = self.storage.read_candles(timeframe, symbol)

            # Merge and deduplicate
            if existing_df.empty:
                merged_df = gmx_df
            else:
                merged_df = pd.concat([existing_df, gmx_df], ignore_index=True)
                merged_df = merged_df.sort_values("timestamp").drop_duplicates(
                    subset=["timestamp"],
                    keep="last",
                )

            # Save if not dry run
            candles_added = len(gmx_df)
            if not self.config.dry_run:
                self.storage.save_candles(merged_df, timeframe, symbol)

            return symbol, timeframe, candles_added, None

        except Exception as e:
            logger.exception(f"Error collecting {symbol} {timeframe}: {e}")
            return symbol, timeframe, 0, e

    def _collect_symbol(self, symbol: str) -> dict[str, int]:
        """Collect all timeframes for a single symbol.

        :param symbol: Token symbol
        :return: Dictionary of candles added per timeframe
        """
        candles_by_timeframe = {}
        errors = []

        # Collect timeframes concurrently
        with ThreadPoolExecutor(max_workers=self.config.timeframe_concurrency) as executor:
            futures = {
                executor.submit(self._collect_symbol_timeframe, symbol, tf): tf for tf in TIMEFRAMES
            }

            for future in as_completed(futures):
                timeframe = futures[future]
                try:
                    _, _, candles_added, error = future.result()

                    if error:
                        errors.append((timeframe, str(error)))
                        self.health_monitor.record_symbol_failure(symbol, str(error), timeframe)
                    else:
                        candles_by_timeframe[timeframe] = candles_added

                except Exception as e:
                    logger.exception(f"Unexpected error for {symbol} {timeframe}: {e}")
                    errors.append((timeframe, str(e)))
                    self.health_monitor.record_symbol_failure(symbol, str(e), timeframe)

        # Log summary for symbol
        total_candles = sum(candles_by_timeframe.values())
        if total_candles > 0 or errors:
            if errors:
                console.print(
                    f"  [yellow]{symbol}[/yellow]: {total_candles:,} candles, {len(errors)} errors"
                )
            else:
                console.print(f"  [green]{symbol}[/green]: {total_candles:,} candles")

        return candles_by_timeframe

    async def _collect_symbol_via_oracle_fallback(self, symbol: str) -> dict[str, int]:
        """Fallback collection for a single symbol via oracle events.

        Used when GMX API collection fails (e.g., aggregator discovery failed).

        :param symbol: Token symbol
        :return: Dictionary of candles added per timeframe
        """
        from gmx_historical_data.oracle_event_aggregator import (
            aggregate_oracle_events_to_ohlcv,
        )

        if not self.oracle_collector or not self.token_mapper:
            return {}

        # Get token address for the symbol
        try:
            token_mapping = self.token_mapper.get_all_token_mapping()
        except Exception as e:
            logger.warning(f"Failed to get token mapping for oracle fallback: {e}")
            return {}

        # Find the token address for this symbol
        symbol_upper = symbol.upper()
        token_address = None
        for addr, sym in token_mapping.items():
            if sym.upper() == symbol_upper:
                token_address = addr
                break

        if not token_address:
            logger.warning(f"No token address found for {symbol} in oracle fallback")
            return {}

        # Get token decimals for price conversion
        token_decimals = self.token_mapper.get_decimals_for_symbol(symbol_upper) or 18

        # Collect oracle events for this token
        try:
            events = await self.oracle_collector.collect_oracle_events(
                start_block=self._last_oracle_block + 1,
                token_addresses=[token_address],
            )
        except Exception as e:
            logger.warning(f"Oracle fallback failed for {symbol}: {e}")
            return {}

        if not events:
            return {}

        # Aggregate to OHLCV
        candles_by_timeframe = {}
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

                # Save if not dry run
                if not self.config.dry_run:
                    self.storage.save_candles(merged, timeframe, symbol)
                del merged

                candles_by_timeframe[timeframe] = len(ohlcv)

            except Exception as e:
                logger.warning(f"Oracle fallback aggregation failed for {symbol} {timeframe}: {e}")

        gc.collect()
        return candles_by_timeframe

    async def _collect_non_chainlink_markets(self) -> dict[str, int]:
        """Collect data for non-Chainlink markets via OraclePriceUpdate events.

        :return: Dict of symbol -> candles collected
        """
        from gmx_historical_data.oracle_event_aggregator import (
            aggregate_oracle_events_to_ohlcv,
        )

        if not self.oracle_collector or not self.token_mapper:
            return {}

        console.rule("[bold magenta]Non-Chainlink Markets (Oracle Events)[/bold magenta]")

        # Get non-Chainlink token mapping
        try:
            token_mapping = self.token_mapper.get_non_chainlink_tokens()
        except Exception as e:
            logger.error(f"Failed to get non-Chainlink token mapping: {e}")
            console.print(f"[red]Failed to get token mapping: {e}[/red]")
            return {}

        if not token_mapping:
            console.print("[yellow]No non-Chainlink tokens found[/yellow]")
            return {}

        console.print(f"[cyan]Found {len(token_mapping)} non-Chainlink markets[/cyan]")

        # Get token decimals for price conversion
        try:
            token_decimals_map = self.token_mapper.get_token_decimals()
        except Exception as e:
            logger.warning(f"Could not get token decimals: {e}. Using default 18.")
            token_decimals_map = {}

        # Determine block range (incremental from last collection)
        start_block = self._last_oracle_block + 1

        # Collect ALL oracle events at once (efficient batch query)
        token_addresses = list(token_mapping.keys())

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
            transient=True,
        ) as progress:
            progress.add_task("[cyan]Fetching oracle events from HyperSync...", total=None)

            try:
                events = await self.oracle_collector.collect_oracle_events(
                    start_block=start_block,
                    token_addresses=token_addresses,
                )
            except Exception as e:
                logger.error(f"Failed to collect oracle events: {e}")
                console.print(f"[red]Failed to collect oracle events: {e}[/red]")
                return {}

        if not events:
            console.print("[dim]No new oracle events found[/dim]")
            return {}

        # Update last processed block
        self._last_oracle_block = max(e.block_number for e in events)
        console.print(
            f"[dim]Collected {len(events):,} oracle events up to block {self._last_oracle_block:,}[/dim]"
        )

        # Group events by token, then release the flat list to free memory
        events_by_token: dict[str, list] = defaultdict(list)
        for event in events:
            events_by_token[event.token.lower()].append(event)
        del events
        gc.collect()

        # Aggregate to OHLCV per symbol
        results: dict[str, int] = {}
        total_candles = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=False,
        ) as progress:
            task = progress.add_task(
                "[magenta]Aggregating oracle events...",
                total=len(events_by_token),
            )

            for token_addr, token_events in events_by_token.items():
                symbol = token_mapping.get(token_addr)
                if not symbol:
                    progress.update(task, advance=1)
                    continue

                # Skip excluded symbols (case-insensitive)
                if is_excluded_symbol(symbol) or symbol.upper() in {
                    e.upper() for e in self.config.excluded_symbols
                }:
                    progress.update(task, advance=1)
                    continue

                progress.update(task, description=f"[magenta]Aggregating {symbol}...")

                # Get decimals for this token (default 18 if not found)
                token_decimals = token_decimals_map.get(symbol, 18)

                symbol_candles = 0
                for timeframe in TIMEFRAMES:
                    try:
                        ohlcv = aggregate_oracle_events_to_ohlcv(
                            token_events,
                            timeframe,
                            symbol,
                            token_decimals=token_decimals,
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

                        # Save if not dry run
                        if not self.config.dry_run:
                            self.storage.save_candles(merged, timeframe, symbol)

                        symbol_candles += len(ohlcv)

                    except Exception as e:
                        logger.error(f"Failed to aggregate {symbol} {timeframe}: {e}")

                # Release per-token events after aggregation
                del token_events
                gc.collect()

                if symbol_candles > 0:
                    results[symbol] = symbol_candles
                    total_candles += symbol_candles
                    self.health_monitor.record_symbol_success(symbol, {})

                progress.update(task, advance=1)

        # Summary panel
        summary_table = Table(show_header=False, box=None, padding=(0, 2))
        summary_table.add_column("Metric", style="dim")
        summary_table.add_column("Value")

        summary_table.add_row("Symbols processed", f"[green]{len(results)}[/green]")
        summary_table.add_row("Candles added", f"{total_candles:,}")
        summary_table.add_row("Last block", f"{self._last_oracle_block:,}")

        console.print(
            Panel(
                summary_table,
                title="[bold]Oracle Collection Complete[/bold]",
                border_style="magenta",
            )
        )

        return results

    def run_collection_cycle(self) -> None:
        """Execute one complete collection cycle for all symbols."""
        console.print()
        console.rule("[bold cyan]Collection Cycle[/bold cyan]")
        logger.info("Starting collection cycle")

        # Start cycle tracking
        self.health_monitor.start_cycle()

        # Discover symbols
        try:
            symbols = self._discover_symbols()
        except Exception as e:
            logger.exception(f"Failed to discover symbols: {e}")
            console.print(f"[red]Failed to discover symbols: {e}[/red]")
            self.health_monitor.end_cycle(total_symbols=0)
            return

        # Process each symbol with progress bar
        succeeded = 0
        failed = 0
        total_candles = 0
        fallback_succeeded = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            transient=False,
        ) as progress:
            task = progress.add_task(
                "[cyan]Collecting symbols...",
                total=len(symbols),
            )

            for symbol in symbols:
                # Check for shutdown request
                if self.shutdown_requested:
                    console.print("[yellow]Shutdown requested - stopping collection cycle[/yellow]")
                    break

                progress.update(task, description=f"[cyan]Collecting {symbol}...")

                try:
                    candles_by_timeframe = self._collect_symbol(symbol)
                    symbol_candles = sum(candles_by_timeframe.values())
                    total_candles += symbol_candles

                    # Record success if any candles were added
                    if symbol_candles > 0:
                        self.health_monitor.record_symbol_success(symbol, candles_by_timeframe)
                    else:
                        # No candles added (up to date)
                        self.health_monitor.record_symbol_success(symbol, {})

                    succeeded += 1

                except Exception as e:
                    error_msg = str(e)
                    logger.warning(f"GMX API collection failed for {symbol}: {e}")

                    # Try oracle event fallback if hybrid mode is enabled
                    if not self.config.chainlink_only and self.oracle_collector:
                        console.print(
                            f"  [yellow]{symbol}[/yellow]: API failed, trying oracle fallback..."
                        )
                        try:
                            fallback_candles = asyncio.run(
                                self._collect_symbol_via_oracle_fallback(symbol)
                            )
                            fallback_total = sum(fallback_candles.values())

                            if fallback_total > 0:
                                console.print(
                                    f"  [green]{symbol}[/green]: {fallback_total:,} candles via oracle fallback"
                                )
                                total_candles += fallback_total
                                self.health_monitor.record_symbol_success(symbol, fallback_candles)
                                succeeded += 1
                                fallback_succeeded += 1
                            else:
                                # Fallback returned no data - still mark as failure
                                console.print(
                                    f"  [red]{symbol}[/red]: Oracle fallback returned no data"
                                )
                                self.health_monitor.record_symbol_failure(symbol, error_msg)
                                failed += 1

                        except Exception as fallback_error:
                            logger.warning(
                                f"Oracle fallback also failed for {symbol}: {fallback_error}"
                            )
                            console.print(
                                f"  [red]{symbol}[/red]: Both API and oracle fallback failed"
                            )
                            self.health_monitor.record_symbol_failure(symbol, error_msg)
                            failed += 1
                    else:
                        self.health_monitor.record_symbol_failure(symbol, error_msg)
                        failed += 1

                progress.update(task, advance=1)
                gc.collect()

        # End cycle tracking
        self.health_monitor.end_cycle(total_symbols=len(symbols))

        # Build summary table
        summary_table = Table(show_header=False, box=None, padding=(0, 2))
        summary_table.add_column("Metric", style="dim")
        summary_table.add_column("Value")

        summary_table.add_row("Succeeded", f"[green]{succeeded}[/green]")
        if fallback_succeeded > 0:
            summary_table.add_row("  via oracle fallback", f"[yellow]{fallback_succeeded}[/yellow]")
        summary_table.add_row("Failed", f"[red]{failed}[/red]" if failed > 0 else "0")
        summary_table.add_row("Total symbols", str(len(symbols)))
        summary_table.add_row("Candles added", f"{total_candles:,}")

        if self.config.dry_run:
            summary_table.add_row("Status", "[yellow]DRY RUN - no data saved[/yellow]")
        else:
            summary_table.add_row("Data saved to", str(self.config.output_dir))

        console.print(
            Panel(
                summary_table,
                title="[bold]Cycle Complete[/bold]",
                border_style="green" if failed == 0 else "yellow",
            )
        )

        # Collect non-Chainlink markets if enabled
        if not self.config.chainlink_only and not self.shutdown_requested:
            try:
                asyncio.run(self._collect_non_chainlink_markets())
            except Exception as e:
                logger.error(f"Failed to collect non-Chainlink markets: {e}")
                console.print(f"[red]Non-Chainlink collection failed: {e}[/red]")

        # Live funding rate appender
        if (
            self.config.collect_live_funding
            and self.config.live_funding_feather_dir
            and not self.config.dry_run
        ):
            try:
                from gmx_historical_data.live_funding import (
                    fetch_live_funding_rates,
                    upsert_live_rates_to_feather,
                )

                rates = fetch_live_funding_rates()
                updated = upsert_live_rates_to_feather(self.config.live_funding_feather_dir, rates)
                console.print(f"[green]  Live funding rates appended for {updated} symbols[/green]")
            except Exception as exc:
                logger.warning(f"Live funding rate update failed: {exc}")

    def start(self) -> None:
        """Start the periodic collection daemon."""
        # Build configuration table
        config_table = Table(show_header=False, box=None, padding=(0, 2))
        config_table.add_column("Setting", style="dim")
        config_table.add_column("Value", style="cyan")

        config_table.add_row(
            "Collection interval", f"{self.config.collection_interval_minutes} minutes"
        )
        config_table.add_row("Output directory", str(self.config.output_dir))
        config_table.add_row("Timeframes", ", ".join(TIMEFRAMES))

        mode_parts = []
        if self.config.chainlink_only:
            mode_parts.append("[green]API Only (Chainlink markets)[/green]")
        else:
            mode_parts.append("[magenta]Hybrid (API + Oracle Events)[/magenta]")
        if self.config.dry_run:
            mode_parts.append("[yellow]DRY RUN[/yellow]")
        config_table.add_row("Mode", " | ".join(mode_parts))
        if self.config.collect_live_funding and self.config.live_funding_feather_dir:
            config_table.add_row("Live funding", str(self.config.live_funding_feather_dir))

        # Display startup panel
        console.print(
            Panel(
                config_table,
                title="[bold green]GMX Periodic Data Collector[/bold green]",
                border_style="green",
            )
        )

        # Schedule periodic collection
        schedule.every(self.config.collection_interval_minutes).minutes.do(
            self.run_collection_cycle
        )

        # Run first collection immediately
        console.rule("[bold]Initial Collection[/bold]")
        self.run_collection_cycle()

        # Main loop
        self.running = True

        # Calculate next collection time
        next_run = datetime.now(UTC) + timedelta(seconds=schedule.idle_seconds())

        status_table = Table(show_header=False, box=None, padding=(0, 2))
        status_table.add_column("Info", style="dim")
        status_table.add_column("Value", style="cyan")
        status_table.add_row("Next collection", next_run.strftime("%Y-%m-%d %H:%M:%S %Z"))
        status_table.add_row("Stop daemon", "Press Ctrl+C")

        console.print(
            Panel(
                status_table,
                title="[bold green]Daemon Running[/bold green]",
                border_style="green",
            )
        )

        while self.running and not self.shutdown_requested:
            try:
                # Run pending scheduled jobs
                schedule.run_pending()

                # Sleep for 1 second before checking again
                time.sleep(1)

            except KeyboardInterrupt:
                # Already handled by signal handler, but catch here for safety
                break

        # Graceful shutdown
        console.print(
            Panel(
                "[dim]Cleanup complete[/dim]",
                title="[yellow]Daemon Stopped[/yellow]",
                border_style="yellow",
            )
        )
        logger.info("Daemon stopped")


def main() -> None:
    """Main entry point for the periodic collector daemon."""
    try:
        from gmx_historical_data.resource_limiter import apply_nice

        apply_nice()

        # Load configuration from environment
        config = DaemonConfig.from_env()

        # Create and start collector
        collector = GMXPeriodicCollector(config)
        collector.start()

    except ValueError as e:
        console.print(f"[red]Configuration error: {e}[/red]")
        logger.error(f"Configuration error: {e}")
        sys.exit(1)

    except Exception as e:
        console.print(f"[red]Fatal error: {e}[/red]")
        logger.exception(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
