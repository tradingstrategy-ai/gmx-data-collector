"""Periodic GMX data collection daemon.

Collects fresh GMX API data for all tokens at configurable intervals,
producing OHLCV candles suitable for backtesting.
"""

import signal
import sys
import logging
import time
import pandas as pd
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import schedule
from rich.console import Console

from gmx_historical_data.config import TIMEFRAMES, EXCLUDED_SYMBOLS
from gmx_historical_data.storage import ParquetStorage
from gmx_historical_data.gmx_api_integration import GMXDataFetcher
from gmx_historical_data.gmx_token_discovery import GMXTokenDiscovery
from gmx_historical_data.daemon.config import DaemonConfig
from gmx_historical_data.daemon.gap_detector import GapDetector
from gmx_historical_data.daemon.health_monitor import HealthMonitor


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

        # Configure logging
        self._setup_logging()

        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)

    def _setup_logging(self) -> None:
        """Configure logging based on config."""
        logging.basicConfig(
            level=getattr(logging, self.config.log_level),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    def _handle_shutdown_signal(self, signum, frame) -> None:
        """Handle shutdown signals (SIGTERM, SIGINT).

        :param signum: Signal number
        :param frame: Current stack frame
        """
        signal_name = "SIGTERM" if signum == signal.SIGTERM else "SIGINT"
        console.print(f"\n[yellow]Received {signal_name} - initiating graceful shutdown...[/yellow]")
        logger.info(f"Shutdown signal received: {signal_name}")
        self.shutdown_requested = True

    def _discover_symbols(self) -> list[str]:
        """Discover all GMX tokens to collect.

        :return: List of token symbols (filtered by config and exclusions)
        """
        # Use configured symbols if specified
        if self.config.collection_symbols:
            symbols = self.config.collection_symbols
            console.print(f"[cyan]Using configured symbols: {len(symbols)} tokens[/cyan]")
        else:
            # Auto-discover all GMX tokens
            all_symbols = self.gmx_discovery.get_supported_symbols()

            # Filter out excluded symbols
            symbols = [s for s in all_symbols if s not in EXCLUDED_SYMBOLS]
            symbols = [s for s in symbols if s not in self.config.excluded_symbols]

            excluded_count = len(all_symbols) - len(symbols)
            console.print(f"[cyan]Auto-discovered {len(symbols)} GMX tokens[/cyan]")
            if excluded_count > 0:
                console.print(f"[dim]  (excluded {excluded_count} deprecated tokens)[/dim]")

        return symbols

    def _collect_symbol_timeframe(
        self,
        symbol: str,
        timeframe: str,
    ) -> tuple[str, str, int, Exception | None]:
        """Collect data for a single symbol/timeframe combination.

        :param symbol: Token symbol
        :param timeframe: Timeframe string
        :return: Tuple of (symbol, timeframe, candles_added, error)
        """
        try:
            # Detect gap
            fetch_start, fetch_end = self.gap_detector.detect_gap(symbol, timeframe)

            # No gap - skip collection
            if fetch_start is None and fetch_end is None:
                return symbol, timeframe, 0, None

            # Fetch GMX API data
            gmx_df = self.gmx_fetcher.fetch_gmx_candles(
                symbol=symbol,
                period=timeframe,
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
                executor.submit(self._collect_symbol_timeframe, symbol, tf): tf
                for tf in TIMEFRAMES
            }

            for future in as_completed(futures):
                timeframe = futures[future]
                try:
                    _, _, candles_added, error = future.result()

                    if error:
                        errors.append((timeframe, str(error)))
                        self.health_monitor.record_symbol_failure(
                            symbol, str(error), timeframe
                        )
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
                console.print(f"  [yellow]{symbol}[/yellow]: {total_candles:,} candles, {len(errors)} errors")
            else:
                console.print(f"  [green]{symbol}[/green]: {total_candles:,} candles")

        return candles_by_timeframe

    def run_collection_cycle(self) -> None:
        """Execute one complete collection cycle for all symbols."""
        console.print("\n[bold cyan]Starting collection cycle[/bold cyan]")
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

        # Process each symbol
        for i, symbol in enumerate(symbols, 1):
            # Check for shutdown request
            if self.shutdown_requested:
                console.print("[yellow]Shutdown requested - stopping collection cycle[/yellow]")
                break

            console.print(f"\n[cyan]({i}/{len(symbols)})[/cyan] Collecting {symbol}...")

            try:
                candles_by_timeframe = self._collect_symbol(symbol)

                # Record success if any candles were added
                if sum(candles_by_timeframe.values()) > 0:
                    self.health_monitor.record_symbol_success(symbol, candles_by_timeframe)
                else:
                    # No candles added (up to date)
                    self.health_monitor.record_symbol_success(symbol, {})

            except Exception as e:
                logger.exception(f"Failed to collect {symbol}: {e}")
                console.print(f"  [red]Error: {e}[/red]")
                self.health_monitor.record_symbol_failure(symbol, str(e))

        # End cycle tracking
        self.health_monitor.end_cycle(total_symbols=len(symbols))

        # Print cycle summary
        metrics = self.health_monitor.get_metrics_summary()
        console.print(f"\n[bold]Cycle complete:[/bold]")
        console.print(f"  Succeeded: [green]{metrics['current_cycle']['succeeded']}[/green]")
        console.print(f"  Failed: [red]{metrics['current_cycle']['failed']}[/red]")
        console.print(f"  Total: {metrics['current_cycle']['total_attempted']}")

        if not self.config.dry_run:
            console.print(f"  Data saved to: {self.config.output_dir}")
        else:
            console.print(f"  [yellow](DRY RUN - no data saved)[/yellow]")

    def start(self) -> None:
        """Start the periodic collection daemon."""
        console.print("\n[bold green]GMX Periodic Data Collector[/bold green]")
        console.print(f"Collection interval: [cyan]{self.config.collection_interval_minutes}[/cyan] minutes")
        console.print(f"Output directory: [cyan]{self.config.output_dir}[/cyan]")
        console.print(f"Timeframes: [cyan]{', '.join(TIMEFRAMES)}[/cyan]")
        if self.config.dry_run:
            console.print("[yellow]DRY RUN MODE - data will not be saved[/yellow]")

        # Schedule periodic collection
        schedule.every(self.config.collection_interval_minutes).minutes.do(
            self.run_collection_cycle
        )

        # Run first collection immediately
        console.print("\n[bold]Running initial collection...[/bold]")
        self.run_collection_cycle()

        # Main loop
        self.running = True
        console.print(f"\n[bold green]Daemon started successfully[/bold green]")

        # Calculate next collection time
        from datetime import timedelta
        next_run = datetime.now(timezone.utc) + timedelta(seconds=schedule.idle_seconds())
        console.print(f"Next collection at: [cyan]{next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}[/cyan]")
        console.print("\nPress Ctrl+C to stop\n")

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
        console.print("\n[yellow]Shutting down gracefully...[/yellow]")
        logger.info("Daemon stopped")
        console.print("[green]Daemon stopped[/green]\n")


def main() -> None:
    """Main entry point for the periodic collector daemon."""
    try:
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
