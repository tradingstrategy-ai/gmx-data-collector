"""Data coverage analyzer for incremental collection.

This module analyzes existing parquet files to determine what oracle events
are missing per symbol, enabling efficient incremental data collection by
only fetching gaps instead of refetching everything.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from gmx_historical_data.block_timestamp_cache import BlockTimestampCache

logger = logging.getLogger(__name__)


@dataclass
class TimeframeCoverage:
    """Coverage information for a single timeframe.

    :param timeframe: Timeframe string (e.g., '1h', '4h', '1d')
    :param earliest: Earliest timestamp in Unix seconds
    :param latest: Latest timestamp in Unix seconds
    :param candle_count: Number of candles in this timeframe
    """

    timeframe: str
    earliest: int
    latest: int
    candle_count: int


@dataclass
class SymbolCoverage:
    """Coverage information for a symbol across all timeframes.

    :param symbol: Token symbol (e.g., 'ETH', 'BTC')
    :param has_data: Whether any data exists for this symbol
    :param earliest_timestamp: Earliest timestamp across all timeframes (Unix seconds)
    :param latest_timestamp: Latest timestamp across all timeframes (Unix seconds)
    :param timeframe_coverage: Coverage details per timeframe
    """

    symbol: str
    has_data: bool = False
    earliest_timestamp: int | None = None
    latest_timestamp: int | None = None
    timeframe_coverage: dict[str, TimeframeCoverage] = field(default_factory=dict)


class DataCoverageAnalyzer:
    """Analyze existing data coverage to determine missing block ranges.

    Reads parquet files for each timeframe and identifies gaps in historical
    data coverage. This enables incremental collection by only fetching
    missing data instead of refetching everything.

    :param data_dir: Base data directory containing candles subdirectory
    """

    def __init__(self, data_dir: Path):
        """Initialize the data coverage analyzer.

        :param data_dir: Base data directory path
        """
        self.data_dir = Path(data_dir)
        self.candles_dir = self.data_dir / "candles" / "arbitrum"
        logger.info(f"Initialized DataCoverageAnalyzer: candles_dir={self.candles_dir}")

    def analyze_symbol_coverage(self, symbol: str) -> SymbolCoverage:
        """Analyze data coverage for a symbol across all timeframes.

        Reads all available timeframe parquet files and determines the
        earliest and latest timestamps. This identifies the largest gap
        (from genesis to earliest data).

        :param symbol: Token symbol to analyze (e.g., 'ETH')
        :return: SymbolCoverage with details per timeframe
        """
        logger.info(f"Analyzing coverage for symbol: {symbol}")
        coverage = SymbolCoverage(symbol=symbol)

        symbol_dir = self.candles_dir / symbol
        if not symbol_dir.exists():
            logger.info(f"No data directory found for {symbol}")
            return coverage

        # Read all timeframe files
        timeframe_files = list(symbol_dir.glob("*.parquet"))
        if not timeframe_files:
            logger.info(f"No parquet files found for {symbol}")
            return coverage

        overall_earliest = None
        overall_latest = None

        for parquet_file in timeframe_files:
            # Extract timeframe from filename (e.g., '1h.parquet' -> '1h')
            timeframe_filename = parquet_file.stem

            logger.debug(f"Reading coverage from {parquet_file}")
            try:
                df = pd.read_parquet(parquet_file)
                if df.empty:
                    logger.warning(f"Empty parquet file: {parquet_file}")
                    continue

                # Convert timestamps to Unix seconds (int)
                # Timestamps in parquet are already datetime64, convert to unix timestamp
                timestamps = pd.to_datetime(df["timestamp"])
                earliest_ts = int(timestamps.min().timestamp())
                latest_ts = int(timestamps.max().timestamp())
                candle_count = len(df)

                # Store timeframe coverage
                coverage.timeframe_coverage[timeframe_filename] = TimeframeCoverage(
                    timeframe=timeframe_filename,
                    earliest=earliest_ts,
                    latest=latest_ts,
                    candle_count=candle_count,
                )

                # Track overall earliest/latest
                if overall_earliest is None or earliest_ts < overall_earliest:
                    overall_earliest = earliest_ts
                if overall_latest is None or latest_ts > overall_latest:
                    overall_latest = latest_ts

                logger.debug(
                    f"{symbol} {timeframe_filename}: "
                    f"{candle_count} candles, "
                    f"earliest={earliest_ts}, latest={latest_ts}"
                )

            except Exception as e:
                logger.error(f"Failed to read {parquet_file}: {e}")
                continue

        # Set overall coverage
        if coverage.timeframe_coverage:
            coverage.has_data = True
            coverage.earliest_timestamp = overall_earliest
            coverage.latest_timestamp = overall_latest
            logger.info(
                f"{symbol}: {len(coverage.timeframe_coverage)} timeframes, "
                f"earliest={overall_earliest}, latest={overall_latest}"
            )
        else:
            logger.info(f"No valid data found for {symbol}")

        return coverage

    def get_missing_block_range(
        self,
        coverage: SymbolCoverage,
        cache: BlockTimestampCache,
        genesis_block: int,
        safety_margin: int = 1000,
    ) -> tuple[int | None, int | None]:
        """Calculate missing block range for incremental collection.

        Determines which blocks need to be fetched based on existing data
        coverage. Returns the range from genesis to the earliest existing data.

        :param coverage: Symbol coverage information
        :param cache: Block-timestamp cache for conversions
        :param genesis_block: Genesis block number (start of oracle data)
        :param safety_margin: Safety margin in blocks (default: 1000)
        :return: Tuple of (start_block, end_block) or (None, None) if no gap
        """
        # No data: fetch from genesis to latest
        if not coverage.has_data:
            logger.info(f"{coverage.symbol}: No existing data, fetch from genesis {genesis_block}")
            return (genesis_block, None)

        # Convert genesis block to timestamp for comparison
        genesis_ts = cache.get_timestamp_for_block(genesis_block)
        earliest_ts = coverage.earliest_timestamp

        # Data exists at or before genesis: no gap to fill
        if earliest_ts <= genesis_ts:
            logger.info(
                f"{coverage.symbol}: Data exists before genesis "
                f"(earliest_ts={earliest_ts} <= genesis_ts={genesis_ts}). No gap."
            )
            return (None, None)

        # Gap exists: fetch from genesis to earliest_block + safety_margin
        earliest_block = cache.get_block_for_timestamp(earliest_ts)
        end_block = earliest_block + safety_margin
        logger.info(
            f"{coverage.symbol}: Gap found. "
            f"Fetch blocks {genesis_block} to {end_block} "
            f"(earliest_data={earliest_block}, margin={safety_margin})"
        )
        return (genesis_block, end_block)
