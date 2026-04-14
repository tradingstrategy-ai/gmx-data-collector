"""Parquet storage for Chainlink event data and OHLCV candles.

Uses partitioned Parquet files with zstd compression for efficient storage
and fast querying.
"""

import logging
from pathlib import Path

import pandas as pd
import polars as pl
import pyarrow as pa

from gmx_historical_data.config import TIMEFRAME_TO_FILENAME
from gmx_historical_data.event_decoder import AnswerUpdatedEvent
from gmx_historical_data.gmx_event_parser import GMXPositionEvent

logger = logging.getLogger(__name__)


def _coverage_stats(df: "pl.DataFrame", ts_col: str) -> dict:
    """Return row count and timestamp coverage for a Polars DataFrame.

    :param df: Polars DataFrame to inspect.
    :param ts_col: Name of the timestamp column.
    :return: Dict with ``rows``, ``earliest``, and ``latest`` keys.
    """
    if df.is_empty():
        return {"rows": 0, "earliest": None, "latest": None}
    return {
        "rows": df.height,
        "earliest": df.select(pl.col(ts_col).min()).item(),
        "latest": df.select(pl.col(ts_col).max()).item(),
    }


def _assert_history_preserved(
    existing_stats: dict,
    incoming_stats: dict,
    merged_stats: dict,
    *,
    ts_label: str,
    location: str,
) -> None:
    """Raise ValueError if a merge would shorten stored history.

    :param existing_stats: Coverage stats from the on-disk DataFrame.
    :param incoming_stats: Coverage stats from the incoming DataFrame.
    :param merged_stats: Coverage stats from the merged result.
    :param ts_label: Human-readable label for the timestamp column (for error messages).
    :param location: Caller location string (for error messages).
    :raises ValueError: If the merge would lose the earliest or latest timestamp.
    """
    if existing_stats["rows"] == 0:
        return

    if merged_stats["earliest"] is None or merged_stats["earliest"] > existing_stats["earliest"]:
        raise ValueError(
            f"{location}: merge would shorten history for {ts_label}: "
            f"existing earliest={existing_stats['earliest']}, merged earliest={merged_stats['earliest']}"
        )

    expected_latest = max(
        ts for ts in (existing_stats["latest"], incoming_stats["latest"]) if ts is not None
    )
    if merged_stats["latest"] is None or merged_stats["latest"] < expected_latest:
        raise ValueError(
            f"{location}: merge would lose tail coverage for {ts_label}: "
            f"expected latest={expected_latest}, merged latest={merged_stats['latest']}"
        )


# Raw events schema
RAW_EVENTS_SCHEMA = pa.schema(
    [
        ("block_number", pa.uint64()),
        ("block_timestamp", pa.uint64()),
        ("transaction_hash", pa.string()),
        ("log_index", pa.uint32()),
        ("round_id", pa.string()),  # String to handle L2 Chainlink round IDs > 2^64
        ("price", pa.int64()),  # Raw price (divide by 10^decimals)
        ("timestamp", pa.uint64()),  # Event timestamp
        ("symbol", pa.string()),
        ("aggregator_address", pa.string()),
    ]
)

# OHLCV schema
OHLCV_SCHEMA = pa.schema(
    [
        ("timestamp", pa.timestamp("s", tz="UTC")),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("symbol", pa.string()),
    ]
)

# Position events schema
POSITION_EVENTS_SCHEMA = pa.schema(
    [
        ("block_number", pa.uint64()),
        ("block_timestamp", pa.uint64()),
        ("transaction_hash", pa.string()),
        ("log_index", pa.uint32()),
        ("event_name", pa.string()),
        ("market", pa.string()),
        ("account", pa.string()),
        ("is_long", pa.bool_()),
        (
            "index_token_price_min",
            pa.string(),
        ),  # 30 decimals, oracle min price from Chainlink
        (
            "index_token_price_max",
            pa.string(),
        ),  # 30 decimals, oracle max price from Chainlink
        (
            "execution_price",
            pa.string(),
        ),  # 30 decimals, execution price (includes price impact)
        ("size_delta_usd", pa.string()),  # 30 decimals, stored as string
        ("size_delta_in_tokens", pa.string()),  # Stored as string
        (
            "price_impact_usd",
            pa.string(),
        ),  # 30 decimals, stored as string (can be negative)
        ("position_key", pa.string()),
        ("collateral_token", pa.string()),
        ("symbol", pa.string()),
    ]
)


class ParquetStorage:
    """Manage Parquet storage for event data and candles.

    :param base_dir: Base directory for all data storage
    """

    def __init__(self, base_dir: Path):
        """Initialize Parquet storage manager.

        :param base_dir: Base directory for data storage
        """
        self.base_dir = Path(base_dir)
        self.raw_dir = self.base_dir / "raw" / "arbitrum"
        self.candles_dir = self.base_dir / "candles" / "arbitrum"

    @property
    def events_dir(self) -> Path:
        """Directory for raw position events.

        :return: Path to events directory
        """
        return self.base_dir / "events" / "arbitrum"

    def _ensure_dir(self, path: Path) -> Path:
        """Ensure directory exists.

        :param path: Directory path
        :return: Path object
        """
        path.mkdir(parents=True, exist_ok=True)
        return path

    def save_raw_events(
        self,
        events: list[AnswerUpdatedEvent],
        symbol: str,
        partition_id: int = 0,
    ) -> Path:
        """Save raw events to partitioned Parquet file.

        :param events: List of AnswerUpdated events
        :param symbol: Token symbol (e.g., 'ETH')
        :param partition_id: Partition ID for file organization
        :return: Path to saved Parquet file
        """
        if not events:
            raise ValueError("Cannot save empty events list")

        # Create symbol directory
        symbol_dir = self._ensure_dir(self.raw_dir / symbol)
        partition_dir = self._ensure_dir(symbol_dir / f"partition={partition_id}")

        # Convert events to DataFrame
        data = {
            "block_number": [e.block_number for e in events],
            "block_timestamp": [e.block_timestamp for e in events],
            "transaction_hash": [e.transaction_hash for e in events],
            "log_index": [e.log_index for e in events],
            "round_id": [str(e.round_id) for e in events],  # String for L2 round IDs > 2^64
            "price": [e.price for e in events],
            "timestamp": [e.timestamp for e in events],
            "symbol": [symbol] * len(events),
            "aggregator_address": [e.aggregator_address for e in events],
        }

        df = pd.DataFrame(data)

        # Convert to Arrow table with schema
        table = pa.Table.from_pandas(df, schema=RAW_EVENTS_SCHEMA)

        # Write to Parquet with compression
        output_path = partition_dir / "data.parquet"
        pl.from_arrow(table).write_parquet(
            str(output_path), compression="zstd", compression_level=3
        )

        return output_path

    def read_raw_events(
        self,
        symbol: str,
        partition_id: int | None = None,
    ) -> pd.DataFrame:
        """Read raw events from Parquet file(s).

        :param symbol: Token symbol (e.g., 'ETH')
        :param partition_id: Optional partition ID (None = all partitions)
        :return: DataFrame with raw events
        """
        symbol_dir = self.raw_dir / symbol

        if not symbol_dir.exists():
            return pd.DataFrame()

        if partition_id is not None:
            # Read single partition
            partition_file = symbol_dir / f"partition={partition_id}" / "data.parquet"
            if not partition_file.exists():
                return pd.DataFrame()
            return pd.read_parquet(partition_file)
        else:
            # Read all partitions using Polars streaming scan for memory efficiency.
            # This avoids loading N separate DataFrames into memory simultaneously.
            parquet_files = list(symbol_dir.glob("partition=*/data.parquet"))
            if not parquet_files:
                return pd.DataFrame()

            combined = pl.concat([pl.scan_parquet(f) for f in parquet_files]).collect()
            return combined.to_pandas()

    def save_candles(
        self,
        df: pd.DataFrame,
        timeframe: str,
        symbol: str,
        overwrite: bool = False,
    ) -> Path:
        """Save OHLCV candles to Parquet file.

        By default existing rows are preserved (merge-by-default). Overlapping
        timestamps resolve to the newer value (``keep='last'`` after
        ``[existing, new]`` concat). Pass ``overwrite=True`` to replace the file
        entirely — use only when you intentionally want to discard history.

        :param df: DataFrame with OHLCV data.
        :param timeframe: Timeframe string (e.g., ``'1h'``).
        :param symbol: Token symbol (e.g., ``'ETH'``).
        :param overwrite: If ``True``, replace existing file instead of merging.
            Defaults to ``False`` (merge-by-default).
        :return: Path to saved Parquet file.
        """
        if df.empty:
            raise ValueError("Cannot save empty DataFrame")

        required_columns = ["timestamp", "open", "high", "low", "close", "symbol"]
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        if pd.api.types.is_datetime64_any_dtype(df["timestamp"]) and df["timestamp"].dt.tz is None:
            raise ValueError(
                f"save_candles: 'timestamp' column for {symbol}/{timeframe} is timezone-naive. "
                "Localize to UTC before calling: df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')"
            )

        symbol_dir = self._ensure_dir(self.candles_dir / symbol)
        filename = TIMEFRAME_TO_FILENAME.get(timeframe, timeframe)
        output_path = symbol_dir / f"{filename}.parquet"

        # Normalise incoming to microsecond UTC to match on-disk dtype.
        incoming = pl.from_pandas(df).with_columns(
            pl.col("timestamp").cast(pl.Datetime("us", "UTC"))
        )

        if not overwrite and output_path.exists():
            existing = pl.read_parquet(output_path).with_columns(
                pl.col("timestamp").cast(pl.Datetime("us", "UTC"))
            )
            existing_stats = _coverage_stats(existing, "timestamp")
            incoming_stats = _coverage_stats(incoming, "timestamp")
            merged = (
                pl.concat([existing, incoming])
                .unique(subset=["timestamp"], keep="last", maintain_order=True)
                .sort("timestamp")
            )
            merged_stats = _coverage_stats(merged, "timestamp")
            _assert_history_preserved(
                existing_stats,
                incoming_stats,
                merged_stats,
                ts_label="timestamp",
                location=f"save_candles({symbol}/{timeframe})",
            )
            incoming = merged

        table = pa.Table.from_pandas(incoming.to_pandas(), schema=OHLCV_SCHEMA)
        pl.from_arrow(table).write_parquet(str(output_path), compression="zstd", compression_level=3)

        return output_path

    def read_candles(
        self,
        timeframe: str,
        symbol: str,
    ) -> pd.DataFrame:
        """Read OHLCV candles from Parquet file.

        :param timeframe: Timeframe string (e.g., '1min', '1h', '1d')
        :param symbol: Token symbol (e.g., 'ETH')
        :return: DataFrame with OHLCV data
        """
        # Map timeframe to filename format (e.g., '1min' -> '1m')
        filename = TIMEFRAME_TO_FILENAME.get(timeframe, timeframe)
        candles_file = self.candles_dir / symbol / f"{filename}.parquet"

        if not candles_file.exists():
            return pd.DataFrame()

        return pd.read_parquet(candles_file)

    def list_symbols(self) -> list[str]:
        """List all symbols with candle data.

        :return: Sorted list of symbol names that have candle data
        """
        if not self.candles_dir.exists():
            return []

        symbols = []
        for symbol_dir in self.candles_dir.iterdir():
            if symbol_dir.is_dir() and not symbol_dir.name.startswith("."):
                # Skip hidden / macOS AppleDouble (._*) sidecars to avoid
                # treating filesystem metadata as parquet data.
                parquet_files = [
                    p for p in symbol_dir.glob("*.parquet") if not p.name.startswith(".")
                ]
                if parquet_files:
                    symbols.append(symbol_dir.name)

        return sorted(symbols)

    def list_timeframes(self, symbol: str) -> list[str]:
        """List available timeframes for a symbol.

        :param symbol: Token symbol (e.g., 'ETH')
        :return: Sorted list of timeframe strings (e.g., ['1d', '1h', '4h'])
        """
        symbol_dir = self.candles_dir / symbol
        if not symbol_dir.exists():
            return []

        timeframes = []
        for parquet_file in symbol_dir.glob("*.parquet"):
            # Skip macOS AppleDouble sidecars (._1h.parquet) and other dotfiles.
            if parquet_file.name.startswith("."):
                continue
            tf = parquet_file.stem
            timeframes.append(tf)

        return sorted(timeframes)

    def append_raw_events(
        self,
        events: list[AnswerUpdatedEvent],
        symbol: str,
    ) -> Path:
        """Append events to existing raw data.

        Finds the highest partition ID and creates a new partition.

        :param events: List of AnswerUpdated events
        :param symbol: Token symbol
        :return: Path to saved Parquet file
        """
        symbol_dir = self.raw_dir / symbol

        # Find highest existing partition
        max_partition = -1
        if symbol_dir.exists():
            partitions = list(symbol_dir.glob("partition=*"))
            for p in partitions:
                try:
                    partition_id = int(p.name.split("=")[1])
                    max_partition = max(max_partition, partition_id)
                except (ValueError, IndexError):
                    continue

        # Create new partition
        new_partition = max_partition + 1
        return self.save_raw_events(events, symbol, new_partition)

    def save_position_events(
        self,
        events: list[GMXPositionEvent],
        symbol: str,
        partition_id: int = 0,
    ) -> Path:
        """Save position events to partitioned Parquet file.

        :param events: List of position events
        :param symbol: Token symbol
        :param partition_id: Partition ID for file organization
        :return: Path to saved Parquet file
        """
        if not events:
            raise ValueError("Cannot save empty events list")

        # Create symbol directory
        symbol_dir = self._ensure_dir(self.events_dir / symbol)
        partition_dir = self._ensure_dir(symbol_dir / f"partition={partition_id}")

        # Convert events to DataFrame
        # GMX uses 30-decimal precision (10^30) which exceeds int64 max (9.2 × 10^18).
        # Store as strings to preserve exact precision without data loss.
        data = {
            "block_number": [e.block_number for e in events],
            "block_timestamp": [e.block_timestamp for e in events],
            "transaction_hash": [e.transaction_hash for e in events],
            "log_index": [e.log_index for e in events],
            "event_name": [e.event_name for e in events],
            "market": [e.market for e in events],
            "account": [e.account for e in events],
            "is_long": [e.is_long for e in events],
            "index_token_price_min": [str(e.index_token_price_min) for e in events],
            "index_token_price_max": [str(e.index_token_price_max) for e in events],
            "execution_price": [str(e.execution_price) for e in events],
            "size_delta_usd": [str(e.size_delta_usd) for e in events],
            "size_delta_in_tokens": [str(e.size_delta_in_tokens) for e in events],
            "price_impact_usd": [str(e.price_impact_usd) for e in events],
            "position_key": [e.position_key for e in events],
            "collateral_token": [e.collateral_token for e in events],
            # Symbol is added at storage time (not from the event dataclass)
            "symbol": [symbol] * len(events),
        }

        df = pd.DataFrame(data)

        # Convert to Arrow table with schema
        table = pa.Table.from_pandas(df, schema=POSITION_EVENTS_SCHEMA)

        # Write to Parquet with compression
        output_path = partition_dir / "data.parquet"
        pl.from_arrow(table).write_parquet(
            str(output_path), compression="zstd", compression_level=3
        )

        return output_path
