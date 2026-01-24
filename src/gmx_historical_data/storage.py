"""Parquet storage for Chainlink event data and OHLCV candles.

Uses partitioned Parquet files with zstd compression for efficient storage
and fast querying.
"""

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd

from gmx_historical_data.event_decoder import AnswerUpdatedEvent


# Raw events schema
RAW_EVENTS_SCHEMA = pa.schema(
    [
        ("block_number", pa.uint64()),
        ("block_timestamp", pa.uint64()),
        ("transaction_hash", pa.string()),
        ("log_index", pa.uint32()),
        ("round_id", pa.uint64()),
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
            "round_id": [e.round_id for e in events],
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
        pq.write_table(
            table,
            output_path,
            compression="zstd",
            compression_level=22,
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
            # Read all partitions
            parquet_files = list(symbol_dir.glob("partition=*/data.parquet"))
            if not parquet_files:
                return pd.DataFrame()

            # Read and concatenate all partitions
            dfs = [pd.read_parquet(f) for f in parquet_files]
            return pd.concat(dfs, ignore_index=True)

    def save_candles(
        self,
        df: pd.DataFrame,
        timeframe: str,
        symbol: str,
    ) -> Path:
        """Save OHLCV candles to Parquet file.

        :param df: DataFrame with OHLCV data
        :param timeframe: Timeframe string (e.g., '1min', '1h', '1D')
        :param symbol: Token symbol (e.g., 'ETH')
        :return: Path to saved Parquet file
        """
        if df.empty:
            raise ValueError("Cannot save empty DataFrame")

        # Validate required columns
        required_columns = ["timestamp", "open", "high", "low", "close", "symbol"]
        missing = set(required_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        # Create symbol directory
        symbol_dir = self._ensure_dir(self.candles_dir / symbol)

        # Convert to Arrow table with schema
        table = pa.Table.from_pandas(df, schema=OHLCV_SCHEMA)

        # Write to Parquet with compression
        output_path = symbol_dir / f"{timeframe}.parquet"
        pq.write_table(
            table,
            output_path,
            compression="zstd",
            compression_level=22,
        )

        return output_path

    def read_candles(
        self,
        timeframe: str,
        symbol: str,
    ) -> pd.DataFrame:
        """Read OHLCV candles from Parquet file.

        :param timeframe: Timeframe string (e.g., '1min', '1h', '1D')
        :param symbol: Token symbol (e.g., 'ETH')
        :return: DataFrame with OHLCV data
        """
        candles_file = self.candles_dir / symbol / f"{timeframe}.parquet"

        if not candles_file.exists():
            return pd.DataFrame()

        return pd.read_parquet(candles_file)

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

    def get_latest_block(self, symbol: str) -> int | None:
        """Get the latest block number for a symbol.

        :param symbol: Token symbol
        :return: Latest block number or None if no data
        """
        df = self.read_raw_events(symbol)
        if df.empty:
            return None
        return int(df["block_number"].max())
