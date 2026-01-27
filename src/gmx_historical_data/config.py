"""Configuration for GMX historical data collection via HyperSync."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class CollectionConfig:
    """Configuration for historical data collection.

    :param hypersync_endpoint: HyperSync API endpoint URL
                               (e.g., 'https://arbitrum.hypersync.xyz')
    :param hypersync_api_token: Optional API token for HyperSync
                               (recommended for production)
    :param output_dir: Base directory for storing collected data
    :param rpc_url: Arbitrum RPC URL for aggregator discovery
    :param chain_id: Chain ID (42161 for Arbitrum)
    :param start_block: Optional starting block number for collection
    :param end_block: Optional ending block number for collection
    """

    hypersync_endpoint: str = "https://arbitrum.hypersync.xyz"
    hypersync_api_token: str | None = None
    output_dir: Path = Path("./data")
    rpc_url: str = ""  # Must be set by user
    chain_id: int = 42161  # Arbitrum
    start_block: int | None = None  # None = from genesis
    end_block: int | None = None  # None = latest

    def __post_init__(self):
        """Ensure output_dir is a Path object."""
        if not isinstance(self.output_dir, Path):
            self.output_dir = Path(self.output_dir)

    @property
    def raw_data_dir(self) -> Path:
        """Directory for raw event data."""
        return self.output_dir / "raw" / "arbitrum"

    @property
    def candles_dir(self) -> Path:
        """Directory for resampled OHLCV candles."""
        return self.output_dir / "candles" / "arbitrum"

    @property
    def checkpoints_dir(self) -> Path:
        """Directory for checkpoint/resume state."""
        return self.output_dir / "checkpoints"

    def ensure_directories(self):
        """Create all required directories if they don't exist.

        Creates raw_data_dir, candles_dir, and checkpoints_dir with parent directories.
        """
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.candles_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)


# Timeframes for OHLCV resampling (pandas format)
# Uses "min" for minutes (pandas requirement), "h" for hours, "d" for days
TIMEFRAMES = ["1min", "5min", "15min", "1h", "4h", "1d"]

# Mapping from pandas timeframe to filename format
# File naming uses short format: 1m, 5m, 15m, 1h, 4h, 1d
TIMEFRAME_TO_FILENAME = {
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
}

# Reverse mapping for reading files
FILENAME_TO_TIMEFRAME = {v: k for k, v in TIMEFRAME_TO_FILENAME.items()}

# AnswerUpdated event signature
ANSWER_UPDATED_TOPIC = (
    "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
)

# GMX V2 EventEmitter contract address (Arbitrum)
EVENT_EMITTER_ADDRESS = "0xC8ee91A54287DB53897056e12D9819156D3822Fb"

# GMX V2 Launch Information (Arbitrum)
GMX_V2_GENESIS_BLOCK = 120_000_000  # Aug 2023 (approximate)
GMX_V2_GENESIS_TIMESTAMP = 1691366400  # Aug 7, 2023 00:00:00 UTC (approximate)

# Excluded symbols (deprecated or problematic tokens)
EXCLUDED_SYMBOLS = {
    "APE_DEPRECATED",  # Deprecated APE market
}
