"""Configuration for GMX historical data collection via HyperSync."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class CollectionConfig:
    """Configuration for historical data collection.

    :param hypersync_endpoint: HyperSync API endpoint URL
    :param hypersync_api_token: Optional API token for HyperSync (recommended for production)
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
        """Create all required directories if they don't exist."""
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.candles_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoints_dir.mkdir(parents=True, exist_ok=True)


# Timeframes for OHLCV resampling
# Note: Pandas 3.0+ uses lowercase for hour/day frequencies
TIMEFRAMES = ["1min", "5min", "15min", "1h", "4h", "1D"]

# AnswerUpdated event signature
ANSWER_UPDATED_TOPIC = (
    "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"
)

# Arbitrum One mainnet launch: August 31, 2021
# Chainlink Price Feeds went live: August 12, 2021
# First significant block with activity: ~100,000
# Start from a safe early block to capture all oracle history
ARBITRUM_CHAINLINK_START_BLOCK = 100_000  # ~August 2021
