"""Configuration for GMX historical data collection via HyperSync."""

from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class FetchMode(Enum):
    """Collection mode for data fetching.

    :cvar FULL: Collect all available historical data from genesis
    :cvar INCREMENTAL: Only fetch new data since last collection
    :cvar NO_FETCH: Data is current, no fetch needed
    """

    FULL = "full"
    INCREMENTAL = "incremental"
    NO_FETCH = "no_fetch"


@dataclass
class CollectionConfig:
    """Configuration for historical data collection.

    :param hypersync_endpoint: HyperSync API endpoint URL
                               (e.g., 'https://arbitrum.hypersync.xyz')
    :param hypersync_api_token: Optional API token for HyperSync
                               (recommended for production)
    :param output_dir: Base directory for storing collected data
    :param rpc_url: Primary Arbitrum RPC URL (comma-separated for multiple providers)
    :param fallback_rpc_urls: List of fallback RPC URLs (optional)
    :param chain_id: Chain ID (42161 for Arbitrum)
    :param start_block: Optional starting block number for collection
    :param end_block: Optional ending block number for collection
    """

    hypersync_endpoint: str = "https://arbitrum.hypersync.xyz"
    hypersync_api_token: str | None = None
    output_dir: Path = Path("./data")
    rpc_url: str = ""  # Primary RPC or comma-separated list
    fallback_rpc_urls: list[str] = None  # Explicit fallback list
    chain_id: int = 42161  # Arbitrum
    start_block: int | None = None  # None = from genesis
    end_block: int | None = None  # None = latest

    def __post_init__(self):
        """Ensure output_dir is a Path object and parse RPC URLs."""
        if not isinstance(self.output_dir, Path):
            self.output_dir = Path(self.output_dir)

        # Initialize fallback_rpc_urls if None
        if self.fallback_rpc_urls is None:
            self.fallback_rpc_urls = []

    def get_all_rpc_urls(self) -> list[str]:
        """Get all RPC URLs (primary + fallbacks).

        Supports comma-separated RPC URLs in rpc_url field for backward compatibility.

        :return: List of all RPC URLs (primary first, then fallbacks)
        """
        urls = []

        # Parse primary rpc_url (may be comma-separated)
        if self.rpc_url:
            primary_urls = [url.strip() for url in self.rpc_url.split(",") if url.strip()]
            urls.extend(primary_urls)

        # Add explicit fallback URLs
        urls.extend(self.fallback_rpc_urls)

        # Remove duplicates while preserving order
        seen = set()
        unique_urls = []
        for url in urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)

        return unique_urls

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
ANSWER_UPDATED_TOPIC = "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"

# GMX V2 EventEmitter contract address (Arbitrum)
EVENT_EMITTER_ADDRESS = "0xC8ee91A54287DB53897056e12D9819156D3822Fb"

# GMX V2 Launch Information (Arbitrum)
GMX_V2_GENESIS_BLOCK = 120_000_000  # Aug 2023 (approximate)
GMX_V2_GENESIS_TIMESTAMP = 1691366400  # Aug 7, 2023 00:00:00 UTC (approximate)

# Excluded symbols (deprecated or problematic tokens)
# Note: All symbols should be UPPERCASE for case-insensitive matching
EXCLUDED_SYMBOLS = {
    "APE_DEPRECATED",  # Deprecated APE market (may appear as "APE_deprecated" in GMX metadata)
}

# Symbol prefixes to exclude (covers all current and future variants)
# GLV vaults (e.g. "GLV [ETH-USDC]", "GLV [WBTC.b-USDC]") are liquidity vault
# tokens, not tradeable perpetual markets — they have no price feed suitable for
# OHLCV candle generation.
EXCLUDED_SYMBOL_PREFIXES = (
    "GLV",
)


def is_excluded_symbol(symbol: str) -> bool:
    """Check if a symbol is in the excluded list (case-insensitive).

    Returns ``True`` for exact matches against :data:`EXCLUDED_SYMBOLS` and
    for any symbol whose uppercase form starts with a prefix in
    :data:`EXCLUDED_SYMBOL_PREFIXES`.

    :param symbol: Token symbol to check.
    :return: ``True`` if the symbol should be excluded from collection.
    """
    upper = symbol.upper()
    if upper in EXCLUDED_SYMBOLS:
        return True
    return any(upper.startswith(prefix) for prefix in EXCLUDED_SYMBOL_PREFIXES)


# Block-timestamp cache configuration
BLOCK_SAMPLE_INTERVAL = 1000  # Sample every 1000 blocks (~4 minutes on Arbitrum)
CACHE_STALE_THRESHOLD = 10000  # Rebuild if cache is 10k blocks behind (~11 hours)
ARBITRUM_AVG_BLOCK_TIME = 0.25  # seconds per block (used for estimation)
