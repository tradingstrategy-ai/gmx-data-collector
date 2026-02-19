"""GMX Historical Data Collection."""

from gmx_historical_data.aggregator_discovery import AggregatorDiscovery
from gmx_historical_data.chainlink_feeds_complete import (
    CHAINLINK_FEEDS_ARBITRUM,
    find_chainlink_symbol,
    get_all_symbols,
    get_feed_address,
    get_feed_address_for_gmx_symbol,
)
from gmx_historical_data.config import TIMEFRAMES, CollectionConfig
from gmx_historical_data.gap_analyzer import DataGapAnalyzer
from gmx_historical_data.gmx_token_discovery import GMXToken, GMXTokenDiscovery

try:
    from gmx_historical_data.hypersync_collector import (
        CollectionStats,
        HyperSyncCollector,
    )
except ImportError:
    # hypersync is optional - allow daemon to work without it
    HyperSyncCollector = None
    CollectionStats = None
from gmx_historical_data.checkpoint import CheckpointManager
from gmx_historical_data.gmx_api_integration import (
    GMXDataFetcher,
    combine_gmx_and_chainlink_data,
    map_timeframe_to_gmx_period,
)
from gmx_historical_data.resampler import OHLCVResampler
from gmx_historical_data.storage import ParquetStorage

__version__ = "0.1.0"

__all__ = [
    # Config
    "CollectionConfig",
    "TIMEFRAMES",
    # Chainlink
    "get_feed_address",
    "get_all_symbols",
    "find_chainlink_symbol",
    "get_feed_address_for_gmx_symbol",
    "CHAINLINK_FEEDS_ARBITRUM",
    "AggregatorDiscovery",
    # GMX
    "GMXTokenDiscovery",
    "GMXToken",
    "GMXDataFetcher",
    # Analysis
    "DataGapAnalyzer",
    # Collection
    "HyperSyncCollector",
    "CollectionStats",
    # Storage
    "ParquetStorage",
    "CheckpointManager",
    "OHLCVResampler",
    # Integration
    "combine_gmx_and_chainlink_data",
    "map_timeframe_to_gmx_period",
]
