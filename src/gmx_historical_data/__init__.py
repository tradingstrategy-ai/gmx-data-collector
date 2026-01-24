"""GMX Historical Data Collection via Chainlink Oracles.

This package provides tools to collect historical price data for GMX tokens
by querying Chainlink oracle events on Arbitrum using HyperSync.
"""

from gmx_historical_data.config import CollectionConfig, TIMEFRAMES
from gmx_historical_data.chainlink_feeds import get_feed_address, get_all_symbols
from gmx_historical_data.hypersync_collector import HyperSyncCollector
from gmx_historical_data.storage import ParquetStorage
from gmx_historical_data.checkpoint import CheckpointManager
from gmx_historical_data.resampler import OHLCVResampler
from gmx_historical_data.gmx_api_integration import GMXDataFetcher, combine_gmx_and_chainlink_data
from gmx_historical_data.cli import DataCollector

__version__ = "0.1.0"

__all__ = [
    "CollectionConfig",
    "TIMEFRAMES",
    "get_feed_address",
    "get_all_symbols",
    "HyperSyncCollector",
    "ParquetStorage",
    "CheckpointManager",
    "OHLCVResampler",
    "GMXDataFetcher",
    "combine_gmx_and_chainlink_data",
    "DataCollector",
]
