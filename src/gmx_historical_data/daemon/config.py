"""Configuration for GMX periodic data collection daemon."""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DaemonConfig:
    """Configuration for periodic data collection daemon.

    :param collection_interval_minutes: How often to collect data (in minutes)
    :param output_dir: Directory for storing collected data
    :param rpc_url: Arbitrum RPC URL for GMX API
    :param collection_symbols: Optional list of symbols to collect (None = all)
    :param excluded_symbols: Set of symbols to exclude from collection
    :param timeframe_concurrency: Number of timeframes to fetch concurrently (1-6)
    :param health_check_port: Port for health check HTTP endpoint
    :param log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    :param dry_run: If True, don't save data (testing only)
    """

    collection_interval_minutes: int = 60
    output_dir: Path = Path("./data")
    rpc_url: str = ""
    collection_symbols: list[str] | None = None
    excluded_symbols: set[str] = None
    timeframe_concurrency: int = 6
    health_check_port: int = 8080
    log_level: str = "INFO"
    dry_run: bool = False

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Ensure output_dir is a Path object
        if not isinstance(self.output_dir, Path):
            self.output_dir = Path(self.output_dir)

        # Validate interval
        if self.collection_interval_minutes < 1:
            raise ValueError("collection_interval_minutes must be >= 1")

        # Validate timeframe concurrency
        if not 1 <= self.timeframe_concurrency <= 6:
            raise ValueError("timeframe_concurrency must be between 1 and 6")

        # Validate log level
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if self.log_level.upper() not in valid_levels:
            raise ValueError(f"log_level must be one of: {valid_levels}")
        self.log_level = self.log_level.upper()

        # Initialize excluded_symbols if None
        if self.excluded_symbols is None:
            self.excluded_symbols = set()

    @classmethod
    def from_env(cls) -> "DaemonConfig":
        """Create configuration from environment variables.

        Environment Variables:
            JSON_RPC_ARBITRUM: Required - Arbitrum RPC URL
            COLLECTION_INTERVAL_MINUTES: Collection interval in minutes (default: 60)
            COLLECTION_SYMBOLS: Comma-separated symbols (empty = all tokens)
            EXCLUDED_SYMBOLS: Comma-separated symbols to exclude
            OUTPUT_DIR: Data directory (default: ./data)
            TIMEFRAME_CONCURRENCY: Concurrent timeframe fetching (default: 6)
            HEALTH_CHECK_PORT: Health check port (default: 8080)
            LOG_LEVEL: Logging level (default: INFO)
            DRY_RUN: Dry run mode - don't save data (default: false)

        :return: DaemonConfig instance
        :raises ValueError: If required environment variables are missing
        """
        # Required
        rpc_url = os.getenv("JSON_RPC_ARBITRUM")
        if not rpc_url:
            raise ValueError("JSON_RPC_ARBITRUM environment variable is required")

        # Optional - collection interval
        interval_str = os.getenv("COLLECTION_INTERVAL_MINUTES", "60")
        try:
            collection_interval_minutes = int(interval_str)
        except ValueError:
            raise ValueError(
                f"COLLECTION_INTERVAL_MINUTES must be an integer, got: {interval_str}"
            )

        # Optional - specific symbols (comma-separated)
        symbols_str = os.getenv("COLLECTION_SYMBOLS", "").strip()
        collection_symbols = None
        if symbols_str:
            collection_symbols = [s.strip().upper() for s in symbols_str.split(",")]

        # Optional - excluded symbols (comma-separated)
        excluded_str = os.getenv("EXCLUDED_SYMBOLS", "").strip()
        excluded_symbols = set()
        if excluded_str:
            excluded_symbols = {s.strip().upper() for s in excluded_str.split(",")}

        # Optional - output directory
        output_dir = Path(os.getenv("OUTPUT_DIR", "./data"))

        # Optional - timeframe concurrency
        concurrency_str = os.getenv("TIMEFRAME_CONCURRENCY", "6")
        try:
            timeframe_concurrency = int(concurrency_str)
        except ValueError:
            raise ValueError(
                f"TIMEFRAME_CONCURRENCY must be an integer, got: {concurrency_str}"
            )

        # Optional - health check port
        port_str = os.getenv("HEALTH_CHECK_PORT", "8080")
        try:
            health_check_port = int(port_str)
        except ValueError:
            raise ValueError(f"HEALTH_CHECK_PORT must be an integer, got: {port_str}")

        # Optional - log level
        log_level = os.getenv("LOG_LEVEL", "INFO")

        # Optional - dry run
        dry_run_str = os.getenv("DRY_RUN", "false").lower()
        dry_run = dry_run_str in ("true", "1", "yes")

        return cls(
            collection_interval_minutes=collection_interval_minutes,
            output_dir=output_dir,
            rpc_url=rpc_url,
            collection_symbols=collection_symbols,
            excluded_symbols=excluded_symbols,
            timeframe_concurrency=timeframe_concurrency,
            health_check_port=health_check_port,
            log_level=log_level,
            dry_run=dry_run,
        )
