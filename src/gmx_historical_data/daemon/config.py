"""Configuration for GMX periodic data collection daemon."""

import os
from dataclasses import dataclass
from pathlib import Path


def get_gmx_markets_with_chainlink_feeds() -> list[str]:
    """Get list of GMX markets that have public Chainlink price feeds.

    This function filters the GMX markets to only those with available Chainlink
    oracle data, ensuring we can collect reliable OHLCV candles.

    Markets WITH Chainlink feeds (33 total):
        AAVE, APE, ARB, ATOM, AVAX, BNB, BTC, CRV, DAI, DOGE, ETH, GMX,
        LDO, LINK, LTC, MKR, NEAR, OP, PENDLE, PEPE, POL, SEI, SHIB, SOL,
        STETH, TAO, UNI, USDC, USDC.e, USDT, WBTC.b, WIF, XRP

    Markets WITHOUT Chainlink feeds (86 total - not collected by default):
        0G, ADA, AERO, AI16Z, AIXBT, ALGO, ANIME, APE_deprecated, APT, AR,
        ASTER, AVNT, BCH, BERA, BOME, BONK, BRETT, CAKE, CHZ, CRO, CVX,
        DASH, DOLO, DOT, DYDX, EIGEN, ENA, FARTCOIN, FET, FIL, FLOKI,
        FTM, GLV [ETH-USDC], GLV [WBTC.b-USDC], HBAR, HYPE, ICP, INJ, IP,
        JTO, JUP, KAS, KTA, LINEA, LIT, MELANIA, MEME, MEW, MNT, MON,
        MOODENG, MORPHO, OKB, OM, ONDO, ORDI, PENGU, PI, PUMP, RENDER, S,
        SATS, SKY, SPX6900, STX, SUI, SYRUP, TIA, TON, TRUMP,
        TRX, USDe, VIRTUAL, VVV, WELL, WLD, WLFI, XAUT, XAUT.v2, XLM, XMR,
        XPL, ZEC, ZORA, ZRO, tBTC, wstETH, rETH, cbETH

    NOTE: wstETH, rETH, and cbETH only have ETH-denominated feeds (not USD) on Arbitrum.
          STETH has a USD feed, so it's included in the Chainlink list.

    :return: List of GMX market symbols with Chainlink feeds
    """
    return [
        "AAVE",
        "APE",
        "ARB",
        "ATOM",
        "AVAX",
        "BNB",
        "BTC",
        "CRV",
        "DAI",
        "DOGE",
        "ETH",
        "GMX",
        "LDO",
        "LINK",
        "LTC",
        "MKR",
        "NEAR",
        "OP",
        "PENDLE",
        "PEPE",
        "POL",
        "SEI",
        "SHIB",
        "SOL",
        "STETH",
        "TAO",
        "UNI",
        "USDC",
        "USDC.e",
        "USDT",
        "WBTC.b",
        "WIF",
        "XRP",
    ]


def get_gmx_markets_without_chainlink_feeds() -> list[str]:
    """Get list of GMX markets that do NOT have public Chainlink price feeds.

    These 86 markets require OraclePriceUpdate event collection from
    GMX EventEmitter contract to build OHLCV candles.

    :return: List of GMX market symbols without Chainlink feeds
    """
    return [
        "0G",
        "ADA",
        "AERO",
        "AI16Z",
        "AIXBT",
        "ALGO",
        "ANIME",
        "APT",
        "AR",
        "ASTER",
        "AVNT",
        "BCH",  # No Chainlink feed on Arbitrum
        "BERA",
        "BOME",
        "BONK",  # No Chainlink feed on Arbitrum
        "BRETT",
        "CAKE",
        "CHZ",
        "CRO",
        "CVX",
        "DASH",
        "DOLO",
        "DOT",
        "DYDX",
        "EIGEN",
        "ENA",
        "FARTCOIN",
        "FET",
        "FIL",  # No Chainlink feed on Arbitrum
        "FLOKI",
        "FTM",  # No Chainlink feed on Arbitrum
        "HBAR",
        "HYPE",
        "ICP",
        "INJ",
        "IP",
        "JTO",
        "JUP",
        "KAS",
        "KTA",
        "LINEA",
        "LIT",
        "MELANIA",
        "MEME",
        "MEW",
        "MNT",
        "MON",
        "MOODENG",
        "MORPHO",
        "OKB",
        "OM",
        "ONDO",
        "ORDI",
        "PENGU",
        "PI",
        "PUMP",
        "RENDER",
        "S",
        "SATS",
        "SKY",
        "SPX6900",
        "STX",
        "SUI",
        "SYRUP",
        "TIA",
        "TON",
        "TRUMP",
        "TRX",
        "USDe",
        "VIRTUAL",
        "VVV",
        "WELL",
        "WLD",
        "WLFI",
        "XAUT",
        "XAUT.v2",
        "XLM",
        "XMR",
        "XPL",
        "ZEC",
        "ZORA",
        "ZRO",
        "tBTC",
        "wstETH",  # Only has ETH-denominated feed (wstETH/ETH), not USD
        "rETH",  # Only has ETH-denominated feed (rETH/ETH), not USD
        "cbETH",  # Only has ETH-denominated feed (cbETH/ETH), not USD
    ]


@dataclass
class DaemonConfig:
    """Configuration for periodic data collection daemon.

    :param collection_interval_minutes: How often to collect data (in minutes)
    :param output_dir: Directory for storing collected data
    :param rpc_url: Primary Arbitrum RPC URL (comma-separated for multiple providers)
    :param fallback_rpc_urls: List of fallback RPC URLs
    :param collection_symbols: Optional list of symbols to collect (None = all)
    :param excluded_symbols: Set of symbols to exclude from collection
    :param timeframe_concurrency: Number of timeframes to fetch concurrently (1-6)
    :param health_check_port: Port for health check HTTP endpoint
    :param log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
    :param dry_run: If True, don't save data (testing only)
    :param chainlink_only: If True, only collect markets with Chainlink feeds (skip oracle events)
    :param hypersync_api_token: Optional HyperSync API token for authentication
    :param hypersync_endpoint: HyperSync API endpoint URL
    :param enable_adaptive_gap_detection: If True, query API for actual data range to detect data loss
    :param collect_live_funding: If True, append live GMX funding rate after each collection cycle
    :param live_funding_feather_dir: Directory containing funding rate feather files to update
        (required when collect_live_funding is True)
    """

    collection_interval_minutes: int = 60
    output_dir: Path = Path("./data")
    rpc_url: str = ""
    fallback_rpc_urls: list[str] = None
    collection_symbols: list[str] | None = None
    excluded_symbols: set[str] = None
    timeframe_concurrency: int = 6
    health_check_port: int = 8080
    log_level: str = "INFO"
    dry_run: bool = False
    chainlink_only: bool = True
    hypersync_api_token: str | None = None
    hypersync_endpoint: str = "https://arbitrum.hypersync.xyz"
    enable_adaptive_gap_detection: bool = True
    collect_live_funding: bool = False
    live_funding_feather_dir: Path | None = None

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Ensure output_dir is a Path object
        if not isinstance(self.output_dir, Path):
            self.output_dir = Path(self.output_dir)

        # Ensure live_funding_feather_dir is a Path object (if set)
        if self.live_funding_feather_dir is not None and not isinstance(
            self.live_funding_feather_dir, Path
        ):
            self.live_funding_feather_dir = Path(self.live_funding_feather_dir)

        # Initialize fallback_rpc_urls if None
        if self.fallback_rpc_urls is None:
            self.fallback_rpc_urls = []

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
            JSON_RPC_ARBITRUM: Required - Primary Arbitrum RPC URL (comma-separated for multiple)
            FALLBACK_RPC_URLS: Optional - Comma-separated fallback RPC URLs
            COLLECTION_INTERVAL_MINUTES: Collection interval in minutes (default: 60)
            COLLECTION_SYMBOLS: Comma-separated symbols (empty = all tokens)
            EXCLUDED_SYMBOLS: Comma-separated symbols to exclude
            OUTPUT_DIR: Data directory (default: ./data)
            TIMEFRAME_CONCURRENCY: Concurrent timeframe fetching (default: 6)
            HEALTH_CHECK_PORT: Health check port (default: 8080)
            LOG_LEVEL: Logging level (default: INFO)
            DRY_RUN: Dry run mode - don't save data (default: false)
            CHAINLINK_ONLY: Only collect markets with Chainlink feeds (default: true)
            HYPERSYNC_API_TOKEN: Optional HyperSync API token
            HYPERSYNC_ENDPOINT: HyperSync API endpoint (default: https://arbitrum.hypersync.xyz)
            ENABLE_ADAPTIVE_GAP_DETECTION: Enable API-aware gap detection to detect data loss (default: true)

        :return: DaemonConfig instance
        :raises ValueError: If required environment variables are missing
        """
        # Required
        rpc_url = os.getenv("JSON_RPC_ARBITRUM")
        if not rpc_url:
            raise ValueError("JSON_RPC_ARBITRUM environment variable is required")

        # Optional - fallback RPC URLs (comma-separated)
        fallback_str = os.getenv("FALLBACK_RPC_URLS", "").strip()
        fallback_rpc_urls = []
        if fallback_str:
            fallback_rpc_urls = [url.strip() for url in fallback_str.split(",") if url.strip()]

        # Optional - collection interval
        interval_str = os.getenv("COLLECTION_INTERVAL_MINUTES", "60")
        try:
            collection_interval_minutes = int(interval_str)
        except ValueError:
            raise ValueError(f"COLLECTION_INTERVAL_MINUTES must be an integer, got: {interval_str}")

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
            raise ValueError(f"TIMEFRAME_CONCURRENCY must be an integer, got: {concurrency_str}")

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

        # Optional - collect only Chainlink markets (skip oracle events)
        chainlink_only_str = os.getenv("CHAINLINK_ONLY", "true").lower()
        chainlink_only = chainlink_only_str in ("true", "1", "yes")

        # Optional - HyperSync configuration
        hypersync_api_token = os.getenv("HYPERSYNC_API_TOKEN")
        hypersync_endpoint = os.getenv("HYPERSYNC_ENDPOINT", "https://arbitrum.hypersync.xyz")

        # Optional - adaptive gap detection (enabled by default)
        adaptive_gap_str = os.getenv("ENABLE_ADAPTIVE_GAP_DETECTION", "true").lower()
        enable_adaptive_gap_detection = adaptive_gap_str in ("true", "1", "yes")

        # Optional - live funding rate appender
        collect_live_funding_str = os.getenv("COLLECT_LIVE_FUNDING", "false").lower()
        collect_live_funding = collect_live_funding_str in ("true", "1", "yes")

        live_feather_str = os.getenv("LIVE_FUNDING_FEATHER_DIR", "").strip()
        live_funding_feather_dir = Path(live_feather_str) if live_feather_str else None

        return cls(
            collection_interval_minutes=collection_interval_minutes,
            output_dir=output_dir,
            rpc_url=rpc_url,
            fallback_rpc_urls=fallback_rpc_urls,
            collection_symbols=collection_symbols,
            excluded_symbols=excluded_symbols,
            timeframe_concurrency=timeframe_concurrency,
            health_check_port=health_check_port,
            log_level=log_level,
            dry_run=dry_run,
            chainlink_only=chainlink_only,
            hypersync_api_token=hypersync_api_token,
            hypersync_endpoint=hypersync_endpoint,
            enable_adaptive_gap_detection=enable_adaptive_gap_detection,
            collect_live_funding=collect_live_funding,
            live_funding_feather_dir=live_funding_feather_dir,
        )
