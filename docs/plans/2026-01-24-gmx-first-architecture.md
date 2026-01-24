# GMX-First Data Collection Architecture

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Redesign data collection to use GMX API as primary source, with Chainlink backfill for historical gaps

**Architecture:** GMX-first hybrid approach - discover all GMX tokens, fetch GMX OHLCV data, identify gaps, backfill with Chainlink where feeds exist

**Tech Stack:** GMX API (`eth_defi.gmx.api`), HyperSync, Chainlink oracles, Pandas, PyArrow

---

## Task 1: Create GMX Token Discovery Module

**Files:**
- Create: `src/gmx_historical_data/gmx_token_discovery.py`
- Test: `tests/test_gmx_token_discovery.py`

**Step 1: Write failing test**

Create `tests/test_gmx_token_discovery.py`:

```python
"""Tests for GMX token discovery."""

from gmx_historical_data.gmx_token_discovery import GMXTokenDiscovery


def test_fetch_all_tokens():
    """Test fetching all GMX tokens."""
    discovery = GMXTokenDiscovery(chain="arbitrum")
    tokens = discovery.fetch_all_tokens()

    # Should get ~97 tokens
    assert len(tokens) > 90
    assert len(tokens) < 120

    # Check token structure
    assert all(isinstance(t, dict) for t in tokens)
    assert all("symbol" in t for t in tokens)
    assert all("address" in t for t in tokens)

    # ETH should be in the list
    symbols = [t["symbol"] for t in tokens]
    assert "ETH" in symbols


def test_get_supported_symbols():
    """Test getting list of symbol strings."""
    discovery = GMXTokenDiscovery(chain="arbitrum")
    symbols = discovery.get_supported_symbols()

    assert len(symbols) > 90
    assert "ETH" in symbols
    assert "BTC" in symbols or "WBTC" in symbols or "BTC.b" in symbols
    assert all(isinstance(s, str) for s in symbols)
```

**Step 2: Run test to verify it fails**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && pytest tests/test_gmx_token_discovery.py -v`
Expected: FAIL with "No module named 'gmx_token_discovery'"

**Step 3: Implement GMX token discovery**

Create `src/gmx_historical_data/gmx_token_discovery.py`:

```python
"""Discover all tokens supported by GMX using the official API."""

from dataclasses import dataclass
from typing import Any
import requests


@dataclass
class GMXToken:
    """GMX token metadata.

    :param symbol: Token symbol (e.g., 'ETH', 'BTC')
    :param address: Token contract address
    :param decimals: Token decimals
    :param is_stable: Whether this is a stablecoin
    """
    symbol: str
    address: str
    decimals: int
    is_stable: bool = False


class GMXTokenDiscovery:
    """Discover tokens supported by GMX.

    :param chain: Blockchain network (e.g., 'arbitrum', 'avalanche')
    """

    # GMX API endpoints by chain
    API_ENDPOINTS = {
        "arbitrum": "https://arbitrum-api.gmxinfra2.io",
        "avalanche": "https://avalanche-api.gmxinfra.io",
    }

    def __init__(self, chain: str = "arbitrum") -> None:
        """Initialize GMX token discovery.

        :param chain: Blockchain network
        """
        if chain not in self.API_ENDPOINTS:
            raise ValueError(f"Unsupported chain: {chain}. Must be one of {list(self.API_ENDPOINTS.keys())}")

        self.chain = chain
        self.base_url = self.API_ENDPOINTS[chain]

    def fetch_all_tokens(self) -> list[dict[str, Any]]:
        """Fetch all tokens supported by GMX.

        :return: List of token dictionaries with symbol, address, decimals
        """
        url = f"{self.base_url}/tokens"

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            tokens = response.json()

            if not isinstance(tokens, list):
                raise ValueError(f"Expected list of tokens, got {type(tokens)}")

            return tokens

        except requests.RequestException as e:
            raise RuntimeError(f"Failed to fetch GMX tokens from {url}: {e}") from e

    def get_supported_symbols(self) -> list[str]:
        """Get list of all GMX-supported token symbols.

        :return: List of token symbols (e.g., ['ETH', 'BTC', ...])
        """
        tokens = self.fetch_all_tokens()
        return [token["symbol"] for token in tokens]

    def get_token_by_symbol(self, symbol: str) -> dict[str, Any] | None:
        """Find token metadata by symbol.

        :param symbol: Token symbol (case-insensitive)
        :return: Token dict or None if not found
        """
        symbol = symbol.upper()
        tokens = self.fetch_all_tokens()

        for token in tokens:
            if token["symbol"].upper() == symbol:
                return token

        return None
```

**Step 4: Run tests to verify they pass**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && pytest tests/test_gmx_token_discovery.py -v`
Expected: PASS (both tests)

**Step 5: Commit**

```bash
git add src/gmx_historical_data/gmx_token_discovery.py tests/test_gmx_token_discovery.py
git commit -m "feat: add GMX token discovery module

- Fetch all GMX-supported tokens from official API
- Support Arbitrum and Avalanche chains
- Extract token symbols, addresses, and metadata"
```

---

## Task 2: Create Chainlink Symbol Mapper

**Files:**
- Modify: `src/gmx_historical_data/chainlink_feeds_complete.py` (already exists)
- Test: `tests/test_chainlink_symbol_mapper.py`

**Step 1: Write failing test**

Create `tests/test_chainlink_symbol_mapper.py`:

```python
"""Tests for Chainlink symbol mapping."""

from gmx_historical_data.chainlink_feeds_complete import (
    find_chainlink_symbol,
    get_feed_address_for_gmx_symbol,
)


def test_find_chainlink_symbol_direct_match():
    """Test direct symbol match."""
    assert find_chainlink_symbol("ETH") == "ETH"
    assert find_chainlink_symbol("BTC") == "BTC"
    assert find_chainlink_symbol("ARB") == "ARB"


def test_find_chainlink_symbol_manual_override():
    """Test manual override mapping."""
    assert find_chainlink_symbol("WBTC.b") == "BTC"
    assert find_chainlink_symbol("WETH") == "ETH"
    assert find_chainlink_symbol("USDC.e") == "USDC"


def test_find_chainlink_symbol_fuzzy_match():
    """Test fuzzy matching (strip suffixes/prefixes)."""
    # Should strip .e suffix
    result = find_chainlink_symbol("USDT.e")
    assert result == "USDT" or result is None  # Depends on if override exists

    # Should strip .b suffix
    result = find_chainlink_symbol("BTC.b")
    assert result == "BTC"


def test_find_chainlink_symbol_not_found():
    """Test symbol not found."""
    assert find_chainlink_symbol("NOTAREALTOKEN") is None
    assert find_chainlink_symbol("FAKE123") is None


def test_get_feed_address_for_gmx_symbol():
    """Test getting feed address for GMX symbols."""
    # Direct match
    eth_feed = get_feed_address_for_gmx_symbol("ETH")
    assert eth_feed == "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"

    # Via override
    wbtc_feed = get_feed_address_for_gmx_symbol("WBTC.b")
    assert wbtc_feed == "0x6ce185860a4963106506C203335A2910413708e9"

    # Not found
    assert get_feed_address_for_gmx_symbol("NOTREAL") is None
```

**Step 2: Run test to verify it fails**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && pytest tests/test_chainlink_symbol_mapper.py -v`
Expected: PASS (functions already implemented in previous task)

**Step 3: Verify implementation is correct**

The functions `find_chainlink_symbol()` and `get_feed_address_for_gmx_symbol()` already exist in `chainlink_feeds_complete.py`. Review the code to ensure it matches the test expectations.

**Step 4: Run tests to verify they pass**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && pytest tests/test_chainlink_symbol_mapper.py -v`
Expected: PASS (all tests)

**Step 5: Commit**

```bash
git add tests/test_chainlink_symbol_mapper.py
git commit -m "test: add tests for Chainlink symbol mapping

- Test direct symbol matches
- Test manual override mappings
- Test fuzzy matching (strip .e, .b, W prefix)
- Test not-found cases"
```

---

## Task 3: Create Data Gap Analyzer

**Files:**
- Create: `src/gmx_historical_data/gap_analyzer.py`
- Test: `tests/test_gap_analyzer.py`

**Step 1: Write failing test**

Create `tests/test_gap_analyzer.py`:

```python
"""Tests for data gap analysis."""

import pandas as pd
from datetime import datetime, timezone
from gmx_historical_data.gap_analyzer import DataGapAnalyzer


def test_calculate_gap_with_chainlink_available():
    """Test gap calculation when Chainlink feed exists."""
    # Create mock GMX data (starts 2024-07-01)
    gmx_df = pd.DataFrame({
        "timestamp": pd.date_range("2024-07-01", "2024-12-31", freq="1h", tz=timezone.utc),
        "close": [2000.0] * 4392,  # Dummy prices
    })

    analyzer = DataGapAnalyzer()
    backfill_start, backfill_end = analyzer.calculate_gap(
        gmx_df=gmx_df,
        chainlink_available=True
    )

    # Should backfill from beginning to just before GMX start
    assert backfill_start == 0  # Start from genesis
    assert backfill_end is not None

    # backfill_end should be ~1 second before GMX earliest
    gmx_earliest = gmx_df["timestamp"].min().timestamp()
    assert abs(backfill_end - gmx_earliest) < 2  # Within 2 seconds


def test_calculate_gap_no_chainlink():
    """Test gap when Chainlink feed doesn't exist."""
    gmx_df = pd.DataFrame({
        "timestamp": pd.date_range("2024-07-01", "2024-12-31", freq="1h", tz=timezone.utc),
        "close": [2000.0] * 4392,
    })

    analyzer = DataGapAnalyzer()
    backfill_start, backfill_end = analyzer.calculate_gap(
        gmx_df=gmx_df,
        chainlink_available=False
    )

    # No backfill needed
    assert backfill_start is None
    assert backfill_end is None


def test_calculate_gap_empty_gmx_data():
    """Test gap when GMX data is empty."""
    gmx_df = pd.DataFrame()

    analyzer = DataGapAnalyzer()
    backfill_start, backfill_end = analyzer.calculate_gap(
        gmx_df=gmx_df,
        chainlink_available=True
    )

    # Should collect all historical data
    assert backfill_start == 0
    assert backfill_end is None  # No upper limit (collect to latest)
```

**Step 2: Run test to verify it fails**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && pytest tests/test_gap_analyzer.py -v`
Expected: FAIL with "No module named 'gap_analyzer'"

**Step 3: Implement gap analyzer**

Create `src/gmx_historical_data/gap_analyzer.py`:

```python
"""Analyze data gaps between GMX and Chainlink data sources."""

import pandas as pd


class DataGapAnalyzer:
    """Calculate what historical data needs to be backfilled."""

    def calculate_gap(
        self,
        gmx_df: pd.DataFrame,
        chainlink_available: bool,
    ) -> tuple[int | None, int | None]:
        """Calculate the gap that needs to be filled with Chainlink data.

        :param gmx_df: GMX OHLCV DataFrame with 'timestamp' column
        :param chainlink_available: Whether Chainlink feed exists for this token
        :return: Tuple of (backfill_start_block, backfill_end_timestamp)
            - backfill_start_block: Block to start Chainlink collection (0 = genesis)
            - backfill_end_timestamp: Unix timestamp to end collection (GMX earliest - 1)
            - Returns (None, None) if no backfill needed
        """
        # No backfill if Chainlink not available
        if not chainlink_available:
            return None, None

        # If GMX data is empty, collect all Chainlink data
        if gmx_df.empty:
            return 0, None  # From genesis to latest

        # Get earliest GMX timestamp
        gmx_earliest = gmx_df["timestamp"].min()
        gmx_earliest_unix = int(gmx_earliest.timestamp())

        # Backfill from genesis to just before GMX coverage starts
        backfill_start_block = 0
        backfill_end_timestamp = gmx_earliest_unix - 1

        return backfill_start_block, backfill_end_timestamp
```

**Step 4: Run tests to verify they pass**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && pytest tests/test_gap_analyzer.py -v`
Expected: PASS (all tests)

**Step 5: Commit**

```bash
git add src/gmx_historical_data/gap_analyzer.py tests/test_gap_analyzer.py
git commit -m "feat: add data gap analyzer

- Calculate backfill range needed for Chainlink data
- Handle cases: Chainlink available, not available, empty GMX data
- Return start block and end timestamp for HyperSync queries"
```

---

## Task 4: Refactor DataCollector to Use GMX-First Approach

**Files:**
- Modify: `src/gmx_historical_data/cli.py` (lines 32-203 - DataCollector class)
- Reference: `src/gmx_historical_data/gmx_token_discovery.py`
- Reference: `src/gmx_historical_data/chainlink_feeds_complete.py`
- Reference: `src/gmx_historical_data/gap_analyzer.py`

**Step 1: Update imports**

In `src/gmx_historical_data/cli.py`, update imports (around line 18):

```python
from gmx_historical_data.config import CollectionConfig, TIMEFRAMES
from gmx_historical_data.chainlink_feeds_complete import (
    get_feed_address_for_gmx_symbol,
    find_chainlink_symbol,
)
from gmx_historical_data.gmx_token_discovery import GMXTokenDiscovery
from gmx_historical_data.gap_analyzer import DataGapAnalyzer
from gmx_historical_data.aggregator_discovery import AggregatorDiscovery
from gmx_historical_data.hypersync_collector import HyperSyncCollector
from gmx_historical_data.storage import ParquetStorage
from gmx_historical_data.checkpoint import CheckpointManager
from gmx_historical_data.resampler import OHLCVResampler
from gmx_historical_data.gmx_api_integration import (
    GMXDataFetcher,
    combine_gmx_and_chainlink_data,
    map_timeframe_to_gmx_period,
)
```

**Step 2: Add GMX token discovery to DataCollector.__init__**

Modify `DataCollector.__init__()` (around line 39):

```python
def __init__(self, config: CollectionConfig, use_gmx_api: bool = True) -> None:
    """Initialize data collector.

    :param config: Collection configuration
    :param use_gmx_api: If True, fetch latest data from GMX API
    """
    self.config = config
    self.use_gmx_api = use_gmx_api
    self.web3 = Web3(Web3.HTTPProvider(config.rpc_url))
    self.hypersync = HyperSyncCollector(
        config.hypersync_endpoint,
        config.hypersync_api_token,
    )
    self.storage = ParquetStorage(config.output_dir)
    self.checkpoint_mgr = CheckpointManager(config.checkpoints_dir)
    self.resampler = OHLCVResampler(decimals=8)

    # GMX token discovery
    self.gmx_discovery = GMXTokenDiscovery(chain="arbitrum")

    # Gap analyzer
    self.gap_analyzer = DataGapAnalyzer()

    # Initialize GMX API fetcher if enabled
    if use_gmx_api:
        self.gmx_fetcher = GMXDataFetcher(chain="arbitrum")
    else:
        self.gmx_fetcher = None

    # Ensure directories exist
    config.ensure_directories()
```

**Step 3: Rewrite collect_symbol() method**

Replace the entire `collect_symbol()` method (lines 65-202) with the new GMX-first implementation:

```python
async def collect_symbol(
    self,
    symbol: str,
    full: bool = False,
) -> None:
    """Collect data for a single symbol using GMX-first approach.

    :param symbol: Token symbol (e.g., 'ETH')
    :param full: If True, collect from genesis; if False, resume from checkpoint
    """
    console.print()
    console.print(Panel(f"[bold cyan]Collecting data for {symbol}[/bold cyan]", box=box.ROUNDED))

    # Step 1: Fetch GMX data (all timeframes)
    console.print("\n[bold]Fetching latest data from GMX API...[/bold]")
    gmx_candles = {}

    if self.use_gmx_api and self.gmx_fetcher:
        for timeframe in TIMEFRAMES:
            gmx_period = map_timeframe_to_gmx_period(timeframe)
            gmx_df = self.gmx_fetcher.fetch_gmx_candles(symbol, period=gmx_period)

            if not gmx_df.empty:
                earliest, latest = gmx_df["timestamp"].min(), gmx_df["timestamp"].max()
                console.print(f"  [green]✓[/green] {timeframe}: [cyan]{len(gmx_df):,}[/cyan] candles from GMX [dim]({earliest} to {latest})[/dim]")
                gmx_candles[timeframe] = gmx_df
            else:
                console.print(f"  [yellow]○[/yellow] {timeframe}: No GMX data available")

    if not gmx_candles:
        console.print(f"[yellow]No GMX data available for {symbol}[/yellow]")
        return

    # Step 2: Find Chainlink feed (if exists)
    console.print(f"\n[bold]Checking for Chainlink feed...[/bold]")
    chainlink_symbol = find_chainlink_symbol(symbol)
    chainlink_feed_address = get_feed_address_for_gmx_symbol(symbol) if chainlink_symbol else None

    if chainlink_feed_address:
        console.print(f"  [green]✓[/green] Found Chainlink feed: [yellow]{chainlink_feed_address}[/yellow]")
        console.print(f"  [dim]Mapped symbol:[/dim] {symbol} → {chainlink_symbol}")
    else:
        console.print(f"  [yellow]○[/yellow] No Chainlink feed found - using GMX data only")

    # Step 3: Calculate gap and backfill with Chainlink
    chainlink_candles = {}

    if chainlink_feed_address:
        # Use 1h candles to determine the gap (representative)
        gmx_1h = gmx_candles.get("1h")

        if gmx_1h is not None:
            backfill_start, backfill_end = self.gap_analyzer.calculate_gap(
                gmx_df=gmx_1h,
                chainlink_available=True
            )

            console.print(f"\n[bold]Analyzing data gap...[/bold]")
            if backfill_end is not None:
                gmx_earliest = gmx_1h["timestamp"].min()
                console.print(f"  [dim]GMX coverage starts:[/dim] {gmx_earliest}")
                console.print(f"  [dim]Backfill needed:[/dim] Genesis → {gmx_earliest}")
            else:
                console.print(f"  [green]✓[/green] No gap - GMX data covers full history")

            # Collect Chainlink data to fill the gap
            if backfill_start is not None:
                console.print(f"\n[bold]Backfilling with Chainlink data...[/bold]")

                # Discover aggregator address
                discovery = AggregatorDiscovery(self.web3)
                try:
                    aggregator_info = discovery.get_aggregator_info(chainlink_feed_address)
                    aggregator_address = aggregator_info["current_aggregator"]
                    console.print(f"  [dim]Aggregator:[/dim] [yellow]{aggregator_address}[/yellow]")
                except Exception as e:
                    console.print(f"[red]✗ Aggregator discovery failed: {e}[/red]")
                    chainlink_feed_address = None  # Disable Chainlink backfill

                if chainlink_feed_address:
                    # Determine start block
                    if full:
                        start_block = self.config.start_block or 0
                    else:
                        start_block = self.checkpoint_mgr.get_resume_block(symbol, default=0)

                    # Convert backfill_end timestamp to block
                    end_block = None  # Will query up to backfill_end timestamp

                    # Collect Chainlink events
                    try:
                        events, stats = await self.hypersync.collect_all_events(
                            aggregator_addresses=[aggregator_address],
                            start_block=start_block,
                            end_block=end_block,
                            auto_detect_start=False,
                        )

                        if events:
                            # Filter events to only those before GMX coverage
                            events = [e for e in events if e.timestamp <= backfill_end]

                            console.print(f"  [green]✓[/green] Collected [cyan]{len(events):,}[/cyan] Chainlink events")

                            # Save raw events
                            if full:
                                output_path = self.storage.save_raw_events(events, symbol, partition_id=0)
                            else:
                                output_path = self.storage.append_raw_events(events, symbol)

                            # Resample to OHLCV
                            raw_df = self.storage.read_raw_events(symbol)
                            chainlink_candles = self.resampler.resample_all_timeframes(raw_df, symbol)

                            console.print(f"  [green]✓[/green] Resampled to OHLCV candles")

                    except Exception as e:
                        console.print(f"[red]✗ Chainlink collection failed: {e}[/red]")

    # Step 4: Combine and save
    console.print("\n[bold]Saving combined candles...[/bold]")
    for timeframe in TIMEFRAMES:
        chainlink_df = chainlink_candles.get(timeframe)
        gmx_df = gmx_candles.get(timeframe)

        # Combine if both sources have data
        if chainlink_df is not None and gmx_df is not None and not gmx_df.empty:
            combined_df = combine_gmx_and_chainlink_data(gmx_df, chainlink_df)
            candle_path = self.storage.save_candles(combined_df, timeframe, symbol)
            earliest, latest = combined_df["timestamp"].min(), combined_df["timestamp"].max()
            console.print(f"  [green]✓[/green] {timeframe}: [cyan]{len(combined_df):,}[/cyan] total candles [dim]({earliest} to {latest})[/dim]")
        elif chainlink_df is not None:
            # Only Chainlink data
            candle_path = self.storage.save_candles(chainlink_df, timeframe, symbol)
            earliest, latest = chainlink_df["timestamp"].min(), chainlink_df["timestamp"].max()
            console.print(f"  [green]✓[/green] {timeframe}: [cyan]{len(chainlink_df):,}[/cyan] candles [dim]({earliest} to {latest})[/dim]")
        elif gmx_df is not None and not gmx_df.empty:
            # Only GMX data
            candle_path = self.storage.save_candles(gmx_df, timeframe, symbol)
            earliest, latest = gmx_df["timestamp"].min(), gmx_df["timestamp"].max()
            console.print(f"  [green]✓[/green] {timeframe}: [cyan]{len(gmx_df):,}[/cyan] candles [dim]({earliest} to {latest})[/dim]")

    console.print(f"\n[bold green]✓ Collection complete for {symbol}[/bold green]")
```

**Step 4: Update collect_all_symbols() to use GMX discovery**

Modify `collect_all_symbols()` method (around line 204):

```python
async def collect_all_symbols(self, full: bool = False) -> None:
    """Collect data for all GMX-supported symbols.

    :param full: If True, collect from genesis; if False, resume from checkpoints
    """
    # Discover all GMX tokens
    console.print(f"\n[bold]Discovering GMX tokens...[/bold]")
    symbols = self.gmx_discovery.get_supported_symbols()
    console.print(f"  [green]✓[/green] Found [cyan]{len(symbols)}[/cyan] GMX-supported tokens")

    total = len(symbols)
    successful = 0
    failed = 0
    failed_symbols = []

    console.print(f"\n[bold]Collecting data for [cyan]{total}[/cyan] symbols...[/bold]")

    for i, symbol in enumerate(symbols, 1):
        console.print(f"\n[bold blue][{i}/{total}][/bold blue] Processing {symbol}...")
        try:
            await self.collect_symbol(symbol, full=full)
            successful += 1
        except Exception as e:
            console.print(f"[red]✗ Error processing {symbol}: {e}[/red]")
            failed += 1
            failed_symbols.append(symbol)
            continue

    # Create summary table
    summary_table = Table(title="Collection Summary", box=box.ROUNDED, show_header=False)
    summary_table.add_column("Status", style="bold")
    summary_table.add_column("Count", justify="right")

    summary_table.add_row("[green]✓ Successful[/green]", f"[green]{successful}/{total}[/green]")
    summary_table.add_row("[red]✗ Failed[/red]", f"[red]{failed}/{total}[/red]")
    if failed_symbols:
        summary_table.add_row("[yellow]Failed symbols[/yellow]", f"[yellow]{', '.join(failed_symbols)}[/yellow]")

    console.print()
    console.print(summary_table)
```

**Step 5: Test manually**

Run:
```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run gmx_historical_data --full --symbol ETH --output-dir ./data
```

Expected output:
- Fetching GMX data for ETH
- Finding Chainlink feed
- Calculating gap
- Backfilling with Chainlink
- Combining and saving

**Step 6: Commit**

```bash
git add src/gmx_historical_data/cli.py
git commit -m "refactor: implement GMX-first data collection

- Discover all GMX tokens dynamically
- Fetch GMX OHLCV as primary source
- Find matching Chainlink feeds automatically
- Calculate gaps and backfill with Chainlink
- Combine data seamlessly (Chainlink + GMX)
- Continue on errors, report summary"
```

---

## Task 5: Update Package Exports

**Files:**
- Modify: `src/gmx_historical_data/__init__.py`

**Step 1: Add new module exports**

Update `src/gmx_historical_data/__init__.py`:

```python
"""GMX Historical Data Collection."""

from gmx_historical_data.config import CollectionConfig, TIMEFRAMES
from gmx_historical_data.chainlink_feeds_complete import (
    get_feed_address,
    get_all_symbols,
    find_chainlink_symbol,
    get_feed_address_for_gmx_symbol,
    CHAINLINK_FEEDS_ARBITRUM,
)
from gmx_historical_data.gmx_token_discovery import GMXTokenDiscovery, GMXToken
from gmx_historical_data.gap_analyzer import DataGapAnalyzer
from gmx_historical_data.aggregator_discovery import AggregatorDiscovery
from gmx_historical_data.hypersync_collector import HyperSyncCollector, CollectionStats
from gmx_historical_data.storage import ParquetStorage
from gmx_historical_data.checkpoint import CheckpointManager
from gmx_historical_data.resampler import OHLCVResampler
from gmx_historical_data.gmx_api_integration import (
    GMXDataFetcher,
    combine_gmx_and_chainlink_data,
    map_timeframe_to_gmx_period,
)

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
```

**Step 2: Commit**

```bash
git add src/gmx_historical_data/__init__.py
git commit -m "feat: export new modules for GMX-first architecture

- Export GMXTokenDiscovery
- Export DataGapAnalyzer
- Export Chainlink symbol mapping functions
- Update __all__ list"
```

---

## Task 6: Add Tests for End-to-End Flow

**Files:**
- Create: `tests/test_integration_gmx_first.py`

**Step 1: Write integration test**

Create `tests/test_integration_gmx_first.py`:

```python
"""Integration test for GMX-first data collection."""

import pytest
import os
from pathlib import Path
from gmx_historical_data import (
    GMXTokenDiscovery,
    DataGapAnalyzer,
    find_chainlink_symbol,
    get_feed_address_for_gmx_symbol,
)


@pytest.mark.skipif(
    not os.getenv("JSON_RPC_ARBITRUM"),
    reason="Requires JSON_RPC_ARBITRUM env var"
)
def test_gmx_first_flow():
    """Test the complete GMX-first data collection flow."""
    # Step 1: Discover GMX tokens
    discovery = GMXTokenDiscovery(chain="arbitrum")
    symbols = discovery.get_supported_symbols()

    assert len(symbols) > 90
    assert "ETH" in symbols

    # Step 2: Find Chainlink feed for ETH
    chainlink_symbol = find_chainlink_symbol("ETH")
    assert chainlink_symbol == "ETH"

    feed_address = get_feed_address_for_gmx_symbol("ETH")
    assert feed_address == "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"

    # Step 3: Test gap analysis (mock GMX data)
    import pandas as pd
    from datetime import timezone

    gmx_df = pd.DataFrame({
        "timestamp": pd.date_range("2024-07-01", "2024-12-31", freq="1h", tz=timezone.utc),
        "close": [2000.0] * 4392,
    })

    analyzer = DataGapAnalyzer()
    backfill_start, backfill_end = analyzer.calculate_gap(
        gmx_df=gmx_df,
        chainlink_available=True
    )

    assert backfill_start == 0
    assert backfill_end is not None

    print(f"\nGMX-first flow test passed:")
    print(f"  - Discovered {len(symbols)} GMX tokens")
    print(f"  - Found Chainlink feed for ETH: {feed_address}")
    print(f"  - Gap analysis: backfill from block {backfill_start} to timestamp {backfill_end}")
```

**Step 2: Run test**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && pytest tests/test_integration_gmx_first.py -v -s`
Expected: PASS

**Step 3: Commit**

```bash
git add tests/test_integration_gmx_first.py
git commit -m "test: add integration test for GMX-first flow

- Test GMX token discovery
- Test Chainlink feed mapping
- Test gap analysis
- Verify end-to-end workflow"
```

---

## Task 7: Update Documentation

**Files:**
- Modify: `README.md` (already updated in previous session)
- Create: `docs/architecture/gmx-first-design.md`

**Step 1: Create architecture document**

Create `docs/architecture/gmx-first-design.md`:

```markdown
# GMX-First Data Collection Architecture

## Overview

This document describes the GMX-first hybrid data collection approach that maximizes token coverage and data availability.

## Design Principles

1. **GMX as Primary Source**: Start with GMX API to get recent high-quality OHLCV data
2. **Chainlink as Backfill**: Use Chainlink oracles only to fill historical gaps
3. **Maximum Coverage**: Collect data for ALL ~97 GMX-supported tokens
4. **Smart Symbol Mapping**: Automatic fuzzy matching between GMX and Chainlink symbols
5. **Resilient Collection**: Continue on errors, report summary at end

## Data Flow

```
1. GMX Token Discovery
   ↓
2. Fetch GMX OHLCV (all timeframes)
   ↓
3. Find Matching Chainlink Feed
   ↓
4. Calculate Data Gap
   ↓
5. Backfill with Chainlink (if needed)
   ↓
6. Combine & Save
```

## Components

### GMXTokenDiscovery
- Fetches all tokens from GMX API
- Returns ~97 tokens with metadata
- Supports Arbitrum and Avalanche

### Symbol Mapper
- Hybrid approach:
  1. Check manual overrides (WBTC.b → BTC)
  2. Try direct match (ETH → ETH)
  3. Try fuzzy match (strip .e, .b, W prefix)
- Returns Chainlink feed address or None

### DataGapAnalyzer
- Calculates backfill range
- Inputs: GMX DataFrame, Chainlink availability
- Outputs: (start_block, end_timestamp)

### DataCollector (Refactored)
- GMX-first collection logic
- Automatic gap detection
- Seamless data combination
- Error handling with continue-on-failure

## Token Coverage

| Category | Count | Data Source | Historical Depth |
|----------|-------|-------------|------------------|
| Tokens with Chainlink | ~50 | GMX + Chainlink | 2021+ to present |
| Tokens GMX-only | ~47 | GMX only | Last ~6 months |
| **Total** | **~97** | **Hybrid** | **Maximum available** |

## Example Flow: ETH

1. Discover ETH from GMX API
2. Fetch GMX OHLCV for 1m, 5m, 15m, 1h, 4h, 1D
   - Coverage: 2024-07-01 to 2026-01-24 (latest)
3. Find Chainlink feed: ETH → 0x639Fe...a612
4. Calculate gap: Need data from 2021-07-13 to 2024-07-01
5. Query Chainlink events via HyperSync
6. Resample Chainlink events to OHLCV
7. Combine: Chainlink (2021-2024) + GMX (2024-present)
8. Save: ~4.5 years of complete OHLCV data

## Example Flow: Newer Token (GMX-only)

1. Discover NEWTOKEN from GMX API
2. Fetch GMX OHLCV for all timeframes
   - Coverage: 2024-07-01 to 2026-01-24
3. Try to find Chainlink feed: None found
4. Skip Chainlink backfill
5. Save: ~6 months of GMX OHLCV data

## Performance

- **GMX API**: Fast, ~1-2 seconds per token per timeframe
- **Chainlink via HyperSync**: 100-2000x faster than RPC
- **Total collection time**: Minutes for all ~97 tokens (vs hours/days with RPC)

## Error Handling

- Continue on failure (resilient to individual token failures)
- Log errors with clear messages
- Report summary at end:
  - ✓ Successful: 85/97
  - ✗ Failed: 12/97
  - Failed tokens: [list]

## Future Enhancements

- [ ] Support Avalanche chain
- [ ] Parallel token collection
- [ ] Real-time streaming updates
- [ ] Data quality validation
```

**Step 2: Commit**

```bash
mkdir -p docs/architecture
git add docs/architecture/gmx-first-design.md
git commit -m "docs: add GMX-first architecture documentation

- Document design principles
- Explain data flow and components
- Show example flows for different token types
- Describe error handling strategy"
```

---

## Verification

After completing all tasks, verify the implementation:

1. **Test single token collection:**
   ```bash
   export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
   poetry run gmx_historical_data --full --symbol ETH --output-dir ./data
   ```

2. **Test all tokens collection:**
   ```bash
   export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
   poetry run gmx_historical_data --full --output-dir ./data
   ```

3. **Run all tests:**
   ```bash
   export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
   pytest tests/ -v
   ```

4. **Check data output:**
   ```bash
   python scripts/plot_historical_data.py ETH --data-dir ./data
   ```

## Success Criteria

- ✅ All ~97 GMX tokens discovered
- ✅ GMX data fetched for all timeframes
- ✅ Chainlink feeds found for ~50 tokens
- ✅ Data gaps calculated correctly
- ✅ Chainlink backfill working
- ✅ Data combined seamlessly
- ✅ Tests passing
- ✅ Documentation complete
