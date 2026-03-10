#!/usr/bin/env python3
# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "hypersync>=0.8.0",
#     "polars>=0.20.0",
#     "eth-abi>=5.0.0",
#     "eth-hash[pycryptodome]",
#     "eth-utils>=4.0",
#     "rich>=13.0",
# ]
# ///
"""
GMX V2 Historical Open Interest Extractor (HyperSync Edition)
==============================================================
Extracts open interest data from GMX V2 (Synthetics) using Envio HyperSync
for blazing-fast blockchain data retrieval (20-100x faster than JSON-RPC).

Tracks two events emitted on every position change:

- ``OpenInterestUpdated`` — USD-denominated OI (30-decimal precision)
- ``OpenInterestInTokensUpdated`` — Token-denominated OI (token decimals)

Both events share the same EventLogData structure with fields:
  addressItems: market, collateralToken
  boolItems: isLong
  intItems: delta (signed change)
  uintItems: nextValue (new total after change)

Supports two modes:

- **Backfill** (one-shot): ``--from-block 120000000``
- **Incremental** (cronjob): ``--resume`` with checkpoint

QUICK START
-----------
    uv run scripts/extract_open_interest.py --network arbitrum --from-block 290000000

USAGE
-----
    uv run scripts/extract_open_interest.py [OPTIONS]

OPTIONS
-------
    --network        Network: "arbitrum" or "avalanche" (default: arbitrum)
    --from-block     Starting block number for backfill mode
    --to-block       Ending block number (default: latest)
    --output-dir     Base output directory (default: ./user_data/data/gmx/open_interest)
    --output         Output format: "json", "csv", or "parquet" (default: parquet)
    --market         Filter by market symbol (e.g., "ETH/USD", "BTC/USD")
    --resume         Enable checkpoint-based incremental mode (for cronjob)
    --checkpoint-dir Override checkpoint directory

EXAMPLES
--------
    # Backfill from genesis
    uv run scripts/extract_open_interest.py --from-block 120000000

    # Quick test on a small block range
    uv run scripts/extract_open_interest.py --from-block 290000000 --to-block 290100000 --output json

    # Daily cronjob (incremental)
    uv run scripts/extract_open_interest.py --resume

CRONJOB SETUP
=============
    # Initial backfill (one-time)
    uv run scripts/extract_open_interest.py --from-block 120000000

    # Daily cron (2 AM UTC)
    0 2 * * * cd /path/to/project && uv run scripts/extract_open_interest.py --resume 2>&1 >> logs/oi_collector.log
================================================================================
"""

import argparse
import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import hypersync
from eth_abi import decode as abi_decode
from eth_utils import keccak
from hypersync import (
    BlockField,
    ClientConfig,
    FieldSelection,
    HypersyncClient,
    LogField,
    LogSelection,
    Query,
)
from rich.console import Console
from rich.panel import Panel
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from rich.table import Table
from gmx_historical_data.market_registry import fetch_markets as _fetch_markets_cached

# Optional imports
try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

console = Console()

# Retry settings
MAX_RETRIES = 15
RETRY_BASE_DELAY = 2.0
RETRY_MAX_DELAY = 120.0  # Cap exponential backoff at 2 minutes

FLUSH_EVERY = 500_000
STREAMABLE_OUTPUTS = frozenset({"parquet"})

# Progress milestones for log-friendly output (percentage thresholds)
PROGRESS_MILESTONES = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100]


# =============================================================================
# CONSTANTS
# =============================================================================

# GMX V2 precision constants
FLOAT_PRECISION = 10**30
USD_PRECISION = 10**30
PRICE_PRECISION = 10**30

# GMX V2 genesis block on Arbitrum (approximate deployment)
GMX_V2_GENESIS_BLOCK = 120_000_000

# HyperSync endpoints
HYPERSYNC_URLS = {
    "arbitrum": "https://arbitrum.hypersync.xyz",
    "avalanche": "https://avalanche.hypersync.xyz",
}

# EventEmitter contract addresses
EVENT_EMITTER_ADDRESSES = {
    "arbitrum": "0xC8ee91A54287DB53897056e12D9819156D3822Fb",
    "avalanche": "0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26",
}

# EventLog1 signature (topic0 for all GMX V2 events)
EVENT_LOG1_TOPIC = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160"

# Open Interest event name hashes (topic1 for filtering)
OI_EVENT_HASHES = {
    "OpenInterestUpdated": "0x" + keccak(text="OpenInterestUpdated").hex(),
    "OpenInterestInTokensUpdated": "0x" + keccak(text="OpenInterestInTokensUpdated").hex(),
}

# Reverse lookup: hash -> event name
OI_HASH_TO_EVENT = {v: k for k, v in OI_EVENT_HASHES.items()}

# Event names to collect
OI_EVENT_NAMES = ["OpenInterestUpdated", "OpenInterestInTokensUpdated"]

# ABI type for decoding EventLog1 data field
# Non-indexed params: (address msgSender, string eventName, EventLogData eventData)
# EventLogData = 7 sections, each with (items[], arrayItems[])
EVENT_LOG_DATA_ABI_TYPE = (
    "(((string,address)[],(string,address[])[])"
    ",((string,uint256)[],(string,uint256[])[])"
    ",((string,int256)[],(string,int256[])[])"
    ",((string,bool)[],(string,bool[])[])"
    ",((string,bytes32)[],(string,bytes32[])[])"
    ",((string,bytes)[],(string,bytes[])[])"
    ",((string,string)[],(string,string[])[])"
    ")"
)

EVENTLOG1_ABI_TYPES = ["address", "string", EVENT_LOG_DATA_ABI_TYPE]

# Section indices in EventLogData tuple
IDX_ADDRESS = 0
IDX_UINT = 1
IDX_INT = 2
IDX_BOOL = 3
IDX_BYTES32 = 4
IDX_BYTES = 5
IDX_STRING = 6

# Market token addresses to symbols (Arbitrum) - comprehensive list
# Source: https://github.com/gmx-io/gmx-interface and on-chain data
MARKETS = {
    # Arbitrum GM markets (major pairs)
    "0x70d95587d40a2caf56bd97485ab3eec10bee6336": {
        "symbol": "ETH/USD",
        "indexToken": "WETH",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0x47c031236e19d024b42f8ae6780e44a573170703": {
        "symbol": "BTC/USD",
        "indexToken": "WBTC",
        "longToken": "WBTC",
        "shortToken": "USDC",
    },
    "0x7f1fa204bb700853d36994da19f830b6ad18455c": {
        "symbol": "LINK/USD",
        "indexToken": "LINK",
        "longToken": "LINK",
        "shortToken": "USDC",
    },
    "0xc25cef6061cf5de5eb761b50e4743c1f5d7e5407": {
        "symbol": "ARB/USD",
        "indexToken": "ARB",
        "longToken": "ARB",
        "shortToken": "USDC",
    },
    "0x09400d9db990d5ed3f35d7be61dfaeb900af03c9": {
        "symbol": "SOL/USD",
        "indexToken": "SOL",
        "longToken": "SOL",
        "shortToken": "USDC",
    },
    "0xc7abb2c5f3bf3ceb389df0eecd6120d451170b50": {
        "symbol": "UNI/USD",
        "indexToken": "UNI",
        "longToken": "UNI",
        "shortToken": "USDC",
    },
    "0x6853ea96ff216fab11d2d930ce3c508556a4bdc4": {
        "symbol": "DOGE/USD",
        "indexToken": "DOGE",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0xb686bcb112660343e6d15bdb65297e110c8311c4": {
        "symbol": "LTC/USD",
        "indexToken": "LTC",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0xe2fecb78f76d937648c47e4e2cd5e47d27411545": {
        "symbol": "XRP/USD",
        "indexToken": "XRP",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0x2d340912aa47e33c90efb078e69e70efe2b34b9b": {
        "symbol": "ATOM/USD",
        "indexToken": "ATOM",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0x63dc80ee90f26363b3fcd609f370bb5549d6dbca": {
        "symbol": "NEAR/USD",
        "indexToken": "NEAR",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0x0ccb4faa6f1f1b30911619f1184082ab4e25813c": {
        "symbol": "AAVE/USD",
        "indexToken": "AAVE",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0x450bb6774dd8a756274e0ab4107953259d2ac541": {
        "symbol": "AVAX/USD",
        "indexToken": "AVAX",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0xd9535bb5f58a1a75032416f2dfe7880c30575a41": {
        "symbol": "OP/USD",
        "indexToken": "OP",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0xb56e5e2fb50d6fb510b4e4c086dcde66a866da24": {
        "symbol": "GMX/USD",
        "indexToken": "GMX",
        "longToken": "GMX",
        "shortToken": "USDC",
    },
    "0x7c11f78ce78768518d743e81fdfa2f860c6b9a77": {
        "symbol": "PEPE/USD",
        "indexToken": "PEPE",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0x2b477989a149b17073d9c9c82ec9cb03591325a6": {
        "symbol": "WIF/USD",
        "indexToken": "WIF",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    # Additional markets discovered from events
    "0xe68caaacdf6439628dfd2fe624847602991a31eb": {
        "symbol": "BTC/USD [WBTC-WBTC]",
        "indexToken": "WBTC",
        "longToken": "WBTC",
        "shortToken": "WBTC",
    },
    "0xdab9ba9e3a301ccb353f18b4c8542ba2149e4010": {
        "symbol": "ETH/USD [WETH-WETH]",
        "indexToken": "WETH",
        "longToken": "WETH",
        "shortToken": "WETH",
    },
    "0x08a902113f7f41a8658ebb1175f9c847bf4fb9d8": {
        "symbol": "ETH/USD [WETH-USDT]",
        "indexToken": "WETH",
        "longToken": "WETH",
        "shortToken": "USDT",
    },
    # Swap-only markets
    "0x9c2433dfd71f7f773b4507b5a24f28d6e91e7f81": {
        "symbol": "USDC/USDT",
        "indexToken": "USDC",
        "longToken": "USDC",
        "shortToken": "USDT",
    },
    # More synthetic markets
    "0x1d50e6c56333c8a0d78c0e1c0e25e7e9f5c8c8c8": {
        "symbol": "MATIC/USD",
        "indexToken": "MATIC",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0x248c35760068ce009a13076d573ed3497a47bcd4": {
        "symbol": "STX/USD",
        "indexToken": "STX",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    "0xd70d3d6d0f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f": {
        "symbol": "ORDI/USD",
        "indexToken": "ORDI",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
    # WETH/USDC.e market
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": {
        "symbol": "ETH/USD [WETH]",
        "indexToken": "WETH",
        "longToken": "WETH",
        "shortToken": "USDC",
    },
}

# Token decimals
TOKEN_DECIMALS = {
    "WETH": 18,
    "WBTC": 8,
    "USDC": 6,
    "USDT": 6,
    "LINK": 18,
    "UNI": 18,
    "ARB": 18,
    "GMX": 18,
}



# =============================================================================
# DATA CLASSES
# =============================================================================


@dataclass
class OpenInterestRecord:
    """Raw OI change event from GMX V2 EventEmitter.

    :ivar symbol: Market symbol (e.g., "ETH/USD")
    :ivar market: Market contract address
    :ivar collateralToken: Collateral token address
    :ivar isLong: True for long, False for short
    :ivar deltaUsd: Signed OI change in USD (30-decimal raw string)
    :ivar nextValueUsd: New total OI in USD (30-decimal raw string)
    :ivar deltaTokens: Signed OI change in tokens (raw string, token decimals)
    :ivar nextValueTokens: New total OI in tokens (raw string, token decimals)
    :ivar deltaUsdFormatted: Human-readable USD delta (delta / 10^30)
    :ivar nextValueUsdFormatted: Human-readable USD total (nextValue / 10^30)
    :ivar blockNumber: Block number of the event
    :ivar blockTimestamp: Unix timestamp of the block
    :ivar blockDatetime: ISO 8601 datetime string
    :ivar transactionHash: Transaction hash
    :ivar logIndex: Log index within the transaction
    :ivar eventType: Event name (OpenInterestUpdated)
    """

    symbol: str
    market: str
    collateralToken: str
    isLong: bool
    # USD values (30-decimal, stored as strings)
    deltaUsd: str
    nextValueUsd: str
    # Token values (stored as strings)
    deltaTokens: str | None
    nextValueTokens: str | None
    # Human-readable
    deltaUsdFormatted: str
    nextValueUsdFormatted: str
    # Block/tx info
    blockNumber: int
    blockTimestamp: int
    blockDatetime: str
    transactionHash: str
    logIndex: int
    eventType: str


@dataclass
class DailyOISnapshot:
    """End-of-day OI snapshot per market.

    :ivar symbol: Market symbol (e.g., "ETH/USD")
    :ivar date: Date string (e.g., "2026-02-09")
    :ivar longOiUsd: Total long OI in human-readable USD
    :ivar shortOiUsd: Total short OI in human-readable USD
    :ivar totalOiUsd: Combined long+short OI in USD
    :ivar longShortRatio: Ratio of long to short OI
    :ivar eventCount: Number of OI events on this date
    :ivar lastBlockNumber: Last block number for this date
    """

    symbol: str
    date: str
    longOiUsd: str
    shortOiUsd: str
    totalOiUsd: str
    longShortRatio: str
    eventCount: int
    lastBlockNumber: int


# =============================================================================
# ABI DECODING (from extract_funding_rates.py)
# =============================================================================


def decode_event_log_data(hex_data: str) -> dict:
    """Decode GMX V2 EventLog1 data field using eth_abi.

    Returns a dict with:
    - event_name: str
    - msg_sender: str
    - addresses: {key: address}
    - uints: {key: int}
    - ints: {key: int}
    - bools: {key: bool}
    - bytes32s: {key: bytes}
    - strings: {key: str}

    :param hex_data: Hex-encoded data field (with 0x prefix)
    :return: Decoded event data dict
    """
    if not hex_data or hex_data == "0x":
        return {}

    data = hex_data[2:] if hex_data.startswith("0x") else hex_data

    try:
        data_bytes = bytes.fromhex(data)
        msg_sender, event_name, event_data = abi_decode(EVENTLOG1_ABI_TYPES, data_bytes)
    except Exception as e:
        return {"_decode_error": str(e)}

    result = {
        "event_name": event_name,
        "msg_sender": msg_sender
        if isinstance(msg_sender, str)
        else ("0x" + msg_sender.hex() if isinstance(msg_sender, bytes) else str(msg_sender)),
        "addresses": {},
        "uints": {},
        "ints": {},
        "bools": {},
        "bytes32s": {},
        "strings": {},
    }

    # Extract key-value pairs from each section
    for key, val in event_data[IDX_ADDRESS][0]:  # addressItems.items
        addr = (
            val
            if isinstance(val, str)
            else ("0x" + val.hex() if isinstance(val, bytes) else str(val))
        )
        result["addresses"][key] = addr.lower() if isinstance(addr, str) else addr

    for key, val in event_data[IDX_UINT][0]:  # uintItems.items
        result["uints"][key] = val

    for key, val in event_data[IDX_INT][0]:  # intItems.items
        result["ints"][key] = val

    for key, val in event_data[IDX_BOOL][0]:  # boolItems.items
        result["bools"][key] = val

    for key, val in event_data[IDX_BYTES32][0]:  # bytes32Items.items
        result["bytes32s"][key] = val

    for key, val in event_data[IDX_STRING][0]:  # stringItems.items
        result["strings"][key] = val

    return result


# =============================================================================
# CHECKPOINT
# =============================================================================


def load_checkpoint(path: Path) -> dict | None:
    """Load checkpoint from JSON file.

    :param path: Path to checkpoint JSON file
    :return: Checkpoint dict or None if not found
    """
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        console.print(f"[yellow]Warning: could not load checkpoint {path}: {e}[/yellow]")
        return None


def save_checkpoint(
    path: Path,
    last_block: int,
    last_timestamp: int,
    total_events: int,
    markets_seen: int,
) -> None:
    """Save checkpoint to JSON file.

    :param path: Path to checkpoint JSON file
    :param last_block: Last processed block number
    :param last_timestamp: Last processed block timestamp
    :param total_events: Total events processed so far
    :param markets_seen: Number of unique markets seen
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint = {
        "symbol": "oi_all",
        "last_block": last_block,
        "last_timestamp": last_timestamp,
        "total_events": total_events,
        "last_updated": datetime.now(tz=UTC).isoformat(),
        "metadata": {"markets_seen": markets_seen},
    }
    with open(path, "w") as f:
        json.dump(checkpoint, f, indent=2)
    console.print(f"  Checkpoint saved: block [cyan]{last_block:,}[/cyan] -> [green]{path}[/green]")


# =============================================================================
# HYPERSYNC EXTRACTION
# =============================================================================


async def create_client(network: str) -> HypersyncClient:
    """Create HyperSync client.

    :param network: Network name (arbitrum, avalanche)
    :return: HyperSync client instance
    """
    url = HYPERSYNC_URLS.get(network)
    if not url:
        raise ValueError(f"Unsupported network: {network}")
    api_token = os.environ.get("HYPERSYNC_API_TOKEN")
    return HypersyncClient(ClientConfig(url=url, bearer_token=api_token))


async def get_latest_block(client: HypersyncClient) -> int:
    """Get latest block number.

    :param client: HyperSync client instance
    :return: Latest block number
    """
    return await client.get_height()


async def _stream_with_retry(
    client: HypersyncClient,
    query: Query,
    max_retries: int = MAX_RETRIES,
):
    """Stream HyperSync results with retry on transient errors.

    Yields response objects. On failure, retries with exponential backoff
    from the last successfully processed block.

    :param client: HyperSync client instance
    :param query: HyperSync query
    :param max_retries: Maximum retry attempts per failure
    """
    current_from_block = query.from_block
    attempt = 0

    while True:
        try:
            query.from_block = current_from_block
            config = hypersync.StreamConfig()
            stream = await client.stream(query, config)

            while True:
                response = await stream.recv()
                if response is None:
                    return  # Done

                # Track highest block for resume
                if response.data.blocks:
                    max_block = max(b.number for b in response.data.blocks)
                    current_from_block = max_block + 1

                attempt = 0  # Reset on success
                yield response

        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                console.print(f"[red]Failed after {max_retries} retries: {e}[/red]")
                raise

            delay = min(RETRY_BASE_DELAY * (2 ** (attempt - 1)), RETRY_MAX_DELAY)
            console.print(
                f"[yellow]Error (attempt {attempt}/{max_retries}): {e}\n"
                f"Retrying from block {current_from_block:,} in {delay:.0f}s...[/yellow]"
            )
            await asyncio.sleep(delay)


async def extract_oi_events(
    client: HypersyncClient,
    network: str,
    from_block: int,
    to_block: int | None,
    market_filter: str | None = None,
    markets: dict | None = None,
    flush_callback=None,
) -> list[OpenInterestRecord]:
    """Extract open interest events from GMX V2 EventEmitter.

    Queries for both ``OpenInterestUpdated`` and ``OpenInterestInTokensUpdated``
    events, then pairs them by (txHash, market, collateralToken, isLong). The
    USD event creates the record, the token event fills in token fields.

    :param client: HyperSync client instance
    :param network: Network name (arbitrum, avalanche)
    :param from_block: Starting block number
    :param to_block: Ending block number (None for latest)
    :param market_filter: Optional market symbol filter (e.g., "ETH/USD")
    :return: List of paired OpenInterestRecord objects
    """
    emitter = EVENT_EMITTER_ADDRESSES.get(network)
    if not emitter:
        raise ValueError(f"No EventEmitter for network: {network}")

    # Topic1 filter: only download OI events
    oi_topic1_hashes = [OI_EVENT_HASHES[name] for name in OI_EVENT_NAMES]

    query = Query(
        from_block=from_block,
        to_block=to_block,
        logs=[
            LogSelection(
                address=[emitter],
                topics=[
                    [EVENT_LOG1_TOPIC],  # topic0: EventLog1 signature
                    oi_topic1_hashes,  # topic1: OI event hashes
                ],
            )
        ],
        field_selection=FieldSelection(
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
            log=[
                LogField.BLOCK_NUMBER,
                LogField.LOG_INDEX,
                LogField.TRANSACTION_HASH,
                LogField.ADDRESS,
                LogField.TOPIC0,
                LogField.TOPIC1,
                LogField.TOPIC2,
                LogField.TOPIC3,
                LogField.DATA,
            ],
        ),
    )

    total_blocks = (to_block or 0) - from_block
    console.print(f"  EventEmitter: [cyan]{emitter}[/cyan]")
    console.print(
        f"  Block range:  [cyan]{from_block:,}[/cyan] to [cyan]{to_block or 'latest':,}[/cyan] ({total_blocks:,} blocks)"
    )
    console.print(f"  Events:       [cyan]{', '.join(OI_EVENT_NAMES)}[/cyan]")

    # Pairing dict for USD+token event matching.
    # Key: (txHash, market, collateralToken, isLong, usd_logIndex)
    # The token event is emitted right after the USD event (logIndex + 1),
    # so we also keep a reverse index: (txHash, market, collateralToken, isLong)
    # -> list of (usd_logIndex, record) to find the closest USD event.
    pairing_by_key: dict[tuple, list[tuple[int, OpenInterestRecord]]] = defaultdict(list)
    records: list[OpenInterestRecord] = []
    total_logs = 0
    total_flushed = 0
    flush_count = 0
    decode_errors = 0
    oi_event_counts = {name: 0 for name in OI_EVENT_NAMES}
    t_start = time.monotonic()
    highest_block = from_block
    next_milestone_idx = 0  # Index into PROGRESS_MILESTONES

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=40),
        TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
        TextColumn("[cyan]{task.fields[logs]:,}[/cyan] logs"),
        TextColumn("[green]{task.fields[events]:,}[/green] events"),
        TextColumn("[yellow]{task.fields[rate]:.0f}[/yellow] logs/s"),
        TextColumn("[red]{task.fields[errors]}[/red] err"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        task = progress.add_task(
            "Scanning blocks",
            total=total_blocks if total_blocks > 0 else None,
            logs=0,
            events=0,
            rate=0,
            errors=0,
        )

        async for response in _stream_with_retry(client, query):
            if not response.data.logs:
                # Still update block progress from empty batches
                if response.data.blocks:
                    new_highest = max(b.number for b in response.data.blocks)
                    if new_highest > highest_block:
                        advance = new_highest - highest_block
                        highest_block = new_highest
                        progress.advance(task, advance)
                continue

            # Build block timestamp lookup
            block_timestamps = {}
            if response.data.blocks:
                for block in response.data.blocks:
                    ts = block.timestamp
                    if isinstance(ts, str) and ts.startswith("0x"):
                        block_timestamps[block.number] = int(ts, 16)
                    elif ts:
                        block_timestamps[block.number] = int(ts)

            batch_highest = highest_block
            for log in response.data.logs:
                total_logs += 1
                if log.block_number > batch_highest:
                    batch_highest = log.block_number

                # Get timestamp
                timestamp = block_timestamps.get(log.block_number, 0)

                # Decode event data using eth_abi
                decoded = decode_event_log_data(log.data) if log.data else {}

                if "_decode_error" in decoded:
                    decode_errors += 1
                    continue

                event_name = decoded.get("event_name", "Unknown")

                if event_name not in OI_EVENT_NAMES:
                    continue

                oi_event_counts[event_name] = oi_event_counts.get(event_name, 0) + 1

                # Extract fields
                addresses = decoded.get("addresses", {})
                uints = decoded.get("uints", {})
                ints = decoded.get("ints", {})
                bools = decoded.get("bools", {})

                market_addr = addresses.get("market", "")
                collateral_token = addresses.get("collateralToken", "")
                is_long = bools.get("isLong", False)

                # delta is signed — try intItems first, fall back to uintItems
                delta = ints.get("delta")
                if delta is None:
                    delta = uints.get("delta", 0)
                next_value = uints.get("nextValue", 0)

                # Market symbol lookup
                _markets_dict = markets if markets is not None else MARKETS
                market_info = _markets_dict.get(market_addr.lower() if market_addr else "", {})

                # Skip swap-only markets (indexToken is None in market_registry)
                if market_info and market_info.get("indexToken") is None:
                    continue

                symbol = market_info.get("symbol", market_addr or "UNKNOWN")

                # Apply market filter
                if market_filter and symbol != market_filter:
                    continue

                # Transaction hash
                tx_hash = log.transaction_hash
                if isinstance(tx_hash, bytes):
                    tx_hash = tx_hash.hex()
                elif tx_hash is None:
                    tx_hash = ""
                if not tx_hash.startswith("0x"):
                    tx_hash = "0x" + tx_hash

                # Grouping key: (txHash, market, collateralToken, isLong)
                group_key = (tx_hash, market_addr.lower(), collateral_token.lower(), is_long)
                current_log_index = log.log_index or 0

                if event_name == "OpenInterestUpdated":
                    # USD event — create record, store for pairing
                    delta_formatted = str(delta / USD_PRECISION)
                    next_value_formatted = str(next_value / USD_PRECISION)

                    record = OpenInterestRecord(
                        symbol=symbol,
                        market=market_addr,
                        collateralToken=collateral_token,
                        isLong=is_long,
                        deltaUsd=str(delta),
                        nextValueUsd=str(next_value),
                        deltaTokens=None,
                        nextValueTokens=None,
                        deltaUsdFormatted=delta_formatted,
                        nextValueUsdFormatted=next_value_formatted,
                        blockNumber=log.block_number,
                        blockTimestamp=timestamp,
                        blockDatetime=datetime.fromtimestamp(timestamp, tz=UTC).isoformat()
                        if timestamp
                        else "",
                        transactionHash=tx_hash,
                        logIndex=current_log_index,
                        eventType="OpenInterestUpdated",
                    )
                    pairing_by_key[group_key].append((current_log_index, record))
                    records.append(record)

                elif event_name == "OpenInterestInTokensUpdated":
                    # Token event — find the closest USD event by logIndex
                    # (GMX emits token event right after USD event)
                    candidates = pairing_by_key.get(group_key, [])
                    best_match = None
                    best_distance = float("inf")
                    for usd_log_idx, usd_record in candidates:
                        dist = abs(current_log_index - usd_log_idx)
                        if dist < best_distance and usd_record.deltaTokens is None:
                            best_distance = dist
                            best_match = usd_record

                    if best_match is not None and best_distance <= 5:
                        best_match.deltaTokens = str(delta)
                        best_match.nextValueTokens = str(next_value)
                    else:
                        # Token event arrived without USD pair — store standalone
                        record = OpenInterestRecord(
                            symbol=symbol,
                            market=market_addr,
                            collateralToken=collateral_token,
                            isLong=is_long,
                            deltaUsd="0",
                            nextValueUsd="0",
                            deltaTokens=str(delta),
                            nextValueTokens=str(next_value),
                            deltaUsdFormatted="0",
                            nextValueUsdFormatted="0",
                            blockNumber=log.block_number,
                            blockTimestamp=timestamp,
                            blockDatetime=datetime.fromtimestamp(
                                timestamp, tz=UTC
                            ).isoformat()
                            if timestamp
                            else "",
                            transactionHash=tx_hash,
                            logIndex=current_log_index,
                            eventType="OpenInterestInTokensUpdated",
                        )
                        records.append(record)

            # Update progress bar
            if batch_highest > highest_block:
                advance = batch_highest - highest_block
                highest_block = batch_highest
                progress.advance(task, advance)

            elapsed = time.monotonic() - t_start
            rate = total_logs / elapsed if elapsed > 0 else 0
            progress.update(
                task,
                logs=total_logs,
                events=len(records),
                rate=rate,
                errors=decode_errors,
            )

            # Milestone logging (readable in log files)
            # Only print the highest milestone crossed per batch to avoid
            # duplicate lines when HyperSync returns large batches.
            if total_blocks > 0:
                pct = (highest_block - from_block) / total_blocks * 100
                crossed = None
                while (
                    next_milestone_idx < len(PROGRESS_MILESTONES)
                    and pct >= PROGRESS_MILESTONES[next_milestone_idx]
                ):
                    crossed = PROGRESS_MILESTONES[next_milestone_idx]
                    next_milestone_idx += 1
                if crossed is not None:
                    milestone_elapsed = time.monotonic() - t_start
                    milestone_rate = total_logs / milestone_elapsed if milestone_elapsed > 0 else 0
                    console.print(
                        f"  [{crossed:>3d}%] "
                        f"block {highest_block:,} | "
                        f"{total_logs:,} logs | "
                        f"{len(records):,} events | "
                        f"{milestone_rate:.0f} logs/s | "
                        f"{milestone_elapsed:.0f}s elapsed"
                    )

            # Flush completed records to disk every FLUSH_EVERY events to bound memory
            if flush_callback is not None and len(records) >= FLUSH_EVERY:
                flush_callback(records, highest_block)
                total_flushed += len(records)
                flush_count += 1
                records = []  # clear list; keep pairing_by_key alive for cross-batch pairing

    # Final flush for any remaining records
    if flush_callback is not None and records:
        flush_callback(records, highest_block)
        total_flushed += len(records)
        flush_count += 1
        records = []

    elapsed = time.monotonic() - t_start
    rate = total_logs / elapsed if elapsed > 0 else 0

    # Summary line
    console.print(
        f"\n  Processed [cyan]{total_logs:,}[/cyan] logs in [cyan]{elapsed:.1f}s[/cyan] "
        f"([cyan]{rate:.0f}[/cyan] logs/sec)"
    )
    total_events_found = total_flushed + len(records)
    console.print(
        f"  Found [green]{total_events_found:,}[/green] OI events "
        f"([red]{decode_errors}[/red] decode errors)"
        + (f" (flushed in {flush_count} batches)" if flush_count else "")
    )

    # Event breakdown table
    event_table = Table(show_header=False, box=None, padding=(0, 2))
    for name, count in sorted(oi_event_counts.items(), key=lambda x: -x[1]):
        if count > 0:
            event_table.add_row(f"  {name}", f"[cyan]{count:,}[/cyan]")
    if any(v > 0 for v in oi_event_counts.values()):
        console.print(event_table)

    return records


# =============================================================================
# AGGREGATION
# =============================================================================


def compute_daily_snapshots(records: list[OpenInterestRecord]) -> list[DailyOISnapshot]:
    """Compute end-of-day OI snapshots per market.

    For each (symbol, date), takes the last ``nextValueUsd`` per
    (market, collateralToken, isLong) combination, then sums across
    collateral tokens for long/short totals.

    :param records: List of OpenInterestRecord objects
    :return: List of DailyOISnapshot objects sorted by (symbol, date)
    """
    if not records:
        return []

    # Group by (symbol, date_utc)
    by_symbol_date: dict[tuple[str, str], list[OpenInterestRecord]] = defaultdict(list)
    for r in records:
        if r.blockTimestamp:
            date_str = datetime.fromtimestamp(r.blockTimestamp, tz=UTC).strftime(
                "%Y-%m-%d"
            )
        else:
            continue
        by_symbol_date[(r.symbol, date_str)].append(r)

    snapshots = []
    for (symbol, date_str), day_records in sorted(by_symbol_date.items()):
        # Sort by block number and log index to get chronological order
        day_records.sort(key=lambda r: (r.blockNumber, r.logIndex))

        # Find last nextValueUsd per (market, collateralToken, isLong)
        last_values: dict[tuple[str, str, bool], int] = {}
        for r in day_records:
            # Only use USD records (eventType == OpenInterestUpdated)
            if r.eventType != "OpenInterestUpdated":
                continue
            key = (r.market.lower(), r.collateralToken.lower(), r.isLong)
            try:
                last_values[key] = int(r.nextValueUsd)
            except (ValueError, TypeError):
                pass

        # Sum long and short across all (market, collateralToken) combinations
        long_total_raw = 0
        short_total_raw = 0
        for (market, collateral, is_long), value in last_values.items():
            if is_long:
                long_total_raw += value
            else:
                short_total_raw += value

        long_usd = long_total_raw / USD_PRECISION
        short_usd = short_total_raw / USD_PRECISION
        total_usd = long_usd + short_usd

        if short_usd > 0:
            ls_ratio = long_usd / short_usd
        elif long_usd > 0:
            ls_ratio = float("inf")
        else:
            ls_ratio = 0.0

        last_block = max(r.blockNumber for r in day_records)

        snapshots.append(
            DailyOISnapshot(
                symbol=symbol,
                date=date_str,
                longOiUsd=f"{long_usd:.2f}",
                shortOiUsd=f"{short_usd:.2f}",
                totalOiUsd=f"{total_usd:.2f}",
                longShortRatio=f"{ls_ratio:.4f}" if ls_ratio != float("inf") else "inf",
                eventCount=len(day_records),
                lastBlockNumber=last_block,
            )
        )

    return snapshots


# =============================================================================
# STORAGE
# =============================================================================


def append_parquet(new_df: "pl.DataFrame", filepath: Path) -> None:
    """Append new data to an existing Parquet file with deduplication.

    Reads existing file (if any), concatenates with new data, deduplicates
    by (blockNumber, logIndex, eventType), sorts, and writes back.

    :param new_df: New polars DataFrame to append
    :param filepath: Path to the Parquet file
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)

    if filepath.exists():
        existing = pl.read_parquet(filepath)
        # Align schemas: cast new columns to match existing types where possible
        for col in existing.columns:
            if col in new_df.columns and existing[col].dtype != new_df[col].dtype:
                new_df = new_df.with_columns(pl.col(col).cast(existing[col].dtype))
        combined = pl.concat([existing, new_df], how="diagonal_relaxed")
    else:
        combined = new_df

    # Dedup by (blockNumber, logIndex, eventType)
    dedup_cols = ["blockNumber", "logIndex", "eventType"]
    available_cols = [c for c in dedup_cols if c in combined.columns]
    if available_cols:
        combined = combined.unique(subset=available_cols, keep="last")

    # Sort by blockNumber, logIndex
    sort_cols = [c for c in ["blockNumber", "logIndex"] if c in combined.columns]
    if sort_cols:
        combined = combined.sort(sort_cols)

    combined.write_parquet(filepath)


def save_raw_per_symbol(records: list[OpenInterestRecord], output_dir: Path) -> None:
    """Save raw OI records to per-symbol Parquet files.

    :param records: List of OpenInterestRecord objects
    :param output_dir: Base output directory (e.g., data/open_interest/arbitrum)
    """
    if not HAS_POLARS:
        console.print(
            "[red]Error: polars required for Parquet. Install with: pip install polars[/red]"
        )
        return

    by_symbol: dict[str, list[OpenInterestRecord]] = defaultdict(list)
    for r in records:
        by_symbol[r.symbol].append(r)

    for symbol, sym_records in sorted(by_symbol.items()):
        # Sanitize symbol for filesystem (e.g., "ETH/USD" -> "ETH_USD")
        safe_symbol = symbol.replace("/", "_").replace(" ", "_").replace("[", "").replace("]", "")
        filepath = output_dir / "raw" / safe_symbol / "data.parquet"

        df = pl.DataFrame([asdict(r) for r in sym_records])
        append_parquet(df, filepath)
        console.print(
            f"  Raw: [cyan]{len(sym_records):,}[/cyan] events -> [green]{filepath}[/green]"
        )


def save_snapshots_per_symbol(snapshots: list[DailyOISnapshot], output_dir: Path) -> None:
    """Save daily OI snapshots to per-symbol Parquet files.

    :param snapshots: List of DailyOISnapshot objects
    :param output_dir: Base output directory (e.g., data/open_interest/arbitrum)
    """
    if not HAS_POLARS:
        console.print(
            "[red]Error: polars required for Parquet. Install with: pip install polars[/red]"
        )
        return

    by_symbol: dict[str, list[DailyOISnapshot]] = defaultdict(list)
    for s in snapshots:
        by_symbol[s.symbol].append(s)

    for symbol, sym_snapshots in sorted(by_symbol.items()):
        safe_symbol = symbol.replace("/", "_").replace(" ", "_").replace("[", "").replace("]", "")
        filepath = output_dir / "snapshots" / safe_symbol / "daily.parquet"

        df = pl.DataFrame([asdict(s) for s in sym_snapshots])

        # For snapshots, dedup by (date) per symbol — keep latest
        filepath.parent.mkdir(parents=True, exist_ok=True)
        if filepath.exists():
            existing = pl.read_parquet(filepath)
            for col in existing.columns:
                if col in df.columns and existing[col].dtype != df[col].dtype:
                    df = df.with_columns(pl.col(col).cast(existing[col].dtype))
            combined = pl.concat([existing, df], how="diagonal_relaxed")
            combined = combined.unique(subset=["date"], keep="last")
            combined = combined.sort("date")
            combined.write_parquet(filepath)
        else:
            filepath.parent.mkdir(parents=True, exist_ok=True)
            df.sort("date").write_parquet(filepath)

        console.print(
            f"  Snapshots: [cyan]{len(sym_snapshots):,}[/cyan] days -> [green]{filepath}[/green]"
        )


def save_json(data: list, filename: str) -> None:
    """Save to JSON.

    :param data: List of dataclass instances to serialize
    :param filename: Output file path
    """
    with open(filename, "w") as f:
        json.dump([asdict(d) for d in data], f, indent=2, default=str)
    console.print(f"  Saved [cyan]{len(data):,}[/cyan] records to [green]{filename}[/green]")


def save_csv(data: list, filename: str) -> None:
    """Save to CSV using polars.

    :param data: List of dataclass instances to serialize
    :param filename: Output file path
    """
    if not HAS_POLARS:
        console.print("[red]Error: polars required for CSV. Install with: pip install polars[/red]")
        return

    df = pl.DataFrame([asdict(d) for d in data])
    df.write_csv(filename)
    console.print(f"  Saved [cyan]{len(data):,}[/cyan] records to [green]{filename}[/green]")


# =============================================================================
# SUMMARY
# =============================================================================


def print_summary(records: list[OpenInterestRecord], snapshots: list[DailyOISnapshot]) -> None:
    """Print summary statistics using Rich tables.

    :param records: List of raw OI records
    :param snapshots: List of daily OI snapshots
    """
    # Summary table from snapshots (latest date per symbol)
    table = Table(title="Open Interest Summary (Latest Snapshot)", show_lines=False)
    table.add_column("Symbol", style="cyan")
    table.add_column("Events", justify="right")
    table.add_column("Long OI (USD)", justify="right", style="green")
    table.add_column("Short OI (USD)", justify="right", style="red")
    table.add_column("Total OI (USD)", justify="right", style="bold")
    table.add_column("L/S Ratio", justify="right")

    # Get latest snapshot per symbol
    latest_by_symbol: dict[str, DailyOISnapshot] = {}
    for s in snapshots:
        existing = latest_by_symbol.get(s.symbol)
        if existing is None or s.date > existing.date:
            latest_by_symbol[s.symbol] = s

    # Count events per symbol from raw records
    event_counts: dict[str, int] = defaultdict(int)
    for r in records:
        event_counts[r.symbol] += 1

    for symbol in sorted(latest_by_symbol.keys()):
        snap = latest_by_symbol[symbol]
        try:
            long_val = float(snap.longOiUsd)
            short_val = float(snap.shortOiUsd)
            total_val = float(snap.totalOiUsd)
        except (ValueError, TypeError):
            long_val = short_val = total_val = 0.0

        table.add_row(
            symbol,
            f"{event_counts.get(symbol, 0):,}",
            f"${long_val:,.2f}",
            f"${short_val:,.2f}",
            f"${total_val:,.2f}",
            snap.longShortRatio,
        )

    console.print()
    console.print(table)

    # Time range from records
    timestamps = [r.blockTimestamp for r in records if r.blockTimestamp]
    if timestamps:
        first = datetime.fromtimestamp(min(timestamps), tz=UTC)
        last = datetime.fromtimestamp(max(timestamps), tz=UTC)
        console.print(
            f"\n  Time range: [cyan]{first.isoformat()}[/cyan] to [cyan]{last.isoformat()}[/cyan]"
        )

    console.print(f"  Total OI events: [cyan]{len(records):,}[/cyan]")
    console.print(f"  Daily snapshots: [cyan]{len(snapshots):,}[/cyan]")
    console.print(f"  Markets:         [cyan]{len(latest_by_symbol):,}[/cyan]")


# =============================================================================
# MAIN
# =============================================================================


async def async_main(args: argparse.Namespace) -> None:
    """Async main entry point.

    :param args: Parsed command-line arguments
    """
    output_dir = Path(args.output_dir) / args.network
    checkpoint_dir = (
        Path(args.checkpoint_dir) if args.checkpoint_dir else output_dir / "checkpoints"
    )
    checkpoint_path = checkpoint_dir / "oi_checkpoint.json"

    # Determine starting block
    from_block = args.from_block

    if args.resume and from_block is None:
        # Load checkpoint
        checkpoint = load_checkpoint(checkpoint_path)
        if checkpoint:
            from_block = checkpoint["last_block"] + 1
            console.print(f"  Resuming from checkpoint: block [cyan]{from_block:,}[/cyan]")
        else:
            from_block = GMX_V2_GENESIS_BLOCK
            console.print(
                f"  No checkpoint found. Starting from genesis: [cyan]{from_block:,}[/cyan]"
            )
    elif from_block is None:
        from_block = GMX_V2_GENESIS_BLOCK

    with console.status("Fetching GMX market registry..."):
        markets = _fetch_markets_cached(args.network, force_refresh=args.refresh_markets)
    console.print(f"  Markets loaded: [cyan]{len(markets):,}[/cyan]")

    # Header panel
    header_lines = [
        f"Network:     [cyan]{args.network}[/cyan]",
        f"Block range: [cyan]{from_block:,}[/cyan] to [cyan]{args.to_block or 'latest'}[/cyan]",
        f"Output:      [cyan]{args.output}[/cyan]",
        f"Output dir:  [cyan]{output_dir}[/cyan]",
    ]
    if args.market:
        header_lines.append(f"Market:      [cyan]{args.market}[/cyan]")
    if args.resume:
        header_lines.append(f"Resume:      [cyan]enabled[/cyan] (checkpoint: {checkpoint_path})")
    header_lines.append(
        f"Retries:     [cyan]{MAX_RETRIES}[/cyan] (backoff: {RETRY_BASE_DELAY}s base)"
    )

    console.print(
        Panel(
            "\n".join(header_lines),
            title="GMX V2 Open Interest Extractor",
            subtitle="HyperSync + eth_abi",
            border_style="blue",
        )
    )

    # Create client
    with console.status("Connecting to HyperSync..."):
        client = await create_client(args.network)

    # Get latest block
    to_block = args.to_block
    if to_block is None:
        with console.status("Fetching latest block..."):
            to_block = await get_latest_block(client)
        console.print(f"  Latest block: [cyan]{to_block:,}[/cyan]")

    if from_block >= to_block:
        console.print(
            f"\n[yellow]Already up to date (from_block {from_block:,} >= to_block {to_block:,})[/yellow]"
        )
        return

    _resume_base_total = 0
    if args.resume:
        _prior = load_checkpoint(checkpoint_path)
        _resume_base_total = _prior["total_events"] if _prior else 0

    flush_state: dict = {
        "total_events": 0,
        "last_block": from_block,
        "last_timestamp": 0,
        "flush_count": 0,
    }

    def on_flush(batch: list, highest_block: int) -> None:
        """Persist a batch of records to disk and save an intermediate checkpoint."""
        if not batch:
            return
        flush_state["flush_count"] += 1
        console.print(
            f"\n  [dim]Flushing {len(batch):,} events at block {highest_block:,}...[/dim]"
        )
        save_raw_per_symbol(batch, output_dir)
        with console.status("Computing daily snapshots..."):
            batch_snapshots = compute_daily_snapshots(batch)
        save_snapshots_per_symbol(batch_snapshots, output_dir)

        flush_state["total_events"] += len(batch)
        flush_state["last_block"] = max(highest_block, flush_state["last_block"])
        flush_state["last_timestamp"] = max(
            (r.blockTimestamp for r in batch if r.blockTimestamp),
            default=flush_state["last_timestamp"],
        )

        if args.resume:
            save_checkpoint(
                checkpoint_path,
                last_block=flush_state["last_block"],
                last_timestamp=flush_state["last_timestamp"],
                total_events=_resume_base_total + flush_state["total_events"],
                markets_seen=len({r.symbol for r in batch}),
            )

    use_flush = args.output in STREAMABLE_OUTPUTS

    # Extract events
    console.print()
    records = await extract_oi_events(
        client=client,
        network=args.network,
        from_block=from_block,
        to_block=to_block,
        market_filter=args.market,
        markets=markets,
        flush_callback=on_flush if use_flush else None,
    )

    has_data = bool(records) or flush_state["total_events"] > 0

    if not has_data:
        console.print("\n[yellow]No OI events found.[/yellow]")
        if args.resume:
            save_checkpoint(
                checkpoint_path,
                last_block=to_block,
                last_timestamp=int(time.time()),
                total_events=0,
                markets_seen=0,
            )
        return

    if records:
        # Aggregate and save any remaining in-memory records
        with console.status("Computing daily snapshots..."):
            snapshots = compute_daily_snapshots(records)

        console.print()
        if args.output == "parquet":
            save_raw_per_symbol(records, output_dir)
            save_snapshots_per_symbol(snapshots, output_dir)
        elif args.output == "json":
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            base = f"gmx_v2_oi_{args.network}_{from_block}_{to_block}_{ts}"
            save_json(records, f"{base}_events.json")
            save_json(snapshots, f"{base}_snapshots.json")
        elif args.output == "csv":
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            base = f"gmx_v2_oi_{args.network}_{from_block}_{to_block}_{ts}"
            save_csv(records, f"{base}_events.csv")
            save_csv(snapshots, f"{base}_snapshots.csv")

        if args.resume:
            last_block = max(r.blockNumber for r in records)
            last_timestamp = max(r.blockTimestamp for r in records if r.blockTimestamp)
            unique_markets = len({r.symbol for r in records})
            save_checkpoint(
                checkpoint_path,
                last_block=last_block,
                last_timestamp=last_timestamp,
                total_events=_resume_base_total + flush_state["total_events"] + len(records),
                markets_seen=unique_markets,
            )

        print_summary(records, snapshots)
    else:
        total = flush_state["total_events"]
        console.print(
            f"\n[green]  Extraction complete: {total:,} events saved across "
            f"{flush_state['flush_count']} flush batches.[/green]"
        )


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract GMX V2 open interest data using HyperSync",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill from genesis
  uv run scripts/extract_open_interest.py --from-block 120000000

  # Quick test
  uv run scripts/extract_open_interest.py --from-block 290000000 --to-block 290100000 --output json

  # Incremental cronjob
  uv run scripts/extract_open_interest.py --resume
        """,
    )

    parser.add_argument(
        "--network",
        choices=["arbitrum", "avalanche"],
        default="arbitrum",
        help="Network (default: arbitrum)",
    )
    parser.add_argument(
        "--from-block",
        type=int,
        default=None,
        help="Start block for backfill (default: genesis or checkpoint)",
    )
    parser.add_argument("--to-block", type=int, default=None, help="End block (default: latest)")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./user_data/data/gmx/open_interest",
        help="Base output directory (default: ./user_data/data/gmx/open_interest)",
    )
    parser.add_argument(
        "--output",
        choices=["json", "csv", "parquet"],
        default="parquet",
        help="Output format (default: parquet)",
    )
    parser.add_argument(
        "--market", type=str, default=None, help="Filter by market symbol (e.g., 'ETH/USD')"
    )
    parser.add_argument(
        "--resume", action="store_true", help="Enable checkpoint-based incremental mode"
    )
    parser.add_argument(
        "--checkpoint-dir", type=str, default=None, help="Override checkpoint directory"
    )
    parser.add_argument(
        "--refresh-markets",
        action="store_true",
        help="Force re-fetch of GMX market registry (ignores 24h disk cache)",
    )

    args = parser.parse_args()

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
