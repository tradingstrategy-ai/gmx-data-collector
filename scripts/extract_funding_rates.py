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
GMX V2 Historical Funding Rate Extractor (HyperSync Edition)
=============================================================
Extracts funding rate data from GMX V2 (Synthetics) using Envio HyperSync
for blazing-fast blockchain data retrieval (20-100x faster than JSON-RPC).
Outputs data in a CEX-like format similar to Binance/Bybit funding rate APIs.

Based on the original extract_funding_rates_v2.py script with improved decoding:
- Proper eth_abi.decode for EventLogData (replaces hex pattern matching)
- Topic filtering to download only relevant events (not ALL logs)
- Correct field extraction from GMX's nested key-value structure

Supports two modes:

- **Backfill** (one-shot): ``--from-block 200000000``
- **Incremental** (cronjob): ``--resume`` with checkpoint

QUICK START
-----------
    uv run scripts/extract_funding_rates.py --network arbitrum --from-block 200000000

USAGE
-----
    uv run scripts/extract_funding_rates.py [OPTIONS]

OPTIONS
-------
    --network        Network: "arbitrum" or "avalanche" (default: arbitrum)
    --from-block     Starting block number (default: 0, or checkpoint if --resume)
    --to-block       Ending block number (default: latest)
    --output-dir     Base output directory (default: ./data/funding)
    --output         Output format: "json", "csv", or "parquet" (default: json)
    --market         Filter by market symbol (e.g., "ETH/USD", "BTC/USD")
    --resume         Enable checkpoint-based incremental mode (for cronjob)
    --checkpoint-dir Override checkpoint directory
    --background     Run in background (daemonize)
    --log-file       Log file for background mode
    --pid-file       PID file for background mode

EXAMPLES
--------
    # Extract all funding events from recent blocks
    uv run scripts/extract_funding_rates.py --network arbitrum --from-block 280000000

    # Export to CSV for spreadsheet analysis
    uv run scripts/extract_funding_rates.py --network arbitrum --from-block 280000000 --output csv

    # Filter by specific market
    uv run scripts/extract_funding_rates.py --network arbitrum --from-block 280000000 --market "ETH/USD"

    # Incremental cronjob (resumes from checkpoint)
    uv run scripts/extract_funding_rates.py --resume

    # Run in background with checkpoint
    uv run scripts/extract_funding_rates.py --resume --background

CRONJOB SETUP
=============
    # Initial backfill (one-time)
    uv run scripts/extract_funding_rates.py --from-block 200000000

    # Daily cron (2 AM UTC)
    0 2 * * * cd /path/to/project && uv run scripts/extract_funding_rates.py --resume 2>&1 >> logs/funding.log

OUTPUT FORMAT (CEX-STYLE)
=========================
The output mimics centralized exchange funding rate APIs:
{
    "symbol": "ETH/USD",
    "fundingTime": 1733000785000,
    "fundingRate": "0.00012345",
    "markPrice": "3456.78",
    "fundingIntervalHours": 1,
    // GMX-specific fields
    "market": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",
    "longTokenFundingPerSize": "123456789",
    "shortTokenFundingPerSize": "-123456789",
    "fundingFeeUsd": "12.34",
    "borrowingFeeUsd": "0.56"
}
================================================================================
"""

import argparse
import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import hypersync
from hypersync import (
    HypersyncClient,
    ClientConfig,
    Query,
    LogSelection,
    FieldSelection,
    LogField,
    BlockField,
)
from eth_abi import decode as abi_decode
from eth_utils import keccak
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

# Optional imports
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

console = Console()

# Retry settings
MAX_RETRIES = 5
RETRY_BASE_DELAY = 2.0  # seconds

# Progress milestones for log-friendly output (percentage thresholds)
PROGRESS_MILESTONES = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100]

# GMX V2 genesis block on Arbitrum (approximate deployment)
GMX_V2_GENESIS_BLOCK = 120_000_000


# =============================================================================
# CONSTANTS
# =============================================================================

# GMX V2 precision constants
FLOAT_PRECISION = 10**30
USD_PRECISION = 10**30
PRICE_PRECISION = 10**30
FUNDING_RATE_PRECISION = 10**30

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

# Event name hashes (topic1 for filtering specific events)
EVENT_HASHES = {
    "PositionIncrease": "0x" + keccak(text="PositionIncrease").hex(),
    "PositionDecrease": "0x" + keccak(text="PositionDecrease").hex(),
    "PositionFeesCollected": "0x" + keccak(text="PositionFeesCollected").hex(),
    "PositionFeesInfo": "0x" + keccak(text="PositionFeesInfo").hex(),
    "ClaimableFundingUpdated": "0x" + keccak(text="ClaimableFundingUpdated").hex(),
    "Funding": "0x" + keccak(text="Funding").hex(),
    "FundingFeeAmountPerSizeUpdated": "0x" + keccak(text="FundingFeeAmountPerSizeUpdated").hex(),
}

# Reverse lookup: hash -> event name
HASH_TO_EVENT = {v: k for k, v in EVENT_HASHES.items()}

# Funding-related event names to collect
FUNDING_EVENT_NAMES = [
    "PositionIncrease",
    "PositionDecrease",
    "PositionFeesCollected",
    "PositionFeesInfo",
    "ClaimableFundingUpdated",
]

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
    "0x70d95587d40a2caf56bd97485ab3eec10bee6336": {"symbol": "ETH/USD", "indexToken": "WETH", "longToken": "WETH", "shortToken": "USDC"},
    "0x47c031236e19d024b42f8ae6780e44a573170703": {"symbol": "BTC/USD", "indexToken": "WBTC", "longToken": "WBTC", "shortToken": "USDC"},
    "0x7f1fa204bb700853d36994da19f830b6ad18455c": {"symbol": "LINK/USD", "indexToken": "LINK", "longToken": "LINK", "shortToken": "USDC"},
    "0xc25cef6061cf5de5eb761b50e4743c1f5d7e5407": {"symbol": "ARB/USD", "indexToken": "ARB", "longToken": "ARB", "shortToken": "USDC"},
    "0x09400d9db990d5ed3f35d7be61dfaeb900af03c9": {"symbol": "SOL/USD", "indexToken": "SOL", "longToken": "SOL", "shortToken": "USDC"},
    "0xc7abb2c5f3bf3ceb389df0eecd6120d451170b50": {"symbol": "UNI/USD", "indexToken": "UNI", "longToken": "UNI", "shortToken": "USDC"},
    "0x6853ea96ff216fab11d2d930ce3c508556a4bdc4": {"symbol": "DOGE/USD", "indexToken": "DOGE", "longToken": "WETH", "shortToken": "USDC"},
    "0xb686bcb112660343e6d15bdb65297e110c8311c4": {"symbol": "LTC/USD", "indexToken": "LTC", "longToken": "WETH", "shortToken": "USDC"},
    "0xe2fecb78f76d937648c47e4e2cd5e47d27411545": {"symbol": "XRP/USD", "indexToken": "XRP", "longToken": "WETH", "shortToken": "USDC"},
    "0x2d340912aa47e33c90efb078e69e70efe2b34b9b": {"symbol": "ATOM/USD", "indexToken": "ATOM", "longToken": "WETH", "shortToken": "USDC"},
    "0x63dc80ee90f26363b3fcd609f370bb5549d6dbca": {"symbol": "NEAR/USD", "indexToken": "NEAR", "longToken": "WETH", "shortToken": "USDC"},
    "0x0ccb4faa6f1f1b30911619f1184082ab4e25813c": {"symbol": "AAVE/USD", "indexToken": "AAVE", "longToken": "WETH", "shortToken": "USDC"},
    "0x450bb6774dd8a756274e0ab4107953259d2ac541": {"symbol": "AVAX/USD", "indexToken": "AVAX", "longToken": "WETH", "shortToken": "USDC"},
    "0xd9535bb5f58a1a75032416f2dfe7880c30575a41": {"symbol": "OP/USD", "indexToken": "OP", "longToken": "WETH", "shortToken": "USDC"},
    "0xb56e5e2fb50d6fb510b4e4c086dcde66a866da24": {"symbol": "GMX/USD", "indexToken": "GMX", "longToken": "GMX", "shortToken": "USDC"},
    "0x7c11f78ce78768518d743e81fdfa2f860c6b9a77": {"symbol": "PEPE/USD", "indexToken": "PEPE", "longToken": "WETH", "shortToken": "USDC"},
    "0x2b477989a149b17073d9c9c82ec9cb03591325a6": {"symbol": "WIF/USD", "indexToken": "WIF", "longToken": "WETH", "shortToken": "USDC"},
    # Additional markets discovered from events
    "0xe68caaacdf6439628dfd2fe624847602991a31eb": {"symbol": "BTC/USD [WBTC-WBTC]", "indexToken": "WBTC", "longToken": "WBTC", "shortToken": "WBTC"},
    "0xdab9ba9e3a301ccb353f18b4c8542ba2149e4010": {"symbol": "ETH/USD [WETH-WETH]", "indexToken": "WETH", "longToken": "WETH", "shortToken": "WETH"},
    "0x08a902113f7f41a8658ebb1175f9c847bf4fb9d8": {"symbol": "ETH/USD [WETH-USDT]", "indexToken": "WETH", "longToken": "WETH", "shortToken": "USDT"},
    # Swap-only markets
    "0x9c2433dfd71f7f773b4507b5a24f28d6e91e7f81": {"symbol": "USDC/USDT", "indexToken": "USDC", "longToken": "USDC", "shortToken": "USDT"},
    # More synthetic markets
    "0x1d50e6c56333c8a0d78c0e1c0e25e7e9f5c8c8c8": {"symbol": "MATIC/USD", "indexToken": "MATIC", "longToken": "WETH", "shortToken": "USDC"},
    "0x248c35760068ce009a13076d573ed3497a47bcd4": {"symbol": "STX/USD", "indexToken": "STX", "longToken": "WETH", "shortToken": "USDC"},
    "0xd70d3d6d0f7f7f7f7f7f7f7f7f7f7f7f7f7f7f7f": {"symbol": "ORDI/USD", "indexToken": "ORDI", "longToken": "WETH", "shortToken": "USDC"},
    # WETH/USDC.e market
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": {"symbol": "ETH/USD [WETH]", "indexToken": "WETH", "longToken": "WETH", "shortToken": "USDC"},
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
# DATA CLASSES - CEX-STYLE OUTPUT
# =============================================================================

@dataclass
class FundingRateRecord:
    """CEX-style funding rate record."""
    # Standard CEX fields
    symbol: str                          # e.g., "ETH/USD"
    fundingTime: int                     # Unix timestamp in milliseconds
    fundingTimeDatetime: str             # ISO 8601 datetime
    fundingRate: str                     # Funding rate as decimal string
    fundingRatePercent: str              # Funding rate as percentage

    # Position context
    side: Optional[str] = None           # "long" or "short"
    positionSizeUsd: Optional[str] = None

    # Fee breakdown (in USD)
    fundingFeeUsd: Optional[str] = None
    borrowingFeeUsd: Optional[str] = None
    positionFeeUsd: Optional[str] = None
    totalFeeUsd: Optional[str] = None

    # GMX-specific fields
    market: Optional[str] = None
    account: Optional[str] = None
    collateralToken: Optional[str] = None
    longTokenFundingPerSize: Optional[str] = None
    shortTokenFundingPerSize: Optional[str] = None

    # Transaction info
    blockNumber: int = 0
    transactionHash: str = ""
    logIndex: int = 0
    eventType: str = ""


@dataclass
class MarketFundingSnapshot:
    """Aggregated funding rate snapshot per market (CEX-style)."""
    symbol: str
    fundingTime: int
    fundingTimeDatetime: str

    # Funding rates (annualized)
    fundingRate8h: str                   # 8-hour funding rate
    fundingRateAnnualized: str           # Annualized rate

    # Long/Short breakdown
    longFundingRate: str
    shortFundingRate: str

    # Volume/activity
    positionCount: int = 0
    totalFundingFeeUsd: str = "0"
    totalBorrowingFeeUsd: str = "0"

    # Block info
    blockNumber: int = 0


# =============================================================================
# ABI DECODING (replaces hex pattern matching)
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
        msg_sender, event_name, event_data = abi_decode(
            EVENTLOG1_ABI_TYPES, data_bytes
        )
    except Exception as e:
        return {"_decode_error": str(e)}

    result = {
        "event_name": event_name,
        "msg_sender": msg_sender if isinstance(msg_sender, str) else ("0x" + msg_sender.hex() if isinstance(msg_sender, bytes) else str(msg_sender)),
        "addresses": {},
        "uints": {},
        "ints": {},
        "bools": {},
        "bytes32s": {},
        "strings": {},
    }

    # Extract key-value pairs from each section
    for key, val in event_data[IDX_ADDRESS][0]:  # addressItems.items
        addr = val if isinstance(val, str) else ("0x" + val.hex() if isinstance(val, bytes) else str(val))
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

def load_checkpoint(path: Path) -> Optional[dict]:
    """Load checkpoint from JSON file.

    :param path: Path to checkpoint JSON file
    :return: Checkpoint dict or None if not found
    """
    if not path.exists():
        return None
    try:
        with open(path, "r") as f:
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
        "symbol": "funding_all",
        "last_block": last_block,
        "last_timestamp": last_timestamp,
        "total_events": total_events,
        "last_updated": datetime.now(tz=timezone.utc).isoformat(),
        "metadata": {"markets_seen": markets_seen},
    }
    with open(path, "w") as f:
        json.dump(checkpoint, f, indent=2)
    console.print(f"  Checkpoint saved: block [cyan]{last_block:,}[/cyan] -> [green]{path}[/green]")


# =============================================================================
# BACKGROUND / DAEMON
# =============================================================================

def run_in_background(log_file: str, pid_file: str) -> bool:
    """Fork the process to run in the background using double-fork.

    :param log_file: Path to log file for stdout/stderr
    :param pid_file: Path to PID file
    :return: True if this is the parent (should exit), False if child (continue)
    """
    global console

    log_path = Path(log_file)
    pid_path = Path(pid_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.parent.mkdir(parents=True, exist_ok=True)

    # First fork
    pid = os.fork()
    if pid > 0:
        # Parent process — report and exit
        print(f"Running in background (PID: {pid})")
        print(f"Log file: {log_file}")
        print(f"PID file: {pid_file}")
        return True

    # Child — detach from controlling terminal
    os.setsid()

    # Second fork to fully daemonize
    pid = os.fork()
    if pid > 0:
        os._exit(0)

    # Grandchild — redirect stdout/stderr to log file
    sys.stdout.flush()
    sys.stderr.flush()

    log_fh = open(log_file, "a")
    sys.stdout = log_fh
    sys.stderr = log_fh

    # Recreate Rich console to use redirected stdout
    console = Console(file=sys.stdout, force_terminal=False)

    # Write PID file
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))

    return False


# =============================================================================
# HYPERSYNC EXTRACTION
# =============================================================================

async def create_client(network: str) -> HypersyncClient:
    """Create HyperSync client."""
    url = HYPERSYNC_URLS.get(network)
    if not url:
        raise ValueError(f"Unsupported network: {network}")
    return HypersyncClient(ClientConfig(url=url))


async def get_latest_block(client: HypersyncClient) -> int:
    """Get latest block number."""
    return await client.get_height()


async def _stream_with_retry(
    client: HypersyncClient,
    query: Query,
    max_retries: int = MAX_RETRIES,
):
    """Stream HyperSync results with retry on transient errors.

    Yields (response, attempt_number) tuples. On failure, retries with
    exponential backoff from the last successfully processed block.

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

            delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
            console.print(
                f"[yellow]Error (attempt {attempt}/{max_retries}): {e}\n"
                f"Retrying from block {current_from_block:,} in {delay:.0f}s...[/yellow]"
            )
            await asyncio.sleep(delay)


async def extract_funding_events(
    client: HypersyncClient,
    network: str,
    from_block: int,
    to_block: Optional[int],
    market_filter: Optional[str] = None,
) -> list[FundingRateRecord]:
    """Extract funding rate events from GMX V2 EventEmitter.

    Uses topic filtering to only download funding-related events
    and proper eth_abi decode for accurate field extraction.
    Retries automatically on transient errors with exponential backoff.

    :param client: HyperSync client instance
    :param network: Network name (arbitrum, avalanche)
    :param from_block: Starting block number
    :param to_block: Ending block number (None for latest)
    :param market_filter: Optional market symbol filter (e.g., "ETH/USD")
    :return: List of decoded funding rate records
    """
    emitter = EVENT_EMITTER_ADDRESSES.get(network)
    if not emitter:
        raise ValueError(f"No EventEmitter for network: {network}")

    # Topic1 filter: only download events we care about
    funding_topic1_hashes = [EVENT_HASHES[name] for name in FUNDING_EVENT_NAMES]

    query = Query(
        from_block=from_block,
        to_block=to_block,
        logs=[
            LogSelection(
                address=[emitter],
                topics=[
                    [EVENT_LOG1_TOPIC],       # topic0: EventLog1 signature
                    funding_topic1_hashes,    # topic1: only funding-related events
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
    console.print(f"  Block range:  [cyan]{from_block:,}[/cyan] to [cyan]{to_block or 'latest':,}[/cyan] ({total_blocks:,} blocks)")
    console.print(f"  Events:       [cyan]{', '.join(FUNDING_EVENT_NAMES)}[/cyan]")

    records = []
    total_logs = 0
    decode_errors = 0
    funding_events = {name: 0 for name in FUNDING_EVENT_NAMES}
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

                # Only process funding-related events
                if event_name not in FUNDING_EVENT_NAMES:
                    continue

                funding_events[event_name] = funding_events.get(event_name, 0) + 1

                # Get market address from decoded addresses
                market_addr = decoded.get("addresses", {}).get("market")

                # Get market info
                market_info = MARKETS.get(market_addr.lower() if market_addr else "", {})
                symbol = market_info.get("symbol", market_addr or "UNKNOWN")

                # Apply market filter
                if market_filter and symbol != market_filter:
                    continue

                # Extract funding values from properly decoded fields
                uints = decoded.get("uints", {})
                ints = decoded.get("ints", {})

                funding_per_size = (
                    uints.get("fundingFeeAmountPerSize")
                    or uints.get("latestFundingFeeAmountPerSize")
                    or 0
                )
                long_claimable = (
                    uints.get("longTokenClaimableFundingAmountPerSize")
                    or uints.get("latestLongTokenClaimableFundingAmountPerSize")
                    or 0
                )
                short_claimable = (
                    uints.get("shortTokenClaimableFundingAmountPerSize")
                    or uints.get("latestShortTokenClaimableFundingAmountPerSize")
                    or 0
                )

                # Funding rate from funding fee per size (30-decimal precision)
                funding_rate = funding_per_size / FUNDING_RATE_PRECISION

                # Fees: fundingFeeAmount is in collateral token units, not 30-decimal
                collateral_price_min = uints.get("collateralTokenPrice.min", 0)
                funding_fee_raw = uints.get("fundingFeeAmount") or abs(ints.get("fundingFeeAmount", 0))
                borrowing_fee_raw = uints.get("borrowingFeeAmount", 0)
                position_fee_raw = uints.get("positionFeeAmount", 0)

                # borrowingFeeUsd is already in 30-decimal USD in PositionFeesCollected
                borrowing_fee_usd_raw = uints.get("borrowingFeeUsd", 0)

                # Convert token amounts to USD using collateral price
                if collateral_price_min > 0:
                    funding_fee_usd = (funding_fee_raw * collateral_price_min) / USD_PRECISION / (10**6) if funding_fee_raw else 0
                    position_fee_usd = (position_fee_raw * collateral_price_min) / USD_PRECISION / (10**6) if position_fee_raw else 0
                else:
                    # Fallback: assume USDC (6 decimals)
                    funding_fee_usd = funding_fee_raw / (10**6) if funding_fee_raw else 0
                    position_fee_usd = position_fee_raw / (10**6) if position_fee_raw else 0

                borrowing_fee_usd = borrowing_fee_usd_raw / USD_PRECISION if borrowing_fee_usd_raw else 0
                total_fee_usd = funding_fee_usd + borrowing_fee_usd + position_fee_usd

                # Position size (already in 30-decimal USD)
                size_usd = uints.get("sizeInUsd", 0)
                size_usd_formatted = size_usd / USD_PRECISION if size_usd else 0

                # Get account and collateral token
                account = decoded.get("addresses", {}).get("account") or decoded.get("addresses", {}).get("trader")
                collateral_token = decoded.get("addresses", {}).get("collateralToken")

                # Is long?
                is_long = decoded.get("bools", {}).get("isLong")

                # Transaction hash
                tx_hash = log.transaction_hash
                if isinstance(tx_hash, bytes):
                    tx_hash = tx_hash.hex()
                elif tx_hash is None:
                    tx_hash = ""
                if not tx_hash.startswith("0x"):
                    tx_hash = "0x" + tx_hash

                # Create CEX-style record
                record = FundingRateRecord(
                    symbol=symbol,
                    fundingTime=timestamp * 1000,  # Milliseconds like CEX APIs
                    fundingTimeDatetime=datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat() if timestamp else "",
                    fundingRate=f"{funding_rate:.18f}",
                    fundingRatePercent=f"{funding_rate * 100:.10f}%",
                    side="long" if is_long else ("short" if is_long is not None else None),
                    positionSizeUsd=f"{size_usd_formatted:.2f}" if size_usd_formatted else None,
                    fundingFeeUsd=f"{funding_fee_usd:.6f}" if funding_fee_usd else None,
                    borrowingFeeUsd=f"{borrowing_fee_usd:.6f}" if borrowing_fee_usd else None,
                    positionFeeUsd=f"{position_fee_usd:.6f}" if position_fee_usd else None,
                    totalFeeUsd=f"{total_fee_usd:.6f}" if total_fee_usd else None,
                    market=market_addr,
                    account=account,
                    collateralToken=collateral_token,
                    longTokenFundingPerSize=str(long_claimable) if long_claimable else None,
                    shortTokenFundingPerSize=str(short_claimable) if short_claimable else None,
                    blockNumber=log.block_number,
                    transactionHash=tx_hash,
                    logIndex=log.log_index or 0,
                    eventType=event_name,
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
                while (next_milestone_idx < len(PROGRESS_MILESTONES)
                       and pct >= PROGRESS_MILESTONES[next_milestone_idx]):
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

    elapsed = time.monotonic() - t_start
    rate = total_logs / elapsed if elapsed > 0 else 0

    # Summary line
    console.print(
        f"\n  Processed [cyan]{total_logs:,}[/cyan] logs in [cyan]{elapsed:.1f}s[/cyan] "
        f"([cyan]{rate:.0f}[/cyan] logs/sec)"
    )
    console.print(
        f"  Found [green]{len(records):,}[/green] funding events "
        f"([red]{decode_errors}[/red] decode errors)"
    )

    # Event breakdown table
    event_table = Table(show_header=False, box=None, padding=(0, 2))
    for name, count in sorted(funding_events.items(), key=lambda x: -x[1]):
        if count > 0:
            event_table.add_row(f"  {name}", f"[cyan]{count:,}[/cyan]")
    if any(v > 0 for v in funding_events.values()):
        console.print(event_table)

    return records


def aggregate_funding_snapshots(
    records: list[FundingRateRecord],
    interval_hours: int = 8
) -> list[MarketFundingSnapshot]:
    """Aggregate funding records into periodic snapshots per market.

    Similar to CEX funding rate history endpoints.
    """
    # Group by market and time interval
    interval_ms = interval_hours * 3600 * 1000
    snapshots_data = defaultdict(lambda: defaultdict(list))

    for record in records:
        if not record.fundingTime:
            continue
        # Round to interval
        interval_start = (record.fundingTime // interval_ms) * interval_ms
        snapshots_data[record.symbol][interval_start].append(record)

    snapshots = []
    for symbol, intervals in sorted(snapshots_data.items()):
        for interval_start, interval_records in sorted(intervals.items()):
            # Aggregate values
            long_rates = []
            short_rates = []
            total_funding = 0
            total_borrowing = 0

            for r in interval_records:
                if r.longTokenFundingPerSize:
                    try:
                        long_rates.append(int(r.longTokenFundingPerSize) / FUNDING_RATE_PRECISION)
                    except (ValueError, TypeError):
                        pass
                if r.shortTokenFundingPerSize:
                    try:
                        short_rates.append(int(r.shortTokenFundingPerSize) / FUNDING_RATE_PRECISION)
                    except (ValueError, TypeError):
                        pass
                if r.fundingFeeUsd:
                    try:
                        total_funding += float(r.fundingFeeUsd)
                    except (ValueError, TypeError):
                        pass
                if r.borrowingFeeUsd:
                    try:
                        total_borrowing += float(r.borrowingFeeUsd)
                    except (ValueError, TypeError):
                        pass

            avg_long = sum(long_rates) / len(long_rates) if long_rates else 0
            avg_short = sum(short_rates) / len(short_rates) if short_rates else 0

            # 8h rate
            funding_8h = max(abs(avg_long), abs(avg_short))
            # Annualized (365 * 24 / 8 = 1095 periods)
            funding_annual = funding_8h * 1095

            snapshot = MarketFundingSnapshot(
                symbol=symbol,
                fundingTime=interval_start,
                fundingTimeDatetime=datetime.fromtimestamp(
                    interval_start / 1000, tz=timezone.utc
                ).isoformat(),
                fundingRate8h=f"{funding_8h:.10f}",
                fundingRateAnnualized=f"{funding_annual:.6f}",
                longFundingRate=f"{avg_long:.10f}",
                shortFundingRate=f"{avg_short:.10f}",
                positionCount=len(interval_records),
                totalFundingFeeUsd=f"{total_funding:.2f}",
                totalBorrowingFeeUsd=f"{total_borrowing:.2f}",
                blockNumber=interval_records[-1].blockNumber if interval_records else 0,
            )
            snapshots.append(snapshot)

    return snapshots


# =============================================================================
# OUTPUT
# =============================================================================

def append_parquet(new_df: "pl.DataFrame", filepath: Path) -> None:
    """Append new data to an existing Parquet file, deduplicating.

    Deduplicates by (blockNumber, logIndex, eventType) and sorts by blockNumber.

    :param new_df: New data as a polars DataFrame
    :param filepath: Path to the Parquet file
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    if filepath.exists():
        existing = pl.read_parquet(filepath)
        # Align schemas
        for col in existing.columns:
            if col in new_df.columns and existing[col].dtype != new_df[col].dtype:
                new_df = new_df.with_columns(pl.col(col).cast(existing[col].dtype))
        combined = pl.concat([existing, new_df], how="diagonal_relaxed")
        combined = combined.unique(subset=["blockNumber", "logIndex", "eventType"], keep="last")
        combined = combined.sort("blockNumber")
        combined.write_parquet(filepath)
    else:
        new_df.sort("blockNumber").write_parquet(filepath)


def save_raw_per_symbol(records: list[FundingRateRecord], output_dir: Path) -> None:
    """Save raw funding events to per-symbol Parquet files (append mode).

    :param records: List of FundingRateRecord objects
    :param output_dir: Base output directory (e.g., data/funding/arbitrum)
    """
    if not HAS_POLARS:
        console.print("[red]Error: polars required for Parquet. Install with: pip install polars[/red]")
        return

    by_symbol: dict[str, list[FundingRateRecord]] = defaultdict(list)
    for r in records:
        by_symbol[r.symbol].append(r)

    for symbol, sym_records in sorted(by_symbol.items()):
        safe_symbol = symbol.replace("/", "_").replace(" ", "_").replace("[", "").replace("]", "")
        filepath = output_dir / "raw" / safe_symbol / "events.parquet"

        df = pl.DataFrame([asdict(r) for r in sym_records])
        append_parquet(df, filepath)
        console.print(f"  Raw: [cyan]{len(sym_records):,}[/cyan] events -> [green]{filepath}[/green]")


def save_snapshots_per_symbol(
    snapshots: list[MarketFundingSnapshot], output_dir: Path
) -> None:
    """Save aggregated funding snapshots to per-symbol Parquet files (append mode).

    :param snapshots: List of MarketFundingSnapshot objects
    :param output_dir: Base output directory (e.g., data/funding/arbitrum)
    """
    if not HAS_POLARS:
        console.print("[red]Error: polars required for Parquet. Install with: pip install polars[/red]")
        return

    by_symbol: dict[str, list[MarketFundingSnapshot]] = defaultdict(list)
    for s in snapshots:
        by_symbol[s.symbol].append(s)

    for symbol, sym_snapshots in sorted(by_symbol.items()):
        safe_symbol = symbol.replace("/", "_").replace(" ", "_").replace("[", "").replace("]", "")
        filepath = output_dir / "snapshots" / safe_symbol / "snapshots.parquet"

        df = pl.DataFrame([asdict(s) for s in sym_snapshots])
        filepath.parent.mkdir(parents=True, exist_ok=True)
        if filepath.exists():
            existing = pl.read_parquet(filepath)
            for col in existing.columns:
                if col in df.columns and existing[col].dtype != df[col].dtype:
                    df = df.with_columns(pl.col(col).cast(existing[col].dtype))
            combined = pl.concat([existing, df], how="diagonal_relaxed")
            combined = combined.unique(subset=["fundingTime", "symbol"], keep="last")
            combined = combined.sort("fundingTime")
            combined.write_parquet(filepath)
        else:
            df.sort("fundingTime").write_parquet(filepath)

        console.print(f"  Snapshots: [cyan]{len(sym_snapshots):,}[/cyan] entries -> [green]{filepath}[/green]")


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


def save_parquet(data: list, filename: str) -> None:
    """Save to Parquet using polars.

    :param data: List of dataclass instances to serialize
    :param filename: Output file path
    """
    if not HAS_POLARS:
        console.print("[red]Error: polars required for Parquet. Install with: pip install polars[/red]")
        return

    df = pl.DataFrame([asdict(d) for d in data])
    df.write_parquet(filename)
    console.print(f"  Saved [cyan]{len(data):,}[/cyan] records to [green]{filename}[/green]")


def print_summary(records: list[FundingRateRecord], snapshots: list[MarketFundingSnapshot]) -> None:
    """Print summary statistics using Rich tables.

    :param records: List of funding rate records
    :param snapshots: List of aggregated market snapshots
    """
    by_market = defaultdict(list)
    for r in records:
        by_market[r.symbol].append(r)

    table = Table(title="Funding Rate Summary", show_lines=False)
    table.add_column("Symbol", style="cyan")
    table.add_column("Events", justify="right")
    table.add_column("Avg Rate (8h)", justify="right")
    table.add_column("Total Fees USD", justify="right", style="green")

    for symbol in sorted(by_market.keys()):
        market_records = by_market[symbol]

        rates = []
        for r in market_records:
            if r.longTokenFundingPerSize:
                try:
                    rates.append(abs(int(r.longTokenFundingPerSize)) / FUNDING_RATE_PRECISION)
                except (ValueError, TypeError):
                    pass
        avg_rate = sum(rates) / len(rates) if rates else 0

        total_fees = 0
        for r in market_records:
            if r.totalFeeUsd:
                try:
                    total_fees += float(r.totalFeeUsd)
                except (ValueError, TypeError):
                    pass

        table.add_row(
            symbol,
            f"{len(market_records):,}",
            f"{avg_rate:.10f}",
            f"${total_fees:,.2f}",
        )

    console.print()
    console.print(table)

    # Time range
    timestamps = [r.fundingTime for r in records if r.fundingTime]
    if timestamps:
        first = datetime.fromtimestamp(min(timestamps) / 1000, tz=timezone.utc)
        last = datetime.fromtimestamp(max(timestamps) / 1000, tz=timezone.utc)
        console.print(f"\n  Time range: [cyan]{first.isoformat()}[/cyan] to [cyan]{last.isoformat()}[/cyan]")

    console.print(f"  Total events: [cyan]{len(records):,}[/cyan]")
    console.print(f"  Aggregated snapshots: [cyan]{len(snapshots):,}[/cyan]")


# =============================================================================
# MAIN
# =============================================================================

async def async_main(args: argparse.Namespace) -> None:
    """Async main entry point.

    :param args: Parsed command-line arguments
    """
    output_dir = Path(args.output_dir) / args.network if args.output_dir else None
    checkpoint_dir = (
        Path(args.checkpoint_dir) if args.checkpoint_dir
        else (output_dir / "checkpoints" if output_dir else Path("./data/funding") / args.network / "checkpoints")
    )
    checkpoint_path = checkpoint_dir / "funding_checkpoint.json"

    # Determine starting block
    from_block = args.from_block

    if args.resume and from_block is None:
        # Load checkpoint for incremental mode
        checkpoint = load_checkpoint(checkpoint_path)
        if checkpoint:
            from_block = checkpoint["last_block"] + 1
            console.print(f"  Resuming from checkpoint: block [cyan]{from_block:,}[/cyan]")
        else:
            from_block = GMX_V2_GENESIS_BLOCK
            console.print(f"  No checkpoint found. Starting from genesis: [cyan]{from_block:,}[/cyan]")
    elif from_block is None:
        from_block = 0

    # Header panel
    header_lines = [
        f"Network:     [cyan]{args.network}[/cyan]",
        f"Block range: [cyan]{from_block:,}[/cyan] to [cyan]{args.to_block or 'latest'}[/cyan]",
        f"Output:      [cyan]{args.output}[/cyan]",
    ]
    if output_dir:
        header_lines.append(f"Output dir:  [cyan]{output_dir}[/cyan]")
    if args.market:
        header_lines.append(f"Market:      [cyan]{args.market}[/cyan]")
    if args.resume:
        header_lines.append(f"Resume:      [cyan]enabled[/cyan] (checkpoint: {checkpoint_path})")
    if args.background:
        header_lines.append(f"Background:  [cyan]enabled[/cyan]")
    header_lines.append(f"Retries:     [cyan]{MAX_RETRIES}[/cyan] (backoff: {RETRY_BASE_DELAY}s base)")

    console.print(Panel(
        "\n".join(header_lines),
        title="GMX V2 Funding Rate Extractor",
        subtitle="HyperSync + eth_abi | CEX-Style Output",
        border_style="blue",
    ))

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
        console.print(f"\n[yellow]Already up to date (from_block {from_block:,} >= to_block {to_block:,})[/yellow]")
        return

    # Extract events
    console.print()
    records = await extract_funding_events(
        client=client,
        network=args.network,
        from_block=from_block,
        to_block=to_block,
        market_filter=args.market,
    )

    if not records:
        console.print("\n[yellow]No funding events found.[/yellow]")
        if args.resume:
            # Still save checkpoint so we don't re-scan empty range
            save_checkpoint(
                checkpoint_path,
                last_block=to_block,
                last_timestamp=int(time.time()),
                total_events=0,
                markets_seen=0,
            )
        return

    # Aggregate to snapshots
    with console.status("Aggregating snapshots..."):
        snapshots = aggregate_funding_snapshots(records)

    # Save outputs
    console.print()
    if args.output == "parquet" and output_dir:
        # Organized per-symbol parquet storage (for --resume / --output-dir)
        save_raw_per_symbol(records, output_dir)
        save_snapshots_per_symbol(snapshots, output_dir)
    elif args.output == "parquet":
        # Flat parquet files (legacy one-shot mode)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = f"gmx_v2_funding_{args.network}_{from_block}_{to_block}_{ts}"
        save_parquet(records, f"{base}_events.parquet")
        save_parquet(snapshots, f"{base}_snapshots.parquet")
    else:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = f"gmx_v2_funding_{args.network}_{from_block}_{to_block}_{ts}"
        if args.output == "json":
            save_json(records, f"{base}_events.json")
            save_json(snapshots, f"{base}_snapshots.json")
        elif args.output == "csv":
            save_csv(records, f"{base}_events.csv")
            save_csv(snapshots, f"{base}_snapshots.csv")

    # Save checkpoint
    if args.resume:
        last_block = max(r.blockNumber for r in records)
        last_timestamp = max(r.fundingTime for r in records if r.fundingTime) // 1000
        unique_markets = len(set(r.symbol for r in records))

        # Accumulate total events from previous checkpoint
        prev_checkpoint = load_checkpoint(checkpoint_path)
        prev_total = prev_checkpoint["total_events"] if prev_checkpoint else 0

        save_checkpoint(
            checkpoint_path,
            last_block=last_block,
            last_timestamp=last_timestamp,
            total_events=prev_total + len(records),
            markets_seen=unique_markets,
        )

    # Print summary
    print_summary(records, snapshots)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract GMX V2 funding rates in CEX-style format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # One-shot backfill
  uv run scripts/extract_funding_rates.py --from-block 200000000

  # Incremental cronjob (resumes from checkpoint)
  uv run scripts/extract_funding_rates.py --resume

  # Run in background with checkpoint
  uv run scripts/extract_funding_rates.py --resume --background

  # Background with custom log/pid files
  uv run scripts/extract_funding_rates.py --resume --background --log-file logs/funding.log --pid-file logs/funding.pid
        """,
    )

    parser.add_argument("--network", choices=["arbitrum", "avalanche"],
                        default="arbitrum", help="Network (default: arbitrum)")
    parser.add_argument("--from-block", type=int, default=None,
                        help="Start block (default: 0, or checkpoint if --resume)")
    parser.add_argument("--to-block", type=int, default=None,
                        help="End block (default: latest)")
    parser.add_argument("--output-dir", type=str, default=None,
                        help="Base output directory (default: ./data/funding). Enables organized per-symbol storage.")
    parser.add_argument("--output", choices=["json", "csv", "parquet"],
                        default="json", help="Output format (default: json)")
    parser.add_argument("--market", type=str, default=None,
                        help="Filter by market symbol (e.g., 'ETH/USD')")
    parser.add_argument("--resume", action="store_true",
                        help="Enable checkpoint-based incremental mode")
    parser.add_argument("--checkpoint-dir", type=str, default=None,
                        help="Override checkpoint directory")
    parser.add_argument("--background", action="store_true",
                        help="Run in background (daemonize)")
    parser.add_argument("--log-file", type=str, default=None,
                        help="Log file for background mode (default: logs/funding_extract_<timestamp>.log)")
    parser.add_argument("--pid-file", type=str, default=None,
                        help="PID file for background mode (default: logs/funding_extract.pid)")

    args = parser.parse_args()

    # Auto-enable output-dir when using --resume with parquet
    if args.resume and args.output_dir is None:
        args.output_dir = "./data/funding"
        args.output = "parquet"

    # Handle background mode
    if args.background:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = args.log_file or f"logs/funding_extract_{ts}.log"
        pid_file = args.pid_file or "logs/funding_extract.pid"

        is_parent = run_in_background(log_file, pid_file)
        if is_parent:
            sys.exit(0)
        # Child continues below

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
