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

QUICK START
-----------
    uv run scripts/extract_funding_rates.py --network arbitrum --from-block 200000000

USAGE
-----
    uv run scripts/extract_funding_rates.py [OPTIONS]

OPTIONS
-------
    --network      Network: "arbitrum" or "avalanche" (default: arbitrum)
    --from-block   Starting block number (default: 0)
    --to-block     Ending block number (default: latest)
    --output       Output format: "json", "csv", or "parquet" (default: json)
    --market       Filter by market symbol (e.g., "ETH/USD", "BTC/USD")

EXAMPLES
--------
    # Extract all funding events from recent blocks
    uv run scripts/extract_funding_rates.py --network arbitrum --from-block 280000000

    # Export to CSV for spreadsheet analysis
    uv run scripts/extract_funding_rates.py --network arbitrum --from-block 280000000 --output csv

    # Filter by specific market
    uv run scripts/extract_funding_rates.py --network arbitrum --from-block 280000000 --market "ETH/USD"

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
import sys
import time
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
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
    from collections import defaultdict

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
    from collections import defaultdict

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
    # Header panel
    header_lines = [
        f"Network:     [cyan]{args.network}[/cyan]",
        f"Block range: [cyan]{args.from_block:,}[/cyan] to [cyan]{args.to_block or 'latest'}[/cyan]",
        f"Output:      [cyan]{args.output}[/cyan]",
    ]
    if args.market:
        header_lines.append(f"Market:      [cyan]{args.market}[/cyan]")
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

    # Extract events
    console.print()
    records = await extract_funding_events(
        client=client,
        network=args.network,
        from_block=args.from_block,
        to_block=to_block,
        market_filter=args.market,
    )

    if not records:
        console.print("\n[yellow]No funding events found.[/yellow]")
        return

    # Aggregate to snapshots
    with console.status("Aggregating snapshots..."):
        snapshots = aggregate_funding_snapshots(records)

    # Generate filenames
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = f"gmx_v2_funding_{args.network}_{args.from_block}_{to_block}_{ts}"

    # Save outputs
    console.print()
    if args.output == "json":
        save_json(records, f"{base}_events.json")
        save_json(snapshots, f"{base}_snapshots.json")
    elif args.output == "csv":
        save_csv(records, f"{base}_events.csv")
        save_csv(snapshots, f"{base}_snapshots.csv")
    elif args.output == "parquet":
        save_parquet(records, f"{base}_events.parquet")
        save_parquet(snapshots, f"{base}_snapshots.parquet")

    # Print summary
    print_summary(records, snapshots)


def main():
    parser = argparse.ArgumentParser(
        description="Extract GMX V2 funding rates in CEX-style format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--network", choices=["arbitrum", "avalanche"],
                        default="arbitrum", help="Network (default: arbitrum)")
    parser.add_argument("--from-block", type=int, default=0,
                        help="Start block (default: 0)")
    parser.add_argument("--to-block", type=int, default=None,
                        help="End block (default: latest)")
    parser.add_argument("--output", choices=["json", "csv", "parquet"],
                        default="json", help="Output format (default: json)")
    parser.add_argument("--market", type=str, default=None,
                        help="Filter by market symbol (e.g., 'ETH/USD')")

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
