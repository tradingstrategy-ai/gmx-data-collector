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
GMX V2 Funding Factor Extractor (HyperSync Edition)
====================================================
Extracts ``Funding`` events from GMX V2 EventEmitter to get the market-level
``fundingFactorPerSecond`` — the actual per-second funding rate as a
30-decimal fixed-point integer.

This is the CORRECT source for GMX funding rates. The companion script
``extract_funding_rates.py`` captures position-level fee events which are
cumulative counters, NOT rates.

Outputs:
- Raw events: ``data/funding/{network}/raw/funding/{SYMBOL}/partition=0/data.parquet``
- Aggregated rates: ``data/funding/{network}/rates/{SYMBOL}/1h.parquet``

The aggregated ``rates/`` files are consumed by ``FreqtradeExporter`` to
produce ``.feather`` files for FreqTrade backtesting.

QUICK START
-----------
    poetry run python scripts/extract_funding_factor.py --from-block 120000000

USAGE
-----
    poetry run python scripts/extract_funding_factor.py [OPTIONS]

OPTIONS
-------
    --network        Network: "arbitrum" or "avalanche" (default: arbitrum)
    --from-block     Starting block number (default: genesis or checkpoint)
    --to-block       Ending block number (default: latest)
    --output-dir     Base output directory (default: ./data/funding)
    --output         Output format: "json", "csv", or "parquet" (default: parquet)
    --market         Filter by market symbol (e.g., "ETH/USD")
    --resume         Enable checkpoint-based incremental mode
    --checkpoint-dir Override checkpoint directory
    --background     Run in background (daemonize)
    --log-file       Log file for background mode
    --pid-file       PID file for background mode

EXAMPLES
--------
    # Full historical extraction
    poetry run python scripts/extract_funding_factor.py --from-block 120000000

    # Quick test on small block range
    poetry run python scripts/extract_funding_factor.py --from-block 290000000 --to-block 290100000 --output json

    # Incremental cronjob
    poetry run python scripts/extract_funding_factor.py --resume

    # Background mode
    poetry run python scripts/extract_funding_factor.py --resume --background
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
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

console = Console()

# Retry settings — HyperSync 500 errors can be persistent, use generous retries
MAX_RETRIES = 15
RETRY_BASE_DELAY = 2.0
RETRY_MAX_DELAY = 120.0  # Cap backoff at 2 minutes

# Progress milestones
PROGRESS_MILESTONES = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100]

# Flush records to disk every N events
FLUSH_EVERY = 10_000

# GMX V2 genesis block on Arbitrum
GMX_V2_GENESIS_BLOCK = 120_000_000

# Precision: fundingFactorPerSecond is a 30-decimal fixed-point integer
FUNDING_FACTOR_PRECISION = 10**30

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

# EventLog1 signature (topic0)
EVENT_LOG1_TOPIC = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160"

# Funding event hash (topic1)
FUNDING_EVENT_HASH = "0x" + keccak(text="Funding").hex()

# ABI types for EventLog1 decoding
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

# Section indices
IDX_ADDRESS = 0
IDX_UINT = 1
IDX_INT = 2
IDX_BOOL = 3
IDX_BYTES32 = 4
IDX_BYTES = 5
IDX_STRING = 6

# GMX V2 Market Addresses on Arbitrum
# Source: https://github.com/gmx-io/gmx-interface (sdk/src/configs/markets.ts)
# Last updated: 2026-02-18
MARKETS = {
    # Major perpetual markets
    "0x47c031236e19d024b42f8ae6780e44a573170703": {"symbol": "BTC/USD", "indexToken": "BTC"},
    "0x70d95587d40a2caf56bd97485ab3eec10bee6336": {"symbol": "ETH/USD", "indexToken": "ETH"},
    "0x6853ea96ff216fab11d2d930ce3c508556a4bdc4": {"symbol": "DOGE/USD", "indexToken": "DOGE"},
    "0x09400d9db990d5ed3f35d7be61dfaeb900af03c9": {"symbol": "SOL/USD", "indexToken": "SOL"},
    "0xd9535bb5f58a1a75032416f2dfe7880c30575a41": {"symbol": "LTC/USD", "indexToken": "LTC"},
    "0xc7abb2c5f3bf3ceb389df0eecd6120d451170b50": {"symbol": "UNI/USD", "indexToken": "UNI"},
    "0x7f1fa204bb700853d36994da19f830b6ad18455c": {"symbol": "LINK/USD", "indexToken": "LINK"},
    "0xc25cef6061cf5de5eb761b50e4743c1f5d7e5407": {"symbol": "ARB/USD", "indexToken": "ARB"},
    "0x0ccb4faa6f1f1b30911619f1184082ab4e25813c": {"symbol": "XRP/USD", "indexToken": "XRP"},
    "0x2d340912aa47e33c90efb078e69e70efe2b34b9b": {"symbol": "BNB/USD", "indexToken": "BNB"},
    "0x248c35760068ce009a13076d573ed3497a47bcd4": {"symbol": "ATOM/USD", "indexToken": "ATOM"},
    "0x1cbba6346f110c8a5ea739ef2d1eb182990e4eb2": {"symbol": "AAVE/USD", "indexToken": "AAVE"},
    "0x7bbbf946883a5701350007320f525c5379b8178a": {"symbol": "AVAX/USD", "indexToken": "AVAX"},
    "0x4fdd333ff9ca409df583f306b6f5a7ffde790739": {"symbol": "OP/USD", "indexToken": "OP"},
    "0xb56e5e2fb50d6fb510b4e4c086dcde66a866da24": {"symbol": "GMX/USD", "indexToken": "GMX"},
    "0x2b477989a149b17073d9c9c82ec9cb03591325a6": {"symbol": "WIF/USD", "indexToken": "WIF"},
    # Single-asset / alternative collateral markets
    "0x7c11f78ce78768518d743e81fdfa2f860c6b9a77": {"symbol": "BTC/USD [WBTC.e-WBTC.e]", "indexToken": "BTC"},
    "0x450bb6774dd8a756274e0ab4107953259d2ac541": {"symbol": "ETH/USD [WETH-WETH]", "indexToken": "ETH"},
    "0xe68caaacdf6439628dfd2fe624847602991a31eb": {"symbol": "BTC/USD [WBTC-WBTC]", "indexToken": "BTC"},
    "0xdab9ba9e3a301ccb353f18b4c8542ba2149e4010": {"symbol": "ETH/USD [WETH-WETH-2]", "indexToken": "ETH"},
    "0x08a902113f7f41a8658ebb1175f9c847bf4fb9d8": {"symbol": "ETH/USD [WETH-USDT]", "indexToken": "ETH"},
    "0x0cf1fb4d1ff67a3d8ca92c9d6643f8f9be8e03e5": {"symbol": "ETH/USD [wstETH-USDe]", "indexToken": "ETH"},
    "0xd62068697bcc92af253225676d618b0c9f17c663": {"symbol": "BTC/USD [tBTC-tBTC]", "indexToken": "BTC"},
    # Newer perpetual markets
    "0xb62369752d8ad08392572db6d0cc872127888bed": {"symbol": "SHIB/USD", "indexToken": "SHIB"},
    "0x6ecf2133e2c9751caadcb6958b9654bae198a797": {"symbol": "SUI/USD", "indexToken": "SUI"},
    "0xb489711b1cb86afda48924730084e23310eb4883": {"symbol": "SEI/USD", "indexToken": "SEI"},
    "0x66a69c8eb98a7efe22a22611d1967dfec786a708": {"symbol": "APT/USD", "indexToken": "APT"},
    "0xbeb1f4ebc9af627ca1e5a75981ce1ae97efeda22": {"symbol": "TIA/USD", "indexToken": "TIA"},
    "0x3680d7bfe9260d3c5de81aeb2194c119a59a99d1": {"symbol": "TRX/USD", "indexToken": "TRX"},
    "0x872b5d567a2469ed92d252eacb0eb3bb0769e05b": {"symbol": "WLD/USD", "indexToken": "WLD"},
    "0xe55e1a29985488a2c8846a91e925c2b7c6564db1": {"symbol": "TAO/USD", "indexToken": "TAO"},
    "0xfd46a5702d4d97ce0164375744c65f0c31a3901b": {"symbol": "FLOKI/USD", "indexToken": "FLOKI"},
    "0x6cb901cc64c024c3fe4404c940ff9a3acc229d2c": {"symbol": "MEME/USD", "indexToken": "MEME"},
    "0x784292e87715d93afd7cb8c941bacafaaa9a5102": {"symbol": "PENDLE/USD", "indexToken": "PENDLE"},
    "0xcacb964144f9056a8f99447a303e60b4873ca9b4": {"symbol": "ADA/USD", "indexToken": "ADA"},
    "0x62feb8ec060a7de5b32bbbf4ac70050f8a043c17": {"symbol": "BCH/USD", "indexToken": "BCH"},
    "0xdc4e96a251ff43eeac710462cd8a9d18dc802f18": {"symbol": "ICP/USD", "indexToken": "ICP"},
    "0x467c4a46287f6c4918ddf780d4fd7b46419c2291": {"symbol": "DYDX/USD", "indexToken": "DYDX"},
    "0x16466a03449cb9218eb6a980aa4a44aaced27c25": {"symbol": "INJ/USD", "indexToken": "INJ"},
    "0xfec8f404fbca3b11afd3b3f0c57507c2a06de636": {"symbol": "TRUMP/USD", "indexToken": "TRUMP"},
    "0x12fd1a4bdb96219e637180ff5293409502b2951d": {"symbol": "MELANIA/USD", "indexToken": "MELANIA"},
    "0xd0a1afdde31eb51e8b53bdce989eb8c2404828a4": {"symbol": "POL/USD", "indexToken": "POL"},
    "0xdab21c4d1f569486334c93685da2b3f9b0a078e8": {"symbol": "APE/USD", "indexToken": "APE"},
    "0xe2730ffe2136aa549327ebce93d58160df7821cb": {"symbol": "FARTCOIN/USD", "indexToken": "FARTCOIN"},
    "0x876ff160d63809674e03f82dc4d3c3ae8b0acf28": {"symbol": "BERA/USD", "indexToken": "BERA"},
    "0x0c11ed89889fd03394e8d9d685cc5b85be569c99": {"symbol": "PENGU/USD", "indexToken": "PENGU"},
    "0x970e578ff01589bb470ce38a2f1753152a009366": {"symbol": "ONDO/USD", "indexToken": "ONDO"},
    "0x04decfb37e46075189324817df80a32d22b9ed8d": {"symbol": "AIXBT/USD", "indexToken": "AIXBT"},
    "0x4d9ba415649c4b3c703562770c8ff3033478cea1": {"symbol": "S/USD", "indexToken": "S"},
    "0xbcb8fe13d02b023e8f94f6881cc0192fd918a5c0": {"symbol": "HYPE/USD", "indexToken": "HYPE"},
    "0x7de8e1a1fba845a330a6bd91118afda09610fb02": {"symbol": "JUP/USD", "indexToken": "JUP"},
    "0x4d3eb91efd36c2b74181f34b111bc1e91a0d0cb4": {"symbol": "DOLO/USD", "indexToken": "DOLO"},
    "0x9e79146b3a022af44e0708c6794f03ef798381a5": {"symbol": "ZRO/USD", "indexToken": "ZRO"},
    "0x0e46941f9bff8d0784bffa3d0d7883cdb82d7ae7": {"symbol": "CRV/USD", "indexToken": "CRV"},
    "0x7c54d547fad72f8afbf6e5b04403a0168b654c6f": {"symbol": "XMR/USD", "indexToken": "XMR"},
    "0x39ac3c494950a4363d739201ba5a0861265c9ae5": {"symbol": "PI/USD", "indexToken": "PI"},
    "0x4c0bb704529fa49a26bd854802d70206982c6f1b": {"symbol": "PUMP/USD", "indexToken": "PUMP"},
    "0x8263bc3766a09f6dd4bab04b4bf8d45f2b0973ff": {"symbol": "SPX6900/USD", "indexToken": "SPX6900"},
    "0x40daeac02dcf6b3c51f9151f532c21dcef2f7e63": {"symbol": "MNT/USD", "indexToken": "MNT"},
    "0x9f0849fb830679829d1fb759b11236d375d15c78": {"symbol": "HBAR/USD", "indexToken": "HBAR"},
    "0x41e3bc5b72384c8b26b559b7d16c2b81fd36fba2": {"symbol": "CVX/USD", "indexToken": "CVX"},
    "0x4024418592450e4d62fab15e2f833fc03a3447dc": {"symbol": "KAS/USD", "indexToken": "KAS"},
    "0x970b730b5dd18de53a230ee8f4af088dbc3a6f8d": {"symbol": "KTA/USD", "indexToken": "KTA"},
    "0xac484106d935f0f20f1485b631fa6f65aeeff550": {"symbol": "ZORA/USD", "indexToken": "ZORA"},
    "0x4b67aa8f754b17b1029ad2db4fb6a276cce350c4": {"symbol": "XPL/USD", "indexToken": "XPL"},
    "0x0164b6c847c65e07c9f6226149adbfa7c1de40cf": {"symbol": "ASTER/USD", "indexToken": "ASTER"},
    "0xe024188850a822409f362209c1ef2cfdc7c4de4c": {"symbol": "0G/USD", "indexToken": "0G"},
    "0xceff9d261a96cb78df35f9333ba9f2f4cfcb8a68": {"symbol": "AVNT/USD", "indexToken": "AVNT"},
    "0x6d9430a116ed4d4fc6fe1996a5493662d555b07e": {"symbol": "LINEA/USD", "indexToken": "LINEA"},
    "0x66ab9d61a0124b61c8892a4ac687ac48dba8ff2c": {"symbol": "MON/USD", "indexToken": "MON"},
    "0x587759c237acca739bce3911647bacf56c876e60": {"symbol": "ZEC/USD", "indexToken": "ZEC"},
    "0x5707673d95a8fd317e2745c4217acd64ca021b68": {"symbol": "ANIME/USD", "indexToken": "ANIME"},
    "0x728ff0679c89267434d6ef1824c8c8eed4ac3dbc": {"symbol": "DASH/USD", "indexToken": "DASH"},
    "0x3b4689d69516b9d4b1aaf7545c6fc4d3ed70b70b": {"symbol": "JTO/USD", "indexToken": "JTO"},
    "0x8965e821c7c8c09c6eb3cb9ccf7eb6f386441ea2": {"symbol": "SYRUP/USD", "indexToken": "SYRUP"},
    "0x3600592dded7e6e0b05029dfb637ffc5a85d6f6b": {"symbol": "CHZ/USD", "indexToken": "CHZ"},
    "0xeb28ad1a2e497f4acc5d9b87e7b496623c93061e": {"symbol": "XAUT/USD", "indexToken": "XAUT"},
    "0x5ff52be1968107d7886a8e9a64874a45c8f5d96a": {"symbol": "IP/USD", "indexToken": "IP"},
    "0xb3588455858a49d3244237cee00880ccb84b91dd": {"symbol": "WLFI/USD", "indexToken": "WLFI"},
    "0x947c521e44f727219542b0f91a85182193c1d2ad": {"symbol": "VVV/USD", "indexToken": "VVV"},
    # Alternative collateral variants
    "0x0bb2a83f995e1e1eae9d7fdce68ab1ac55b2cc85": {"symbol": "PEPE/USD [WETH-USDC]", "indexToken": "PEPE"},
    "0xf913b4748031ef569898ed91e5ba0d602bb93298": {"symbol": "LINK/USD [WETH-USDC]", "indexToken": "LINK"},
    "0xcf083d35ad306a042d4fb312fcdd8228b52b82f8": {"symbol": "SOL/USD [WBTC.e-USDC]", "indexToken": "SOL"},
    "0x065577d05c3d4c11505ed7bc97bbf85d462a6a6f": {"symbol": "BNB/USD [WBTC.e-USDC]", "indexToken": "BNB"},
    # Swap-only markets (may emit Funding events with 0 rate)
    "0xb686bcb112660343e6d15bdb65297e110c8311c4": {"symbol": "USDC-USDT [swap]", "indexToken": None},
    "0xe2fecb78f76d937648c47e4e2cd5e47d27411545": {"symbol": "XRP/USD [legacy]", "indexToken": "XRP"},
    "0x63dc80ee90f26363b3fcd609f370bb5549d6dbca": {"symbol": "NEAR/USD [legacy]", "indexToken": "NEAR"},
}


# =============================================================================
# DATA CLASSES & HELPERS
# =============================================================================

def market_symbol(address: str) -> str:
    """Look up human-readable symbol for a market address.

    :param address: Lowercase hex market address.
    :returns: Symbol string (e.g., ``'ETH'``). Falls back to truncated address.
    """
    info = MARKETS.get(address.lower())
    if info:
        sym = info["symbol"]
        # "ETH/USD" -> "ETH", "BTC/USD [WBTC-WBTC]" -> "BTC_WBTC-WBTC"
        # "USDC-USDT [swap]" -> "USDC-USDT_swap"
        base = sym.split("/")[0]
        bracket = sym.find("[")
        if bracket != -1:
            suffix = sym[bracket + 1 : -1].strip()
            return f"{base}_{suffix}"
        # Sanitize: remove characters that break filesystem/polars glob
        return base.replace("[", "").replace("]", "").replace(" ", "_")
    return address[:10]


@dataclass
class FundingFactorRecord:
    """Raw Funding event from GMX V2 EventEmitter.

    The ``Funding`` event carries ``fundingFactorPerSecond`` — the per-second
    funding rate as a 30-decimal fixed-point integer.

    :ivar symbol: Derived symbol (e.g., ``'ETH'``)
    :ivar market: Market contract address
    :ivar funding_factor_per_second: Raw 30-decimal integer as string
    :ivar funding_rate_per_second: Decimal per-second rate
    :ivar longs_pay_shorts: True when longs pay shorts
    :ivar block_number: Block number
    :ivar block_timestamp: Unix timestamp (seconds)
    :ivar block_datetime: ISO 8601 datetime string
    :ivar transaction_hash: Transaction hash
    :ivar log_index: Log index within the block
    """

    symbol: str
    market: str
    funding_factor_per_second: str
    funding_rate_per_second: float
    longs_pay_shorts: bool
    block_number: int
    block_timestamp: int
    block_datetime: str
    transaction_hash: str
    log_index: int


# =============================================================================
# ABI DECODING
# =============================================================================

def decode_event_log_data(hex_data: str) -> dict:
    """Decode GMX V2 EventLog1 data field using eth_abi.

    :param hex_data: Hex-encoded data field (with ``0x`` prefix).
    :returns: Dict with ``event_name``, ``addresses``, ``uints``, ``ints``,
        ``bools``, etc.
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
        "msg_sender": (
            msg_sender
            if isinstance(msg_sender, str)
            else (
                "0x" + msg_sender.hex()
                if isinstance(msg_sender, bytes)
                else str(msg_sender)
            )
        ),
        "addresses": {},
        "uints": {},
        "ints": {},
        "bools": {},
        "bytes32s": {},
        "strings": {},
    }

    for key, val in event_data[IDX_ADDRESS][0]:
        addr = (
            val
            if isinstance(val, str)
            else ("0x" + val.hex() if isinstance(val, bytes) else str(val))
        )
        result["addresses"][key] = addr.lower() if isinstance(addr, str) else addr

    for key, val in event_data[IDX_UINT][0]:
        result["uints"][key] = val

    for key, val in event_data[IDX_INT][0]:
        result["ints"][key] = val

    for key, val in event_data[IDX_BOOL][0]:
        result["bools"][key] = val

    for key, val in event_data[IDX_BYTES32][0]:
        result["bytes32s"][key] = val

    for key, val in event_data[IDX_STRING][0]:
        result["strings"][key] = val

    return result


# =============================================================================
# CHECKPOINT
# =============================================================================

def load_checkpoint(path: Path) -> Optional[dict]:
    """Load checkpoint from JSON file.

    :param path: Path to checkpoint JSON file.
    :returns: Checkpoint dict or ``None`` if not found.
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

    :param path: Path to checkpoint JSON file.
    :param last_block: Last processed block number.
    :param last_timestamp: Last processed block timestamp.
    :param total_events: Total events processed so far.
    :param markets_seen: Number of unique markets seen.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint = {
        "symbol": "funding_factor_all",
        "last_block": last_block,
        "last_timestamp": last_timestamp,
        "total_events": total_events,
        "last_updated": datetime.now(tz=timezone.utc).isoformat(),
        "metadata": {"markets_seen": markets_seen},
    }
    with open(path, "w") as f:
        json.dump(checkpoint, f, indent=2)
    console.print(
        f"  Checkpoint saved: block [cyan]{last_block:,}[/cyan] -> [green]{path}[/green]"
    )


# =============================================================================
# BACKGROUND / DAEMON
# =============================================================================

def run_in_background(log_file: str, pid_file: str) -> bool:
    """Fork the process to run in the background.

    :param log_file: Path to log file for stdout/stderr.
    :param pid_file: Path to PID file.
    :returns: ``True`` if parent (should exit), ``False`` if child (continue).
    """
    global console

    log_path = Path(log_file)
    pid_path = Path(pid_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.parent.mkdir(parents=True, exist_ok=True)

    pid = os.fork()
    if pid > 0:
        print(f"Running in background (PID: {pid})")
        print(f"  Log file: {log_file}")
        print(f"  PID file: {pid_file}")
        return True

    os.setsid()
    pid2 = os.fork()
    if pid2 > 0:
        os._exit(0)

    sys.stdout.flush()
    sys.stderr.flush()
    log_fd = open(log_path, "a")
    os.dup2(log_fd.fileno(), sys.stdout.fileno())
    os.dup2(log_fd.fileno(), sys.stderr.fileno())

    with open(pid_path, "w") as f:
        f.write(str(os.getpid()))

    console = Console(file=log_fd, force_terminal=False)
    return False


# =============================================================================
# HYPERSYNC CLIENT
# =============================================================================

async def create_client(network: str) -> HypersyncClient:
    """Create HyperSync client.

    Reads the ``HYPERSYNC_API_TOKEN`` environment variable for authentication.

    :param network: Network name (``arbitrum``, ``avalanche``).
    :returns: HyperSync client instance.
    """
    url = HYPERSYNC_URLS.get(network)
    if not url:
        raise ValueError(f"Unsupported network: {network}")
    api_token = os.environ.get("HYPERSYNC_API_TOKEN")
    if api_token:
        console.print(f"  Using HyperSync API token: [cyan]{api_token[:8]}...[/cyan]")
    else:
        console.print("  [yellow]No HYPERSYNC_API_TOKEN set — may get 403 errors[/yellow]")
    return HypersyncClient(ClientConfig(url=url, bearer_token=api_token))


async def get_latest_block(client: HypersyncClient) -> int:
    """Get latest block number.

    :param client: HyperSync client instance.
    :returns: Latest block number.
    """
    return await client.get_height()


async def _stream_with_retry(
    client: HypersyncClient,
    query: Query,
    max_retries: int = MAX_RETRIES,
):
    """Stream HyperSync results with retry on transient errors.

    :param client: HyperSync client instance.
    :param query: HyperSync query.
    :param max_retries: Maximum retry attempts per failure.
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
                    return

                if response.data.blocks:
                    max_block = max(b.number for b in response.data.blocks)
                    current_from_block = max_block + 1

                attempt = 0
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


# =============================================================================
# EXTRACTION
# =============================================================================

async def extract_funding_events(
    client: HypersyncClient,
    network: str,
    from_block: int,
    to_block: Optional[int],
    market_filter: Optional[str] = None,
) -> list[FundingFactorRecord]:
    """Extract Funding events from GMX V2 EventEmitter.

    :param client: HyperSync client instance.
    :param network: Network name.
    :param from_block: Starting block number.
    :param to_block: Ending block number (``None`` for latest).
    :param market_filter: Optional market symbol filter (e.g., ``'ETH/USD'``).
    :returns: List of :class:`FundingFactorRecord` objects.
    """
    emitter = EVENT_EMITTER_ADDRESSES.get(network)
    if not emitter:
        raise ValueError(f"No EventEmitter for network: {network}")

    query = Query(
        from_block=from_block,
        to_block=to_block,
        logs=[
            LogSelection(
                address=[emitter],
                topics=[
                    [EVENT_LOG1_TOPIC],
                    [FUNDING_EVENT_HASH],
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
                LogField.DATA,
            ],
        ),
    )

    total_blocks = (to_block or 0) - from_block
    console.print(f"  EventEmitter: [cyan]{emitter}[/cyan]")
    console.print(
        f"  Block range:  [cyan]{from_block:,}[/cyan] to "
        f"[cyan]{to_block or 'latest':,}[/cyan] ({total_blocks:,} blocks)"
    )
    console.print(f"  Event:        [cyan]Funding (fundingFactorPerSecond)[/cyan]")

    records: list[FundingFactorRecord] = []
    block_timestamps: dict[int, int] = {}
    total_logs = 0
    decode_errors = 0
    unknown_markets: set[str] = set()
    t_start = time.monotonic()
    highest_block = from_block
    next_milestone_idx = 0

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
            "Extracting Funding events",
            total=total_blocks,
            logs=0,
            events=0,
            rate=0.0,
            errors=0,
        )

        async for response in _stream_with_retry(client, query):
            # Build block timestamp map
            # HyperSync returns timestamps as hex strings (e.g., "0x6789abcd")
            for block in response.data.blocks:
                if block.number is not None and block.timestamp is not None:
                    ts_val = block.timestamp
                    if isinstance(ts_val, str) and ts_val.startswith("0x"):
                        ts_val = int(ts_val, 16)
                    elif isinstance(ts_val, str):
                        ts_val = int(ts_val)
                    block_timestamps[block.number] = ts_val

            for log in response.data.logs:
                total_logs += 1

                if not log.data:
                    decode_errors += 1
                    continue

                decoded = decode_event_log_data(log.data)
                if "_decode_error" in decoded:
                    decode_errors += 1
                    continue

                event_name = decoded.get("event_name", "")
                if event_name != "Funding":
                    continue

                market_addr = decoded["addresses"].get("market", "")

                # Skip zero-address market (placeholder events)
                if market_addr == "0x0000000000000000000000000000000000000000":
                    continue

                # Skip swap-only markets (no perpetual trading, 0% funding)
                market_info = MARKETS.get(market_addr.lower())
                if market_info and market_info.get("indexToken") is None:
                    continue

                # fundingFactorPerSecond is always in uints (unsigned).
                # Direction (longs pay shorts or vice versa) is NOT encoded
                # in the Funding event — it depends on OI imbalance.
                # We treat the rate as the absolute magnitude.
                longs_pay = True  # not determinable from event
                factor_raw = decoded["uints"].get("fundingFactorPerSecond", 0)

                symbol = market_symbol(market_addr)

                # Track unknown markets
                if market_addr.lower() not in MARKETS:
                    unknown_markets.add(market_addr.lower())

                # Apply market filter
                if market_filter:
                    market_info = MARKETS.get(market_addr.lower(), {})
                    if market_info.get("symbol", "") != market_filter:
                        continue

                block_num = log.block_number or 0
                ts = block_timestamps.get(block_num, 0)
                dt_str = (
                    datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                    if ts
                    else ""
                )

                rate = factor_raw / FUNDING_FACTOR_PRECISION

                record = FundingFactorRecord(
                    symbol=symbol,
                    market=market_addr.lower(),
                    funding_factor_per_second=str(factor_raw),
                    funding_rate_per_second=rate,
                    longs_pay_shorts=longs_pay,
                    block_number=block_num,
                    block_timestamp=ts,
                    block_datetime=dt_str,
                    transaction_hash=log.transaction_hash or "",
                    log_index=log.log_index or 0,
                )
                records.append(record)

                if block_num > highest_block:
                    highest_block = block_num

            # Update progress
            elapsed = time.monotonic() - t_start
            rate = total_logs / elapsed if elapsed > 0 else 0
            blocks_done = highest_block - from_block
            progress.update(
                task,
                completed=blocks_done,
                logs=total_logs,
                events=len(records),
                rate=rate,
                errors=decode_errors,
            )

            # Log-friendly milestones
            if total_blocks > 0:
                pct = blocks_done / total_blocks * 100
                while (
                    next_milestone_idx < len(PROGRESS_MILESTONES)
                    and pct >= PROGRESS_MILESTONES[next_milestone_idx]
                ):
                    console.print(
                        f"  [{PROGRESS_MILESTONES[next_milestone_idx]}%] "
                        f"block {highest_block:,} | "
                        f"{len(records):,} events | "
                        f"{rate:.0f} logs/s"
                    )
                    next_milestone_idx += 1

    elapsed = time.monotonic() - t_start
    console.print(
        f"\n  Extraction complete in {elapsed:.1f}s: "
        f"{len(records):,} Funding events from {total_logs:,} logs"
    )
    if decode_errors:
        console.print(f"  [yellow]Decode errors: {decode_errors:,}[/yellow]")
    if unknown_markets:
        console.print(
            f"  [yellow]Unknown markets ({len(unknown_markets)}): "
            f"{', '.join(sorted(unknown_markets)[:5])}{'...' if len(unknown_markets) > 5 else ''}[/yellow]"
        )

    return records


# =============================================================================
# AGGREGATION
# =============================================================================

def aggregate_hourly_rates(
    records: list[FundingFactorRecord],
) -> dict[str, "pl.DataFrame"]:
    """Aggregate raw Funding events into hourly rate snapshots per symbol.

    Produces one DataFrame per symbol with columns matching the existing
    ``rates/{SYMBOL}/1h.parquet`` schema.

    :param records: List of :class:`FundingFactorRecord` objects.
    :returns: Dict mapping symbol to hourly DataFrame.
    """
    if not HAS_POLARS:
        raise RuntimeError("polars required for aggregation")

    if not records:
        return {}

    df = pl.DataFrame([asdict(r) for r in records])

    # Convert block_timestamp to datetime
    df = df.with_columns(
        pl.from_epoch(pl.col("block_timestamp"), time_unit="s")
        .alias("timestamp")
        .cast(pl.Datetime("ms", "UTC")),
    )

    # Truncate to hour
    df = df.with_columns(
        pl.col("timestamp").dt.truncate("1h").alias("hour"),
    )

    # Aggregate per symbol per hour
    hourly = (
        df.group_by(["symbol", "market", "hour"])
        .agg(
            pl.col("funding_rate_per_second").mean().alias("funding_rate"),
            pl.col("funding_rate_per_second").min().alias("funding_rate_min"),
            pl.col("funding_rate_per_second").max().alias("funding_rate_max"),
            pl.col("longs_pay_shorts").mode().first().alias("longs_pay_shorts"),
            pl.len().alias("update_count"),
        )
        .sort(["symbol", "hour"])
    )

    # Compute derived columns
    hourly = hourly.with_columns(
        (pl.col("funding_rate") * 3600).alias("funding_rate_hourly"),
        (pl.col("funding_rate") * 3600 * 8760).alias("funding_rate_annualized"),
        # Signed fee columns: positive = pays, negative = receives
        pl.when(pl.col("longs_pay_shorts"))
        .then(pl.col("funding_rate") * 3600)
        .otherwise(pl.col("funding_rate") * -3600)
        .alias("funding_fee_long"),
        pl.when(pl.col("longs_pay_shorts"))
        .then(pl.col("funding_rate") * -3600)
        .otherwise(pl.col("funding_rate") * 3600)
        .alias("funding_fee_short"),
    )

    # Rename hour -> timestamp
    hourly = hourly.rename({"hour": "timestamp"})

    # Split by symbol
    result = {}
    for symbol in hourly["symbol"].unique().sort().to_list():
        sym_df = hourly.filter(pl.col("symbol") == symbol)
        # Select columns in the expected order
        sym_df = sym_df.select([
            "timestamp",
            "funding_rate",
            "funding_rate_min",
            "funding_rate_max",
            "funding_rate_hourly",
            "funding_rate_annualized",
            "longs_pay_shorts",
            "funding_fee_long",
            "funding_fee_short",
            "update_count",
            "symbol",
            "market",
        ]).cast({"update_count": pl.UInt32})
        result[symbol] = sym_df

    return result


# =============================================================================
# STORAGE
# =============================================================================

def append_parquet(df: "pl.DataFrame", filepath: Path) -> None:
    """Append DataFrame to existing Parquet file, deduplicating.

    :param df: New data to append.
    :param filepath: Parquet file path.
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)

    if filepath.exists():
        existing = pl.read_parquet(filepath)
        for col in existing.columns:
            if col in df.columns and existing[col].dtype != df[col].dtype:
                df = df.with_columns(pl.col(col).cast(existing[col].dtype))
        combined = pl.concat([existing, df], how="diagonal_relaxed")
    else:
        combined = df

    # Dedup
    if "block_number" in combined.columns and "log_index" in combined.columns:
        combined = combined.unique(subset=["block_number", "log_index"], keep="last")
        combined = combined.sort(["block_number", "log_index"])
    elif "timestamp" in combined.columns:
        combined = combined.unique(subset=["timestamp"], keep="last")
        combined = combined.sort("timestamp")

    combined.write_parquet(filepath)


def save_raw_per_symbol(records: list[FundingFactorRecord], output_dir: Path) -> None:
    """Save raw Funding events to per-symbol Parquet files.

    :param records: List of :class:`FundingFactorRecord` objects.
    :param output_dir: Base output directory (e.g., ``data/funding/arbitrum``).
    """
    if not HAS_POLARS:
        console.print("[red]polars required for Parquet[/red]")
        return

    by_symbol: dict[str, list[FundingFactorRecord]] = defaultdict(list)
    for r in records:
        by_symbol[r.symbol].append(r)

    for symbol, sym_records in sorted(by_symbol.items()):
        filepath = output_dir / "raw" / "funding" / symbol / "partition=0" / "data.parquet"
        df = pl.DataFrame([asdict(r) for r in sym_records])
        append_parquet(df, filepath)
        console.print(
            f"  Raw: [cyan]{len(sym_records):,}[/cyan] events -> [green]{filepath}[/green]"
        )


def save_rates_per_symbol(
    hourly_by_symbol: dict[str, "pl.DataFrame"], output_dir: Path
) -> None:
    """Save hourly aggregated rates to per-symbol Parquet files.

    :param hourly_by_symbol: Dict from :func:`aggregate_hourly_rates`.
    :param output_dir: Base output directory (e.g., ``data/funding/arbitrum``).
    """
    for symbol, df in sorted(hourly_by_symbol.items()):
        filepath = output_dir / "rates" / symbol / "1h.parquet"
        append_parquet(df, filepath)
        console.print(
            f"  Rates: [cyan]{len(df):,}[/cyan] hours -> [green]{filepath}[/green]"
        )


def save_json(data: list, filename: str) -> None:
    """Save to JSON.

    :param data: List of dataclass instances.
    :param filename: Output file path.
    """
    with open(filename, "w") as f:
        json.dump([asdict(d) for d in data], f, indent=2, default=str)
    console.print(f"  Saved [cyan]{len(data):,}[/cyan] records to [green]{filename}[/green]")


def save_csv(data: list, filename: str) -> None:
    """Save to CSV using polars.

    :param data: List of dataclass instances.
    :param filename: Output file path.
    """
    if not HAS_POLARS:
        console.print("[red]polars required for CSV[/red]")
        return
    df = pl.DataFrame([asdict(d) for d in data])
    df.write_csv(filename)
    console.print(f"  Saved [cyan]{len(data):,}[/cyan] records to [green]{filename}[/green]")


# =============================================================================
# SUMMARY
# =============================================================================

def print_summary(records: list[FundingFactorRecord]) -> None:
    """Print summary statistics.

    :param records: List of :class:`FundingFactorRecord` objects.
    """
    table = Table(title="Funding Factor Summary", show_lines=False)
    table.add_column("Symbol", style="cyan")
    table.add_column("Events", justify="right")
    table.add_column("Avg Rate/s", justify="right")
    table.add_column("Avg Hourly", justify="right")
    table.add_column("Avg Annual", justify="right")
    table.add_column("Direction", justify="center")

    by_symbol: dict[str, list[FundingFactorRecord]] = defaultdict(list)
    for r in records:
        by_symbol[r.symbol].append(r)

    for symbol in sorted(by_symbol.keys()):
        sym_records = by_symbol[symbol]
        avg_rate = sum(r.funding_rate_per_second for r in sym_records) / len(sym_records)
        avg_hourly = avg_rate * 3600
        avg_annual = avg_rate * 3600 * 8760
        longs_pay_count = sum(1 for r in sym_records if r.longs_pay_shorts)
        direction = (
            "Longs pay"
            if longs_pay_count > len(sym_records) / 2
            else "Shorts pay"
        )

        table.add_row(
            symbol,
            f"{len(sym_records):,}",
            f"{avg_rate:.2e}",
            f"{avg_hourly:.6f}",
            f"{avg_annual:.2%}",
            direction,
        )

    console.print()
    console.print(table)

    timestamps = [r.block_timestamp for r in records if r.block_timestamp]
    if timestamps:
        first = datetime.fromtimestamp(min(timestamps), tz=timezone.utc)
        last = datetime.fromtimestamp(max(timestamps), tz=timezone.utc)
        console.print(
            f"\n  Time range: [cyan]{first.isoformat()}[/cyan] to "
            f"[cyan]{last.isoformat()}[/cyan]"
        )

    console.print(f"  Total Funding events: [cyan]{len(records):,}[/cyan]")
    console.print(f"  Markets:              [cyan]{len(by_symbol):,}[/cyan]")


# =============================================================================
# MAIN
# =============================================================================

async def async_main(args: argparse.Namespace) -> None:
    """Async main entry point.

    :param args: Parsed command-line arguments.
    """
    output_dir = Path(args.output_dir) / args.network
    checkpoint_dir = (
        Path(args.checkpoint_dir) if args.checkpoint_dir else output_dir / "checkpoints"
    )
    checkpoint_path = checkpoint_dir / "funding_factor_checkpoint.json"

    from_block = args.from_block

    if args.resume and from_block is None:
        checkpoint = load_checkpoint(checkpoint_path)
        if checkpoint:
            from_block = checkpoint["last_block"] + 1
            console.print(
                f"  Resuming from checkpoint: block [cyan]{from_block:,}[/cyan]"
            )
        else:
            from_block = GMX_V2_GENESIS_BLOCK
            console.print(
                f"  No checkpoint found. Starting from genesis: "
                f"[cyan]{from_block:,}[/cyan]"
            )
    elif from_block is None:
        from_block = GMX_V2_GENESIS_BLOCK

    header_lines = [
        f"Network:     [cyan]{args.network}[/cyan]",
        f"Block range: [cyan]{from_block:,}[/cyan] to [cyan]{args.to_block or 'latest'}[/cyan]",
        f"Output:      [cyan]{args.output}[/cyan]",
        f"Output dir:  [cyan]{output_dir}[/cyan]",
    ]
    if args.market:
        header_lines.append(f"Market:      [cyan]{args.market}[/cyan]")
    if args.resume:
        header_lines.append(
            f"Resume:      [cyan]enabled[/cyan] (checkpoint: {checkpoint_path})"
        )

    console.print(
        Panel(
            "\n".join(header_lines),
            title="GMX V2 Funding Factor Extractor",
            subtitle="HyperSync + eth_abi",
            border_style="blue",
        )
    )

    with console.status("Connecting to HyperSync..."):
        client = await create_client(args.network)

    to_block = args.to_block
    if to_block is None:
        with console.status("Fetching latest block..."):
            to_block = await get_latest_block(client)
        console.print(f"  Latest block: [cyan]{to_block:,}[/cyan]")

    if from_block >= to_block:
        console.print(
            f"\n[yellow]Already up to date "
            f"(from_block {from_block:,} >= to_block {to_block:,})[/yellow]"
        )
        return

    console.print()
    records = await extract_funding_events(
        client=client,
        network=args.network,
        from_block=from_block,
        to_block=to_block,
        market_filter=args.market,
    )

    if not records:
        console.print("\n[yellow]No Funding events found.[/yellow]")
        if args.resume:
            save_checkpoint(
                checkpoint_path,
                last_block=to_block,
                last_timestamp=int(time.time()),
                total_events=0,
                markets_seen=0,
            )
        return

    # Save outputs
    console.print()
    if args.output == "parquet":
        save_raw_per_symbol(records, output_dir)

        with console.status("Aggregating hourly rates..."):
            hourly = aggregate_hourly_rates(records)

        save_rates_per_symbol(hourly, output_dir)
    elif args.output == "json":
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = f"gmx_v2_funding_factor_{args.network}_{from_block}_{to_block}_{ts}"
        save_json(records, f"{base}.json")
    elif args.output == "csv":
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        base = f"gmx_v2_funding_factor_{args.network}_{from_block}_{to_block}_{ts}"
        save_csv(records, f"{base}.csv")

    # Save checkpoint
    if args.resume:
        last_block = max(r.block_number for r in records)
        last_timestamp = max(r.block_timestamp for r in records if r.block_timestamp)
        unique_markets = len(set(r.symbol for r in records))

        prev_checkpoint = load_checkpoint(checkpoint_path)
        prev_total = prev_checkpoint["total_events"] if prev_checkpoint else 0

        save_checkpoint(
            checkpoint_path,
            last_block=last_block,
            last_timestamp=last_timestamp,
            total_events=prev_total + len(records),
            markets_seen=unique_markets,
        )

    print_summary(records)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract GMX V2 funding factor data using HyperSync",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full historical extraction
  poetry run python scripts/extract_funding_factor.py --from-block 120000000

  # Quick test
  poetry run python scripts/extract_funding_factor.py --from-block 290000000 --to-block 290100000 --output json

  # Incremental cronjob
  poetry run python scripts/extract_funding_factor.py --resume

  # Background mode
  poetry run python scripts/extract_funding_factor.py --resume --background
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
    parser.add_argument(
        "--to-block",
        type=int,
        default=None,
        help="End block (default: latest)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data/funding",
        help="Base output directory (default: ./data/funding)",
    )
    parser.add_argument(
        "--output",
        choices=["json", "csv", "parquet"],
        default="parquet",
        help="Output format (default: parquet)",
    )
    parser.add_argument(
        "--market",
        type=str,
        default=None,
        help="Filter by market symbol (e.g., 'ETH/USD')",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Enable checkpoint-based incremental mode",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default=None,
        help="Override checkpoint directory",
    )
    parser.add_argument(
        "--background",
        action="store_true",
        help="Run in background (daemonize)",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default="./logs/funding_factor.log",
        help="Log file for background mode",
    )
    parser.add_argument(
        "--pid-file",
        type=str,
        default="./logs/funding_factor.pid",
        help="PID file for background mode",
    )

    args = parser.parse_args()

    # Background mode
    if args.background:
        is_parent = run_in_background(args.log_file, args.pid_file)
        if is_parent:
            return

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
