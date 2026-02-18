#!/usr/bin/env python3
"""
GMX V2 Funding Rate Historical Backfill via DataStore Reads
============================================================
Reads ``savedFundingFactorPerSecond`` from the GMX DataStore contract at
hourly block intervals using an archive node. This value is a **signed int256**
stored on-chain since GMX V2 launch (August 2023).

- Positive value → longs pay shorts
- Negative value → shorts pay longs
- Precision: 30-decimal fixed-point (divide by 10^30 for decimal rate)

This script fills the gap from GMX V2 genesis (block ~120M, Aug 2023) to
when the ``Funding`` event was introduced in V2.2 (~block 370M, Aug 2025).
For post-Aug-2025 data, use ``extract_funding_factor.py`` (HyperSync).

QUICK START
-----------
    export JSON_RPC_ARBITRUM=<your-archive-node-url>
    poetry run python scripts/extract_funding_datastore.py

USAGE
-----
    poetry run python scripts/extract_funding_datastore.py [OPTIONS]

OPTIONS
-------
    --from-block     Starting block number (default: 120000000)
    --to-block       Ending block number (default: 370000000)
    --output-dir     Base output directory (default: ./data/funding)
    --output         Output format: "json", "csv", or "parquet" (default: parquet)
    --market         Filter by market symbol (e.g., "ETH/USD")
    --interval       Sampling interval in blocks (default: 1200 = ~1 hour on Arbitrum)
    --batch-size     Number of concurrent RPC calls per batch (default: 50)
    --resume         Enable checkpoint-based incremental mode
    --list-markets   List available markets and exit
"""

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from concurrent.futures import ThreadPoolExecutor, as_completed
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from web3 import Web3

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    from rich.console import Console
    from rich.progress import (
        BarColumn,
        Progress,
        SpinnerColumn,
        TextColumn,
        TimeElapsedColumn,
    )

    console = Console()
except ImportError:
    import builtins

    class _FallbackConsole:
        def print(self, *a, **kw):
            builtins.print(*a)

    console = _FallbackConsole()


# =============================================================================
# CONSTANTS
# =============================================================================

# DataStore contract on Arbitrum
DATASTORE_ADDRESS = "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8"

# Precision: fundingFactorPerSecond is stored as 30-decimal fixed-point
FUNDING_FACTOR_PRECISION = 10**30

# Approximate blocks per hour on Arbitrum (~0.25s block time = 14,400 blocks/hour)
# Note: block time varied from ~0.3s (2023) to ~0.25s (2025+), so this is approximate
BLOCKS_PER_HOUR = 14_400

# GMX V2 genesis and V2.2 cutoff on Arbitrum
GMX_V2_GENESIS_BLOCK = 120_000_000  # ~Aug 2023
GMX_V22_FUNDING_EVENT_START = 370_000_000  # ~Aug 2025

# DataStore key: keccak256(abi.encode("SAVED_FUNDING_FACTOR_PER_SECOND"))
SAVED_FUNDING_FACTOR_KEY_BASE = Web3.keccak(
    abi_encode(["string"], ["SAVED_FUNDING_FACTOR_PER_SECOND"])
)

# DataStore ABI (just the getInt function we need)
DATASTORE_ABI = [
    {
        "inputs": [{"name": "key", "type": "bytes32"}],
        "name": "getInt",
        "outputs": [{"name": "", "type": "int256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"name": "key", "type": "bytes32"}],
        "name": "getUint",
        "outputs": [{"name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# Multicall3 contract (deployed on all major EVM chains, including Arbitrum)
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
MULTICALL3_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "target", "type": "address"},
                    {"name": "allowFailure", "type": "bool"},
                    {"name": "callData", "type": "bytes"},
                ],
                "name": "calls",
                "type": "tuple[]",
            }
        ],
        "name": "aggregate3",
        "outputs": [
            {
                "components": [
                    {"name": "success", "type": "bool"},
                    {"name": "returnData", "type": "bytes"},
                ],
                "name": "returnData",
                "type": "tuple[]",
            }
        ],
        "stateMutability": "payable",
        "type": "function",
    }
]

# Markets that existed at GMX V2 launch (Aug 2023). Newer markets are added
# later and will return 0 until their creation block.
# Source: https://github.com/gmx-io/gmx-interface (sdk/src/configs/markets.ts)
MARKETS = {
    # Major perpetual markets (from launch or shortly after)
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
    # Single-asset / alternative collateral markets
    "0x7c11f78ce78768518d743e81fdfa2f860c6b9a77": {"symbol": "BTC/USD [WBTC.e-WBTC.e]", "indexToken": "BTC"},
    "0x450bb6774dd8a756274e0ab4107953259d2ac541": {"symbol": "ETH/USD [WETH-WETH]", "indexToken": "ETH"},
    "0xe68caaacdf6439628dfd2fe624847602991a31eb": {"symbol": "BTC/USD [WBTC-WBTC]", "indexToken": "BTC"},
    # Newer perpetual markets (added later, will show 0 before creation)
    "0x2b477989a149b17073d9c9c82ec9cb03591325a6": {"symbol": "WIF/USD", "indexToken": "WIF"},
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
    "0xdab9ba9e3a301ccb353f18b4c8542ba2149e4010": {"symbol": "ETH/USD [WETH-WETH-2]", "indexToken": "ETH"},
    "0x08a902113f7f41a8658ebb1175f9c847bf4fb9d8": {"symbol": "ETH/USD [WETH-USDT]", "indexToken": "ETH"},
    "0x0cf1fb4d1ff67a3d8ca92c9d6643f8f9be8e03e5": {"symbol": "ETH/USD [wstETH-USDe]", "indexToken": "ETH"},
    "0xd62068697bcc92af253225676d618b0c9f17c663": {"symbol": "BTC/USD [tBTC-tBTC]", "indexToken": "BTC"},
}


# =============================================================================
# DATA CLASSES & HELPERS
# =============================================================================


def market_symbol(address: str) -> str:
    """Look up human-readable symbol for a market address.

    :param address: Lowercase hex market address.
    :returns: Symbol string (e.g., ``'ETH/USD'``). Falls back to truncated address.
    """
    info = MARKETS.get(address.lower())
    if info:
        sym = info["symbol"]
        base = sym.split("/")[0]
        bracket = sym.find("[")
        if bracket != -1:
            suffix = sym[bracket + 1 : -1].strip()
            return f"{base}_{suffix}"
        return base.replace("[", "").replace("]", "").replace(" ", "_")
    return address[:10]


def build_market_key(market_address: str) -> bytes:
    """Build the DataStore key for savedFundingFactorPerSecond for a market.

    :param market_address: Checksummed or lowercase market address.
    :returns: 32-byte key hash.
    """
    return Web3.keccak(
        abi_encode(
            ["bytes32", "address"],
            [SAVED_FUNDING_FACTOR_KEY_BASE, Web3.to_checksum_address(market_address)],
        )
    )


def read_all_markets_multicall(
    multicall_contract,
    datastore_contract,
    market_keys: dict[str, bytes],
    block_number: int,
) -> dict[str, int]:
    """Read savedFundingFactorPerSecond for all markets in one Multicall3 call.

    Batches all ``getInt`` calls into a single ``aggregate3`` RPC call,
    reducing N market reads to 1 RPC round-trip.

    :param multicall_contract: Web3 contract instance for Multicall3.
    :param datastore_contract: Web3 contract instance for DataStore.
    :param market_keys: Dict of market address → pre-computed 32-byte key.
    :param block_number: Block to query at.
    :returns: Dict of market address → signed int256 value.
    """
    ds_checksum = Web3.to_checksum_address(DATASTORE_ADDRESS)
    market_addrs = list(market_keys.keys())

    # Build multicall3 calls: each is (target, allowFailure, callData)
    calls = []
    for addr in market_addrs:
        calldata = datastore_contract.encode_abi("getInt", [market_keys[addr]])
        calls.append((ds_checksum, True, bytes.fromhex(calldata[2:])))

    # Single RPC call for all markets at this block
    results = multicall_contract.functions.aggregate3(calls).call(
        block_identifier=block_number
    )

    # Decode results
    market_values = {}
    for i, result in enumerate(results):
        success = result[0]
        return_data = result[1]
        if success and len(return_data) >= 32:
            value = abi_decode(["int256"], return_data)[0]
            market_values[market_addrs[i]] = value
        else:
            market_values[market_addrs[i]] = 0

    return market_values


@dataclass
class FundingDatastoreRecord:
    """A single funding rate reading from the DataStore.

    :ivar symbol: Derived symbol (e.g., ``'ETH'``)
    :ivar market: Market contract address (lowercase)
    :ivar funding_factor_per_second: Raw signed 30-decimal integer as string
    :ivar funding_rate_per_second: Decimal per-second rate (signed)
    :ivar longs_pay_shorts: True when longs pay shorts
    :ivar block_number: Block number sampled
    :ivar block_timestamp: Unix timestamp (seconds)
    :ivar block_datetime: ISO 8601 datetime string
    """

    symbol: str
    market: str
    funding_factor_per_second: str
    funding_rate_per_second: float
    longs_pay_shorts: bool
    block_number: int
    block_timestamp: int
    block_datetime: str


# =============================================================================
# RPC HELPERS
# =============================================================================


def get_rpc_url() -> str:
    """Get the first valid RPC URL from the environment.

    :returns: RPC URL string.
    :raises ValueError: If no RPC URL is found.
    """
    rpc_raw = os.environ.get("JSON_RPC_ARBITRUM") or os.environ.get(
        "ARBITRUM_CHAIN_JSON_RPC", ""
    )
    if not rpc_raw.strip():
        raise ValueError(
            "Set JSON_RPC_ARBITRUM or ARBITRUM_CHAIN_JSON_RPC to an archive node URL"
        )
    # Handle space-separated URLs (take the first one)
    return rpc_raw.strip().split()[0]


def get_block_timestamp(w3: Web3, block_number: int) -> int:
    """Get the timestamp for a block.

    :param w3: Web3 instance.
    :param block_number: Block number.
    :returns: Unix timestamp.
    """
    block = w3.eth.get_block(block_number)
    return block["timestamp"]


# =============================================================================
# EXTRACTION
# =============================================================================


def extract_funding_rates(
    w3: Web3,
    from_block: int,
    to_block: int,
    markets: dict[str, dict],
    interval_blocks: int = BLOCKS_PER_HOUR,
    market_filter: Optional[str] = None,
    workers: int = 1,
    checkpoint_dir: Optional[Path] = None,
    checkpoint_interval: int = 500,
) -> list[FundingDatastoreRecord]:
    """Extract historical funding rates using Multicall3-batched DataStore reads.

    Uses Multicall3 to read all markets in a single RPC call per block,
    reducing N individual ``getInt`` calls to 1 ``aggregate3`` call.
    Optionally uses ThreadPoolExecutor for parallel block processing.

    :param w3: Web3 instance connected to archive node.
    :param from_block: Starting block number.
    :param to_block: Ending block number.
    :param markets: Market address → info dict.
    :param interval_blocks: Block interval between samples (default: ~1 hour).
    :param market_filter: Optional symbol filter (e.g., ``'ETH/USD'``).
    :param workers: Number of concurrent workers (default: 1).
    :param checkpoint_dir: Optional directory for periodic checkpoint saves.
    :param checkpoint_interval: Save checkpoint every N sample blocks.
    :returns: List of :class:`FundingDatastoreRecord` objects.
    """
    ds = w3.eth.contract(
        address=Web3.to_checksum_address(DATASTORE_ADDRESS),
        abi=DATASTORE_ABI,
    )
    mc = w3.eth.contract(
        address=Web3.to_checksum_address(MULTICALL3_ADDRESS),
        abi=MULTICALL3_ABI,
    )

    # Filter markets
    target_markets = {}
    for addr, info in markets.items():
        if info.get("indexToken") is None:
            continue
        if market_filter and info.get("symbol") != market_filter:
            continue
        target_markets[addr] = info

    # Pre-compute DataStore keys
    market_keys = {addr: build_market_key(addr) for addr in target_markets}

    # Generate sample blocks
    sample_blocks = list(range(from_block, to_block + 1, interval_blocks))
    total_samples = len(sample_blocks)

    console.print(f"\n  Markets:       {len(target_markets)}")
    console.print(f"  Block range:   {from_block:,} → {to_block:,}")
    console.print(f"  Interval:      {interval_blocks} blocks (~{interval_blocks / BLOCKS_PER_HOUR:.1f}h)")
    console.print(f"  Sample points: {total_samples:,}")
    console.print(f"  RPC calls:     ~{total_samples * 2:,} (Multicall3 batched, {len(target_markets)} markets/call)")
    if workers > 1:
        console.print(f"  Workers:       {workers}")

    records: list[FundingDatastoreRecord] = []
    errors = 0
    t_start = time.monotonic()

    def process_block(block_num: int) -> list[FundingDatastoreRecord]:
        """Process a single block: fetch timestamp + multicall all markets."""
        ts = w3.eth.get_block(block_num)["timestamp"]
        dt_str = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()

        # Multicall: read ALL markets in ONE RPC call
        market_values = read_all_markets_multicall(mc, ds, market_keys, block_num)

        block_records = []
        for addr, value in market_values.items():
            if value == 0:
                continue
            rate = value / FUNDING_FACTOR_PRECISION
            sym = market_symbol(addr)
            block_records.append(FundingDatastoreRecord(
                symbol=sym,
                market=addr.lower(),
                funding_factor_per_second=str(value),
                funding_rate_per_second=rate,
                longs_pay_shorts=(value > 0),
                block_number=block_num,
                block_timestamp=ts,
                block_datetime=dt_str,
            ))
        return block_records

    # Setup progress bar
    try:
        progress_ctx = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=40),
            TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
            TextColumn("[cyan]{task.fields[blocks]:,}[/cyan] blocks"),
            TextColumn("[green]{task.fields[records]:,}[/green] records"),
            TextColumn("[yellow]{task.fields[rate]:.1f}[/yellow] blk/s"),
            TextColumn("[red]{task.fields[errors]}[/red] err"),
            TimeElapsedColumn(),
            console=console,
            transient=False,
        )
    except Exception:
        progress_ctx = None

    def update_progress(progress, task_id, completed):
        """Update progress bar if available."""
        elapsed = time.monotonic() - t_start
        rate = completed / max(1, elapsed)
        if progress:
            progress.update(
                task_id, completed=completed,
                blocks=completed, records=len(records),
                rate=rate, errors=errors,
            )
        elif completed % 100 == 0:
            console.print(
                f"  [{completed/total_samples*100:.1f}%] "
                f"{len(records):,} records | {rate:.1f} blk/s"
            )

    def run_extraction(progress=None, task_id=None):
        """Core extraction loop, works with or without progress bar."""
        nonlocal errors

        if workers <= 1:
            # Sequential processing
            for i, block_num in enumerate(sample_blocks):
                try:
                    block_records = process_block(block_num)
                    records.extend(block_records)
                except Exception as e:
                    errors += 1
                    if errors <= 10:
                        console.print(f"  [yellow]Block {block_num}: {e}[/yellow]")
                update_progress(progress, task_id, i + 1)
                if checkpoint_dir and (i + 1) % checkpoint_interval == 0:
                    save_checkpoint(checkpoint_dir, block_num, len(records))
        else:
            # Parallel processing
            completed = 0
            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(process_block, bn): bn
                    for bn in sample_blocks
                }
                for future in as_completed(futures):
                    block_num = futures[future]
                    completed += 1
                    try:
                        block_records = future.result()
                        records.extend(block_records)
                    except Exception as e:
                        errors += 1
                        if errors <= 10:
                            console.print(f"  [yellow]Block {block_num}: {e}[/yellow]")
                    update_progress(progress, task_id, completed)
                    if checkpoint_dir and completed % checkpoint_interval == 0:
                        save_checkpoint(checkpoint_dir, block_num, len(records))

    if progress_ctx is not None:
        with progress_ctx as progress:
            task_id = progress.add_task(
                "Reading DataStore", total=total_samples,
                blocks=0, records=0, rate=0.0, errors=0,
            )
            run_extraction(progress, task_id)
    else:
        run_extraction()

    # Sort records by block number (important for parallel execution)
    records.sort(key=lambda r: (r.block_number, r.symbol))

    elapsed = time.monotonic() - t_start
    console.print(
        f"\n  Extraction complete in {elapsed:.1f}s: "
        f"{len(records):,} non-zero readings from {total_samples:,} sampled blocks"
    )
    if errors:
        console.print(f"  [yellow]Errors: {errors:,}[/yellow]")

    return records


# =============================================================================
# AGGREGATION
# =============================================================================


def aggregate_hourly_rates(
    records: list[FundingDatastoreRecord],
) -> dict[str, "pl.DataFrame"]:
    """Aggregate DataStore readings into hourly rate snapshots per symbol.

    Since we already sample at hourly intervals, this mostly just formats
    the output into the standard schema.

    :param records: List of :class:`FundingDatastoreRecord` objects.
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

    # Aggregate per symbol per market per hour
    hourly = (
        df.group_by(["symbol", "market", "hour"])
        .agg(
            pl.col("funding_rate_per_second").mean().alias("funding_rate"),
            pl.col("funding_rate_per_second").min().alias("funding_rate_min"),
            pl.col("funding_rate_per_second").max().alias("funding_rate_max"),
            pl.col("longs_pay_shorts").last().alias("longs_pay_shorts"),
            pl.len().alias("update_count"),
        )
        .sort(["symbol", "hour"])
    )

    # Compute derived columns
    hourly = hourly.with_columns(
        (pl.col("funding_rate") * 3600).alias("funding_rate_hourly"),
        (pl.col("funding_rate") * 3600 * 8760).alias("funding_rate_annualized"),
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


def save_parquet(hourly_data: dict[str, "pl.DataFrame"], output_dir: Path) -> None:
    """Save hourly DataFrames to per-symbol Parquet files.

    :param hourly_data: Dict mapping symbol to hourly DataFrame.
    :param output_dir: Base output directory.
    """
    for symbol, df in hourly_data.items():
        filepath = output_dir / "arbitrum" / "rates" / symbol / "1h_datastore.parquet"
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if filepath.exists():
            existing = pl.read_parquet(filepath)
            for col in existing.columns:
                if col in df.columns and existing[col].dtype != df[col].dtype:
                    df = df.with_columns(pl.col(col).cast(existing[col].dtype))
            combined = pl.concat([existing, df], how="diagonal_relaxed")
            combined = combined.unique(subset=["timestamp"], keep="last")
            combined = combined.sort("timestamp")
            combined.write_parquet(filepath)
        else:
            df.write_parquet(filepath)

        console.print(f"  {symbol}: {len(df):,} rows → {filepath}")


def save_json(records: list[FundingDatastoreRecord], output_dir: Path) -> None:
    """Save raw records to JSON file.

    :param records: List of records.
    :param output_dir: Base output directory.
    """
    filepath = output_dir / "arbitrum" / "raw" / "funding_datastore.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w") as f:
        json.dump([asdict(r) for r in records], f, indent=2, default=str)
    console.print(f"  Saved {len(records):,} records → {filepath}")


# =============================================================================
# CHECKPOINT
# =============================================================================


def load_checkpoint(checkpoint_dir: Path) -> Optional[int]:
    """Load the last processed block from checkpoint file.

    :param checkpoint_dir: Directory containing checkpoint file.
    :returns: Last processed block number, or ``None`` if no checkpoint.
    """
    filepath = checkpoint_dir / "funding_datastore_checkpoint.json"
    if not filepath.exists():
        return None
    with open(filepath) as f:
        data = json.load(f)
    return data.get("last_block")


def save_checkpoint(checkpoint_dir: Path, last_block: int, total_records: int) -> None:
    """Save checkpoint with last processed block.

    :param checkpoint_dir: Directory for checkpoint file.
    :param last_block: Last successfully processed block number.
    :param total_records: Total records extracted so far.
    """
    filepath = checkpoint_dir / "funding_datastore_checkpoint.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "last_block": last_block,
        "total_records": total_records,
        "updated_at": datetime.now(tz=timezone.utc).isoformat(),
    }
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


# =============================================================================
# MAIN
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Extract GMX V2 historical funding rates via DataStore archive reads",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--from-block",
        type=int,
        default=GMX_V2_GENESIS_BLOCK,
        help=f"Starting block (default: {GMX_V2_GENESIS_BLOCK:,})",
    )
    parser.add_argument(
        "--to-block",
        type=int,
        default=GMX_V22_FUNDING_EVENT_START,
        help=f"Ending block (default: {GMX_V22_FUNDING_EVENT_START:,})",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data/funding",
        help="Base output directory (default: ./data/funding)",
    )
    parser.add_argument(
        "--output",
        choices=["json", "parquet"],
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
        "--interval",
        type=int,
        default=BLOCKS_PER_HOUR,
        help=f"Sampling interval in blocks (default: {BLOCKS_PER_HOUR})",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of concurrent workers for parallel block processing (default: 1)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint",
    )
    parser.add_argument(
        "--list-markets",
        action="store_true",
        help="List available markets and exit",
    )

    args = parser.parse_args()

    if args.list_markets:
        console.print(f"\n{'Symbol':<30} {'Market Address':<44}")
        console.print("-" * 74)
        for addr, info in sorted(MARKETS.items(), key=lambda x: x[1]["symbol"]):
            if info.get("indexToken"):
                console.print(f"{info['symbol']:<30} {addr}")
        console.print(f"\nTotal: {sum(1 for v in MARKETS.values() if v.get('indexToken'))} perpetual markets")
        sys.exit(0)

    output_dir = Path(args.output_dir)
    checkpoint_dir = output_dir / "arbitrum" / "checkpoints"

    # Resume from checkpoint
    from_block = args.from_block
    if args.resume:
        last = load_checkpoint(checkpoint_dir)
        if last is not None:
            from_block = last + 1
            console.print(f"  Resuming from checkpoint: block {from_block:,}")

    console.print("\n" + "=" * 70)
    console.print("GMX V2 FUNDING RATE BACKFILL (DataStore Archive Reads)")
    console.print("=" * 70)

    # Connect to archive node
    rpc_url = get_rpc_url()
    console.print(f"  RPC: {rpc_url[:50]}...")
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 60}))
    if not w3.is_connected():
        console.print("[red]ERROR: Cannot connect to RPC[/red]")
        sys.exit(1)
    console.print(f"  Connected. Latest block: {w3.eth.block_number:,}")

    # Extract
    records = extract_funding_rates(
        w3=w3,
        from_block=from_block,
        to_block=args.to_block,
        markets=MARKETS,
        interval_blocks=args.interval,
        market_filter=args.market,
        workers=args.workers,
        checkpoint_dir=checkpoint_dir,
    )

    if not records:
        console.print("\n[yellow]No non-zero funding rates found in range.[/yellow]")
        sys.exit(0)

    # Save
    console.print(f"\nSaving {len(records):,} records...")
    if args.output == "json":
        save_json(records, output_dir)
    else:
        hourly = aggregate_hourly_rates(records)
        save_parquet(hourly, output_dir)

    # Save checkpoint
    max_block = max(r.block_number for r in records)
    save_checkpoint(checkpoint_dir, max_block, len(records))

    # Summary
    symbols = set(r.symbol for r in records)
    timestamps = [r.block_timestamp for r in records]
    first = datetime.fromtimestamp(min(timestamps), tz=timezone.utc)
    last = datetime.fromtimestamp(max(timestamps), tz=timezone.utc)

    console.print(f"\n  Symbols:    {len(symbols)}")
    console.print(f"  Time range: {first.date()} → {last.date()}")
    console.print(f"  Records:    {len(records):,}")
    console.print(f"\n  Done!")


if __name__ == "__main__":
    main()
