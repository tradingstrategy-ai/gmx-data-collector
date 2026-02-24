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
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_defi.provider.multi_provider import create_multi_provider_web3
from web3 import Web3

from gmx_historical_data.market_registry import fetch_markets, market_symbol

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    from rich.console import Console

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


# Deployment bytecode for GMXFundingRateBatchRequest.
# Source: contracts/GMXFundingRateBatch.sol; compiled with Foundry, evm_version=paris
# (Paris = pre-Shanghai; no PUSH0 — required for Arbitrum blocks before ArbOS 20 ~block 174M)
# NOT deployed — used exclusively via eth_call with to=null:
#   data = bytes.fromhex(FUNDING_BATCH_BYTECODE[2:]) + abi_encode(["address[]"], [markets])
# Returns: abi_decode(["int256[]"], result)[0]
FUNDING_BATCH_BYTECODE = "0x608060405234801561001057600080fd5b50604051610683380380610683833981810160405281019061003291906103de565b600073fd70de6b91282d8017aa4e741e9ae325cab992d89050600060405160200161005c90610484565b60405160208183030381529060405280519060200120905060008351905060008167ffffffffffffffff8111156100965761009561023d565b5b6040519080825280602002602001820160405280156100c45781602001602082028036833780820191505090505b50905060005b828110156101e6576000848783815181106100e8576100e76104a4565b5b60200260200101516040516020016101019291906104fb565b6040516020818303038152906040528051906020012090508573ffffffffffffffffffffffffffffffffffffffff1663dc97d962826040518263ffffffff1660e01b81526004016101529190610524565b602060405180830381865afa92505050801561018c57506040513d601f19601f820116820180604052508101906101899190610575565b60015b6101b65760008383815181106101a5576101a46104a4565b5b6020026020010181815250506101d8565b808484815181106101ca576101c96104a4565b5b602002602001018181525050505b5080806001019150506100ca565b506000816040516020016101fa9190610660565b6040516020818303038152906040529050805160208201f35b6000604051905090565b600080fd5b600080fd5b600080fd5b6000601f19601f8301169050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052604160045260246000fd5b6102758261022c565b810181811067ffffffffffffffff821117156102945761029361023d565b5b80604052505050565b60006102a7610213565b90506102b3828261026c565b919050565b600067ffffffffffffffff8211156102d3576102d261023d565b5b602082029050602081019050919050565b600080fd5b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b6000610314826102e9565b9050919050565b61032481610309565b811461032f57600080fd5b50565b6000815190506103418161031b565b92915050565b600061035a610355846102b8565b61029d565b9050808382526020820190506020840283018581111561037d5761037c6102e4565b5b835b818110156103a657806103928882610332565b84526020840193505060208101905061037f565b5050509392505050565b600082601f8301126103c5576103c4610227565b5b81516103d5848260208601610347565b91505092915050565b6000602082840312156103f4576103f361021d565b5b600082015167ffffffffffffffff81111561041257610411610222565b5b61041e848285016103b0565b91505092915050565b600082825260208201905092915050565b7f53415645445f46554e44494e475f464143544f525f5045525f5345434f4e4400600082015250565b600061046e601f83610427565b915061047982610438565b602082019050919050565b6000602082019050818103600083015261049d81610461565b9050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052603260045260246000fd5b6000819050919050565b6104e6816104d3565b82525050565b6104f581610309565b82525050565b600060408201905061051060008301856104dd565b61051d60208301846104ec565b9392505050565b600060208201905061053960008301846104dd565b92915050565b6000819050919050565b6105528161053f565b811461055d57600080fd5b50565b60008151905061056f81610549565b92915050565b60006020828403121561058b5761058a61021d565b5b600061059984828501610560565b91505092915050565b600081519050919050565b600082825260208201905092915050565b6000819050602082019050919050565b6105d78161053f565b82525050565b60006105e983836105ce565b60208301905092915050565b6000602082019050919050565b600061060d826105a2565b61061781856105ad565b9350610622836105be565b8060005b8381101561065357815161063a88826105dd565b9750610645836105f5565b925050600181019050610626565b5085935050505092915050565b6000602082019050818103600083015261067a8184610602565b90509291505056fe"

# =============================================================================
# DATA CLASSES & HELPERS
# =============================================================================


def prefetch_block_timestamps(
    rpc_config: str,
    block_numbers: list[int],
    batch_size: int = 200,
    timeout: int = 60,
) -> dict[int, int]:
    """Pre-fetch timestamps for many blocks via JSON-RPC batch requests.

    Sends groups of ``eth_getBlockByNumber`` calls in a single HTTP request,
    reducing N sequential round-trips to ceil(N / batch_size) round-trips.
    This is the primary speedup for the DataStore phase: timestamps for all
    ~17,000 sample blocks can be fetched in ~85 HTTP requests instead of 17,000.
    Batches are distributed across all configured providers in round-robin order.

    :param rpc_config: Space-separated archive node URL(s). ``mev+`` prefixed
        URLs are stripped of that prefix before use.
    :param block_numbers: Ordered list of block numbers to fetch.
    :param batch_size: Max requests per HTTP batch (default: 200).
    :param timeout: Per-request timeout in seconds (default: 60).
    :returns: Dict mapping block number → Unix timestamp.
    """
    # Parse: strip mev+ prefix (those are transaction endpoints, not suitable for eth_call)
    raw_urls = rpc_config.strip().split()
    call_urls = [u[4:] if u.startswith("mev+") else u for u in raw_urls]

    timestamps: dict[int, int] = {}
    total = len(block_numbers)
    fetched = 0

    for i, chunk_start in enumerate(range(0, total, batch_size)):
        chunk = block_numbers[chunk_start : chunk_start + batch_size]
        payload = [
            {
                "id": j,
                "jsonrpc": "2.0",
                "method": "eth_getBlockByNumber",
                "params": [hex(bn), False],
            }
            for j, bn in enumerate(chunk)
        ]
        url = call_urls[i % len(call_urls)]
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        for item in resp.json():
            result = item.get("result")
            if result:
                bn = int(result["number"], 16)
                ts = int(result["timestamp"], 16)
                timestamps[bn] = ts
        fetched += len(chunk)
        if fetched % 2000 == 0 or fetched == total:
            console.print(f"  Timestamps: {fetched:,}/{total:,} ({fetched / total * 100:.0f}%)")

    return timestamps


def batch_fetch_funding_rates(
    rpc_config: str,
    market_addresses: list[str],
    block_numbers: list[int],
    batch_size: int = 130,
    timeout: int = 120,
) -> dict[int, dict[str, int]]:
    """Batch-fetch ``savedFundingFactorPerSecond`` for all markets across many blocks.

    Sends groups of ``eth_call`` requests (using ``GMXFundingRateBatchRequest``
    bytecode) in a single HTTP request, reducing N sequential RPC round-trips
    to ``ceil(N / batch_size)`` round-trips.  Requests are distributed
    round-robin across all configured providers.

    :param rpc_config: Space-separated RPC URL(s). ``mev+`` prefixed URLs
        are stripped of that prefix.
    :param market_addresses: Market contract addresses to query.
    :param block_numbers: Ordered list of block numbers to query.
    :param batch_size: ``eth_call`` requests per HTTP batch (default: 130).
    :param timeout: Per-request timeout in seconds (default: 120).
    :returns: Dict mapping block number → {market address → int256 value}.
    """
    raw_urls = rpc_config.strip().split()
    call_urls = [u[4:] if u.startswith("mev+") else u for u in raw_urls]

    # Pre-encode calldata once — same bytecode + market list for every block
    encoded_args = abi_encode(
        ["address[]"],
        [[Web3.to_checksum_address(a) for a in market_addresses]],
    )
    calldata_hex = "0x" + FUNDING_BATCH_BYTECODE[2:] + encoded_args.hex()

    results: dict[int, dict[str, int]] = {}
    total = len(block_numbers)
    fetched = 0
    errors = 0

    for i, chunk_start in enumerate(range(0, total, batch_size)):
        chunk = block_numbers[chunk_start : chunk_start + batch_size]
        payload = [
            {
                "id": j,
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"data": calldata_hex}, hex(bn)],
            }
            for j, bn in enumerate(chunk)
        ]
        url = call_urls[i % len(call_urls)]
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()

        for item in resp.json():
            bn = chunk[item["id"]]
            raw = item.get("result", "")
            if raw and raw != "0x":
                try:
                    values = abi_decode(["int256[]"], bytes.fromhex(raw[2:]))[0]
                    results[bn] = dict(zip(market_addresses, values))
                except Exception:
                    results[bn] = {addr: 0 for addr in market_addresses}
                    errors += 1
            else:
                # Block before GMX V2 or RPC error — all zeros
                results[bn] = {addr: 0 for addr in market_addresses}

        fetched += len(chunk)
        if fetched % 2000 == 0 or fetched == total:
            console.print(
                f"  DataStore reads: {fetched:,}/{total:,} "
                f"({fetched / total * 100:.0f}%)" + (f" [{errors} decode errors]" if errors else "")
            )

    return results


def read_all_markets_bytecode(
    w3: Web3,
    market_addresses: list[str],
    block_number: int,
) -> dict[str, int]:
    """Read savedFundingFactorPerSecond for all markets in one eth_call.

    Uses the ``GMXFundingRateBatchRequest`` constructor bytecode (see
    ``contracts/GMXFundingRateBatch.sol``).  The constructor is executed by
    the EVM inside an ``eth_call`` with ``to=None`` (contract-creation call,
    never lands on-chain).  It computes the DataStore keys on-chain, reads
    ``getInt`` for each market, and returns all values as ``int256[]``.

    :param w3: Web3 instance (any provider).
    :param market_addresses: Market contract addresses to query.
    :param block_number: Block to query at.
    :returns: Dict of market address → signed int256 value (0 on failure).
    """
    calldata = bytes.fromhex(FUNDING_BATCH_BYTECODE[2:]) + abi_encode(
        ["address[]"],
        [[Web3.to_checksum_address(a) for a in market_addresses]],
    )
    result = w3.eth.call({"data": calldata}, block_identifier=block_number)
    values: tuple[int, ...] = abi_decode(["int256[]"], result)[0]
    return dict(zip(market_addresses, values))


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


def get_rpc_config() -> str:
    """Return the full RPC configuration string from the environment.

    Supports one or more space-separated archive node URLs — e.g.:
    ``JSON_RPC_ARBITRUM="https://rpc1.example.com https://rpc2.example.com"``

    :returns: Raw configuration string for :func:`create_multi_provider_web3`.
    :raises ValueError: If no RPC URL is configured.
    """
    rpc_raw = os.environ.get("JSON_RPC_ARBITRUM") or os.environ.get("ARBITRUM_CHAIN_JSON_RPC", "")
    if not rpc_raw.strip():
        raise ValueError("Set JSON_RPC_ARBITRUM to one or more archive node URLs (space-separated)")
    return rpc_raw.strip()


# =============================================================================
# EXTRACTION
# =============================================================================


def extract_funding_rates(
    w3: Web3,
    rpc_config: str,
    from_block: int,
    to_block: int,
    markets: dict[str, dict],
    interval_blocks: int = BLOCKS_PER_HOUR,
    market_filter: str | None = None,
    checkpoint_dir: Path | None = None,
    checkpoint_interval: int = 500,
) -> list[FundingDatastoreRecord]:
    """Extract historical funding rates via fully-batched JSON-RPC requests.

    Three-phase approach — all I/O is batched upfront, record construction
    is pure CPU:

    1. **Timestamp batch**: ``eth_getBlockByNumber`` for all sample blocks
       in groups of 200, reducing N round-trips to ``ceil(N/200)``.
    2. **DataStore batch**: ``eth_call`` with ``GMXFundingRateBatchRequest``
       bytecode for all sample blocks in groups of 130, reducing N round-trips
       to ``ceil(N/130)``.  All market keys computed on-chain; no pre-computation
       needed.
    3. **Record construction**: pure CPU loop over pre-fetched data — no RPC.

    Total HTTP requests: ``ceil(N/200) + ceil(N/130)`` ≈ 221 for a full run
    (vs. ~17,000 sequential calls in the old approach).

    :param w3: Web3 instance (used only for ``is_connected()`` check).
    :param rpc_config: Space-separated RPC URL(s); round-robin across providers.
    :param from_block: Starting block number.
    :param to_block: Ending block number.
    :param markets: Market address → info dict.
    :param interval_blocks: Block interval between samples (default: ~1 hour).
    :param market_filter: Optional symbol filter (e.g., ``'ETH/USD'``).
    :param checkpoint_dir: Optional directory for periodic checkpoint saves.
    :param checkpoint_interval: Save checkpoint every N sample blocks.
    :returns: List of :class:`FundingDatastoreRecord` objects.
    """
    # Filter markets: exclude swap-only (no index token) and deprecated (isListed=False)
    target_markets = {}
    for addr, info in markets.items():
        if info.get("indexToken") is None:
            continue
        if not info.get("isListed", True):
            continue
        if market_filter and info.get("symbol") != market_filter:
            continue
        target_markets[addr] = info

    market_addrs = list(target_markets.keys())

    # Generate sample blocks
    sample_blocks = list(range(from_block, to_block + 1, interval_blocks))
    total_samples = len(sample_blocks)

    console.print(f"\n  Markets:       {len(target_markets)}")
    console.print(f"  Block range:   {from_block:,} → {to_block:,}")
    console.print(
        f"  Interval:      {interval_blocks} blocks (~{interval_blocks / BLOCKS_PER_HOUR:.1f}h)"
    )
    console.print(f"  Sample points: {total_samples:,}")
    ts_batches = (total_samples + 199) // 200
    ds_batches = (total_samples + 129) // 130
    console.print(
        f"  HTTP batches:  {ts_batches} timestamp + {ds_batches} DataStore = {ts_batches + ds_batches} total"
    )

    # --- Phase 1: batch-fetch all block timestamps upfront ---
    console.print(f"\n  Pre-fetching {total_samples:,} block timestamps in batches of 200...")
    t_ts = time.monotonic()
    block_timestamps = prefetch_block_timestamps(rpc_config, sample_blocks)
    console.print(f"  Timestamps ready in {time.monotonic() - t_ts:.1f}s")

    # --- Phase 2: batch-fetch all DataStore values upfront ---
    console.print(
        f"\n  Batch-fetching DataStore values for {total_samples:,} blocks in batches of 130..."
    )
    t_ds = time.monotonic()
    block_funding = batch_fetch_funding_rates(rpc_config, market_addrs, sample_blocks)
    console.print(f"  DataStore reads ready in {time.monotonic() - t_ds:.1f}s")

    # --- Phase 3: build records (pure CPU — no more RPC calls) ---
    records: list[FundingDatastoreRecord] = []

    for i, block_num in enumerate(sample_blocks):
        ts = block_timestamps.get(block_num, 0)
        dt_str = datetime.fromtimestamp(ts, tz=UTC).isoformat()
        market_values = block_funding.get(block_num, {})
        for addr, value in market_values.items():
            if value == 0:
                continue
            rate = value / FUNDING_FACTOR_PRECISION
            sym = market_symbol(addr, markets)
            records.append(
                FundingDatastoreRecord(
                    symbol=sym,
                    market=addr.lower(),
                    funding_factor_per_second=str(value),
                    funding_rate_per_second=rate,
                    longs_pay_shorts=(value > 0),
                    block_number=block_num,
                    block_timestamp=ts,
                    block_datetime=dt_str,
                )
            )
        if checkpoint_dir and (i + 1) % checkpoint_interval == 0:
            save_checkpoint(checkpoint_dir, block_num, len(records))

    records.sort(key=lambda r: (r.block_number, r.symbol))

    console.print(
        f"\n  Extraction complete in {time.monotonic() - t_ts:.1f}s total: "
        f"{len(records):,} non-zero readings from {total_samples:,} sampled blocks"
    )

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
        .cast(pl.Datetime("ns", "UTC")),
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
        sym_df = sym_df.select(
            [
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
            ]
        ).cast({"update_count": pl.UInt32})
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


def load_checkpoint(checkpoint_dir: Path) -> int | None:
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
        "updated_at": datetime.now(tz=UTC).isoformat(),
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
        "--resume",
        action="store_true",
        help="Resume from last checkpoint",
    )
    parser.add_argument(
        "--list-markets",
        action="store_true",
        help="List available markets and exit",
    )
    parser.add_argument(
        "--refresh-markets",
        action="store_true",
        help="Force re-fetch of GMX market registry (ignores 24h disk cache)",
    )

    args = parser.parse_args()

    markets = fetch_markets("arbitrum", force_refresh=args.refresh_markets)

    if args.list_markets:
        console.print(f"\n{'Symbol':<30} {'Market Address':<44}")
        console.print("-" * 74)
        for addr, info in sorted(markets.items(), key=lambda x: x[1]["symbol"]):
            if info.get("indexToken"):
                console.print(f"{info['symbol']:<30} {addr}")
        console.print(
            f"\nTotal: {sum(1 for v in markets.values() if v.get('indexToken'))} perpetual markets"
        )
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

    # Connect to archive node(s)
    rpc_config = get_rpc_config()
    provider_urls = [u for u in rpc_config.split() if not u.startswith("mev+")]
    console.print(f"  RPC providers: {len(provider_urls)}")
    for url in provider_urls:
        console.print(f"    {url[:60]}...")
    w3 = create_multi_provider_web3(
        rpc_config,
        default_http_timeout=(3.0, 60.0),
        retries=6,
    )
    if not w3.is_connected():
        console.print("[red]ERROR: Cannot connect to any RPC provider[/red]")
        sys.exit(1)
    console.print(f"  Connected. Latest block: {w3.eth.block_number:,}")

    # Extract
    records = extract_funding_rates(
        w3=w3,
        rpc_config=rpc_config,
        from_block=from_block,
        to_block=args.to_block,
        markets=markets,
        interval_blocks=args.interval,
        market_filter=args.market,
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
    first = datetime.fromtimestamp(min(timestamps), tz=UTC)
    last = datetime.fromtimestamp(max(timestamps), tz=UTC)

    console.print(f"\n  Symbols:    {len(symbols)}")
    console.print(f"  Time range: {first.date()} → {last.date()}")
    console.print(f"  Records:    {len(records):,}")
    console.print("\n  Done!")


if __name__ == "__main__":
    main()
