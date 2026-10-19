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

HOURLY MODE (#47 backfill; also what the daily tail-fill uses, #64)
-------------------------------------------------------------------
    --hourly-backfill   Read every market at the first block of each hour from GMX V2
                        genesis (default) to --to-time (default: the funding epoch,
                        2025-08-19T12:00Z), resumable with --resume, and persisted and
                        checkpointed chunk by chunk.
    --from-time / --to-time   ISO-8601 bounds for --hourly-backfill
    --max-runtime-minutes / --deadline-epoch   Stop cleanly (exit 3) between chunks
"""

import argparse
import json
import os
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import requests
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_defi.provider.multi_provider import create_multi_provider_web3
from web3 import Web3

from gmx_historical_data.atomic_parquet import atomic_write_parquet
from gmx_historical_data.datastore_tail import (
    HistoricalStateUnavailable,
    RpcDeadlineExceeded,
    is_historical_state_error,
    merge_ranges,
    read_backfill_checkpoint,
    resolve_hour_blocks,
    uncovered_hours,
    write_backfill_checkpoint,
)
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
DATASTORE_BACKFILL_ANCHOR_BLOCK = 105_000_000  # 2023-06-26, before the first GMX V2 listing
GMX_V22_FUNDING_EVENT_START = 370_000_000  # ~Aug 2025


# Deployment bytecode for GMXFundingRateBatchRequest.
# Source: contracts/GMXFundingRateBatch.sol; compiled with Foundry, evm_version=paris
# (Paris = pre-Shanghai; no PUSH0 — required for Arbitrum blocks before ArbOS 20 ~block 174M)
# NOT deployed — used exclusively via eth_call with to=null:
#   data = bytes.fromhex(FUNDING_BATCH_BYTECODE[2:]) + abi_encode(["address[]"], [markets])
# Returns: abi_decode(["int256[]", "uint8[]"], result) -> (values, statuses); see
# READ_OK / READ_NOT_DEPLOYED / READ_FAILED below.  Rebuild with `forge build` in
# contracts/ and paste out/.../GMXFundingRateBatchRequest.json "bytecode.object".
FUNDING_BATCH_BYTECODE = "0x608060405234801561001057600080fd5b506040516108223803806108228339818101604052810190610032919061049d565b600073fd70de6b91282d8017aa4e741e9ae325cab992d89050600060405160200161005c90610543565b60405160208183030381529060405280519060200120905060008351905060008167ffffffffffffffff811115610096576100956102fc565b5b6040519080825280602002602001820160405280156100c45781602001602082028036833780820191505090505b50905060008267ffffffffffffffff8111156100e3576100e26102fc565b5b6040519080825280602002602001820160405280156101115781602001602082028036833780820191505090505b50905060005b838110156102a357600087828151811061013457610133610563565b5b602002602001015173ffffffffffffffffffffffffffffffffffffffff163b0361018657600182828151811061016d5761016c610563565b5b602002602001019060ff16908160ff1681525050610296565b60008588838151811061019c5761019b610563565b5b60200260200101516040516020016101b59291906105ba565b6040516020818303038152906040528051906020012090508673ffffffffffffffffffffffffffffffffffffffff1663dc97d962826040518263ffffffff1660e01b815260040161020691906105e3565b602060405180830381865afa92505050801561024057506040513d601f19601f8201168201806040525081019061023d9190610634565b60015b61027257600283838151811061025957610258610563565b5b602002602001019060ff16908160ff1681525050610294565b8085848151811061028657610285610563565b5b602002602001018181525050505b505b8080600101915050610117565b50600082826040516020016102b99291906107ea565b6040516020818303038152906040529050805160208201f35b6000604051905090565b600080fd5b600080fd5b600080fd5b6000601f19601f8301169050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052604160045260246000fd5b610334826102eb565b810181811067ffffffffffffffff82111715610353576103526102fc565b5b80604052505050565b60006103666102d2565b9050610372828261032b565b919050565b600067ffffffffffffffff821115610392576103916102fc565b5b602082029050602081019050919050565b600080fd5b600073ffffffffffffffffffffffffffffffffffffffff82169050919050565b60006103d3826103a8565b9050919050565b6103e3816103c8565b81146103ee57600080fd5b50565b600081519050610400816103da565b92915050565b600061041961041484610377565b61035c565b9050808382526020820190506020840283018581111561043c5761043b6103a3565b5b835b81811015610465578061045188826103f1565b84526020840193505060208101905061043e565b5050509392505050565b600082601f830112610484576104836102e6565b5b8151610494848260208601610406565b91505092915050565b6000602082840312156104b3576104b26102dc565b5b600082015167ffffffffffffffff8111156104d1576104d06102e1565b5b6104dd8482850161046f565b91505092915050565b600082825260208201905092915050565b7f53415645445f46554e44494e475f464143544f525f5045525f5345434f4e4400600082015250565b600061052d601f836104e6565b9150610538826104f7565b602082019050919050565b6000602082019050818103600083015261055c81610520565b9050919050565b7f4e487b7100000000000000000000000000000000000000000000000000000000600052603260045260246000fd5b6000819050919050565b6105a581610592565b82525050565b6105b4816103c8565b82525050565b60006040820190506105cf600083018561059c565b6105dc60208301846105ab565b9392505050565b60006020820190506105f8600083018461059c565b92915050565b6000819050919050565b610611816105fe565b811461061c57600080fd5b50565b60008151905061062e81610608565b92915050565b60006020828403121561064a576106496102dc565b5b60006106588482850161061f565b91505092915050565b600081519050919050565b600082825260208201905092915050565b6000819050602082019050919050565b610696816105fe565b82525050565b60006106a8838361068d565b60208301905092915050565b6000602082019050919050565b60006106cc82610661565b6106d6818561066c565b93506106e18361067d565b8060005b838110156107125781516106f9888261069c565b9750610704836106b4565b9250506001810190506106e5565b5085935050505092915050565b600081519050919050565b600082825260208201905092915050565b6000819050602082019050919050565b600060ff82169050919050565b6107618161074b565b82525050565b60006107738383610758565b60208301905092915050565b6000602082019050919050565b60006107978261071f565b6107a1818561072a565b93506107ac8361073b565b8060005b838110156107dd5781516107c48882610767565b97506107cf8361077f565b9250506001810190506107b0565b5085935050505092915050565b6000604082019050818103600083015261080481856106c1565b90508181036020830152610818818461078c565b9050939250505056fe"

# =============================================================================
# DATA CLASSES & HELPERS
# =============================================================================

# Per-market status codes returned by the batch reader (contracts/GMXFundingRateBatch.sol)
READ_OK = 0  # value is a genuine DataStore reading (zero included)
READ_NOT_DEPLOYED = 1  # market token has no code at this block: not created yet
READ_FAILED = 2  # getInt reverted: a failed read, never a zero

# Version of the durable checkpoint format.  Version 2 means "last_block is the
# last sample block whose data has been persisted".  Unversioned (legacy)
# checkpoints were advanced before any data was written and cannot be trusted.
CHECKPOINT_VERSION = 2

# Attempts per JSON-RPC batch before the extraction fails (backoff: 1s, 4s, 16s, 60s)
RPC_MAX_ATTEMPTS = 5


class FundingExtractionError(RuntimeError):
    """Raised when an RPC batch stays failed/empty/undecodable after all retries."""


class InvalidBlockTimestamp(FundingExtractionError):
    """A deterministic invalid header response that cannot improve on retry."""


class LegacyCheckpointError(RuntimeError):
    """Raised when ``--resume`` finds an unversioned (pre-durable) checkpoint."""


def _raise_if_state_unavailable(body) -> None:
    """Raise :class:`HistoricalStateUnavailable` if any response says the node lacks the state.

    A non-archive node answers ``eth_call`` at an old block with "missing trie
    node" or "historical state ... is not available". That is a property of the
    node, not a transient failure, so it is neither retried nor read as a value.
    """
    items = body if isinstance(body, list) else [body]
    for item in items:
        error = item.get("error") if isinstance(item, dict) else None
        if error:
            message = error.get("message", "") if isinstance(error, dict) else str(error)
            if is_historical_state_error(message):
                raise HistoricalStateUnavailable(message)


def _monotonic() -> float:
    """The clock the time budgets run on (a seam for tests)."""
    return time.monotonic()


#: Connect timeout used when a deadline is set (capped further to the time left).
_CONNECT_TIMEOUT_SECONDS = 10


def _http_post_json(url: str, payload: list[dict], timeout: float, deadline: float | None):
    """POST ``payload`` and return the decoded JSON, never waiting past ``deadline``.

    ``requests``' ``timeout`` limits *inactivity* on the socket, not total time: a server
    that drips one byte every 50 ms satisfies any read timeout while the call runs on for
    minutes. With a ``deadline`` the exchange (connect, headers, body) therefore runs in a
    worker thread that the caller waits on only until the deadline; the connect/read
    timeouts are also capped to the time left, so a stalled socket ends on its own, and
    on expiry the call is abandoned (its daemon thread ends at its capped socket timeouts or
    when the server finishes). Not ``signal.alarm``: this may not run on the main thread.

    :raises RpcDeadlineExceeded: If the exchange is not finished by ``deadline``.
    """
    if deadline is None:
        resp = requests.post(url, json=payload, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    remaining = deadline - _monotonic()
    if remaining <= 0:
        raise RpcDeadlineExceeded("time budget reached")
    outcome: dict = {}
    done = threading.Event()

    def work() -> None:
        try:
            resp = requests.post(
                url,
                json=payload,
                timeout=(min(_CONNECT_TIMEOUT_SECONDS, remaining), min(timeout, remaining)),
            )
            resp.raise_for_status()
            outcome["body"] = resp.json()
        except BaseException as exc:  # noqa: BLE001 - handed to the waiting caller
            outcome["error"] = exc
        finally:
            done.set()

    threading.Thread(target=work, name="rpc-post", daemon=True).start()
    if not done.wait(timeout=remaining):
        raise RpcDeadlineExceeded("time budget reached during an RPC call")
    if "error" in outcome:
        raise outcome["error"]
    return outcome["body"]


def _post_batch_with_retry(
    url: str,
    payload: list[dict],
    timeout: int,
    parse: Callable,
    label: str,
    deadline: float | None = None,
):
    """POST a JSON-RPC batch and parse it, retrying the whole batch on any failure.

    ``parse`` receives the decoded JSON and must raise on a missing, errored or
    undecodable item — a bad response is never converted into a value.

    With a ``deadline`` (a :func:`_monotonic` instant) the *total* time is bounded: no
    call, retry or backoff sleep is started that could run past it, each call is abandoned
    at the deadline however slowly the server drips (see :func:`_http_post_json`), and a
    retry whose backoff would cross the deadline is not made.

    :raises FundingExtractionError: If every attempt fails.
    :raises RpcDeadlineExceeded: If the deadline is reached before a call succeeds.
    """
    for attempt in range(RPC_MAX_ATTEMPTS):
        if deadline is not None and deadline - _monotonic() <= 0:
            raise RpcDeadlineExceeded(f"{label}: time budget reached")
        try:
            body = _http_post_json(url, payload, timeout, deadline)
            _raise_if_state_unavailable(body)
            return parse(body)
        except HistoricalStateUnavailable:
            raise  # not transient: the node does not have this state, retrying cannot help
        except InvalidBlockTimestamp:
            raise
        except RpcDeadlineExceeded as exc:
            raise RpcDeadlineExceeded(f"{label}: {exc}") from exc
        except Exception as exc:
            if attempt == RPC_MAX_ATTEMPTS - 1:
                raise FundingExtractionError(
                    f"{label} failed after {RPC_MAX_ATTEMPTS} attempts: {exc}"
                ) from exc
            delay = min(4**attempt, 60)
            if deadline is not None and _monotonic() + delay >= deadline:
                raise RpcDeadlineExceeded(
                    f"{label}: time budget reached while retrying ({exc})"
                ) from exc
            console.print(
                f"  [yellow]{label} error (attempt {attempt + 1}/{RPC_MAX_ATTEMPTS}): {exc} — retry in {delay}s[/yellow]"
            )
            time.sleep(delay)


def prefetch_block_timestamps(
    rpc_config: str,
    block_numbers: list[int],
    batch_size: int = 50,
    timeout: int = 60,
    deadline: float | None = None,
) -> dict[int, int]:
    """Pre-fetch timestamps for many blocks via JSON-RPC batch requests.

    Sends groups of ``eth_getBlockByNumber`` calls in a single HTTP request,
    reducing N sequential round-trips to ceil(N / batch_size) round-trips.
    This is the primary speedup for the DataStore phase: timestamps for all
    ~17,000 sample blocks can be fetched in ~340 HTTP requests instead of 17,000.
    Batches are distributed across all configured providers in round-robin order.

    :param rpc_config: Space-separated archive node URL(s). ``mev+`` prefixed
        URLs are stripped of that prefix before use.
    :param block_numbers: Ordered list of block numbers to fetch.
    :param batch_size: Max requests per HTTP batch (default: 200).
    :param timeout: Per-request timeout in seconds (default: 60).
    :param deadline: Optional :func:`_monotonic` instant no call may run past.
    :returns: Dict mapping block number → Unix timestamp.
    :raises FundingExtractionError: If any block's timestamp stays unavailable.
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

        def parse_timestamps(items, chunk=chunk) -> dict[int, int]:
            by_id = {item.get("id"): item for item in items}
            parsed: dict[int, int] = {}
            for j, bn in enumerate(chunk):
                item = by_id.get(j)
                if item is None or item.get("error"):
                    raise ValueError(f"block {bn}: missing or errored response")
                result = item.get("result")
                if not result or int(result["number"], 16) != bn:
                    raise ValueError(f"block {bn}: missing or mismatched block")
                ts = int(result["timestamp"], 16)
                if ts <= 0:
                    raise InvalidBlockTimestamp(
                        f"block {bn} has invalid timestamp {ts}; refusing to retry a deterministic header error"
                    )
                parsed[bn] = ts
            return parsed

        timestamps.update(
            _post_batch_with_retry(
                url, payload, timeout, parse_timestamps, f"Timestamp batch {i}", deadline
            )
        )
        fetched += len(chunk)
        if fetched % 2000 == 0 or fetched == total:
            console.print(f"  Timestamps: {fetched:,}/{total:,} ({fetched / total * 100:.0f}%)")

    return timestamps


def _decode_funding_row(
    data: bytes, market_addresses: list[str], label: str
) -> dict[str, int | None]:
    """Decode one batch-reader result into ``{market: value}``.

    The value is ``None`` for a market with no code at that block.

    :raises ValueError: On a length mismatch, an unknown status or a failed read.
    """
    values, statuses = abi_decode(["int256[]", "uint8[]"], data)
    if len(values) != len(market_addresses) or len(statuses) != len(market_addresses):
        raise ValueError(
            f"{label}: expected {len(market_addresses)} values, got {len(values)}/{len(statuses)}"
        )
    row: dict[str, int | None] = {}
    for addr, value, status in zip(market_addresses, values, statuses):
        if status == READ_OK:
            row[addr] = value
        elif status == READ_NOT_DEPLOYED:
            row[addr] = None
        else:
            raise ValueError(f"{label}: DataStore read failed for {addr} (status {status})")
    return row


def batch_fetch_funding_rates(
    rpc_config: str,
    market_addresses: list[str],
    block_numbers: list[int],
    batch_size: int = 130,
    timeout: int = 120,
    deadline: float | None = None,
) -> dict[int, dict[str, int | None]]:
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
    :param deadline: Optional :func:`_monotonic` instant no call may run past.
    :returns: Dict mapping block number → {market address → int256 value}.  A
        successful read of ``0`` is a valid value.  The value is ``None`` when
        the market has no code at that block (not created yet), so it must be
        skipped.  Failed reads (reverted ``getInt``, RPC error, empty or
        undecodable response) are retried and never converted to zero.
    :raises FundingExtractionError: If any batch stays failed after all retries.
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

        def parse_funding(items, chunk=chunk) -> dict[int, dict[str, int | None]]:
            by_id = {item.get("id"): item for item in items}
            parsed: dict[int, dict[str, int | None]] = {}
            for j, bn in enumerate(chunk):
                item = by_id.get(j)
                if item is None or item.get("error"):
                    raise ValueError(f"block {bn}: missing or errored response")
                raw = item.get("result")
                if not isinstance(raw, str) or raw in ("", "0x"):
                    raise ValueError(f"block {bn}: empty result")
                row = _decode_funding_row(bytes.fromhex(raw[2:]), market_addresses, f"block {bn}")
                parsed[bn] = row
            return parsed

        results.update(
            _post_batch_with_retry(
                url, payload, timeout, parse_funding, f"DataStore batch {i}", deadline
            )
        )

        fetched += len(chunk)
        if fetched % 2000 == 0 or fetched == total:
            console.print(
                f"  DataStore reads: {fetched:,}/{total:,} ({fetched / total * 100:.0f}%)"
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
    :returns: Dict of market address → signed int256 value.  Markets with no code
        at ``block_number`` are omitted.
    :raises FundingExtractionError: If a DataStore read fails.
    """
    calldata = bytes.fromhex(FUNDING_BATCH_BYTECODE[2:]) + abi_encode(
        ["address[]"],
        [[Web3.to_checksum_address(a) for a in market_addresses]],
    )
    result = w3.eth.call({"data": calldata}, block_identifier=block_number)
    try:
        row = _decode_funding_row(result, market_addresses, f"block {block_number}")
    except ValueError as exc:
        raise FundingExtractionError(str(exc)) from exc
    return {addr: value for addr, value in row.items() if value is not None}


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
    persist: Callable[[list[FundingDatastoreRecord]], None] | None = None,
) -> list[FundingDatastoreRecord]:
    """Extract historical funding rates via fully-batched JSON-RPC requests.

    Per chunk of sample blocks:

    1. **Timestamp batch**: ``eth_getBlockByNumber`` in groups of 50.
    2. **DataStore batch**: ``eth_call`` with ``GMXFundingRateBatchRequest``
       bytecode in groups of 130.  All market keys computed on-chain.
    3. **Record construction**, then ``persist`` and checkpoint.

    Failed, empty or undecodable responses are retried and then raise; they are
    never recorded as zero.  A successful zero reading is kept.

    :param w3: Web3 instance (used only for ``is_connected()`` check).
    :param rpc_config: Space-separated RPC URL(s); round-robin across providers.
    :param from_block: Starting block number.
    :param to_block: Ending block number.
    :param markets: Market address → info dict.
    :param interval_blocks: Block interval between samples (default: ~1 hour).
    :param market_filter: Optional symbol filter (e.g., ``'ETH/USD'``).
    :param checkpoint_dir: Optional directory for checkpoint saves.  Checkpoints
        are written only when ``persist`` is given, and only after ``persist``
        has returned for the records they cover.
    :param checkpoint_interval: Sample blocks fetched, persisted and checkpointed
        per chunk.
    :param persist: Callback that durably writes a list of records.  Called once
        per chunk (in block order); if it raises, no checkpoint is advanced.
    :returns: List of :class:`FundingDatastoreRecord` objects, including valid
        zero readings.  A market is omitted for sample blocks before it exists
        (no code at that block); the sample grid itself is unchanged.
    :raises FundingExtractionError: If an RPC batch cannot be fully read.
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
    ts_batches = (total_samples + 49) // 50
    ds_batches = (total_samples + 129) // 130
    console.print(
        f"  HTTP batches:  {ts_batches} timestamp + {ds_batches} DataStore = {ts_batches + ds_batches} total"
    )

    # Process the sample blocks in chunks: fetch -> build records -> persist ->
    # checkpoint.  A checkpoint only ever covers data ``persist`` has written.
    records: list[FundingDatastoreRecord] = []
    pending: list[FundingDatastoreRecord] = []  # built, not yet persisted
    t_start = time.monotonic()

    def flush(batch: list[FundingDatastoreRecord]) -> None:
        if not batch:
            return
        if persist is not None:
            persist(batch)
            if checkpoint_dir:
                save_checkpoint(checkpoint_dir, max(r.block_number for r in batch), len(records))

    for chunk_start in range(0, total_samples, checkpoint_interval):
        chunk_blocks = sample_blocks[chunk_start : chunk_start + checkpoint_interval]
        block_timestamps = prefetch_block_timestamps(rpc_config, chunk_blocks)
        block_funding = batch_fetch_funding_rates(rpc_config, market_addrs, chunk_blocks)

        chunk_records: list[FundingDatastoreRecord] = []
        for block_num in chunk_blocks:
            ts = block_timestamps[block_num]
            dt_str = datetime.fromtimestamp(ts, tz=UTC).isoformat()
            for addr, value in block_funding[block_num].items():
                if value is None:
                    continue  # market not created yet at this block: no observation
                chunk_records.append(
                    FundingDatastoreRecord(
                        symbol=market_symbol(addr, markets),
                        market=addr.lower(),
                        funding_factor_per_second=str(value),
                        funding_rate_per_second=value / FUNDING_FACTOR_PRECISION,
                        longs_pay_shorts=(value > 0),
                        block_number=block_num,
                        block_timestamp=ts,
                        block_datetime=dt_str,
                    )
                )
        chunk_records.sort(key=lambda r: (r.block_number, r.symbol))
        records.extend(chunk_records)
        pending.extend(chunk_records)

        console.print(
            f"  Fetched {chunk_start + len(chunk_blocks):,}/{total_samples:,} sample blocks"
        )

        # Hold back the newest hour: a sub-hour ``interval`` could split it
        # across chunks, and hourly aggregates are written per hour.
        if pending and chunk_start + checkpoint_interval < total_samples:
            last_hour = pending[-1].block_timestamp // 3600
            flush([r for r in pending if r.block_timestamp // 3600 < last_hour])
            pending = [r for r in pending if r.block_timestamp // 3600 >= last_hour]
        else:
            flush(pending)
            pending = []

    console.print(
        f"\n  Extraction complete in {time.monotonic() - t_start:.1f}s total: "
        f"{len(records):,} readings from {total_samples:,} sampled blocks"
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
            pl.len().alias("update_count"),
        )
        .sort(["symbol", "hour"])
    )

    # Compute derived columns
    hourly = hourly.with_columns(
        (pl.col("funding_rate") * 3600).alias("funding_rate_hourly"),
        (pl.col("funding_rate") * 3600 * 8760).alias("funding_rate_annualized"),
        (pl.col("funding_rate") > 0).alias("longs_pay_shorts"),
    ).with_columns(
        # funding_rate is already signed (positive = longs pay), so the signed
        # fees are derived from it once: long = +rate, short = -rate.
        pl.col("funding_rate_hourly").alias("funding_fee_long"),
        (-pl.col("funding_rate_hourly")).alias("funding_fee_short"),
    )

    # These rows are chain-state reads, never a carried value: say so in the file.
    hourly = hourly.with_columns(
        pl.lit("datastore").alias("source"),
        pl.lit(False).alias("is_gap_filled"),
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
                "source",
                "is_gap_filled",
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
            atomic_write_parquet(combined, filepath)
        else:
            atomic_write_parquet(df, filepath)

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
    """Load the last persisted sample block from the checkpoint file.

    :param checkpoint_dir: Directory containing checkpoint file.
    :returns: Last persisted block number, or ``None`` if no checkpoint.
    :raises LegacyCheckpointError: If the checkpoint is unversioned/legacy.
    """
    filepath = checkpoint_dir / "funding_datastore_checkpoint.json"
    if not filepath.exists():
        return None
    with open(filepath) as f:
        data = json.load(f)
    if data.get("version") != CHECKPOINT_VERSION:
        raise LegacyCheckpointError(
            f"{filepath} is a legacy (unversioned) checkpoint written before data was "
            f"durably persisted; resuming from block {data.get('last_block')} could skip "
            "unwritten observations.  Delete the checkpoint file (or run without --resume) "
            "to restart; existing hourly parquet rows are merged by timestamp."
        )
    return data.get("last_block")


def resume_block(checkpoint_dir: Path, interval_blocks: int, default_block: int) -> int:
    """Return the first sample block to fetch when resuming.

    The checkpoint holds the last *persisted* sample block, so the next sample
    on the same grid is ``last_block + interval_blocks``.

    :param checkpoint_dir: Directory containing the checkpoint file.
    :param interval_blocks: Sampling interval in blocks.
    :param default_block: Start block when no checkpoint exists.
    """
    last = load_checkpoint(checkpoint_dir)
    return default_block if last is None else last + interval_blocks


def save_checkpoint(checkpoint_dir: Path, last_block: int, total_records: int) -> None:
    """Save checkpoint with last processed block.

    :param checkpoint_dir: Directory for checkpoint file.
    :param last_block: Last successfully processed block number.
    :param total_records: Total records extracted so far.
    """
    filepath = checkpoint_dir / "funding_datastore_checkpoint.json"
    filepath.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "version": CHECKPOINT_VERSION,
        "last_block": last_block,
        "total_records": total_records,
        "updated_at": datetime.now(tz=UTC).isoformat(),
    }
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)


# =============================================================================
# HOURLY-ALIGNED READS (tail-fill #64, backfill #47)
# =============================================================================

#: Hours persisted and checkpointed per chunk by the hourly reader.
HOURLY_CHUNK_HOURS = 48

#: Exit code of ``--hourly-backfill`` when it stopped at its deadline with hours left.
EXIT_BACKFILL_INCOMPLETE = 3
#: Exit code when the RPC node has no historical state (not an archive node).
EXIT_STATE_UNAVAILABLE = 4

_HOUR_SECONDS = 3600

#: Seconds before a backfill's deadline after which no new RPC starts, so the chunk in
#: flight can still be persisted and checkpointed.
BACKFILL_PERSIST_RESERVE_SECONDS = 60


def fetch_latest_block(rpc_config: str, timeout: int = 60, deadline: float | None = None) -> int:
    """Return the chain head block number via ``eth_blockNumber``.

    :raises FundingExtractionError: If the node cannot answer after all retries.
    :raises RpcDeadlineExceeded: If ``deadline`` is reached first.
    """
    url = rpc_config.strip().split()[0]
    url = url[4:] if url.startswith("mev+") else url
    payload = [{"id": 0, "jsonrpc": "2.0", "method": "eth_blockNumber", "params": []}]

    def parse(items) -> int:
        item = {i.get("id"): i for i in items}.get(0)
        if item is None or item.get("error") or not isinstance(item.get("result"), str):
            raise ValueError("eth_blockNumber: missing or errored response")
        return int(item["result"], 16)

    return _post_batch_with_retry(url, payload, timeout, parse, "Latest block", deadline)


@dataclass
class HourExtraction:
    """What :func:`extract_funding_hours` did.

    :ivar requested: Hours asked for.
    :ivar read: Records persisted (a market-hour the chain had a value for).
    :ivar stopped: ``None`` when every hour was attempted, else ``"deadline"``
        or ``"unavailable"`` (the node has no state at that block).
    :ivar reason: The unavailable error, or why it stopped.
    :ivar hours_done: Hours attempted and persisted before stopping.
    :ivar last_hour: Newest hour persisted.
    :ivar last_block: Block read for ``last_hour``.
    :ivar skipped_hours: Hours with no block inside them (the clock jumped over the hour).
    """

    requested: int = 0
    read: int = 0
    stopped: str | None = None
    reason: str | None = None
    hours_done: int = 0
    last_hour: int | None = None
    last_block: int | None = None
    skipped_hours: list[int] = field(default_factory=list)

    @property
    def hours_read(self) -> int:
        """Alias used by the tail-fill status: persisted market-hour records."""
        return self.read


def extract_funding_hours(
    rpc_config: str,
    hours: list[int],
    *,
    wanted: Callable[[int], list[str]],
    symbol_of: Callable[[str], str],
    persist: Callable[[list[FundingDatastoreRecord]], None],
    chunk_hours: int = HOURLY_CHUNK_HOURS,
    should_stop: Callable[[], bool] | None = None,
    on_progress: Callable[[int, int, int], None] | None = None,
    deadline: float | None = None,
    anchor_block: int | None = None,
) -> HourExtraction:
    """Read ``savedFundingFactorPerSecond`` at the first block of each requested hour.

    Per chunk of ``chunk_hours`` hours: resolve each hour boundary to the first
    block at or after it (header reads only), read every needed market at those
    blocks with one batch ``eth_call`` per block, keep the ``(hour, market)`` pairs
    asked for, ``persist`` them, then report progress. Order is chronological, so
    whatever stops the run leaves a contiguous, persisted prefix.

    A market with no code at the block (not created yet) yields no record; a
    failed read raises (never a zero); a node without historical state stops the
    run with ``stopped == "unavailable"`` instead of raising, keeping every chunk
    already persisted. An hour with no block inside it (the sequencer clock jumped
    over it) is skipped rather than labelled with a later hour's state.

    :param rpc_config: Space-separated RPC URL(s).
    :param hours: Hour boundaries as epoch seconds.
    :param wanted: ``hour -> market addresses`` to keep for that hour.
    :param symbol_of: ``market address -> symbol`` for the record.
    :param persist: Durably writes a chunk's records; if it raises nothing after it advances.
    :param chunk_hours: Hours per chunk.
    :param should_stop: Checked before the first RPC and before each chunk; ``True`` stops
        with ``"deadline"``.
    :param on_progress: Called with ``(first_hour, last_hour, last_block)`` after each
        persisted chunk.
    :param deadline: A :func:`_monotonic` instant that no RPC call, retry or backoff sleep may
        run past (callers leave time before their own cap for persistence). Reaching it
        stops with ``"deadline"`` -- chunks already persisted are kept, the chunk in
        flight is dropped (it is read again next run) -- and an already-expired budget
        makes no RPC at all.
    :raises FundingExtractionError: On any read that stays failed after retries.
    """
    ordered = sorted(set(hours))
    result = HourExtraction(requested=len(ordered))
    if not ordered:
        return result

    def out_of_time() -> bool:
        return (should_stop is not None and should_stop()) or (
            deadline is not None and _monotonic() >= deadline
        )

    def stop_on_deadline(reason: str = "time budget reached") -> HourExtraction:
        result.stopped, result.reason = "deadline", reason
        return result

    if out_of_time():
        return stop_on_deadline()

    cache: dict[int, int] = {}

    def fetch_ts(blocks: list[int]) -> dict[int, int]:
        return prefetch_block_timestamps(rpc_config, blocks, deadline=deadline)

    try:
        latest = fetch_latest_block(rpc_config, deadline=deadline)
    except RpcDeadlineExceeded as exc:
        return stop_on_deadline(str(exc))
    except HistoricalStateUnavailable as exc:
        result.stopped, result.reason = "unavailable", str(exc)
        return result
    for start in range(0, len(ordered), chunk_hours):
        if out_of_time():
            return stop_on_deadline()
        chunk = ordered[start : start + chunk_hours]
        try:
            blocks = resolve_hour_blocks(
                chunk,
                latest_block=latest,
                fetch_timestamps=fetch_ts,
                cache=cache,
                anchor_block=anchor_block,
            )
            addresses = sorted({a.lower() for h in chunk for a in wanted(h)})
            funding = batch_fetch_funding_rates(
                rpc_config, addresses, sorted(set(blocks.values())), deadline=deadline
            )
        except RpcDeadlineExceeded as exc:
            return stop_on_deadline(str(exc))
        except HistoricalStateUnavailable as exc:
            result.stopped, result.reason = "unavailable", str(exc)
            return result

        records: list[FundingDatastoreRecord] = []
        for hour in chunk:
            block = blocks[hour]
            ts = cache[block]
            if ts // _HOUR_SECONDS * _HOUR_SECONDS != hour:
                result.skipped_hours.append(hour)
                continue
            dt_str = datetime.fromtimestamp(ts, tz=UTC).isoformat()
            row = {a.lower(): v for a, v in funding[block].items()}
            for addr in wanted(hour):
                value = row.get(addr.lower())
                if value is None:
                    continue  # not created yet at this block: no observation
                records.append(
                    FundingDatastoreRecord(
                        symbol=symbol_of(addr),
                        market=addr.lower(),
                        funding_factor_per_second=str(value),
                        funding_rate_per_second=value / FUNDING_FACTOR_PRECISION,
                        longs_pay_shorts=(value > 0),
                        block_number=block,
                        block_timestamp=ts,
                        block_datetime=dt_str,
                    )
                )
        if records:
            persist(records)
        result.read += len(records)
        result.hours_done += len(chunk)
        result.last_hour, result.last_block = chunk[-1], blocks[chunk[-1]]
        if on_progress is not None:
            on_progress(chunk[0], result.last_hour, result.last_block)
        console.print(
            f"  Hours {start + len(chunk):,}/{len(ordered):,} read "
            f"({result.read:,} market-hour records)"
        )
    return result


def genesis_hour(
    rpc_config: str,
    genesis_block: int = DATASTORE_BACKFILL_ANCHOR_BLOCK,
    deadline: float | None = None,
) -> int:
    """First whole hour at or before the pre-listing DataStore anchor block."""
    ts = prefetch_block_timestamps(rpc_config, [genesis_block], deadline=deadline)[genesis_block]
    return ts // _HOUR_SECONDS * _HOUR_SECONDS


def run_hourly_backfill(
    rpc_config: str,
    markets: dict[str, dict],
    output_dir: Path,
    *,
    from_hour: int,
    to_hour: int,
    resume: bool = True,
    chunk_hours: int = 168,
    should_stop: Callable[[], bool] | None = None,
    deadline: float | None = None,
) -> HourExtraction:
    """Backfill ``rates/<SYM>/1h_datastore.parquet`` hourly from ``from_hour`` to ``to_hour``.

    Every market with an index token is read at every hour; markets that do not
    exist yet at a block are skipped by the batch reader (``NOT_DEPLOYED``), so
    genesis can simply be the earliest market creation. Unlisted (deprecated)
    markets are included: their history before delisting is real. Each chunk is
    persisted, then the backfill checkpoint (versioned, atomic) records the hours it
    covers, so a run killed by a step timeout loses at most the chunk in flight.

    ``resume`` reads only the requested hours the checkpoint does not already cover --
    after the end of a previous run *and* any earlier prefix or gap -- so a backfill that
    was bounded (``--from-time 2025-08-01``) and later widened to genesis reads the
    earlier history instead of treating the later position as full coverage.

    :param output_dir: Base output directory (``.../funding``); data goes under ``arbitrum/``.
    :param from_hour: First hour (epoch seconds, hour-aligned).
    :param to_hour: Last hour, inclusive.
    :param resume: Skip hours the checkpoint covers.
    :param should_stop: Checked between chunks; ``True`` stops with ``"deadline"``.
    :param deadline: :func:`_monotonic` instant no RPC may run past (see
        :func:`extract_funding_hours`).
    """
    checkpoint_dir = Path(output_dir) / "arbitrum" / "checkpoints"
    covered: list[list[int]] = []
    highest_block = 0
    if resume:
        saved = read_backfill_checkpoint(checkpoint_dir)
        if saved is not None:
            covered = saved["covered"]
            highest_block = saved["last_block"]
    hours = uncovered_hours(covered, from_hour, to_hour)
    if not hours:
        console.print("  Backfill already complete; nothing to read.")
        return HourExtraction()
    if covered:
        console.print(
            f"  Resuming backfill: {len(hours):,} hour(s) of the requested range are not yet covered"
        )
    addresses = [a for a, info in markets.items() if info.get("indexToken") is not None]
    console.print(f"  Backfill: {len(hours):,} hours x {len(addresses)} markets")

    def persist(batch: list[FundingDatastoreRecord]) -> None:
        save_parquet(aggregate_hourly_rates(batch), Path(output_dir))

    def on_progress(first_hour: int, last_hour: int, last_block: int) -> None:
        # A chunk holds consecutive *uncovered* hours, so everything between its first
        # and last hour is covered once it is persisted.
        nonlocal covered, highest_block
        covered = merge_ranges([*covered, [first_hour, last_hour]])
        highest_block = max(highest_block, last_block)  # never moves back (catch-up compares)
        write_backfill_checkpoint(
            checkpoint_dir,
            first_hour=from_hour,
            last_hour=last_hour,
            last_block=highest_block,
            target_hour=to_hour,
            covered=covered,
        )

    return extract_funding_hours(
        rpc_config,
        hours,
        wanted=lambda _hour: addresses,
        symbol_of=lambda addr: market_symbol(addr, markets),
        persist=persist,
        chunk_hours=chunk_hours,
        should_stop=should_stop,
        on_progress=on_progress,
        deadline=deadline,
        anchor_block=DATASTORE_BACKFILL_ANCHOR_BLOCK,
    )


# =============================================================================
# MAIN
# =============================================================================


def _parse_time(value: str) -> int:
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    stamp = stamp.replace(tzinfo=UTC) if stamp.tzinfo is None else stamp.astimezone(UTC)
    return int(stamp.timestamp()) // _HOUR_SECONDS * _HOUR_SECONDS


def _main_hourly_backfill(args, markets: dict[str, dict], output_dir: Path) -> int:
    """CLI body of ``--hourly-backfill``; returns the process exit code."""
    from gmx_historical_data.series_coverage import FUNDING_EPOCH

    console.print("\n" + "=" * 70)
    console.print("GMX V2 FUNDING BACKFILL (hourly DataStore reads)")
    console.print("=" * 70)
    try:
        rpc_config = get_rpc_config()
    except ValueError as exc:
        console.print(f"[red]ERROR: {exc}[/red]")
        return 1

    deadline = args.deadline_epoch
    if args.max_runtime_minutes is not None:
        by_runtime = int(time.time() + args.max_runtime_minutes * 60)
        deadline = by_runtime if deadline is None else min(deadline, by_runtime)

    def should_stop() -> bool:
        return deadline is not None and time.time() >= deadline

    # No RPC may run past the wall-clock deadline (less a reserve for persisting the chunk).
    rpc_deadline = (
        None
        if deadline is None
        else _monotonic() + (deadline - time.time()) - BACKFILL_PERSIST_RESERVE_SECONDS
    )
    if should_stop() or (rpc_deadline is not None and _monotonic() >= rpc_deadline):
        console.print(
            "[yellow]Time budget already spent; no RPC made. Run again with --resume.[/yellow]"
        )
        return EXIT_BACKFILL_INCOMPLETE

    try:
        minimum_hour = genesis_hour(rpc_config, deadline=rpc_deadline)
        from_hour = (
            max(_parse_time(args.from_time), minimum_hour) if args.from_time else minimum_hour
        )
        to_hour = (
            _parse_time(args.to_time)
            if args.to_time
            else int(FUNDING_EPOCH.timestamp()) // _HOUR_SECONDS * _HOUR_SECONDS
        )
        result = run_hourly_backfill(
            rpc_config,
            markets,
            output_dir,
            from_hour=from_hour,
            to_hour=to_hour,
            resume=args.resume,
            should_stop=should_stop,
            deadline=rpc_deadline,
        )
    except RpcDeadlineExceeded as exc:
        console.print(
            f"[yellow]{exc}; progress is checkpointed -- run again with --resume.[/yellow]"
        )
        return EXIT_BACKFILL_INCOMPLETE
    except FundingExtractionError as exc:
        console.print(f"[red]ERROR: {exc}[/red]")
        return 1

    console.print(f"\n  Records written: {result.read:,}")
    if result.stopped == "unavailable":
        console.print(
            f"[red]ERROR: the RPC node has no historical state ({result.reason}); "
            "the backfill needs an archive node.[/red]"
        )
        return EXIT_STATE_UNAVAILABLE
    if result.stopped == "deadline":
        console.print(
            "[yellow]Stopped at the time budget with hours remaining; "
            "progress is checkpointed -- run again with --resume.[/yellow]"
        )
        return EXIT_BACKFILL_INCOMPLETE
    console.print("\n  Backfill complete.")
    return 0


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
    parser.add_argument(
        "--hourly-backfill",
        action="store_true",
        help=(
            "Read every market at the first block of each hour from GMX V2 genesis to "
            "--to-time (default: the funding epoch). Resumable with --resume."
        ),
    )
    parser.add_argument("--from-time", type=str, default=None, help="ISO-8601 start (backfill)")
    parser.add_argument(
        "--to-time", type=str, default=None, help="ISO-8601 end, inclusive (backfill)"
    )
    parser.add_argument(
        "--max-runtime-minutes",
        type=float,
        default=None,
        help="Stop cleanly between chunks after this long (exit code 3 if hours remain)",
    )
    parser.add_argument(
        "--deadline-epoch",
        type=int,
        default=None,
        help="Stop cleanly between chunks at this Unix time (exit code 3 if hours remain)",
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

    if args.hourly_backfill:
        sys.exit(_main_hourly_backfill(args, markets, output_dir))

    # Resume from checkpoint
    from_block = args.from_block
    if args.resume:
        try:
            from_block = resume_block(checkpoint_dir, args.interval, args.from_block)
        except LegacyCheckpointError as exc:
            console.print(f"[red]ERROR: {exc}[/red]")
            sys.exit(1)
        if from_block != args.from_block:
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

    def persist(batch: list[FundingDatastoreRecord]) -> None:
        save_parquet(aggregate_hourly_rates(batch), output_dir)

    # Extract.  Parquet output is persisted (and checkpointed) chunk by chunk;
    # JSON is a single file written after extraction.
    records = extract_funding_rates(
        w3=w3,
        rpc_config=rpc_config,
        from_block=from_block,
        to_block=args.to_block,
        markets=markets,
        interval_blocks=args.interval,
        market_filter=args.market,
        checkpoint_dir=checkpoint_dir,
        persist=persist if args.output == "parquet" else None,
    )

    if not records:
        console.print("\n[yellow]No funding readings found in range.[/yellow]")
        sys.exit(0)

    if args.output == "json":
        console.print(f"\nSaving {len(records):,} records...")
        save_json(records, output_dir)
        # Checkpoint only after the JSON file is written
        save_checkpoint(checkpoint_dir, max(r.block_number for r in records), len(records))

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
