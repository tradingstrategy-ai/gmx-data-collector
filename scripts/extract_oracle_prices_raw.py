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
#     "pyarrow>=14.0",
# ]
# ///
"""
GMX V2 Raw Oracle Price Extractor (HyperSync Edition)
=====================================================
Extracts ``OraclePriceUpdate`` events from GMX V2 EventEmitter from genesis
(Aug 2023) to present. Each event carries per-transaction oracle prices for
a collateral token, enabling USD conversion of funding fee deltas.

**Purpose:** This script provides raw oracle price data with ``transaction_hash``
as a join key for the USD funding flow builder (``build_usd_funding_flows.py``).
Unlike the existing ``oracle_price_collector.py`` which aggregates to OHLCV
candles, this preserves per-event granularity needed for per-transaction joins.

Outputs:
- Raw events: ``data/funding/{network}/raw/oracle_prices/data.parquet``

QUICK START
-----------
    poetry run python scripts/extract_oracle_prices_raw.py --from-block 120000000

USAGE
-----
    poetry run python scripts/extract_oracle_prices_raw.py [OPTIONS]

EXAMPLES
--------
    # Full historical extraction from genesis
    poetry run python scripts/extract_oracle_prices_raw.py --from-block 120000000

    # Quick test
    poetry run python scripts/extract_oracle_prices_raw.py \\
        --from-block 170000000 --to-block 170100000

    # Incremental mode (resume from checkpoint)
    poetry run python scripts/extract_oracle_prices_raw.py --resume
================================================================================
"""

import argparse
import asyncio
import json
import os
import sys
import time
import traceback
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import hypersync
from eth_abi import decode as abi_decode
from eth_utils import keccak
from hypersync import (
    BlockField,
    FieldSelection,
    HypersyncClient,
    LogField,
    LogSelection,
    Query,
)
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from gmx_historical_data.hypersync_client_factory import RotatingHypersyncClient

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

console = Console()

# Retry settings
MAX_RETRIES = 15
RETRY_BASE_DELAY = 2.0
RETRY_MAX_DELAY = 120.0

# Progress milestones
PROGRESS_MILESTONES = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100]

# GMX V2 genesis block on Arbitrum
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

# EventLog1 signature (topic0)
EVENT_LOG1_TOPIC = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160"

# OraclePriceUpdate event hash (topic1)
ORACLE_PRICE_UPDATE_HASH = "0x" + keccak(text="OraclePriceUpdate").hex()

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


# =============================================================================
# DATA CLASSES & HELPERS
# =============================================================================


@dataclass
class OraclePriceRecord:
    """Raw OraclePriceUpdate event for USD flow joins.

    :ivar token: Token contract address (collateral token)
    :ivar min_price: Minimum price in 30-decimal precision (string)
    :ivar max_price: Maximum price in 30-decimal precision (string)
    :ivar block_number: Block number
    :ivar block_timestamp: Unix timestamp (seconds)
    :ivar transaction_hash: Transaction hash (join key for USD flows)
    :ivar log_index: Log index within the block
    """

    token: str
    min_price: str
    max_price: str
    block_number: int
    block_timestamp: int
    transaction_hash: str
    log_index: int


# =============================================================================
# ABI DECODING
# =============================================================================


def decode_oracle_price_event(hex_data: str) -> dict | None:
    """Decode OraclePriceUpdate from EventLog1 data.

    :param hex_data: Hex-encoded data field (with ``0x`` prefix).
    :returns: Dict with ``token``, ``min_price``, ``max_price``,
        or ``None`` on decode error.
    """
    if not hex_data or hex_data == "0x":
        return None

    data = hex_data[2:] if hex_data.startswith("0x") else hex_data

    try:
        data_bytes = bytes.fromhex(data)
        _, event_name, event_data = abi_decode(EVENTLOG1_ABI_TYPES, data_bytes)
    except Exception:
        return None

    if event_name != "OraclePriceUpdate":
        return None

    addresses = {}
    for key, val in event_data[IDX_ADDRESS][0]:
        addr = (
            val
            if isinstance(val, str)
            else ("0x" + val.hex() if isinstance(val, bytes) else str(val))
        )
        addresses[key] = addr.lower() if isinstance(addr, str) else addr

    uints = {key: val for key, val in event_data[IDX_UINT][0]}

    return {
        "token": addresses.get("token", ""),
        "min_price": uints.get("minPrice", 0),
        "max_price": uints.get("maxPrice", 0),
    }


# =============================================================================
# CHECKPOINT
# =============================================================================


def load_checkpoint(path: Path) -> dict | None:
    """Load checkpoint from JSON file.

    :param path: Path to checkpoint JSON file.
    :returns: Checkpoint dict or ``None`` if not found.
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
) -> None:
    """Save checkpoint to JSON file.

    :param path: Path to checkpoint JSON file.
    :param last_block: Last processed block number.
    :param last_timestamp: Last processed block timestamp.
    :param total_events: Total events processed so far.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint = {
        "symbol": "oracle_prices_all",
        "last_block": last_block,
        "last_timestamp": last_timestamp,
        "total_events": total_events,
        "last_updated": datetime.now(tz=UTC).isoformat(),
    }
    with open(path, "w") as f:
        json.dump(checkpoint, f, indent=2)
    console.print(f"  Checkpoint saved: block [cyan]{last_block:,}[/cyan] -> [green]{path}[/green]")


# =============================================================================
# HYPERSYNC CLIENT
# =============================================================================


async def create_client(network: str) -> RotatingHypersyncClient:
    """Create a HyperSync client, rotating across a key pool when configured.

    Reads the ``HYPERSYNC_API_TOKEN`` environment variable for authentication.
    A comma- or space-separated value builds a rotating pool (via
    :class:`RotatingHypersyncClient`) that rotates to the next key on a
    ``429`` instead of silently using only the first configured key.

    :param network: Network name (``arbitrum``, ``avalanche``).
    :returns: Rotating HyperSync client-pool instance.
    """
    url = HYPERSYNC_URLS.get(network)
    if not url:
        raise ValueError(f"Unsupported network: {network}")
    raw_token = os.environ.get("HYPERSYNC_API_TOKEN")
    pool = RotatingHypersyncClient(raw_token, url)
    if pool.total_keys > 1:
        console.print(f"  Using HyperSync API key pool: [cyan]{pool.total_keys} key(s)[/cyan]")
    elif raw_token:
        console.print(f"  Using HyperSync API token: [cyan]{raw_token[:8]}...[/cyan]")
    else:
        console.print("  [yellow]No HYPERSYNC_API_TOKEN set — may get 403 errors[/yellow]")
    return pool


async def get_latest_block(client: HypersyncClient | RotatingHypersyncClient) -> int:
    """Get latest block number.

    :param client: HyperSync client instance.
    :returns: Latest block number.
    """
    return await client.get_height()


async def _stream_with_retry(
    client: HypersyncClient | RotatingHypersyncClient,
    query: Query,
    max_retries: int = MAX_RETRIES,
):
    """Stream HyperSync results with retry on transient errors.

    :param client: HyperSync client instance, or a :class:`RotatingHypersyncClient`
        pool -- on a rate-limit error the pool rotates to its next configured
        key and retries immediately without counting against ``max_retries``.
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
            if isinstance(client, RotatingHypersyncClient) and client.rotate_on_error(e):
                console.print(
                    f"[yellow]Rate limit hit — rotated HyperSync API key, "
                    f"retrying from block {current_from_block:,}[/yellow]"
                )
                continue

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


async def extract_oracle_price_events(
    client: HypersyncClient | RotatingHypersyncClient,
    network: str,
    from_block: int,
    to_block: int | None,
) -> list[OraclePriceRecord]:
    """Extract OraclePriceUpdate events from GMX V2 EventEmitter.

    :param client: HyperSync client instance.
    :param network: Network name.
    :param from_block: Starting block number.
    :param to_block: Ending block number (``None`` for latest).
    :returns: List of :class:`OraclePriceRecord` objects.
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
                    [ORACLE_PRICE_UPDATE_HASH],
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
    to_block_str = f"{to_block:,}" if to_block is not None else "latest"
    console.print(
        f"  Block range:  [cyan]{from_block:,}[/cyan] to "
        f"[cyan]{to_block_str}[/cyan] ({total_blocks:,} blocks)"
    )
    console.print("  Event:        [cyan]OraclePriceUpdate[/cyan]")

    records: list[OraclePriceRecord] = []
    block_timestamps: dict[int, int] = {}
    total_logs = 0
    decode_errors = 0
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
            "Extracting OraclePriceUpdate",
            total=total_blocks,
            logs=0,
            events=0,
            rate=0.0,
            errors=0,
        )

        async for response in _stream_with_retry(client, query):
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

                decoded = decode_oracle_price_event(log.data)
                if decoded is None:
                    decode_errors += 1
                    continue

                # Skip zero prices
                if decoded["min_price"] == 0 and decoded["max_price"] == 0:
                    continue

                block_num = log.block_number or 0
                ts = block_timestamps.get(block_num, 0)

                record = OraclePriceRecord(
                    token=decoded["token"],
                    min_price=str(decoded["min_price"]),
                    max_price=str(decoded["max_price"]),
                    block_number=block_num,
                    block_timestamp=ts,
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
        f"{len(records):,} events from {total_logs:,} logs"
    )
    if decode_errors:
        console.print(f"  [yellow]Decode errors: {decode_errors:,}[/yellow]")

    return records


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

    if "block_number" in combined.columns and "log_index" in combined.columns:
        # Include "token" in dedup key since oracle prices for all tokens share one file
        dedup_cols = ["block_number", "log_index"]
        if "token" in combined.columns:
            dedup_cols.append("token")
        combined = combined.unique(subset=dedup_cols, keep="last")
        combined = combined.sort(["block_number", "log_index"])

    combined.write_parquet(filepath)


def save_raw_oracle(records: list[OraclePriceRecord], output_dir: Path) -> None:
    """Save raw oracle price events to a single Parquet file.

    :param records: List of :class:`OraclePriceRecord` objects.
    :param output_dir: Base output directory (e.g., ``data/funding/arbitrum``).
    """
    if not HAS_POLARS:
        console.print("[red]polars required for Parquet[/red]")
        return

    filepath = output_dir / "raw" / "oracle_prices" / "data.parquet"
    df = pl.DataFrame([asdict(r) for r in records])
    append_parquet(df, filepath)
    console.print(f"  Oracle: [cyan]{len(records):,}[/cyan] events -> [green]{filepath}[/green]")


# =============================================================================
# SUMMARY
# =============================================================================


def print_summary(records: list[OraclePriceRecord]) -> None:
    """Print summary statistics.

    :param records: List of :class:`OraclePriceRecord` objects.
    """
    table = Table(title="Oracle Price Update Summary", show_lines=False)
    table.add_column("Token", style="cyan")
    table.add_column("Events", justify="right")

    by_token: dict[str, int] = defaultdict(int)
    for r in records:
        by_token[r.token] += 1

    for token in sorted(by_token.keys()):
        table.add_row(
            token[:10] + "..." + token[-6:],
            f"{by_token[token]:,}",
        )

    console.print()
    console.print(table)

    timestamps = [r.block_timestamp for r in records if r.block_timestamp]
    if timestamps:
        first = datetime.fromtimestamp(min(timestamps), tz=UTC)
        last = datetime.fromtimestamp(max(timestamps), tz=UTC)
        console.print(
            f"\n  Time range: [cyan]{first.isoformat()}[/cyan] to [cyan]{last.isoformat()}[/cyan]"
        )

    console.print(f"  Total events: [cyan]{len(records):,}[/cyan]")
    console.print(f"  Tokens:       [cyan]{len(by_token):,}[/cyan]")


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
    checkpoint_path = checkpoint_dir / "oracle_prices_checkpoint.json"

    from_block = args.from_block

    if args.resume and from_block is None:
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

    header_lines = [
        f"Network:     [cyan]{args.network}[/cyan]",
        f"Block range: [cyan]{from_block:,}[/cyan] to [cyan]{args.to_block or 'latest'}[/cyan]",
        f"Output dir:  [cyan]{output_dir}[/cyan]",
    ]
    if args.resume:
        header_lines.append(f"Resume:      [cyan]enabled[/cyan] (checkpoint: {checkpoint_path})")

    console.print(
        Panel(
            "\n".join(header_lines),
            title="GMX V2 Raw Oracle Price Extractor",
            subtitle="HyperSync + OraclePriceUpdate",
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
    records = await extract_oracle_price_events(
        client=client,
        network=args.network,
        from_block=from_block,
        to_block=to_block,
    )

    if not records:
        console.print("\n[yellow]No OraclePriceUpdate events found.[/yellow]")
        if args.resume:
            save_checkpoint(
                checkpoint_path,
                last_block=to_block,
                last_timestamp=int(time.time()),
                total_events=0,
            )
        return

    # Save outputs
    console.print()
    save_raw_oracle(records, output_dir)

    # Save checkpoint
    if args.resume:
        last_block = max(r.block_number for r in records)
        last_timestamp = max(r.block_timestamp for r in records if r.block_timestamp)

        prev_checkpoint = load_checkpoint(checkpoint_path)
        prev_total = prev_checkpoint["total_events"] if prev_checkpoint else 0

        save_checkpoint(
            checkpoint_path,
            last_block=last_block,
            last_timestamp=last_timestamp,
            total_events=prev_total + len(records),
        )

    print_summary(records)


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract GMX V2 OraclePriceUpdate events using HyperSync",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full historical extraction from genesis
  poetry run python scripts/extract_oracle_prices_raw.py --from-block 120000000

  # Quick test
  poetry run python scripts/extract_oracle_prices_raw.py \\
      --from-block 170000000 --to-block 170100000

  # Incremental
  poetry run python scripts/extract_oracle_prices_raw.py --resume
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

    args = parser.parse_args()

    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
