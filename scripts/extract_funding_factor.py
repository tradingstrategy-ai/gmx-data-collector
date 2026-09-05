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
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import hypersync
import polars as pl
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

from gmx_historical_data.atomic_parquet import atomic_write_parquet
from gmx_historical_data.hypersync_client_factory import RotatingHypersyncClient
from gmx_historical_data.market_registry import fetch_markets, market_symbol

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

# =============================================================================
# DATA CLASSES & HELPERS
# =============================================================================


@dataclass
class FundingFactorRecord:
    """Raw Funding event from GMX V2 EventEmitter.

    The ``Funding`` event carries ``fundingFactorPerSecond`` — the per-second
    funding rate as a 30-decimal fixed-point integer.

    Direction (``longs_pay_shorts``) is not encoded in this event; it depends
    on the OI imbalance and is determined downstream in
    :mod:`scripts.extract_unified_funding` by joining direction data from
    ``extract_funding_fee_per_size.py``.

    :ivar symbol: Derived symbol (e.g., ``'ETH'``)
    :ivar market: Market contract address
    :ivar funding_factor_per_second: Raw 30-decimal integer as string
    :ivar funding_rate_per_second: Decimal per-second rate (unsigned magnitude)
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
        msg_sender, event_name, event_data = abi_decode(EVENTLOG1_ABI_TYPES, data_bytes)
    except Exception as e:
        return {"_decode_error": str(e)}

    result = {
        "event_name": event_name,
        "msg_sender": (
            msg_sender
            if isinstance(msg_sender, str)
            else ("0x" + msg_sender.hex() if isinstance(msg_sender, bytes) else str(msg_sender))
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
        "last_updated": datetime.now(tz=UTC).isoformat(),
        "metadata": {"markets_seen": markets_seen},
    }
    with open(path, "w") as f:
        json.dump(checkpoint, f, indent=2)
    console.print(f"  Checkpoint saved: block [cyan]{last_block:,}[/cyan] -> [green]{path}[/green]")


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


async def extract_funding_events(
    client: HypersyncClient | RotatingHypersyncClient,
    network: str,
    from_block: int,
    to_block: int | None,
    market_filter: str | None = None,
    markets: dict | None = None,
) -> list[FundingFactorRecord]:
    """Extract Funding events from GMX V2 EventEmitter.

    :param client: HyperSync client instance.
    :param network: Network name.
    :param from_block: Starting block number.
    :param to_block: Ending block number (``None`` for latest).
    :param market_filter: Optional market symbol filter (e.g., ``'ETH/USD'``).
    :param markets: Market registry from :func:`fetch_markets`. Defaults to
        an empty dict (all unknown markets get truncated-address names).
    :returns: List of :class:`FundingFactorRecord` objects.
    """
    if markets is None:
        markets = {}
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
    console.print("  Event:        [cyan]Funding (fundingFactorPerSecond)[/cyan]")

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
                market_info = markets.get(market_addr.lower())
                if market_info and market_info.get("indexToken") is None:
                    continue

                # fundingFactorPerSecond is always in uints (unsigned).
                # Direction (longs pay shorts or vice versa) is NOT encoded
                # in the Funding event — it depends on OI imbalance. We treat
                # the rate as the absolute magnitude here; direction is joined
                # in by extract_unified_funding at merge time.
                factor_raw = decoded["uints"].get("fundingFactorPerSecond", 0)

                symbol = market_symbol(market_addr, markets)

                # Track unknown markets
                if market_addr.lower() not in markets:
                    unknown_markets.add(market_addr.lower())

                # Apply market filter
                if market_filter:
                    market_info = markets.get(market_addr.lower(), {})
                    if market_info.get("symbol", "") != market_filter:
                        continue

                block_num = log.block_number or 0
                ts = block_timestamps.get(block_num, 0)
                dt_str = datetime.fromtimestamp(ts, tz=UTC).isoformat() if ts else ""

                rate = factor_raw / FUNDING_FACTOR_PRECISION

                record = FundingFactorRecord(
                    symbol=symbol,
                    market=market_addr.lower(),
                    funding_factor_per_second=str(factor_raw),
                    funding_rate_per_second=rate,
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
    if not records:
        return {}

    df = pl.DataFrame([asdict(r) for r in records])

    # Convert block_timestamp to datetime and to a hashable hour bucket.
    df = df.with_columns(
        pl.from_epoch(pl.col("block_timestamp"), time_unit="s")
        .cast(pl.Datetime("ns", "UTC"))
        .alias("timestamp"),
    ).with_columns(
        pl.col("timestamp").dt.truncate("1h").alias("hour"),
    )

    # ------------------------------------------------------------------
    # Time-weighted average (TWAP) within each hourly bucket
    # ------------------------------------------------------------------
    # The rate at any second is the most recent funding_rate_per_second
    # reported by an event up to that second. dt_seconds is the seconds the
    # current event's rate stays in effect within its hour bucket:
    #
    #   dt_i = min(next_event_ts, hour_end) - event_ts
    #
    # For the final event in a bucket (no next event before hour_end),
    # dt = hour_end - event_ts. TWAP = sum(rate * dt) / sum(dt).
    df = df.sort(["symbol", "market", "timestamp"])
    df = df.with_columns(
        pl.col("timestamp").shift(-1).over(["symbol", "market"]).alias("_next_ts"),
        (pl.col("hour") + pl.duration(hours=1)).alias("_hour_end"),
    )
    df = df.with_columns(
        pl.min_horizontal(
            pl.col("_next_ts").fill_null(pl.col("_hour_end")), pl.col("_hour_end")
        ).alias("_window_end"),
    )
    df = df.with_columns(
        (pl.col("_window_end") - pl.col("timestamp")).dt.total_seconds().alias("dt_seconds"),
    )
    # Defensive: any non-positive dt (e.g. duplicate timestamps) contributes
    # zero seconds but its rate still factors into min/max/update_count.
    df = df.with_columns(pl.col("dt_seconds").clip(lower_bound=0))

    hourly = (
        df.group_by(["symbol", "market", "hour"])
        .agg(
            (pl.col("funding_rate_per_second") * pl.col("dt_seconds")).sum().alias("_weighted_sum"),
            pl.col("dt_seconds").sum().alias("_total_seconds"),
            pl.col("funding_rate_per_second").min().alias("funding_rate_min"),
            pl.col("funding_rate_per_second").max().alias("funding_rate_max"),
            pl.len().alias("update_count"),
        )
        .with_columns(
            pl.when(pl.col("_total_seconds") > 0)
            .then(pl.col("_weighted_sum") / pl.col("_total_seconds"))
            # Degenerate case (all events at hour_end): fall back to max as an
            # unsigned representative; the row keeps min/max for downstream use.
            .otherwise(pl.col("funding_rate_max"))
            .alias("funding_rate"),
        )
        .drop(["_weighted_sum", "_total_seconds"])
        .sort(["symbol", "hour"])
    )

    # Derived columns (unsigned magnitudes; direction joined later in unified merge)
    hourly = hourly.with_columns(
        (pl.col("funding_rate") * 3600).alias("funding_rate_hourly"),
        (pl.col("funding_rate") * 3600 * 8760).alias("funding_rate_annualized"),
    )

    hourly = hourly.rename({"hour": "timestamp"})

    # Split by symbol; emit columns in stable order.
    result = {}
    for symbol in hourly["symbol"].unique().sort().to_list():
        sym_df = (
            hourly.filter(pl.col("symbol") == symbol)
            .select(
                [
                    "timestamp",
                    "funding_rate",
                    "funding_rate_min",
                    "funding_rate_max",
                    "funding_rate_hourly",
                    "funding_rate_annualized",
                    "update_count",
                    "symbol",
                    "market",
                ]
            )
            .cast({"update_count": pl.UInt32})
        )
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

    atomic_write_parquet(combined, filepath)


def save_raw_per_symbol(records: list[FundingFactorRecord], output_dir: Path) -> None:
    """Save raw Funding events to per-symbol Parquet files.

    :param records: List of :class:`FundingFactorRecord` objects.
    :param output_dir: Base output directory (e.g., ``data/funding/arbitrum``).
    """
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


def save_rates_per_symbol(hourly_by_symbol: dict[str, "pl.DataFrame"], output_dir: Path) -> None:
    """Save hourly aggregated rates to per-symbol Parquet files.

    :param hourly_by_symbol: Dict from :func:`aggregate_hourly_rates`.
    :param output_dir: Base output directory (e.g., ``data/funding/arbitrum``).
    """
    for symbol, df in sorted(hourly_by_symbol.items()):
        filepath = output_dir / "rates" / symbol / "1h.parquet"
        append_parquet(df, filepath)
        console.print(f"  Rates: [cyan]{len(df):,}[/cyan] hours -> [green]{filepath}[/green]")


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

    by_symbol: dict[str, list[FundingFactorRecord]] = defaultdict(list)
    for r in records:
        by_symbol[r.symbol].append(r)

    for symbol in sorted(by_symbol.keys()):
        sym_records = by_symbol[symbol]
        avg_rate = sum(r.funding_rate_per_second for r in sym_records) / len(sym_records)
        avg_hourly = avg_rate * 3600
        avg_annual = avg_rate * 3600 * 8760
        table.add_row(
            symbol,
            f"{len(sym_records):,}",
            f"{avg_rate:.2e}",
            f"{avg_hourly:.6f}",
            f"{avg_annual:.2%}",
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
        f"Output:      [cyan]{args.output}[/cyan]",
        f"Output dir:  [cyan]{output_dir}[/cyan]",
    ]
    if args.market:
        header_lines.append(f"Market:      [cyan]{args.market}[/cyan]")
    if args.resume:
        header_lines.append(f"Resume:      [cyan]enabled[/cyan] (checkpoint: {checkpoint_path})")

    console.print(
        Panel(
            "\n".join(header_lines),
            title="GMX V2 Funding Factor Extractor",
            subtitle="HyperSync + eth_abi",
            border_style="blue",
        )
    )

    with console.status("Fetching GMX market registry..."):
        markets = fetch_markets(args.network, force_refresh=args.refresh_markets)
    console.print(f"  Markets loaded: [cyan]{len(markets):,}[/cyan]")

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
        markets=markets,
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
    parser.add_argument(
        "--refresh-markets",
        action="store_true",
        help="Force re-fetch of GMX market registry (ignores 24h disk cache)",
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
