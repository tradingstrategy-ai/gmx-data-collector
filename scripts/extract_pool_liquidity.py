#!/usr/bin/env python3
"""
GMX V2 PoolAmountUpdated Event Extractor
=========================================
Extracts ``PoolAmountUpdated`` events from the GMX V2 EventEmitter via
Envio HyperSync, giving a full history of LP pool token changes.

``PoolAmountUpdated`` is emitted on every LP deposit, LP withdrawal, and
position-triggered pool rebalance.  The ``nextValue`` field is the new
unsigned total of pool tokens after the change.

EventLogData layout
-------------------
addressItems : ``market``, ``token``
intItems     : ``delta``      (signed, raw token units — negative on withdrawal)
uintItems    : ``nextValue``  (new total pool token amount)

Output structure::

    {output_dir}/{network}/raw/{SYMBOL}/data.parquet        — raw events per symbol
    {output_dir}/{network}/snapshots/{SYMBOL}/daily.parquet — daily snapshots per symbol
    {output_dir}/{network}/checkpoints/pool_liquidity_checkpoint.json

:envvar HYPERSYNC_API_TOKEN:
    HyperSync bearer token (required for production use).
:envvar HYPERSYNC_ENDPOINT:
    Override HyperSync URL (default per network).
:envvar FROM_BLOCK:
    Start block (default: ``120000000`` — around GMX V2 Synthetics launch).
:envvar TO_BLOCK:
    End block (default: latest).
:envvar GMX_POOL_OUTPUT_DIR:
    Base output directory (default: ``./user_data/data/gmx/pool_liquidity``).

Usage::

    poetry run python scripts/extract_pool_liquidity.py --from-block 120000000
    poetry run python scripts/extract_pool_liquidity.py --resume
    poetry run python scripts/extract_pool_liquidity.py --from-block 290000000 --to-block 291000000
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

import pandas as pd
from eth_defi.gmx.api import GMXAPI
from eth_defi.gmx.config import GMXConfig
from eth_hash.auto import keccak
from eth_utils import to_hex
from extract_open_interest import (
    _stream_with_retry,
    decode_event_log_data,
)
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
from web3 import Web3

from gmx_historical_data.hypersync_client_factory import RotatingHypersyncClient
from gmx_historical_data.market_registry import fetch_markets
from gmx_historical_data.oracle_price_collector import ArbitrumMockProvider

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

console = Console()

# =============================================================================
# CONSTANTS
# =============================================================================

#: EventLog1 topic0 — same for all GMX V2 events
EVENT_LOG1_TOPIC = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160"

#: topic1 hash for PoolAmountUpdated
POOL_AMOUNT_UPDATED_HASH = to_hex(keccak(b"PoolAmountUpdated"))

#: HyperSync endpoints per network
HYPERSYNC_URLS: dict[str, str] = {
    "arbitrum": "https://arbitrum.hypersync.xyz",
    "avalanche": "https://avalanche.hypersync.xyz",
}

#: GMX V2 EventEmitter contract addresses per network
EVENT_EMITTER_ADDRESSES: dict[str, str] = {
    "arbitrum": "0xC8ee91A54287DB53897056e12D9819156D3822Fb",
    "avalanche": "0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26",
}

#: GMX V2 genesis block on Arbitrum (approximate deployment)
GMX_V2_GENESIS_BLOCK = 120_000_000

DEFAULT_FROM_BLOCK = GMX_V2_GENESIS_BLOCK

#: Maximum HyperSync retry attempts per transient failure
MAX_RETRIES = 15

#: Base delay in seconds for exponential backoff
RETRY_BASE_DELAY = 2.0

#: Maximum delay cap for exponential backoff (2 minutes)
RETRY_MAX_DELAY = 120.0

#: Flush records to disk every N events (keeps memory bounded for full-history runs)
FLUSH_EVERY = 500_000

#: Output formats that support incremental streaming flush
STREAMABLE_OUTPUTS = frozenset({"parquet"})

PROGRESS_MILESTONES = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100]


# =============================================================================
# TOKEN DECIMALS
# =============================================================================


def fetch_token_decimals() -> dict[str, int]:
    """Fetch token decimals from GMX API for all supported tokens.

    Uses ``GMXAPI.get_tokens()`` so the map stays accurate as new markets
    are listed without requiring script changes.

    :returns: Mapping of lowercase token address → decimal precision.
    """
    web3 = Web3(ArbitrumMockProvider())
    config = GMXConfig(web3)
    api = GMXAPI(config)
    data = api.get_tokens()
    decimals: dict[str, int] = {}
    for token in data.get("tokens", []):
        addr = token.get("address", "").lower()
        dec = token.get("decimals")
        if addr and dec is not None:
            decimals[addr] = int(dec)
    console.print(f"  Token decimals loaded: [cyan]{len(decimals)}[/cyan] tokens from GMX API")
    return decimals


# =============================================================================
# DATA CLASS
# =============================================================================


@dataclass
class PoolAmountRecord:
    """Single ``PoolAmountUpdated`` event.

    :ivar symbol: Market symbol (e.g. ``"ETH/USD"``).
    :ivar market: Market contract address (lowercase).
    :ivar token: Pool token address (lowercase).
    :ivar delta: Signed change in raw token units (string).
    :ivar next_value: New total pool tokens after change (string).
    :ivar block_number: Block number.
    :ivar block_timestamp: Unix timestamp (seconds).
    :ivar block_datetime: ISO 8601 datetime string.
    :ivar transaction_hash: Transaction hash.
    :ivar log_index: Log index within the block.
    """

    symbol: str
    market: str
    token: str
    delta: str
    next_value: str
    block_number: int
    block_timestamp: int
    block_datetime: str
    transaction_hash: str
    log_index: int


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
    :param last_timestamp: Last processed block timestamp (Unix seconds).
    :param total_events: Cumulative total of events saved so far.
    :param markets_seen: Number of unique markets seen in the current run.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    checkpoint = {
        "symbol": "pool_liquidity_all",
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
# EXTRACTION
# =============================================================================


async def extract_pool_events(
    client: HypersyncClient | RotatingHypersyncClient,
    from_block: int,
    to_block: int | None,
    network: str = "arbitrum",
    markets: dict | None = None,
    flush_callback=None,
) -> list[PoolAmountRecord]:
    """Stream ``PoolAmountUpdated`` events from HyperSync.

    :param client: HyperSync client instance.
    :param from_block: Starting block number.
    :param to_block: Ending block number (``None`` = chain head).
    :param network: Network name (``"arbitrum"`` or ``"avalanche"``).
    :param markets: Market registry from
        :func:`~gmx_historical_data.market_registry.fetch_markets`.
        Unknown addresses fall back to a truncated-address label.
    :param flush_callback: Optional callable ``(batch, highest_block)`` invoked
        every ``FLUSH_EVERY`` events to persist records incrementally and bound
        memory usage. When provided the returned list will be empty.
    :returns: List of :class:`PoolAmountRecord` instances (empty when
        *flush_callback* is supplied).
    """
    if markets is None:
        markets = {}

    emitter = EVENT_EMITTER_ADDRESSES.get(network)
    if not emitter:
        raise ValueError(f"No EventEmitter address for network: {network}")

    if to_block is None:
        to_block = await client.get_height()
        console.print(f"  Latest block: [cyan]{to_block:,}[/cyan]")

    total_blocks = to_block - from_block

    query = Query(
        from_block=from_block,
        to_block=to_block,
        logs=[
            LogSelection(
                address=[emitter],
                topics=[
                    [EVENT_LOG1_TOPIC],
                    [POOL_AMOUNT_UPDATED_HASH],
                ],
            )
        ],
        field_selection=FieldSelection(
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
            log=[
                LogField.BLOCK_NUMBER,
                LogField.LOG_INDEX,
                LogField.TRANSACTION_HASH,
                LogField.TOPIC0,
                LogField.TOPIC1,
                LogField.DATA,
            ],
        ),
    )

    console.print(f"  EventEmitter: [cyan]{emitter}[/cyan]")
    console.print(
        f"  Block range:  [cyan]{from_block:,}[/cyan] to [cyan]{to_block:,}[/cyan] "
        f"({total_blocks:,} blocks)"
    )
    console.print("  Event:        [cyan]PoolAmountUpdated[/cyan]")

    records: list[PoolAmountRecord] = []
    total_logs = 0
    total_flushed = 0
    flush_count = 0
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
            "Scanning blocks",
            total=total_blocks if total_blocks > 0 else None,
            logs=0,
            events=0,
            rate=0,
            errors=0,
        )

        async for response in _stream_with_retry(client, query):
            if not response.data.logs:
                if response.data.blocks:
                    new_highest = max(b.number for b in response.data.blocks)
                    if new_highest > highest_block:
                        progress.advance(task, new_highest - highest_block)
                        highest_block = new_highest
                continue

            block_timestamps: dict[int, int] = {}
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

                decoded = decode_event_log_data(log.data) if log.data else {}
                if "_decode_error" in decoded or decoded.get("event_name") != "PoolAmountUpdated":
                    if "_decode_error" in decoded:
                        decode_errors += 1
                    continue

                addresses = decoded.get("addresses", {})
                uints = decoded.get("uints", {})
                ints = decoded.get("ints", {})

                market_addr = addresses.get("market", "")
                token_addr = addresses.get("token", "")

                delta = ints.get("delta")
                if delta is None:
                    delta = uints.get("delta", 0)
                next_value = uints.get("nextValue", 0)

                market_info = markets.get(market_addr.lower() if market_addr else "", {})

                # Skip swap-only markets:
                # 1. Known swap markets: indexToken is None in the market registry
                # 2. Delisted / unknown swap markets: all perp symbols contain "/"
                if market_info and market_info.get("indexToken") is None:
                    continue

                symbol = market_info.get("symbol", market_addr[:10] if market_addr else "UNKNOWN")

                if "/" not in symbol:
                    continue

                timestamp = block_timestamps.get(log.block_number)
                if not timestamp:
                    # Block timestamp not available for this log — skip to avoid
                    # writing epoch-zero (1970-01-01) timestamps to Parquet.
                    decode_errors += 1
                    continue
                dt_str = datetime.fromtimestamp(timestamp, tz=UTC).isoformat()

                tx_hash = log.transaction_hash or ""
                if isinstance(tx_hash, bytes):
                    tx_hash = tx_hash.hex()

                records.append(
                    PoolAmountRecord(
                        symbol=symbol,
                        market=market_addr.lower(),
                        token=token_addr.lower(),
                        delta=str(delta),
                        next_value=str(next_value),
                        block_number=log.block_number,
                        block_timestamp=timestamp,
                        block_datetime=dt_str,
                        transaction_hash=tx_hash,
                        log_index=log.log_index or 0,
                    )
                )

            # Flush to disk every FLUSH_EVERY events to keep memory bounded
            if flush_callback is not None and len(records) >= FLUSH_EVERY:
                flush_callback(records, highest_block)
                total_flushed += len(records)
                flush_count += 1
                records.clear()

            # Update progress
            if batch_highest > highest_block:
                advance = batch_highest - highest_block
                highest_block = batch_highest
                progress.advance(task, advance)

            elapsed = time.monotonic() - t_start
            rate = total_logs / elapsed if elapsed > 0 else 0
            progress.update(
                task, logs=total_logs, events=len(records), rate=rate, errors=decode_errors
            )

            if total_blocks > 0:
                pct = (highest_block - from_block) / total_blocks * 100
                while (
                    next_milestone_idx < len(PROGRESS_MILESTONES)
                    and pct >= PROGRESS_MILESTONES[next_milestone_idx]
                ):
                    console.print(
                        f"  [{PROGRESS_MILESTONES[next_milestone_idx]}%] "
                        f"block {highest_block:,} | {len(records) + total_flushed:,} events "
                        f"| {rate:.0f} logs/s"
                    )
                    next_milestone_idx += 1

    # Final flush for any remaining records
    if flush_callback is not None and records:
        flush_callback(records, highest_block)
        total_flushed += len(records)
        flush_count += 1
        records.clear()

    elapsed = time.monotonic() - t_start
    total_events = total_flushed + len(records)
    console.print(
        f"\n  Done in {elapsed:.1f}s: {total_events:,} PoolAmountUpdated events "
        f"({decode_errors} decode errors)"
        + (f" (flushed in {flush_count} batches)" if flush_count else "")
    )
    return records


# =============================================================================
# STORAGE
# =============================================================================


def append_parquet(df: "pl.DataFrame", filepath: Path) -> None:
    """Append a polars DataFrame to an existing Parquet file with deduplication.

    Reads existing file (if any), concatenates with new data, deduplicates
    by ``(block_number, log_index)``, sorts, and writes back atomically.

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
        combined = combined.unique(subset=["block_number", "log_index"], keep="last")
        combined = combined.sort(["block_number", "log_index"])

    combined.write_parquet(filepath)


def save_raw_per_symbol(records: list[PoolAmountRecord], output_dir: Path) -> None:
    """Save raw events to per-symbol Parquet files.

    Files are written to ``{output_dir}/raw/{safe_symbol}/data.parquet``.
    Existing data is deduplicated and preserved.

    :param records: List of :class:`PoolAmountRecord` instances.
    :param output_dir: Network-level output directory
        (e.g. ``./user_data/data/gmx/pool_liquidity/arbitrum``).
    """
    if not HAS_POLARS:
        console.print(
            "[red]polars required for Parquet output — install with: pip install polars[/red]"
        )
        return

    by_symbol: dict[str, list[PoolAmountRecord]] = defaultdict(list)
    for r in records:
        by_symbol[r.symbol].append(r)

    for symbol, sym_records in sorted(by_symbol.items()):
        safe = symbol.replace("/", "_").replace(" ", "_").replace("[", "").replace("]", "")
        filepath = output_dir / "raw" / safe / "data.parquet"
        df = pl.DataFrame([asdict(r) for r in sym_records])
        append_parquet(df, filepath)
        console.print(
            f"  Raw: [cyan]{len(sym_records):,}[/cyan] events -> [green]{filepath}[/green]"
        )


def save_daily_per_symbol(
    records: list[PoolAmountRecord],
    output_dir: Path,
    token_decimals: dict[str, int],
) -> None:
    """Build end-of-day pool token snapshots and save per-symbol Parquet files.

    For each ``(date, token)`` pair, takes the last ``next_value`` within the UTC day
    and converts raw token units to human-readable amounts using ``token_decimals``.
    Falls back to 18 decimals for unknown tokens.

    Files are written to ``{output_dir}/snapshots/{safe_symbol}/daily.parquet``.

    :param records: List of :class:`PoolAmountRecord` instances.
    :param output_dir: Network-level output directory.
    :param token_decimals: Mapping of lowercase token address → ERC-20 decimals.
    """
    if not HAS_POLARS or not records:
        return

    by_symbol: dict[str, list[PoolAmountRecord]] = defaultdict(list)
    for r in records:
        by_symbol[r.symbol].append(r)

    for symbol, sym_records in sorted(by_symbol.items()):
        safe = symbol.replace("/", "_").replace(" ", "_").replace("[", "").replace("]", "")

        rows = [
            {
                "block_timestamp": r.block_timestamp,
                "token": r.token,
                "next_value": r.next_value,
            }
            for r in sym_records
        ]
        pdf = pd.DataFrame(rows)
        pdf["ts"] = pd.to_datetime(pdf["block_timestamp"], unit="s", utc=True)
        pdf["date"] = pdf["ts"].dt.normalize()
        pdf["next_value_int"] = pdf["next_value"].astype(float)

        daily = (
            pdf.sort_values("ts")
            .groupby(["date", "token"], sort=False)["next_value_int"]
            .last()
            .reset_index()
        )

        unknown: set[str] = set()

        def _convert(row: "pd.Series") -> float:
            decimals = token_decimals.get(row["token"])
            if decimals is None:
                unknown.add(row["token"])
                decimals = 18
            return row["next_value_int"] / (10**decimals)

        daily["pool_tokens"] = daily.apply(_convert, axis=1)
        daily["symbol"] = symbol
        daily["date"] = pd.to_datetime(daily["date"]).dt.tz_convert("UTC")

        if unknown:
            console.print(
                f"  [yellow]WARN [{symbol}]: {len(unknown)} unknown token(s) — "
                f"using 18-decimal fallback[/yellow]"
            )

        pl_df = pl.from_pandas(
            daily[["date", "symbol", "token", "pool_tokens"]].reset_index(drop=True)
        )
        filepath = output_dir / "snapshots" / safe / "daily.parquet"
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if filepath.exists():
            existing = pl.read_parquet(filepath)
            for col in existing.columns:
                if col in pl_df.columns and existing[col].dtype != pl_df[col].dtype:
                    pl_df = pl_df.with_columns(pl.col(col).cast(existing[col].dtype))
            combined = pl.concat([existing, pl_df], how="diagonal_relaxed")
            combined = combined.unique(subset=["date", "token"], keep="last")
            combined = combined.sort(["date", "token"])
            combined.write_parquet(filepath)
        else:
            pl_df.sort(["date", "token"]).write_parquet(filepath)

        console.print(f"  Snapshots: [cyan]{len(daily):,}[/cyan] days -> [green]{filepath}[/green]")


def build_daily_snapshot(df: pd.DataFrame, token_decimals: dict[str, int]) -> pd.DataFrame:
    """Build end-of-day pool token snapshots from a flat raw-events DataFrame.

    This function is kept for backward-compatibility and ad-hoc use outside
    the main pipeline.  The streaming pipeline uses :func:`save_daily_per_symbol`
    instead.

    :param df: Raw events DataFrame with columns ``block_timestamp``, ``symbol``,
        ``token``, ``next_value``.
    :param token_decimals: Mapping of lowercase token address → ERC-20 decimals.
    :returns: Daily snapshot DataFrame with columns ``date``, ``symbol``,
        ``token``, ``pool_tokens`` (float64).
    """
    if df.empty:
        return pd.DataFrame(columns=["date", "symbol", "token", "pool_tokens"])

    df = df.copy()
    df["ts"] = pd.to_datetime(df["block_timestamp"], unit="s", utc=True)
    df["date"] = df["ts"].dt.normalize()
    df["next_value_int"] = df["next_value"].astype(float)

    daily = (
        df.sort_values("ts")
        .groupby(["date", "symbol", "token"], sort=False)["next_value_int"]
        .last()
        .reset_index()
    )

    unknown: set[str] = set()

    def _convert(row: "pd.Series") -> float:
        decimals = token_decimals.get(row["token"])
        if decimals is None:
            unknown.add(row["token"])
            decimals = 18
        return row["next_value_int"] / (10**decimals)

    daily["pool_tokens"] = daily.apply(_convert, axis=1)
    if unknown:
        console.print(
            f"  [yellow]WARN: {len(unknown)} unknown token(s) — using 18-decimal fallback:[/yellow]"
        )
        for addr in sorted(unknown):
            console.print(f"    {addr}")

    daily = daily[["date", "symbol", "token", "pool_tokens"]].copy()
    daily["date"] = pd.to_datetime(daily["date"]).dt.tz_convert("UTC")
    return daily.sort_values(["date", "symbol", "token"]).reset_index(drop=True)


# =============================================================================
# MAIN
# =============================================================================


async def async_main(args: argparse.Namespace) -> None:
    """Async entry point — reads CLI args / env vars, collects events, saves Parquet.

    :param args: Parsed command-line arguments.
    """
    network = args.network
    base_output_dir = args.output_dir or Path(
        os.environ.get("GMX_POOL_OUTPUT_DIR", "./user_data/data/gmx/pool_liquidity")
    )
    output_dir = base_output_dir / network
    checkpoint_dir = (
        Path(args.checkpoint_dir) if args.checkpoint_dir else output_dir / "checkpoints"
    )
    checkpoint_path = checkpoint_dir / "pool_liquidity_checkpoint.json"

    endpoint = (
        args.endpoint
        or os.environ.get("HYPERSYNC_ENDPOINT")
        or HYPERSYNC_URLS.get(network, HYPERSYNC_URLS["arbitrum"])
    )

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
        from_block = int(os.environ.get("FROM_BLOCK", str(DEFAULT_FROM_BLOCK)))

    to_block_arg: int | None = args.to_block or (
        int(os.environ.get("TO_BLOCK")) if os.environ.get("TO_BLOCK") else None
    )

    resume_line = (
        f"\nResume:      [cyan]enabled[/cyan] (checkpoint: {checkpoint_path})"
        if args.resume
        else ""
    )
    console.print(
        Panel(
            f"Network:     [cyan]{network}[/cyan]\n"
            f"Endpoint:    [cyan]{endpoint}[/cyan]\n"
            f"Block range: [cyan]{from_block:,}[/cyan] to "
            f"[cyan]{to_block_arg or 'latest'}[/cyan]\n"
            f"Output dir:  [cyan]{output_dir}[/cyan]" + resume_line,
            title="GMX V2 Pool Liquidity Extractor",
            border_style="blue",
        )
    )

    with console.status("Fetching GMX market registry..."):
        markets = fetch_markets(network, force_refresh=args.refresh_markets)
    console.print(f"  Markets loaded: [cyan]{len(markets):,}[/cyan]")

    with console.status("Fetching token decimals from GMX API..."):
        token_decimals = fetch_token_decimals()

    raw_token = os.environ.get("HYPERSYNC_API_TOKEN")
    client = RotatingHypersyncClient(raw_token, endpoint)
    if client.total_keys > 1:
        console.print(f"  Using HyperSync API key pool: [cyan]{client.total_keys} key(s)[/cyan]")
    elif raw_token:
        console.print(f"  Using HyperSync API token: [cyan]{raw_token[:8]}...[/cyan]")
    else:
        console.print("  [yellow]No HYPERSYNC_API_TOKEN set — may get 403 errors[/yellow]")

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
        save_daily_per_symbol(batch, output_dir, token_decimals)

        flush_state["total_events"] += len(batch)
        flush_state["last_block"] = max(highest_block, flush_state["last_block"])
        flush_state["last_timestamp"] = max(
            (r.block_timestamp for r in batch if r.block_timestamp),
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

    records = await extract_pool_events(
        client=client,
        from_block=from_block,
        to_block=to_block_arg,
        network=network,
        markets=markets,
        flush_callback=on_flush,
    )

    has_data = bool(records) or flush_state["total_events"] > 0

    if not has_data:
        console.print(
            "\n[yellow]No PoolAmountUpdated events found — try a wider block range.[/yellow]"
        )
        if args.resume:
            save_checkpoint(
                checkpoint_path,
                last_block=to_block_arg or from_block,
                last_timestamp=int(time.time()),
                total_events=0,
                markets_seen=0,
            )
        return

    # Handle any remaining in-memory records (should be empty after flush)
    if records:
        console.print()
        save_raw_per_symbol(records, output_dir)
        save_daily_per_symbol(records, output_dir, token_decimals)

        if args.resume:
            last_block = max(r.block_number for r in records)
            last_timestamp = max(
                (r.block_timestamp for r in records if r.block_timestamp), default=0
            )
            save_checkpoint(
                checkpoint_path,
                last_block=last_block,
                last_timestamp=last_timestamp,
                total_events=_resume_base_total + flush_state["total_events"] + len(records),
                markets_seen=len({r.symbol for r in records}),
            )
    else:
        total = flush_state["total_events"]
        console.print(
            f"\n[green]  Extraction complete: {total:,} events saved across "
            f"{flush_state['flush_count']} flush batches.[/green]"
        )


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="GMX V2 PoolAmountUpdated extractor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full historical extraction from genesis
  poetry run python scripts/extract_pool_liquidity.py --from-block 120000000

  # Quick test on a small block range
  poetry run python scripts/extract_pool_liquidity.py \\
      --from-block 290000000 --to-block 290100000

  # Incremental (resume from checkpoint)
  poetry run python scripts/extract_pool_liquidity.py --resume

  # Avalanche network
  poetry run python scripts/extract_pool_liquidity.py --network avalanche --resume

  # Force refresh market registry cache
  poetry run python scripts/extract_pool_liquidity.py --resume --refresh-markets
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
        help="Start block (overrides FROM_BLOCK env var; default: genesis or checkpoint)",
    )
    parser.add_argument(
        "--to-block",
        type=int,
        default=None,
        help="End block (overrides TO_BLOCK env var; default: latest)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Base output directory (overrides GMX_POOL_OUTPUT_DIR; "
        "default: ./user_data/data/gmx/pool_liquidity)",
    )
    parser.add_argument(
        "--endpoint",
        type=str,
        default=None,
        help="HyperSync endpoint URL (overrides HYPERSYNC_ENDPOINT env var)",
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
