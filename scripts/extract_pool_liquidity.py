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

:envvar HYPERSYNC_ENDPOINT:
    Override HyperSync URL (default: ``https://arbitrum.hypersync.xyz``).
:envvar FROM_BLOCK:
    Start block (default: ``120000000`` — around GMX V2 Synthetics launch).
:envvar TO_BLOCK:
    End block (default: latest).
:envvar GMX_POOL_OUTPUT_DIR:
    Output directory (default: ``./user_data/data/gmx/pool_liquidity/arbitrum``).

Usage::

    poetry run python scripts/extract_pool_liquidity.py
    FROM_BLOCK=290000000 TO_BLOCK=291000000 poetry run python scripts/extract_pool_liquidity.py
"""

import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from eth_abi import decode as abi_decode
from eth_hash.auto import keccak
from eth_utils import to_hex
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
from web3 import Web3
from eth_defi.gmx.api import GMXAPI
from eth_defi.gmx.config import GMXConfig
from gmx_historical_data.oracle_price_collector import ArbitrumMockProvider

import hypersync
from hypersync import (
    BlockField,
    ClientConfig,
    FieldSelection,
    HypersyncClient,
    LogField,
    LogSelection,
    Query,
)

from extract_open_interest import (
    EVENT_LOG_DATA_ABI_TYPE,
    EVENTLOG1_ABI_TYPES,
    IDX_ADDRESS,
    IDX_INT,
    IDX_UINT,
    MARKETS,
    decode_event_log_data,
    _stream_with_retry,
)

console = Console()

# =============================================================================
# CONSTANTS
# =============================================================================

#: EventLog1 topic0 — same for all GMX V2 events
EVENT_LOG1_TOPIC = "0x137a44067c8961cd7e1d876f4754a5a3a75989b4552f1843fc69c3b372def160"

#: topic1 hash for PoolAmountUpdated
POOL_AMOUNT_UPDATED_HASH = to_hex(keccak(b"PoolAmountUpdated"))

#: GMX V2 EventEmitter (Arbitrum)
EVENT_EMITTER_ADDRESS = "0xC8ee91A54287DB53897056e12D9819156D3822Fb"

DEFAULT_FROM_BLOCK = 120_000_000
DEFAULT_HYPERSYNC_ENDPOINT = "https://arbitrum.hypersync.xyz"


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

PROGRESS_MILESTONES = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 95, 100]


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
# EXTRACTION
# =============================================================================


async def extract_pool_events(
    client: HypersyncClient,
    from_block: int,
    to_block: int | None,
) -> list[PoolAmountRecord]:
    """Stream ``PoolAmountUpdated`` events from HyperSync.

    :param client: HyperSync client instance.
    :param from_block: Starting block number.
    :param to_block: Ending block number (``None`` = chain head).
    :returns: List of :class:`PoolAmountRecord` instances.
    """
    if to_block is None:
        to_block = await client.get_height()
        console.print(f"  Latest block: [cyan]{to_block:,}[/cyan]")

    total_blocks = to_block - from_block

    query = Query(
        from_block=from_block,
        to_block=to_block,
        logs=[
            LogSelection(
                address=[EVENT_EMITTER_ADDRESS],
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

    console.print(f"  EventEmitter: [cyan]{EVENT_EMITTER_ADDRESS}[/cyan]")
    console.print(f"  Block range:  [cyan]{from_block:,}[/cyan] to [cyan]{to_block:,}[/cyan]")
    console.print(f"  Event:        [cyan]PoolAmountUpdated[/cyan]")

    records: list[PoolAmountRecord] = []
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

                market_info = MARKETS.get(market_addr.lower() if market_addr else "", {})
                symbol = market_info.get("symbol", market_addr or "UNKNOWN")

                timestamp = block_timestamps.get(log.block_number, 0)
                dt_str = datetime.fromtimestamp(timestamp, tz=UTC).isoformat() if timestamp else ""

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

            # Update progress
            if batch_highest > highest_block:
                advance = batch_highest - highest_block
                highest_block = batch_highest
                progress.advance(task, advance)

            elapsed = time.monotonic() - t_start
            rate = total_logs / elapsed if elapsed > 0 else 0
            progress.update(task, logs=total_logs, events=len(records), rate=rate, errors=decode_errors)

            if total_blocks > 0:
                pct = (highest_block - from_block) / total_blocks * 100
                while (
                    next_milestone_idx < len(PROGRESS_MILESTONES)
                    and pct >= PROGRESS_MILESTONES[next_milestone_idx]
                ):
                    console.print(
                        f"  [{PROGRESS_MILESTONES[next_milestone_idx]}%] "
                        f"block {highest_block:,} | {len(records):,} events | {rate:.0f} logs/s"
                    )
                    next_milestone_idx += 1

    elapsed = time.monotonic() - t_start
    console.print(
        f"\n  Done in {elapsed:.1f}s: {len(records):,} PoolAmountUpdated events "
        f"({decode_errors} decode errors)"
    )
    return records


# =============================================================================
# STORAGE
# =============================================================================


def records_to_df(records: list[PoolAmountRecord]) -> pd.DataFrame:
    """Convert raw records to a Parquet-ready DataFrame.

    Large integers are stored as strings to preserve precision.

    :param records: Collected :class:`PoolAmountRecord` instances.
    :returns: DataFrame with one row per event.
    """
    rows = [
        {
            "symbol": r.symbol,
            "market": r.market,
            "token": r.token,
            "delta": r.delta,
            "next_value": r.next_value,
            "block_number": r.block_number,
            "block_timestamp": r.block_timestamp,
            "block_datetime": r.block_datetime,
            "transaction_hash": r.transaction_hash,
            "log_index": r.log_index,
        }
        for r in records
    ]
    return pd.DataFrame(rows)


def build_daily_snapshot(df: pd.DataFrame, token_decimals: dict[str, int]) -> pd.DataFrame:
    """Build end-of-day pool token snapshots.

    Takes the last ``next_value`` per ``(date, symbol, token)`` within each
    UTC day, then converts raw token units to human-readable amounts using
    decimals fetched from the GMX API.

    :param df: Raw events DataFrame (must contain ``block_timestamp``,
        ``symbol``, ``token``, ``next_value``).
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
            decimals = 18  # safe fallback
        return row["next_value_int"] / (10**decimals)

    daily["pool_tokens"] = daily.apply(_convert, axis=1)
    if unknown:
        console.print(f"  [yellow]WARN: {len(unknown)} unknown token(s) — using 18-decimal fallback:[/yellow]")
        for addr in sorted(unknown):
            console.print(f"    {addr}")

    daily = daily[["date", "symbol", "token", "pool_tokens"]].copy()
    daily["date"] = pd.to_datetime(daily["date"]).dt.tz_convert("UTC")
    return daily.sort_values(["date", "symbol", "token"]).reset_index(drop=True)


# =============================================================================
# MAIN
# =============================================================================


async def main() -> None:
    """Async entry point — reads CLI args / env vars, collects events, saves Parquet."""
    import argparse

    parser = argparse.ArgumentParser(description="GMX V2 PoolAmountUpdated extractor")
    parser.add_argument("--from-block", type=int, default=None, help="Start block (overrides FROM_BLOCK env var)")
    parser.add_argument("--to-block", type=int, default=None, help="End block (overrides TO_BLOCK env var)")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output directory (overrides GMX_POOL_OUTPUT_DIR env var)")
    parser.add_argument("--endpoint", type=str, default=None, help="HyperSync endpoint URL (overrides HYPERSYNC_ENDPOINT env var)")
    args = parser.parse_args()

    endpoint = args.endpoint or os.environ.get("HYPERSYNC_ENDPOINT", DEFAULT_HYPERSYNC_ENDPOINT)
    from_block = args.from_block if args.from_block is not None else int(os.environ.get("FROM_BLOCK", str(DEFAULT_FROM_BLOCK)))
    to_block: int | None = args.to_block if args.to_block is not None else (int(os.environ.get("TO_BLOCK")) if os.environ.get("TO_BLOCK") else None)
    output_dir = args.output_dir or Path(os.environ.get("GMX_POOL_OUTPUT_DIR", "./user_data/data/gmx/pool_liquidity/arbitrum"))

    console.print(
        Panel(
            f"Endpoint:    [cyan]{endpoint}[/cyan]\n"
            f"Block range: [cyan]{from_block:,}[/cyan] to "
            f"[cyan]{to_block or 'latest'}[/cyan]\n"
            f"Output dir:  [cyan]{output_dir}[/cyan]",
            title="GMX V2 Pool Liquidity Extractor",
            border_style="blue",
        )
    )

    token_decimals = fetch_token_decimals()

    api_token = os.environ.get("HYPERSYNC_API_TOKEN")
    client = HypersyncClient(ClientConfig(url=endpoint, bearer_token=api_token))

    records = await extract_pool_events(client, from_block, to_block)

    if not records:
        console.print("\n[yellow]No PoolAmountUpdated events found — try a wider block range.[/yellow]")
        return

    raw_df = records_to_df(records)
    daily_df = build_daily_snapshot(raw_df, token_decimals)

    output_dir.mkdir(parents=True, exist_ok=True)

    raw_path = output_dir / "pool_liquidity_raw.parquet"
    raw_df.to_parquet(raw_path, index=False)
    console.print(f"\n  Raw events:     [cyan]{len(raw_df):,}[/cyan] rows → [green]{raw_path}[/green]")

    daily_path = output_dir / "pool_liquidity_daily.parquet"
    daily_df.to_parquet(daily_path, index=False)
    console.print(f"  Daily snapshot: [cyan]{len(daily_df):,}[/cyan] rows → [green]{daily_path}[/green]")

    # Summary table
    if not daily_df.empty:
        avg_pool = (
            daily_df.groupby(["symbol", "token"])["pool_tokens"]
            .mean()
            .reset_index()
            .sort_values("pool_tokens", ascending=False)
            .head(10)
        )
        table = Table(title="Top 10 Pool Balances (avg daily tokens)", show_lines=False)
        table.add_column("Symbol", style="cyan")
        table.add_column("Token", style="green")
        table.add_column("Avg Pool Tokens", justify="right")
        for _, row in avg_pool.iterrows():
            table.add_row(row["symbol"], row["token"], f"{row['pool_tokens']:,.4f}")
        console.print()
        console.print(table)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
