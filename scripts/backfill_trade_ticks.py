"""Backfill historical GMX trade ticks and the candle volume derived from them.

The daily snapshot only fills volume for the bars it collects, so every bar
older than the day this shipped still carries ``volume=0.0``. This script
walks the chain from GMX v2 genesis forward, writing the same per-day tick
tape and per-symbol volume files the daily phase writes, then stamping the
volume onto the candle feathers.

It is deliberately a separate one-off job rather than part of the daily
cron: the full range is roughly 380M Arbitrum blocks, far past the release
workflow's time budget.

The tick tape writer and the volume re-aggregator are the ones the daily
phase uses (:mod:`gmx_historical_data.candle_volume`), so both paths file a
fill under the same UTC day and compute a bar the same way.

Nothing before GMX v2 genesis (Aug 2023) can ever get volume -- candles run
back to 2021 on Chainlink oracle prices, but the fills that would size them
did not exist on this contract yet. Those bars keep ``volume=0.0``, which is
honest: an empty field beats a wrong one.

Usage::

    # Resume from the checkpoint (or genesis), scan 20M blocks, then stop
    poetry run python scripts/backfill_trade_ticks.py --max-blocks 20000000

    # Backfill one explicit window
    poetry run python scripts/backfill_trade_ticks.py \\
        --from-block 120000000 --to-block 140000000

    # Re-apply volume from tick files already on disk, without rescanning
    poetry run python scripts/backfill_trade_ticks.py --apply-only
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

from eth_defi.gmx.api import GMXAPI
from rich.console import Console
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeElapsedColumn

from gmx_historical_data.candle_volume import (
    CANDLE_TIMEFRAMES,
    apply_volume_from_tapes,
    read_tick_checkpoint,
    write_tick_checkpoint,
    write_tick_tapes,
)
from gmx_historical_data.config import GMX_V2_GENESIS_BLOCK
from gmx_historical_data.hypersync_client_factory import RotatingHypersyncClient
from gmx_historical_data.trade_tick_collector import (
    build_decoder_web3,
    build_market_map,
    chunk_ranges,
    collect_ticks_with_retry,
    fetch_token_map,
)

console = Console()

#: Blocks per HyperSync query. Large enough to keep round-trips down, small
#: enough that one failure loses little work.
CHUNK_BLOCKS = 250_000


async def _scan(
    start_block: int,
    end_block: int,
    ticks_dir: Path,
    checkpoint_path: Path,
    chain: str,
) -> tuple[int, set[str]]:
    """Scan a block range, writing tapes and advancing the checkpoint per chunk.

    :param start_block: First block, inclusive.
    :param end_block: Last block, inclusive.
    :param ticks_dir: Directory of per-day tape files.
    :param checkpoint_path: Checkpoint file to advance after each chunk.
    :param chain: GMX chain name.
    :returns: Tuple of (total ticks collected, dates touched).
    """
    rpc = (
        os.environ.get("ARBITRUM_RPC_URL")
        or os.environ.get("JSON_RPC_ARBITRUM")
        or os.environ.get("ARBITRUM_CHAIN_JSON_RPC")
    )
    web3 = build_decoder_web3(rpc)
    tokens = fetch_token_map(chain)
    markets = build_market_map(GMXAPI(chain=chain).get_markets_info().get("markets", []), tokens)
    console.print(f"  Mapped {len(markets)} markets, {len(tokens)} tokens")

    pool = RotatingHypersyncClient(
        os.environ.get("HYPERSYNC_API_TOKEN"), f"https://{chain}.hypersync.xyz"
    )

    chunks = chunk_ranges(start_block, end_block, CHUNK_BLOCKS)
    total = 0
    touched: set[str] = set()

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(f"Scanning {len(chunks)} chunks", total=len(chunks))
        for chunk_start, chunk_end in chunks:
            try:
                ticks, reached_block = await collect_ticks_with_retry(
                    pool, chunk_start, chunk_end, web3, markets, tokens
                )
            except Exception as e:  # noqa: BLE001 - keep the progress already made
                console.print(
                    f"  [yellow]Chunk {chunk_start}-{chunk_end} failed — {e}; "
                    f"stopping so the checkpoint stays honest[/yellow]"
                )
                break

            total += len(ticks)
            touched |= write_tick_tapes(ticks, ticks_dir)
            # Advance only to what was actually confirmed scanned, and only
            # after the tape is durably written -- a chunk that stalls
            # short of chunk_end (lagging archive, or an --to-block past
            # the real tip) must not be checkpointed as fully covered, or
            # the unscanned tail is silently skipped forever.
            write_tick_checkpoint(checkpoint_path, reached_block)
            progress.update(
                task, description=f"Scanning {reached_block:,} ({total:,} ticks)", advance=1
            )
            if reached_block < chunk_end:
                console.print(
                    f"  [yellow]Chunk {chunk_start}-{chunk_end} stalled at "
                    f"{reached_block:,}; stopping so the checkpoint stays honest[/yellow]"
                )
                break

    return total, touched


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Backfill historical GMX trade ticks and candle volume",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--output-dir", type=Path, default=Path("./user_data"))
    parser.add_argument(
        "--network",
        choices=["arbitrum"],
        default="arbitrum",
        help=(
            "GMX chain. Only arbitrum is supported -- trade_tick_collector "
            "hardcodes the Arbitrum EventEmitter address and this script's RPC "
            "env vars are Arbitrum-only, so another chain would silently scan "
            "against the wrong contract and produce an empty, checkpointed "
            "'success'."
        ),
    )
    parser.add_argument(
        "--from-block",
        type=int,
        default=None,
        help="First block to scan (default: checkpoint, else GMX v2 genesis).",
    )
    parser.add_argument(
        "--to-block", type=int, default=None, help="Last block to scan (default: chain tip)."
    )
    parser.add_argument(
        "--max-blocks",
        type=int,
        default=None,
        help="Stop after this many blocks, so a long backfill can run in sessions.",
    )
    parser.add_argument(
        "--apply-only",
        action="store_true",
        help="Skip scanning; rebuild candle volume from tick tapes already on disk.",
    )
    parser.add_argument(
        "--timeframes",
        nargs="*",
        default=None,
        help=f"Timeframes to fill (default: {' '.join(CANDLE_TIMEFRAMES)}).",
    )
    args = parser.parse_args()

    gmx = args.output_dir / "data" / "gmx"
    ticks_dir = gmx / "ticks"
    tick_volume_dir = gmx / "tick_volume"
    futures_dir = gmx / "futures"
    checkpoint_path = gmx / "checkpoints" / "trade_ticks_backfill.json"
    timeframes = args.timeframes or CANDLE_TIMEFRAMES

    console.print("\n[bold]GMX trade-tick backfill[/bold]")
    console.print(f"  Network:   {args.network}")
    console.print(f"  Ticks:     {ticks_dir}")
    console.print(f"  Futures:   {futures_dir}")

    if args.apply_only:
        console.print("\n[bold]Applying volume from stored tapes[/bold]")
        updated = apply_volume_from_tapes(ticks_dir, tick_volume_dir, futures_dir, timeframes)
        console.print(f"\n[green]Updated {updated} candle files.[/green]")
        return

    if not os.environ.get("HYPERSYNC_API_TOKEN"):
        console.print("[red]HYPERSYNC_API_TOKEN is required to scan.[/red]")
        sys.exit(2)

    start = args.from_block
    if start is None:
        checkpoint = read_tick_checkpoint(checkpoint_path)
        start = checkpoint + 1 if checkpoint is not None else GMX_V2_GENESIS_BLOCK
        console.print(
            f"  Resuming from {'checkpoint' if checkpoint else 'GMX v2 genesis'}: {start:,}"
        )

    client = RotatingHypersyncClient(
        os.environ.get("HYPERSYNC_API_TOKEN"), f"https://{args.network}.hypersync.xyz"
    ).client
    tip = asyncio.run(client.get_height())
    if args.to_block is not None and args.to_block > tip:
        console.print(
            f"  [yellow]--to-block {args.to_block:,} is past the current tip "
            f"{tip:,}; clamping[/yellow]"
        )
    end = min(args.to_block, tip) if args.to_block is not None else tip
    if args.max_blocks is not None:
        end = min(end, start + args.max_blocks - 1)

    if start > end:
        console.print("\n[green]Already current — nothing to scan.[/green]")
        return

    console.print(f"  Range:     {start:,} → {end:,} ({end - start + 1:,} blocks, tip {tip:,})\n")
    total, touched = asyncio.run(_scan(start, end, ticks_dir, checkpoint_path, args.network))
    console.print(f"\n  Collected {total:,} ticks across {len(touched)} days")

    console.print("\n[bold]Applying volume to candles[/bold]")
    updated = apply_volume_from_tapes(
        ticks_dir, tick_volume_dir, futures_dir, timeframes, dates=sorted(touched)
    )
    console.print(f"\n[green]Done — {total:,} ticks, {updated} candle files updated.[/green]")


if __name__ == "__main__":
    main()
