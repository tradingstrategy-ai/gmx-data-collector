#!/usr/bin/env python3
"""
Unified GMX V2 Funding Rate Extractor
======================================
Orchestrates three extraction phases and merges into a single
direction-corrected hourly funding rate parquet per symbol.

Phases:
    1. **DataStore** (opt-in, slow): Reads ``savedFundingFactorPerSecond``
       from the DataStore contract via archive RPC. Covers pre-V2.2 period
       (block 120M to 370M, Nov 2023 - Aug 2025). Already signed.
    2. **Funding Factor** (default): Extracts ``Funding`` events via HyperSync.
       Covers V2.2+ (block 370M+, Aug 2025 - present). Unsigned magnitude.
    3. **Direction** (default): Extracts ``FundingFeeAmountPerSizeUpdated``
       events via HyperSync. Determines which side pays (longs vs shorts)
       by comparing delta sums. Covers full V2 history.
    4. **Merge**: Combines all sources into a single ``1h.parquet`` per symbol
       with correct direction applied to V2.2+ data.

QUICK START
-----------
    # Default: HyperSync phases + merge (fast)
    poetry run python scripts/extract_unified_funding.py

    # Include DataStore phase (slow, requires archive RPC)
    export JSON_RPC_ARBITRUM=<archive-node-url>
    poetry run python scripts/extract_unified_funding.py --include-datastore

    # Via Makefile
    make funding-unified
    make funding-unified INCLUDE_DATASTORE=1

USAGE
-----
    poetry run python scripts/extract_unified_funding.py [OPTIONS]

OPTIONS
-------
    --network            Network: "arbitrum" or "avalanche" (default: arbitrum)
    --output-dir         Base output directory (default: ./data/funding)
    --market             Filter by market symbol (e.g., "ETH/USD")
    --include-datastore  Include Phase 1 DataStore extraction (~200 batched HTTP requests, requires archive node)
    --skip-direction     Skip Phase 3 direction detection
    --merge-only         Only run merge step, skip all extraction
    --resume             Resume each phase from its checkpoint
    --checkpoint-dir     Override checkpoint directory
    --output             Merged output format: "parquet" or "feather" (default: parquet)
    --feather-dir        Export unified rates as FreqTrade feather files
    --list-markets       List available markets and exit
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
from rich.console import Console

from gmx_historical_data.atomic_parquet import atomic_write_parquet

console = Console()


# =============================================================================
# CONSTANTS
# =============================================================================

SCRIPT_DIR = Path(__file__).parent

# V2.2 cutoff block — Funding event introduced around this block
GMX_V22_CUTOFF_BLOCK = 370_000_000


# =============================================================================
# RESUME / EXISTING DATA CHECKS
# =============================================================================


def _read_checkpoint(path: Path) -> dict | None:
    """Read a JSON checkpoint file.

    :param path: Path to checkpoint JSON.
    :returns: Checkpoint dict, or ``None`` if missing/invalid.
    """
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def check_phase_status(
    network_dir: Path,
    checkpoint_dir: Path | None = None,
) -> dict:
    """Check the status of existing data and checkpoints for each phase.

    Returns a dict describing what data exists and what's missing.

    :param network_dir: Network-specific directory (e.g., ``data/funding/arbitrum``).
    :param checkpoint_dir: Override checkpoint directory.
    :returns: Status dict with per-phase information.
    """
    cp_dir = checkpoint_dir or (network_dir / "checkpoints")
    rates_dir = network_dir / "rates"
    direction_dir = network_dir / "direction"

    # Phase 1: DataStore
    ds_cp = _read_checkpoint(cp_dir / "funding_datastore_checkpoint.json")
    ds_symbols = set()
    if rates_dir.exists():
        for sym_dir in rates_dir.iterdir():
            if sym_dir.is_dir() and (sym_dir / "1h_datastore.parquet").exists():
                ds_symbols.add(sym_dir.name)

    # Phase 2: Funding Factor
    factor_cp = _read_checkpoint(cp_dir / "funding_factor_checkpoint.json")
    factor_symbols = set()
    if rates_dir.exists():
        for sym_dir in rates_dir.iterdir():
            if sym_dir.is_dir() and (
                (sym_dir / "1h_factor.parquet").exists() or (sym_dir / "1h.parquet").exists()
            ):
                factor_symbols.add(sym_dir.name)

    # Phase 3: Direction
    fps_cp = _read_checkpoint(cp_dir / "fee_per_size_checkpoint.json")
    direction_symbols = set()
    if direction_dir.exists():
        for sym_dir in direction_dir.iterdir():
            if sym_dir.is_dir() and (sym_dir / "1h.parquet").exists():
                direction_symbols.add(sym_dir.name)

    # Phase 4: Unified
    unified_symbols = set()
    if rates_dir.exists():
        for sym_dir in rates_dir.iterdir():
            if sym_dir.is_dir() and (sym_dir / "1h.parquet").exists():
                unified_symbols.add(sym_dir.name)

    return {
        "datastore": {
            "checkpoint": ds_cp,
            "last_block": ds_cp.get("last_block") if ds_cp else None,
            "symbols": ds_symbols,
            "complete": ds_cp is not None and ds_cp.get("last_block", 0) >= GMX_V22_CUTOFF_BLOCK,
        },
        "factor": {
            "checkpoint": factor_cp,
            "last_block": factor_cp.get("last_block") if factor_cp else None,
            "symbols": factor_symbols,
            "has_data": len(factor_symbols) > 0,
        },
        "direction": {
            "checkpoint": fps_cp,
            "last_block": fps_cp.get("last_block") if fps_cp else None,
            "symbols": direction_symbols,
            "has_data": len(direction_symbols) > 0,
        },
        "unified": {
            "symbols": unified_symbols,
        },
    }


def print_resume_status(status: dict) -> None:
    """Print a summary of existing data before resuming.

    :param status: Status dict from :func:`check_phase_status`.
    """
    console.print("\n  [bold]Existing data check:[/bold]")

    # DataStore
    ds = status["datastore"]
    if ds["complete"]:
        console.print(
            f"    DataStore:  [green]complete[/green] "
            f"(block {ds['last_block']:,}, {len(ds['symbols'])} symbols)"
        )
    elif ds["last_block"]:
        console.print(
            f"    DataStore:  [yellow]partial[/yellow] "
            f"(block {ds['last_block']:,}, {len(ds['symbols'])} symbols)"
        )
    else:
        console.print("    DataStore:  [dim]no data[/dim]")

    # Factor
    fac = status["factor"]
    if fac["last_block"]:
        console.print(
            f"    Factor:     [green]has data[/green] "
            f"(block {fac['last_block']:,}, {len(fac['symbols'])} symbols)"
        )
    else:
        console.print("    Factor:     [dim]no data[/dim]")

    # Direction
    dir_s = status["direction"]
    if dir_s["last_block"]:
        console.print(
            f"    Direction:  [green]has data[/green] "
            f"(block {dir_s['last_block']:,}, {len(dir_s['symbols'])} symbols)"
        )
    else:
        console.print("    Direction:  [dim]no data[/dim]")

    # Unified
    uni = status["unified"]
    if uni["symbols"]:
        console.print(f"    Unified:    [green]{len(uni['symbols'])} symbols[/green] merged")
    else:
        console.print("    Unified:    [dim]not yet merged[/dim]")

    console.print()


# =============================================================================
# SUBPROCESS RUNNER
# =============================================================================


PHASE_MAX_RETRIES = 3
PHASE_RETRY_DELAY = 10  # seconds


def run_phase(phase_name: str, cmd: list[str]) -> None:
    """Run an extraction phase as a subprocess with retries.

    Streams stdout/stderr to the console in real time.
    Retries up to :data:`PHASE_MAX_RETRIES` times on failure (e.g., HyperSync
    500 errors that exhaust the script's internal retries).
    Aborts if all retries are exhausted.

    :param phase_name: Human-readable phase label for display.
    :param cmd: Command and arguments to execute.
    :raises SystemExit: If the subprocess fails after all retries.
    """
    console.print(f"\n{'=' * 70}")
    console.print(f"  {phase_name}")
    console.print(f"{'=' * 70}")
    # Show a shortened command (script name + key args, skip python path)
    script_name = Path(cmd[1]).name if len(cmd) > 1 else cmd[0]
    short_args = " ".join(cmd[2:])
    console.print(f"  Script: {script_name} {short_args}")
    console.print(
        "  [dim](intermediate output always parquet; final format applied at merge)[/dim]\n"
    )

    for attempt in range(1, PHASE_MAX_RETRIES + 1):
        t_start = time.monotonic()
        result = subprocess.run(cmd)
        elapsed = time.monotonic() - t_start

        if result.returncode == 0:
            console.print(f"\n  {phase_name} completed in {elapsed:.1f}s")
            return

        if attempt < PHASE_MAX_RETRIES:
            console.print(
                f"\n  [yellow]{phase_name} failed (attempt {attempt}/"
                f"{PHASE_MAX_RETRIES}, exit code {result.returncode}). "
                f"Retrying in {PHASE_RETRY_DELAY}s...[/yellow]"
            )
            time.sleep(PHASE_RETRY_DELAY)
        else:
            console.print(
                f"\n  [red]ERROR: {phase_name} failed after "
                f"{PHASE_MAX_RETRIES} attempts (exit code "
                f"{result.returncode})[/red]"
            )
            sys.exit(result.returncode)


# =============================================================================
# COMMAND BUILDERS
# =============================================================================


def build_datastore_cmd(args: argparse.Namespace) -> list[str]:
    """Build the command for the DataStore extraction phase.

    :param args: Parsed CLI arguments.
    :returns: Command list for subprocess.run().
    """
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "extract_funding_datastore.py"),
        "--output-dir",
        args.output_dir,
        "--output",
        "parquet",
    ]
    if args.market:
        cmd.extend(["--market", args.market])
    if args.resume:
        cmd.append("--resume")
    if args.refresh_markets:
        cmd.append("--refresh-markets")
    return cmd


def build_factor_cmd(args: argparse.Namespace) -> list[str]:
    """Build the command for the Funding Factor HyperSync phase.

    :param args: Parsed CLI arguments.
    :returns: Command list for subprocess.run().
    """
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "extract_funding_factor.py"),
        "--network",
        args.network,
        "--output-dir",
        args.output_dir,
        "--output",
        "parquet",
    ]
    if args.market:
        cmd.extend(["--market", args.market])
    if args.resume:
        cmd.append("--resume")
    if args.checkpoint_dir:
        cmd.extend(["--checkpoint-dir", args.checkpoint_dir])
    if args.refresh_markets:
        cmd.append("--refresh-markets")
    return cmd


def build_direction_cmd(args: argparse.Namespace) -> list[str]:
    """Build the command for the Direction detection HyperSync phase.

    :param args: Parsed CLI arguments.
    :returns: Command list for subprocess.run().
    """
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "extract_funding_fee_per_size.py"),
        "--network",
        args.network,
        "--output-dir",
        args.output_dir,
        "--output",
        "parquet",
    ]
    if args.market:
        cmd.extend(["--market", args.market])
    if args.resume:
        cmd.append("--resume")
    if args.checkpoint_dir:
        cmd.extend(["--checkpoint-dir", args.checkpoint_dir])
    if args.refresh_markets:
        cmd.append("--refresh-markets")
    return cmd


# =============================================================================
# POST-PHASE RENAME
# =============================================================================


def rename_factor_outputs(network_dir: Path) -> None:
    """Rename 1h.parquet -> 1h_factor.parquet to avoid collision with merge output.

    The ``extract_funding_factor.py`` script writes to ``rates/{SYM}/1h.parquet``.
    The merge step also writes to ``rates/{SYM}/1h.parquet`` as the canonical
    unified output. This function renames the factor output to a distinct
    filename before the merge.

    :param network_dir: Network-specific directory (e.g., ``data/funding/arbitrum``).
    """
    rates_dir = network_dir / "rates"
    if not rates_dir.exists():
        return
    renamed = 0
    for sym_dir in rates_dir.iterdir():
        if not sym_dir.is_dir():
            continue
        src = sym_dir / "1h.parquet"
        dst = sym_dir / "1h_factor.parquet"
        if src.exists():
            # Always overwrite: the factor script may have appended new data
            if dst.exists():
                dst.unlink()
            src.rename(dst)
            renamed += 1
    if renamed:
        console.print(f"  Renamed {renamed} factor output(s) to 1h_factor.parquet")


# =============================================================================
# MERGE LOGIC
# =============================================================================


def market_symbol_from_filter(market_filter: str) -> str:
    """Convert a market filter string like ``'ETH/USD'`` to a symbol like ``'ETH'``.

    :param market_filter: Market symbol filter (e.g., ``'ETH/USD'``).
    :returns: Base token symbol.
    """
    return market_filter.split("/")[0].strip()


def apply_direction_to_rates(
    rates: pl.DataFrame,
    direction: pl.DataFrame | None,
) -> pl.DataFrame:
    """Attach direction (``longs_pay_shorts``) to unsigned hourly rates.

    Direction observations from ``FundingFeeAmountPerSizeUpdated`` events are
    sparse — there may be no observation in many hours. We join by timestamp,
    then **forward-fill** within ``(symbol, market)`` so each hour inherits the
    last observed direction until the next event flips it.

    Hours preceding the first direction observation keep ``longs_pay_shorts``
    as ``null`` rather than silently defaulting to ``True``. Signed-fee
    columns (``funding_fee_long``/``funding_fee_short``) are also ``null``
    for those rows.

    :param rates: Hourly rate frame with at least ``timestamp``, ``symbol``,
        ``market``, ``funding_rate``.
    :param direction: Hourly direction frame with ``timestamp``,
        ``longs_pay_shorts``. Pass ``None`` when no direction data exists;
        all rows then receive ``null`` direction.
    :returns: ``rates`` augmented with ``longs_pay_shorts``,
        ``funding_fee_long``, ``funding_fee_short`` columns.
    """
    if direction is None or direction.is_empty():
        return rates.with_columns(
            pl.lit(None, dtype=pl.Boolean).alias("longs_pay_shorts"),
            pl.lit(None, dtype=pl.Float64).alias("funding_fee_long"),
            pl.lit(None, dtype=pl.Float64).alias("funding_fee_short"),
        )

    dir_df = direction.select(["timestamp", "longs_pay_shorts"]).rename(
        {"longs_pay_shorts": "direction_longs_pay"}
    )

    # Drop any pre-existing longs_pay_shorts (e.g. from legacy parquet).
    if "longs_pay_shorts" in rates.columns:
        rates = rates.drop("longs_pay_shorts")

    # Group-aware forward fill: sort by (symbol, market, timestamp), then
    # forward-fill direction within each (symbol, market). Leading nulls
    # (no prior observation) stay null.
    has_market = "market" in rates.columns
    group_keys = ["symbol", "market"] if has_market else ["symbol"]
    sort_keys = [*group_keys, "timestamp"]

    merged = (
        rates.join(dir_df, on="timestamp", how="left")
        .sort(sort_keys)
        .with_columns(
            pl.col("direction_longs_pay")
            .forward_fill()
            .over(group_keys)
            .alias("longs_pay_shorts")
        )
        .drop("direction_longs_pay")
    )

    # Signed-fee derivation: only when direction is known. Null direction
    # propagates to null signed fees via the otherwise-null branch.
    merged = merged.with_columns(
        pl.when(pl.col("longs_pay_shorts").is_null())
        .then(None)
        .when(pl.col("longs_pay_shorts"))
        .then(pl.col("funding_rate") * 3600)
        .otherwise(pl.col("funding_rate") * -3600)
        .alias("funding_fee_long"),
        pl.when(pl.col("longs_pay_shorts").is_null())
        .then(None)
        .when(pl.col("longs_pay_shorts"))
        .then(pl.col("funding_rate") * -3600)
        .otherwise(pl.col("funding_rate") * 3600)
        .alias("funding_fee_short"),
    )

    return merged


def forward_fill_hourly_grid(rates: pl.DataFrame) -> pl.DataFrame:
    """Expand event-based hourly rates onto a contiguous hourly grid.

    GMX emits ``Funding`` events only on position updates, so low-volume
    markets have silent hours. The on-chain ``fundingFactorPerSecond`` is a
    contract state variable that persists between events — a silent hour
    means the rate is unchanged since the most recent prior event, not
    that the rate is unknown.

    Builds a continuous hourly grid spanning ``[min(timestamp), max(timestamp)]``
    and forward-fills numeric and identifier columns. Filled rows are flagged
    via ``is_gap_filled = True`` and ``update_count = 0``. The grid never
    extends past the last observed event (no backfill, no zombie-fill).

    :param rates: Hourly rate frame from ``aggregate_hourly_rates`` or
        ``rates/{SYM}/1h_factor.parquet``. Must contain at least
        ``timestamp`` and ``update_count``.
    :returns: Contiguous hourly frame with ``is_gap_filled`` column added.
        Empty input returns an empty frame (schema preserved).
    """
    if rates.is_empty():
        return rates.with_columns(pl.lit(False).alias("is_gap_filled"))

    start = rates["timestamp"].min()
    end = rates["timestamp"].max()
    ts_dtype = rates["timestamp"].dtype
    grid = pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                start, end, interval="1h", time_zone="UTC", eager=True
            ).cast(ts_dtype)
        }
    )

    # Left-join factor rows onto the grid; unmatched grid hours get nulls.
    merged = grid.join(rates, on="timestamp", how="left")

    # Flag filled rows BEFORE forward-fill (otherwise we lose the signal).
    merged = merged.with_columns(
        pl.col("update_count").is_null().alias("is_gap_filled"),
    )

    # Forward-fill rate columns and identifiers. update_count is filled with 0
    # (not forward-filled — a filled hour has zero events by definition).
    ff_cols = [
        "funding_rate",
        "funding_rate_min",
        "funding_rate_max",
        "funding_rate_hourly",
        "funding_rate_annualized",
        "symbol",
        "market",
    ]
    merged = merged.with_columns(
        [pl.col(c).forward_fill() for c in ff_cols if c in merged.columns]
    )
    merged = merged.with_columns(
        pl.col("update_count").fill_null(0).cast(pl.UInt32),
    )

    return merged.sort("timestamp")


def merge_symbol(
    symbol: str,
    rates_dir: Path,
    direction_dir: Path,
    output_format: str = "parquet",
) -> int | None:
    """Merge data for a single symbol into unified hourly file.

    Combines DataStore (pre-V2.2, signed) and HyperSync Factor (V2.2+, unsigned)
    data, applying direction correction from fee-per-size delta analysis.

    :param symbol: Symbol string (e.g., ``'ETH'``).
    :param rates_dir: Path to ``rates/`` directory.
    :param direction_dir: Path to ``direction/`` directory.
    :param output_format: Output format: ``'parquet'`` or ``'feather'``.
    :returns: Number of rows in the unified output, or ``None`` if no data.
    """
    frames = []

    # --- Source 1: DataStore (pre-V2.2, signed, direction built-in) ---
    datastore_path = rates_dir / symbol / "1h_datastore.parquet"
    if datastore_path.exists():
        ds_df = pl.read_parquet(datastore_path)
        ds_df = ds_df.with_columns(pl.lit("datastore").alias("source"))
        frames.append(ds_df)

    # --- Source 2: HyperSync Funding Factor (V2.2+, unsigned) ---
    factor_path = rates_dir / symbol / "1h_factor.parquet"
    if factor_path.exists():
        hs_df = pl.read_parquet(factor_path)
        hs_df = forward_fill_hourly_grid(hs_df)

        # --- Source 3: Direction correction (forward-filled, see helper) ---
        direction_path = direction_dir / symbol / "1h.parquet"
        dir_df = pl.read_parquet(direction_path) if direction_path.exists() else None
        hs_df = apply_direction_to_rates(hs_df, dir_df)

        hs_df = hs_df.with_columns(pl.lit("hypersync").alias("source"))
        frames.append(hs_df)

    if not frames:
        return None

    # Concatenate with schema alignment
    unified = pl.concat(frames, how="diagonal_relaxed")

    # DataStore rows predate is_gap_filled; they are real hourly RPC reads, never filled.
    unified = unified.with_columns(pl.col("is_gap_filled").fill_null(False))

    # Deduplicate by timestamp (prefer later source — hypersync appended second)
    unified = unified.unique(subset=["timestamp"], keep="last")
    unified = unified.sort("timestamp")

    # Write unified output
    ext = "feather" if output_format == "feather" else "parquet"
    unified_path = rates_dir / symbol / f"1h.{ext}"
    unified_path.parent.mkdir(parents=True, exist_ok=True)
    if output_format == "feather":
        unified.write_ipc(unified_path, compression="zstd")
    else:
        atomic_write_parquet(unified, unified_path)

    return len(unified)


def merge_unified_rates(
    network_dir: Path,
    market_filter: str | None = None,
    output_format: str = "parquet",
) -> None:
    """Merge DataStore, HyperSync Factor, and Direction data into unified rates.

    For each symbol:

    1. Read ``rates/{SYM}/1h_datastore.parquet`` (pre-V2.2, already signed)
    2. Read ``rates/{SYM}/1h_factor.parquet`` (V2.2+, unsigned)
    3. Read ``direction/{SYM}/1h.parquet`` (direction from fee-per-size)
    4. Correct V2.2+ direction using fee-per-size data
    5. Write to ``rates/{SYM}/1h.{format}``

    :param network_dir: Network-specific directory (e.g., ``data/funding/arbitrum``).
    :param market_filter: Optional symbol filter (e.g., ``'ETH/USD'``).
    :param output_format: Output format: ``'parquet'`` or ``'feather'``.
    """
    console.print(f"\n{'=' * 70}")
    console.print("  Phase 4: Merge Unified Rates")
    console.print(f"{'=' * 70}")

    rates_dir = network_dir / "rates"
    direction_dir = network_dir / "direction"

    if not rates_dir.exists():
        console.print("  [yellow]No rates directory found, nothing to merge[/yellow]")
        return

    # Discover all symbols
    symbols = set()
    for d in rates_dir.iterdir():
        if d.is_dir():
            symbols.add(d.name)

    if market_filter:
        filter_sym = market_symbol_from_filter(market_filter)
        symbols = {s for s in symbols if s == filter_sym}

    if not symbols:
        console.print("  [yellow]No symbols found to merge[/yellow]")
        return

    console.print(f"  Merging {len(symbols)} symbol(s)...\n")

    ext = "feather" if output_format == "feather" else "parquet"
    total_rows = 0
    merged_count = 0
    for symbol in sorted(symbols):
        rows = merge_symbol(symbol, rates_dir, direction_dir, output_format)
        if rows is not None:
            total_rows += rows
            merged_count += 1
            console.print(
                f"  [green]{symbol:<12}[/green] {rows:>8,} hours -> rates/{symbol}/1h.{ext}"
            )

    console.print(f"\n  Merge complete: {merged_count} symbols, {total_rows:,} total hours")


# =============================================================================
# FEATHER EXPORT
# =============================================================================


def export_feather(
    network_dir: Path,
    feather_dir: Path,
    market_filter: str | None = None,
    quote_currency: str = "USDC",
) -> None:
    """Export unified rates as FreqTrade-compatible feather files.

    FreqTrade expects OHLCV format with ``open`` = hourly funding rate.
    File naming follows CCXT convention:
    ``{BASE}_{QUOTE}_{SETTLE}-1h-funding_rate.feather``

    :param network_dir: Network-specific directory (e.g., ``data/funding/arbitrum``).
    :param feather_dir: Output directory for feather files.
    :param market_filter: Optional symbol filter (e.g., ``'ETH/USD'``).
    :param quote_currency: Quote/settlement currency (default: ``'USDC'``).
    """
    console.print(f"\n{'=' * 70}")
    console.print("  Feather Export (FreqTrade format)")
    console.print(f"{'=' * 70}")

    rates_dir = network_dir / "rates"
    if not rates_dir.exists():
        console.print("  [yellow]No rates directory found[/yellow]")
        return

    gmx_dir = feather_dir / "data" / "gmx" / "futures"
    gmx_dir.mkdir(parents=True, exist_ok=True)

    symbols: dict[str, Path] = {}
    for d in rates_dir.iterdir():
        if not d.is_dir():
            continue
        if (d / "1h.feather").exists():
            symbols[d.name] = d / "1h.feather"
        elif (d / "1h.parquet").exists():
            symbols[d.name] = d / "1h.parquet"

    if market_filter:
        filter_sym = market_symbol_from_filter(market_filter)
        symbols = {s: p for s, p in symbols.items() if s == filter_sym}

    exported = 0
    for symbol in sorted(symbols):
        unified_path = symbols[symbol]
        df = (
            pl.read_ipc(unified_path)
            if unified_path.suffix == ".feather"
            else pl.read_parquet(unified_path)
        )
        if df.is_empty():
            continue

        # open = per-settlement funding rate (per 1h for GMX continuous accrual)
        #      = funding_rate_per_second × 3600 (same unit as Hyperliquid 1h settlement).
        rate_expr = (
            pl.col("funding_rate_hourly")
            if "funding_rate_hourly" in df.columns
            else pl.col("funding_rate") * 3600
        )

        result = (
            df.select(
                pl.col("timestamp").cast(pl.Datetime("ns", "UTC")).alias("date"),
                rate_expr.cast(pl.Float64).alias("open"),
            )
            .with_columns(
                pl.lit(0.0).alias("high"),
                pl.lit(0.0).alias("low"),
                pl.lit(0.0).alias("close"),
                pl.lit(0.0).alias("volume"),
            )
            .select(["date", "open", "high", "low", "close", "volume"])
            .sort("date")
            .unique(subset=["date"], keep="first", maintain_order=True)
            .drop_nulls(subset=["open"])
        )

        filename = f"{symbol}_{quote_currency}_{quote_currency}-1h-funding_rate.feather"
        filepath = gmx_dir / filename
        result.write_ipc(filepath, compression="zstd")
        exported += 1
        console.print(f"  [green]{symbol:<12}[/green] {len(result):>8,} hours -> {filepath}")

    console.print(f"\n  Exported {exported} feather file(s) to {gmx_dir}")


# =============================================================================
# CLI
# =============================================================================


def build_parser() -> argparse.ArgumentParser:
    """Build argparse parser for the unified funding rate extractor.

    :returns: Configured ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Unified GMX V2 Funding Rate Extractor - combines DataStore, "
            "HyperSync Funding Factor, and Direction data"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: HyperSync phases + merge (fast)
  poetry run python scripts/extract_unified_funding.py

  # Include DataStore phase (slow, requires archive RPC)
  export JSON_RPC_ARBITRUM=<archive-node-url>
  poetry run python scripts/extract_unified_funding.py --include-datastore

  # Single market
  poetry run python scripts/extract_unified_funding.py --market ETH/USD

  # Incremental update (resume from checkpoints)
  poetry run python scripts/extract_unified_funding.py --resume

  # Only run merge (all raw data already extracted)
  poetry run python scripts/extract_unified_funding.py --merge-only
        """,
    )

    parser.add_argument(
        "--network",
        choices=["arbitrum", "avalanche"],
        default="arbitrum",
        help="Network (default: arbitrum)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./data/funding",
        help="Base output directory (default: ./data/funding)",
    )
    parser.add_argument(
        "--output",
        choices=["parquet", "feather"],
        default="parquet",
        help="Output format for merged unified rates (default: parquet)",
    )
    parser.add_argument(
        "--market",
        type=str,
        default=None,
        help="Filter by market symbol (e.g., 'ETH/USD')",
    )

    # Phase control
    parser.add_argument(
        "--include-datastore",
        action="store_true",
        help=(
            "Include DataStore phase (pre-V2.2, Nov 2023 - Aug 2025). "
            "~200 batched HTTP requests, requires JSON_RPC_ARBITRUM archive node. "
            "Skipped by default."
        ),
    )
    parser.add_argument(
        "--skip-direction",
        action="store_true",
        help="Skip direction detection phase",
    )
    parser.add_argument(
        "--merge-only",
        action="store_true",
        help="Only merge existing data, skip all extraction phases",
    )

    # Resume / checkpoint
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume each phase from its checkpoint",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=str,
        default=None,
        help="Override checkpoint directory",
    )

    # Feather export
    parser.add_argument(
        "--feather-dir",
        type=str,
        default=None,
        help=(
            "Export unified rates as FreqTrade feather files to this directory. "
            "Creates {dir}/data/gmx/futures/{SYM}_USDC_USDC-1h-funding_rate.feather"
        ),
    )

    # Live funding rate fill
    parser.add_argument(
        "--fill-live",
        action=argparse.BooleanOptionalAction,
        default=None,
        help=(
            "Append current live funding rate from GMX API after feather export. "
            "Auto-enabled when --feather-dir is provided. "
            "Disable with --no-fill-live."
        ),
    )

    # Misc
    parser.add_argument(
        "--list-markets",
        action="store_true",
        help="List available markets and exit",
    )
    parser.add_argument(
        "--refresh-markets",
        action="store_true",
        help="Force re-fetch of GMX market registry (bypass 24h cache)",
    )

    return parser


# =============================================================================
# MAIN
# =============================================================================


def main():
    """CLI entry point for unified funding rate extraction."""
    args = build_parser().parse_args()

    if args.list_markets:
        # Delegate to the datastore script which has the most complete list
        subprocess.run(
            [
                sys.executable,
                str(SCRIPT_DIR / "extract_funding_datastore.py"),
                "--list-markets",
            ]
        )
        return

    network_dir = Path(args.output_dir) / args.network
    cp_dir = Path(args.checkpoint_dir) if args.checkpoint_dir else None
    t_start = time.monotonic()

    console.print("\n" + "=" * 70)
    console.print("  UNIFIED GMX V2 FUNDING RATE EXTRACTION")
    console.print("=" * 70)

    phases = []
    if not args.merge_only:
        if args.include_datastore:
            phases.append("DataStore (RPC)")
        phases.append("Funding Factor (HyperSync)")
        if not args.skip_direction:
            phases.append("Direction (HyperSync)")
    phases.append("Merge")
    if args.feather_dir:
        phases.append("Feather Export")

    console.print(f"  Network:  {args.network}")
    console.print(f"  Output:   {args.output_dir}")
    console.print(f"  Format:   {args.output}")
    console.print(f"  Phases:   {' -> '.join(phases)}")
    if args.market:
        console.print(f"  Market:   {args.market}")
    if args.feather_dir:
        console.print(f"  Feather:  {args.feather_dir}")
    if args.resume:
        console.print("  Mode:     incremental (resume from checkpoints)")

    # Show existing data status when resuming
    if args.resume and not args.merge_only:
        status = check_phase_status(network_dir, cp_dir)
        print_resume_status(status)

    # Phase 1: DataStore (sync, archive RPC) - opt-in
    if args.include_datastore and not args.merge_only:
        run_phase("Phase 1: DataStore (Archive RPC)", build_datastore_cmd(args))

    # Phase 2: Funding Factor (async, HyperSync) - V2.2+
    if not args.merge_only:
        run_phase("Phase 2: Funding Factor (HyperSync)", build_factor_cmd(args))

        # Rename 1h.parquet -> 1h_factor.parquet before merge
        rename_factor_outputs(network_dir)

    # Phase 3: Direction (async, HyperSync) - full range
    if not args.skip_direction and not args.merge_only:
        run_phase("Phase 3: Direction (HyperSync)", build_direction_cmd(args))

    # Phase 4: Merge all sources into unified rates
    merge_unified_rates(network_dir, args.market, args.output)

    # Phase 5: Feather export (optional)
    if args.feather_dir:
        export_feather(network_dir, Path(args.feather_dir), args.market)

    # Phase 5b: Live funding rate appender (auto-enabled with --feather-dir)
    fill_live = args.fill_live
    if fill_live is None:
        fill_live = args.feather_dir is not None
    if fill_live and args.feather_dir:
        try:
            from gmx_historical_data.live_funding import (
                fetch_live_funding_rates,
                upsert_live_rates_to_feather,
            )

            console.print(f"\n{'=' * 70}")
            console.print("  Phase 5b: Live Funding Rate Appender (GMX API)")
            console.print(f"{'=' * 70}")
            rates = fetch_live_funding_rates()
            updated = upsert_live_rates_to_feather(Path(args.feather_dir), rates, args.market)
            ts_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
            console.print(
                f"  [green]Live rate appended for {updated} symbols (timestamp: {ts_str})[/green]"
            )
        except Exception as exc:
            console.print(f"  [yellow]WARNING: Live rate append failed: {exc}[/yellow]")
    elif fill_live and not args.feather_dir:
        console.print("  [yellow]WARNING: --fill-live has no effect without --feather-dir[/yellow]")

    elapsed = time.monotonic() - t_start
    console.print(f"\n{'=' * 70}")
    console.print(f"  All phases complete in {elapsed:.1f}s")
    console.print(f"{'=' * 70}\n")


if __name__ == "__main__":
    main()
