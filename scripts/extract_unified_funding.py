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
    5. **DataStore tail-fill** (default when an RPC is configured; fail-soft, #64):
       reads ``savedFundingFactorPerSecond`` at hourly boundaries for each market's
       stale tail and for every carried (``is_gap_filled``) hour of the last 30
       days, writes ``1h_datastore.parquet`` and re-merges. Per hour the merge
       prefers an event-derived HyperSync hour, then a DataStore read, then a carried value.

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
    --skip-datastore-tail  Skip the DataStore tail-fill / gap correction
    --tail-window-days   Trailing days the tail-fill reaches (default: 30)
    --tail-max-minutes   Time budget of the tail-fill (default: 4); partial work is kept
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
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
from rich.console import Console

from gmx_historical_data import datastore_tail
from gmx_historical_data.atomic_parquet import atomic_write_ipc, atomic_write_parquet
from gmx_historical_data.cadence_gate import load_exempt_symbols

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
    if args.from_block is not None:
        cmd.extend(["--from-block", str(args.from_block)])
    if args.to_block is not None:
        cmd.extend(["--to-block", str(args.to_block)])
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
    if args.from_block is not None:
        cmd.extend(["--from-block", str(args.from_block)])
    if args.to_block is not None:
        cmd.extend(["--to-block", str(args.to_block)])
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
    if args.from_block is not None:
        cmd.extend(["--from-block", str(args.from_block)])
    if args.to_block is not None:
        cmd.extend(["--to-block", str(args.to_block)])
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
            pl.col("direction_longs_pay").forward_fill().over(group_keys).alias("longs_pay_shorts")
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
    missing_row = pl.col("update_count").is_null()
    if "is_gap_filled" in merged.columns:
        merged = merged.with_columns(
            pl.col("is_gap_filled").fill_null(missing_row).alias("is_gap_filled")
        )
    else:
        merged = merged.with_columns(missing_row.alias("is_gap_filled"))

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
    merged = merged.with_columns([pl.col(c).forward_fill() for c in ff_cols if c in merged.columns])
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
    raw_factor_path = rates_dir.parent / "raw" / "funding" / symbol / "partition=0" / "data.parquet"
    if factor_path.exists() and raw_factor_path.exists():
        hs_df = pl.read_parquet(factor_path)
        # Legacy unified companions carry a source label and can contain
        # DataStore rows. Never reinterpret them as unsigned factor input.
        if "source" not in hs_df.columns:
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

    # Older DataStore files lack provenance columns; make their meaning explicit.
    if "is_gap_filled" not in unified.columns:
        unified = unified.with_columns(pl.lit(False).alias("is_gap_filled"))
    else:
        unified = unified.with_columns(pl.col("is_gap_filled").fill_null(False))
    if "source" not in unified.columns:
        unified = unified.with_columns(pl.lit("unknown").alias("source"))

    # One row per hour. Precedence: an event-derived HyperSync hour, then a
    # DataStore read (chain state at the hour boundary), then a carried gap fill.
    unified = unified.with_columns(
        pl.when((pl.col("source") == "hypersync") & ~pl.col("is_gap_filled"))
        .then(0)
        .when(pl.col("source") == "datastore")
        .then(1)
        .otherwise(2)
        .alias("_precedence")
    )
    unified = (
        unified.sort(["timestamp", "_precedence"], maintain_order=True)
        .unique(subset=["timestamp"], keep="first", maintain_order=True)
        .drop("_precedence")
    )
    unified = unified.sort("timestamp")

    # Write unified output
    ext = "feather" if output_format == "feather" else "parquet"
    unified_path = rates_dir / symbol / f"1h.{ext}"
    unified_path.parent.mkdir(parents=True, exist_ok=True)
    if unified_path.exists():
        existing = (
            pl.read_ipc(unified_path)
            if output_format == "feather"
            else pl.read_parquet(unified_path)
        )
        if existing.equals(unified):
            return len(unified)
    if output_format == "feather":
        atomic_write_ipc(unified, unified_path)
    else:
        atomic_write_parquet(unified, unified_path)

    return len(unified)


def merge_unified_rates(
    network_dir: Path,
    market_filter: str | None = None,
    output_format: str = "parquet",
    only_symbols: set[str] | None = None,
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
    :param only_symbols: Merge just these symbols (the DataStore tail-fill re-merges
        what it touched).
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
    if only_symbols is not None:
        symbols &= only_symbols

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
# DATASTORE TAIL-FILL (#64)
# =============================================================================

#: Default time budget of the tail-fill.
TAIL_MAX_MINUTES = 4.0
#: The tail-fill never runs past this many minutes after the process started: the
#: daily release caps the whole extraction at 15 minutes, and a cancelled extraction
#: withholds every companion (B4), so the optional tail-fill yields first. What it
#: read is kept and the next run continues.
TAIL_CEILING_MINUTES = 11.0
#: Of the tail-fill's budget, seconds kept free at the end for persisting and re-merging.
TAIL_PERSIST_RESERVE_SECONDS = 45.0


@dataclass
class TailFillResult:
    """Outcome of :func:`tail_fill_datastore`.

    :ivar status: The record written for the gate (``status``: ok / partial /
        unavailable / failed, plus ``reason`` and hour counts).
    :ivar touched: Symbols whose ``1h_datastore.parquet`` gained rows.
    """

    status: dict
    touched: set[str] = field(default_factory=set)


def _load_datastore_script():
    """Import ``extract_funding_datastore`` (a sibling script, not a package module)."""
    import importlib

    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))
    return importlib.import_module("extract_funding_datastore")


def _read_hours(path: Path) -> set[datetime]:
    if not path.exists():
        return set()
    try:
        return set(pl.read_parquet(path, columns=["timestamp"])["timestamp"].to_list())
    except Exception:  # noqa: BLE001 - an unreadable datastore file just means "read it again"
        return set()


def tail_fill_datastore(
    network_dir: Path,
    *,
    now: datetime | None = None,
    rpc_config: str | None = None,
    market_filter: str | None = None,
    window_days: int = datastore_tail.TAIL_WINDOW_DAYS,
    max_minutes: float | None = None,
    chunk_hours: int = 48,
    checkpoint_dir: Path | None = None,
    output_format: str = "parquet",
) -> TailFillResult:
    """Extend quiet markets' tails and correct carried hours from the DataStore.

    Run after the HyperSync phases and the first merge, so the canonical
    ``rates/<SYM>/1h.parquet`` shows each market's last bar and its
    ``is_gap_filled`` hours. For each market the window is (last bar + 1h ..
    ``floor(now) - 1h``) plus every carried hour, both limited to the trailing
    ``window_days``; the DataStore is read at the first block of each hour and the
    rows are appended to ``1h_datastore.parquet`` (signed, ``source="datastore"``).
    Markets on the delisted roster are not touched; markets that did not exist yet
    produce no rows.

    Never raises for an RPC problem: a node without historical state, a missing RPC,
    a time budget or any other failure is recorded in the status file the gate reports
    (``datastore_tail: unavailable (<reason>)``), whatever was read is already
    persisted, and the release carries on with the tails flagged as before.

    :param network_dir: ``<output-dir>/arbitrum``.
    :param now: Run time (defaults to the current time).
    :param rpc_config: RPC URL(s); defaults to ``JSON_RPC_ARBITRUM``.
    :param market_filter: Optional symbol filter (``'ETH/USD'``).
    :param window_days: Trailing window.
    :param max_minutes: Time budget; ``None`` means unlimited.
    :param chunk_hours: Hours persisted per chunk.
    :param checkpoint_dir: Where the status file goes (default ``network_dir/checkpoints``).
    :param output_format: Format of the canonical file the touched symbols are re-merged into.
    :returns: A :class:`TailFillResult`; the symbols it touched are already re-merged.
    """
    now = now or datetime.now(UTC)
    cutoff = datastore_tail.tail_cutoff(now)
    cp_dir = checkpoint_dir or (network_dir / "checkpoints")
    result = TailFillResult(status={})

    def record(status: str, reason: str | None = None, **counts) -> TailFillResult:
        result.status = datastore_tail.write_tail_status(
            cp_dir,
            status=status,
            reason=reason,
            window_start=cutoff - timedelta(days=window_days),
            window_end=cutoff,
            now=now,
            **counts,
        )
        console.print(
            f"  datastore_tail: {datastore_tail.describe_tail_status(result.status, now=now)}"
        )
        return result

    console.print(f"\n{'=' * 70}")
    console.print("  Phase 5: DataStore Tail-Fill (archive RPC, fail-soft)")
    console.print(f"{'=' * 70}")

    try:
        ds = _load_datastore_script()
    except Exception as exc:  # noqa: BLE001 - e.g. a missing optional dependency
        return record("unavailable", f"datastore reader unavailable: {type(exc).__name__}: {exc}")
    if rpc_config is None:
        try:
            rpc_config = ds.get_rpc_config()
        except ValueError:
            return record("unavailable", "no archive RPC configured (JSON_RPC_ARBITRUM)")

    rates_dir = network_dir / "rates"
    frames: dict[str, pl.DataFrame] = {}
    already: dict[str, set[datetime]] = {}
    filter_sym = market_symbol_from_filter(market_filter) if market_filter else None
    if rates_dir.exists():
        for sym_dir in sorted(rates_dir.iterdir()):
            canonical = sym_dir / "1h.parquet"
            if not sym_dir.is_dir() or not canonical.exists():
                continue
            if filter_sym and sym_dir.name != filter_sym:
                continue
            try:
                frames[sym_dir.name] = pl.read_parquet(canonical)
            except Exception as exc:  # noqa: BLE001 - one unreadable file must not stop the rest
                console.print(f"  [yellow]{sym_dir.name}: unreadable 1h.parquet ({exc})[/yellow]")
                continue
            already[sym_dir.name] = _read_hours(sym_dir / "1h_datastore.parquet")

    delisted = load_exempt_symbols([network_dir.parent.parent / "delisted_markets.json"])
    windows = datastore_tail.compute_tail_windows(
        frames, cutoff=cutoff, delisted=delisted, already_read=already, window_days=window_days
    )
    by_hour: dict[int, list[str]] = {}
    symbol_of: dict[str, str] = {}
    for window in windows.values():
        symbol_of[window.market] = window.symbol
        for hour in window.hours:
            by_hour.setdefault(int(hour.timestamp()), []).append(window.market)
    requested = len(by_hour)
    console.print(
        f"  Markets with a window: {len(windows)}; hours to read: {requested}; "
        f"cutoff {cutoff:%Y-%m-%d %H:%M}Z"
    )
    if not windows:
        return record("ok", hours_requested=0, hours_read=0)

    # No RPC call, retry or backoff may run past the budget (less a reserve to persist and
    # re-merge what was read): a chunk that cannot finish in time is dropped and read
    # again next run, never allowed to overrun the step and get the whole extraction killed.
    deadline = (
        None
        if max_minutes is None
        else ds._monotonic() + max_minutes * 60 - TAIL_PERSIST_RESERVE_SECONDS
    )

    def persist(batch) -> None:
        ds.save_parquet(ds.aggregate_hourly_rates(batch), network_dir.parent)
        result.touched.update(r.symbol for r in batch)

    try:
        try:
            outcome = ds.extract_funding_hours(
                rpc_config,
                sorted(by_hour),
                wanted=lambda hour: by_hour[hour],
                symbol_of=lambda addr: symbol_of[addr.lower()],
                persist=persist,
                chunk_hours=chunk_hours,
                deadline=deadline,
            )
        finally:
            # Whatever was persisted, even by a failed run, belongs in the canonical series.
            if result.touched:
                merge_unified_rates(
                    network_dir, market_filter, output_format, only_symbols=result.touched
                )
    except Exception as exc:  # noqa: BLE001 - fail-soft: record it and let the release go on
        return record(
            "failed",
            f"{type(exc).__name__}: {exc}",
            hours_requested=requested,
            hours_read=len(result.touched),
        )

    counts = {"hours_requested": requested, "hours_read": outcome.read}
    if outcome.stopped == "unavailable":
        kind = "partial" if outcome.read else "unavailable"
        return record(kind, outcome.reason, **counts)
    if outcome.stopped == "deadline":
        return record("partial", outcome.reason, **counts)
    return record("ok", **counts)


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

    from gmx_historical_data.freqtrade_exporter import FreqtradeExporter

    rates_dir = network_dir / "rates"
    if not rates_dir.exists():
        console.print("  [yellow]No rates directory found[/yellow]")
        return

    gmx_dir = feather_dir / "gmx" / "futures"
    gmx_dir.mkdir(parents=True, exist_ok=True)

    # The same exporter that backs ``make export-funding``: one signed hourly
    # transformation and one guarded merge writer, so the two entry points
    # cannot drift apart (issue #60).
    exporter = FreqtradeExporter(network_dir, feather_dir, funding_dir=rates_dir)

    symbols: dict[str, Path] = {}
    for d in rates_dir.iterdir():
        if not d.is_dir():
            continue
        # ``1h.parquet`` is the single authoritative source -- the file
        # ``export_funding`` reads -- so both entry points agree whatever
        # stale ``1h.feather`` is lying around. A feather-only lake predates
        # that convention; read it, but say so.
        if (d / "1h.parquet").exists():
            symbols[d.name] = d / "1h.parquet"
        elif (d / "1h.feather").exists():
            console.print(
                f"  [yellow]{d.name}: no 1h.parquet, falling back to legacy 1h.feather "
                "-- re-run the merge to regenerate parquet[/yellow]"
            )
            symbols[d.name] = d / "1h.feather"

    if market_filter:
        filter_sym = market_symbol_from_filter(market_filter)
        symbols = {s: p for s, p in symbols.items() if s == filter_sym}

    exported = 0
    for symbol in sorted(symbols):
        unified_path = symbols[symbol]
        df = (
            pl.read_ipc(unified_path, memory_map=False)
            if unified_path.suffix == ".feather"
            else pl.read_parquet(unified_path)
        )
        if df.is_empty():
            continue

        n_before = len(exporter.funding_reconciliations)
        written = exporter.export_funding_frame(
            symbol,
            "1h",
            df,
            gmx_dir=gmx_dir,
            output_format="feather",
            quote_currency=quote_currency,
        )
        for rec in exporter.funding_reconciliations[n_before:]:
            if not rec.clean:
                console.print(
                    f"  [yellow]{symbol:<12}[/yellow] {len(rec.missing_hours)} source hour(s) "
                    f"missing, {len(rec.null_rate_hours)} null-rate, "
                    f"{len(rec.unknown_direction_hours)} unknown-direction row(s) excluded"
                )
        if not written:
            continue
        exported += 1
        filename = f"{symbol}_{quote_currency}_{quote_currency}-1h-funding_rate.feather"
        console.print(
            f"  [green]{symbol:<12}[/green] {exporter.funding_reconciliations[-1].exported_hours:>8,}"
            f" source hours -> {gmx_dir / filename}"
        )

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
    parser.add_argument(
        "--from-block", type=int, default=None, help="Start block for bounded extraction"
    )
    parser.add_argument(
        "--to-block", type=int, default=None, help="End block for bounded extraction"
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
        "--skip-datastore-tail",
        action="store_true",
        help=(
            "Skip the DataStore tail-fill / gap correction (it runs whenever an archive RPC "
            "is configured, never fails the run, and records datastore_tail: unavailable "
            "when the node has no historical state)"
        ),
    )
    parser.add_argument(
        "--tail-window-days",
        type=int,
        default=datastore_tail.TAIL_WINDOW_DAYS,
        help="Trailing days the DataStore tail-fill reaches (default: 30)",
    )
    parser.add_argument(
        "--tail-max-minutes",
        type=float,
        default=TAIL_MAX_MINUTES,
        help="Time budget of the DataStore tail-fill; partial work is kept (default: 4)",
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
    if not args.merge_only and not args.skip_datastore_tail and args.network == "arbitrum":
        phases.append("DataStore Tail-Fill")
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

    # Phase 3: Direction (async, HyperSync) - full range
    if not args.skip_direction and not args.merge_only:
        run_phase("Phase 3: Direction (HyperSync)", build_direction_cmd(args))

    # Phase 4: Merge all sources into unified rates
    merge_unified_rates(network_dir, args.market, args.output)

    # Phase 5: DataStore tail-fill (fail-soft). Reads what HyperSync cannot see --
    # the chain state after a quiet market's last event and under carried hours --
    # then re-merges just the symbols it wrote.
    if not args.merge_only and not args.skip_datastore_tail and args.network == "arbitrum":
        elapsed_minutes = (time.monotonic() - t_start) / 60
        tail_fill_datastore(
            network_dir,
            market_filter=args.market,
            window_days=args.tail_window_days,
            max_minutes=min(
                args.tail_max_minutes, max(0.0, TAIL_CEILING_MINUTES - elapsed_minutes)
            ),
            checkpoint_dir=cp_dir,
            output_format=args.output,
        )

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
