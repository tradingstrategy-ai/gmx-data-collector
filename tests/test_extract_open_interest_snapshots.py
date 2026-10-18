"""OI daily snapshots retain the latest value for every combo."""

import importlib.util
import sys
from datetime import datetime
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
sys.path.insert(0, str(REPO_ROOT / "src"))
SPEC = importlib.util.spec_from_file_location(
    "extract_open_interest_snapshots", SCRIPTS_DIR / "extract_open_interest.py"
)
oi = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = oi
SPEC.loader.exec_module(oi)


def _record(market: str, is_long: bool, value_usd: int, day: str, block: int):
    timestamp = int(datetime.fromisoformat(f"{day}T12:00:00+00:00").timestamp())
    return oi.OpenInterestRecord(
        symbol="ETH/USD",
        market=market,
        collateralToken="0xCOLLATERAL",
        isLong=is_long,
        deltaUsd="0",
        nextValueUsd=str(value_usd * oi.USD_PRECISION),
        deltaTokens="0",
        nextValueTokens="0",
        deltaUsdFormatted="0",
        nextValueUsdFormatted=str(value_usd),
        blockNumber=block,
        blockTimestamp=timestamp,
        blockDatetime="",
        transactionHash=f"0x{block}",
        logIndex=0,
        eventType="OpenInterestUpdated",
    )


def _snapshot_value(path: Path) -> tuple[str, str, str]:
    row = pl.read_parquet(path).filter(pl.col("date") == "2026-01-02").row(0, named=True)
    return row["longOiUsd"], row["shortOiUsd"], row["totalOiUsd"]


def test_daily_snapshot_carries_silent_combo_across_flushes(tmp_path: Path):
    """A combo with no event on D remains in D's point-in-time total."""
    state = {}
    snapshots = oi.compute_daily_snapshots(
        [
            _record("0xSILENT", False, 30, "2026-01-01", 1),
            _record("0xLONG", True, 100, "2026-01-02", 2),
        ],
        state,
    )
    oi.save_snapshots_per_symbol(snapshots, tmp_path)

    next_flush = oi.compute_daily_snapshots(
        [_record("0xOTHER", False, 50, "2026-01-02", 3)],
        state,
    )
    oi.save_snapshots_per_symbol(next_flush, tmp_path)

    assert _snapshot_value(tmp_path / "snapshots/ETH_USD/daily.parquet") == (
        "100.00",
        "80.00",
        "180.00",
    )


def test_daily_snapshot_restores_combo_state_from_resume_checkpoint(tmp_path: Path):
    """A process restart between flushes retains combo values via checkpoint."""
    checkpoint = tmp_path / "oi_checkpoint.json"
    state_before_restart = {}
    first_batch = [
        _record("0xSILENT", False, 30, "2026-01-01", 1),
        _record("0xLONG", True, 100, "2026-01-02", 2),
    ]
    oi.save_snapshots_per_symbol(
        oi.compute_daily_snapshots(first_batch, state_before_restart), tmp_path
    )
    oi.save_checkpoint(
        checkpoint,
        last_block=2,
        last_timestamp=first_batch[-1].blockTimestamp,
        total_events=2,
        markets_seen=1,
        last_values=state_before_restart,
    )

    resumed_state = oi.checkpoint_last_values(oi.load_checkpoint(checkpoint))
    second_batch = [_record("0xOTHER", False, 50, "2026-01-02", 3)]
    oi.save_snapshots_per_symbol(oi.compute_daily_snapshots(second_batch, resumed_state), tmp_path)

    assert _snapshot_value(tmp_path / "snapshots/ETH_USD/daily.parquet") == (
        "100.00",
        "80.00",
        "180.00",
    )
