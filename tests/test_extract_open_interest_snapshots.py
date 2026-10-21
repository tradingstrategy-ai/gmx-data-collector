"""OI daily snapshots retain the latest value for every combo."""

import importlib.util
import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"


def _load_script_module(name: str, filename: str) -> ModuleType:
    """Load a standalone script with the repository's source imports enabled."""
    for path in (REPO_ROOT / "src", SCRIPTS_DIR):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))

    spec = importlib.util.spec_from_file_location(name, SCRIPTS_DIR / filename)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


oi = _load_script_module("extract_open_interest_snapshots", "extract_open_interest.py")


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


def test_explicit_replay_does_not_apply_later_checkpoint_state():
    checkpoint = {
        "last_block": 200,
        "last_values": [
            {
                "symbol": "ETH/USD",
                "market": "0xLONG",
                "collateral": "0xCOLLATERAL",
                "is_long": True,
                "value": 100 * oi.USD_PRECISION,
            }
        ],
    }

    start_block, state, legacy = oi.resolve_resume_state(checkpoint, 150)

    assert start_block == 150
    assert state == {}
    assert legacy is False


def test_legacy_resume_checkpoint_restarts_from_genesis():
    checkpoint = {"last_block": 200, "total_events": 25}

    start_block, state, legacy = oi.resolve_resume_state(checkpoint, None)

    assert start_block == oi.GMX_V2_GENESIS_BLOCK
    assert state == {}
    assert legacy is True
