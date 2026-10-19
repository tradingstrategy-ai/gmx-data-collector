import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta

import polars as pl

from scripts.verify_gmx_lake import run


def fixture_lake(tmp_path, now):
    root = tmp_path / "lake"
    rates = root / "funding/arbitrum/rates/BTC"
    rates.mkdir(parents=True)
    (root / "funding/arbitrum/checkpoints").mkdir()
    (root / "futures").mkdir()
    hours = [now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=i) for i in (2, 1)]
    pl.DataFrame(
        {"timestamp": hours, "rate": [0.1, 0.2], "longs_pay_shorts": [True, False]}
    ).write_parquet(rates / "1h.parquet")
    pl.DataFrame({"timestamp": hours, "factor": [1.0, 1.0]}).write_parquet(
        rates / "1h_factor.parquet"
    )
    pl.DataFrame(
        {
            "date": hours,
            "open": [1.0, 1.0],
            "high": [1.0, 1.0],
            "low": [1.0, 1.0],
            "close": [1.0, 1.0],
            "volume": [1.0, 1.0],
        }
    ).write_ipc(root / "futures/BTC_USDC_USDC-1h-futures.feather")
    return root


def test_factor_source_and_canonical_order_findings(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    rates = root / "funding/arbitrum/rates/BTC"
    frame = pl.read_parquet(rates / "1h_factor.parquet").with_columns(
        pl.lit("factor").alias("source")
    )
    frame.write_parquet(rates / "1h_factor.parquet")
    frame = pl.read_parquet(rates / "1h.parquet").reverse()
    frame.write_parquet(rates / "1h.parquet")
    findings = run(root, {"G1a", "G1b"}, now, 0.05, 3, False)
    assert {item.check for item in findings} == {"G1a", "G1b"}


def test_tail_status_and_checkpoints_flag_missing(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    findings = run(root, {"G1e", "G1f"}, now, 0.05, 3, True)
    assert {item.check for item in findings} == {"G1e", "G1f"}


def test_recent_holes_and_unknown_direction_are_flagged(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    path = root / "funding/arbitrum/rates/BTC/1h.parquet"
    pl.DataFrame(
        {
            "timestamp": [now - timedelta(hours=10), now - timedelta(hours=1)],
            "rate": [0.1, 0.2],
            "longs_pay_shorts": [None, None],
        }
    ).write_parquet(path)
    findings = run(root, {"G1c", "G1d"}, now, 0.05, 3, False)
    assert {item.check for item in findings} == {"G1c", "G1d"}


def test_exported_sign_and_open_latest_bar_are_flagged(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    candle = root / "futures/BTC_USDC_USDC-1h-futures.feather"
    canonical = pl.read_parquet(root / "funding/arbitrum/rates/BTC/1h.parquet")
    pl.DataFrame({"date": [canonical["timestamp"][-1]], "open": [1.0]}).write_ipc(
        root / "futures/BTC_USDC_USDC-1h-funding_rate.feather"
    )
    pl.DataFrame({"date": [now - timedelta(minutes=20)], "open": [1.0]}).write_ipc(candle)
    findings = run(root, {"G3", "G4"}, now, 0.05, 3, False)
    assert {item.check for item in findings} == {"G3", "G4"}


def test_coverage_is_staged_and_pass_failure_is_reported(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    findings = run(root, {"G2"}, now, 0.05, 3, False)
    assert findings
    assert all(item.check == "G2" for item in findings)


def test_report_only_and_root_hashes_are_unchanged(tmp_path):
    root = fixture_lake(tmp_path, datetime.now(UTC))

    def hashes():
        return {
            str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in root.rglob("*")
            if p.is_file()
        }

    before = hashes()
    report_path = tmp_path / "report.json"
    proc = subprocess.run(
        [
            sys.executable,
            "scripts/verify_gmx_lake.py",
            str(root),
            "--checks",
            "G1a",
            "G1e",
            "--tail-fill-expected",
            "--report-only",
            "--json",
            str(report_path),
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0
    assert "VERIFY_LAKE: FAIL 1 findings" in proc.stdout
    assert json.loads(report_path.read_text())["failed"] is True
    assert hashes() == before
