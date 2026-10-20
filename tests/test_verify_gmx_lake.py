import hashlib
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

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


def test_daily_job_can_configure_checkpoint_and_tail_status_ages(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    checkpoints = root / "funding/arbitrum/checkpoints"
    checkpoint_time = int((now - timedelta(hours=26)).timestamp())
    for name in ("funding_factor_checkpoint.json", "fee_per_size_checkpoint.json"):
        (checkpoints / name).write_text(json.dumps({"last_timestamp": checkpoint_time}))
    (checkpoints / "funding_datastore_tail_status.json").write_text(
        json.dumps({"status": "ok", "updated_at": (now - timedelta(hours=40)).isoformat()})
    )

    strict = run(root, {"G1e", "G1f"}, now, 0.05, 3, True)
    assert {item.check for item in strict} == {"G1e", "G1f"}

    daily = run(root, {"G1e", "G1f"}, now, 0.05, 3, True, 30, 48)
    assert daily == []


def test_datastore_fee_and_direction_signs_are_counted_per_symbol(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    rates = root / "funding/arbitrum/rates/BTC"
    hours = [now - timedelta(hours=i) for i in (3, 2, 1)]
    pl.DataFrame(
        {
            "timestamp": hours[:2],
            "funding_rate_hourly": [0.1, -0.1],
            "funding_fee_long": [1.0, 2.0],  # second row should be negative
            "longs_pay_shorts": [True, False],
        }
    ).write_parquet(rates / "1h_datastore.parquet")
    pl.DataFrame(
        {
            "timestamp": hours,
            "source": ["datastore", "hypersync", "datastore"],
            "funding_rate_hourly": [0.1, 0.2, -0.1],
            "funding_fee_long": [1.0, 0.2, -1.0],
            "longs_pay_shorts": [False, True, False],  # first row disagrees
        }
    ).write_parquet(rates / "1h.parquet")

    findings = run(root, {"G1g"}, now, 0.05, 3, False)
    assert len(findings) == 1
    assert findings[0].symbol == "BTC" and findings[0].failed
    assert "datastore rows=4" in findings[0].message
    assert "fee/rate sign mismatches=1" in findings[0].message
    assert "direction/rate sign mismatches=1" in findings[0].message

    datastore = pl.read_parquet(rates / "1h_datastore.parquet").with_columns(
        pl.Series("funding_fee_long", [1.0, -2.0])
    )
    datastore.write_parquet(rates / "1h_datastore.parquet")
    canonical = pl.read_parquet(rates / "1h.parquet").with_columns(
        pl.Series("longs_pay_shorts", [True, True, False])
    )
    canonical.write_parquet(rates / "1h.parquet")
    passing = run(root, {"G1g"}, now, 0.05, 3, False)
    assert len(passing) == 1 and not passing[0].failed
    assert "fee/rate sign mismatches=0" in passing[0].message


def test_listing_grace_exempts_new_active_market_without_funding(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    (root / "funding/arbitrum/rates/BTC/1h.parquet").unlink()
    futures = root / "futures/BTC_USDC_USDC-1h-futures.feather"
    pl.DataFrame(
        {
            "date": [now - timedelta(days=4), now - timedelta(hours=1)],
            "open": [1.0, 1.0],
        }
    ).write_ipc(futures)
    (root / "snapshots").mkdir()
    pl.DataFrame({"symbol": ["BTC"], "listing_date": [now - timedelta(days=4)]}).write_parquet(
        root / "snapshots/current.parquet"
    )
    assert run(root, {"G1c"}, now, 0.05, 3, False) == []

    pl.DataFrame({"symbol": ["BTC"], "listing_date": [now - timedelta(days=10)]}).write_parquet(
        root / "snapshots/current.parquet"
    )
    findings = run(root, {"G1c"}, now, 0.05, 3, False)
    assert [item.check for item in findings] == ["G1c"]


def test_recent_holes_and_unknown_direction_are_flagged(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    (root / "snapshots").mkdir()
    pl.DataFrame({"symbol": ["BTC"], "listing_date": [now - timedelta(days=20)]}).write_parquet(
        root / "snapshots/current.parquet"
    )
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


def test_exported_sign_and_open_latest_bar_are_checked_without_flagging_open_bar(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    candle = root / "futures/BTC_USDC_USDC-1h-futures.feather"
    canonical = pl.read_parquet(root / "funding/arbitrum/rates/BTC/1h.parquet")
    pl.DataFrame({"date": [canonical["timestamp"][-1]], "open": [1.0]}).write_ipc(
        root / "futures/BTC_USDC_USDC-1h-funding_rate.feather"
    )
    current_hour = now.replace(minute=0, second=0, microsecond=0)
    pl.DataFrame(
        {"date": [current_hour - timedelta(hours=1), current_hour], "open": [1.0, 1.0]}
    ).write_ipc(candle)
    findings = run(root, {"G3", "G4"}, now, 0.05, 3, False)
    assert {item.check for item in findings} == {"G3"}


def test_coverage_is_staged_and_pass_failure_is_reported(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    findings = run(root, {"G2"}, now, 0.05, 3, False)
    assert findings
    assert all(item.check == "G2" for item in findings)


def test_coverage_uses_listing_dates_checkpoint_threshold_and_stale_enum(tmp_path, monkeypatch):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    from gmx_historical_data import funding_release, funding_state, series_coverage

    listing = {"BTC": now - timedelta(days=20)}
    monkeypatch.setattr(funding_release, "load_listing_dates", lambda _path: listing)
    monkeypatch.setattr(funding_release, "stage_companions", lambda *_args: {})
    monkeypatch.setattr(
        funding_state, "assess_funding_state", lambda _root: SimpleNamespace(value="ready")
    )
    pair = SimpleNamespace(symbol="BTC", tail=series_coverage.TailStatus.STALE)
    seen = {}

    def assess(**kwargs):
        seen.update(kwargs)
        return SimpleNamespace(pairs=[pair], passed=True, reasons=[])

    monkeypatch.setattr(series_coverage, "assess_series_coverage", assess)
    findings = run(root, {"G2"}, now, 0.05, 3, False, 30, 48)
    assert len(findings) == 1
    assert (findings[0].check, findings[0].symbol, findings[0].failed) == ("G2", "BTC", False)
    assert findings[0].message == "STALE funding tail"
    assert seen["listing_dates"] == listing
    assert seen["thresholds"].head_lag_hours == 30


def test_8h_sign_check_is_opt_in(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    canonical = pl.read_parquet(root / "funding/arbitrum/rates/BTC/1h.parquet")
    date = canonical["timestamp"][-1]
    pl.DataFrame({"date": [date], "open": [1.0]}).write_ipc(
        root / "futures/BTC_USDC_USDC-8h-funding_rate.feather"
    )
    assert run(root, {"G3"}, now, 0.05, 3, False) == []
    findings = run(root, {"G3"}, now, 0.05, 3, False, check_8h=True)
    assert [item.check for item in findings] == ["G3"]


def test_8h_sign_check_uses_hourly_export_timestamp(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    canonical = pl.read_parquet(root / "funding/arbitrum/rates/BTC/1h.parquet")
    timestamp = canonical["timestamp"][0]
    pl.DataFrame({"date": [timestamp], "open": [1.0]}).write_ipc(
        root / "futures/BTC_USDC_USDC-8h-funding_rate.feather"
    )
    findings = run(root, {"G3"}, now, 0.05, 3, False, check_8h=True)
    assert findings == []


def test_g4_ignores_fast_candle_lag_within_four_hour_refresh_window(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    candle = root / "futures/BTC_USDC_USDC-1m-futures.feather"
    last_closed = now.replace(second=0, microsecond=0) - timedelta(minutes=15)
    pl.DataFrame({"date": [last_closed], "open": [1.0]}).write_ipc(candle)
    findings = run(root, {"G4"}, now, 0.05, 3, False)
    assert findings == []


def test_g4_flags_oldest_closed_bar_not_an_open_latest_bar(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    candle = root / "futures/BTC_USDC_USDC-1h-futures.feather"
    old = now - timedelta(hours=8)
    opening = old.replace(minute=0, second=0, microsecond=0)
    open_latest = now.replace(minute=0, second=0, microsecond=0)
    pl.DataFrame({"date": [opening, open_latest], "open": [1.0, 1.0]}).write_ipc(candle)
    findings = run(root, {"G4"}, now, 0.05, 3, False)
    assert len(findings) == 1
    assert "newest closed 1h bar" in findings[0].message


def test_g4_fails_when_active_market_has_only_an_open_candle(tmp_path):
    now = datetime.now(UTC)
    root = fixture_lake(tmp_path, now)
    candle = root / "futures/BTC_USDC_USDC-1h-futures.feather"
    open_bar = now.replace(minute=0, second=0, microsecond=0)
    pl.DataFrame({"date": [open_bar], "open": [1.0]}).write_ipc(candle)

    findings = run(root, {"G4"}, now, 0.05, 3, False)

    assert len(findings) == 1
    assert findings[0].check == "G4"
    assert findings[0].symbol == "BTC"
    assert findings[0].failed
    assert findings[0].message == "active market has no readable closed 1h candle"


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


def test_rejected_in_root_json_path_does_not_overwrite_lake_file(tmp_path):
    root = fixture_lake(tmp_path, datetime.now(UTC))
    target = root / "futures/BTC_USDC_USDC-1h-futures.feather"
    original = target.read_bytes()
    proc = subprocess.run(
        [
            sys.executable,
            "scripts/verify_gmx_lake.py",
            str(root),
            "--checks",
            "G1a",
            "--json",
            str(target),
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert "--json path must be outside the data root" in proc.stdout
    assert target.read_bytes() == original


def test_incomplete_lake_json_path_is_rejected_before_report_write(tmp_path):
    root = tmp_path / "incomplete-lake"
    root.mkdir()
    target = root / "delisted_markets.json"
    original = b'{"symbols": ["OLD"]}\n'
    target.write_bytes(original)
    proc = subprocess.run(
        [
            sys.executable,
            "scripts/verify_gmx_lake.py",
            str(root),
            "--report-only",
            "--json",
            str(target),
        ],
        capture_output=True,
        text=True,
    )
    assert proc.returncode != 0
    assert "--json path must be outside the data root" in proc.stdout
    assert target.read_bytes() == original
