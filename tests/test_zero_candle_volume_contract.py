"""GMX oracle candles always carry zero; trade volume remains a separate input."""

import os
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from gmx_historical_data.candle_volume import apply_volume_to_candles
from gmx_historical_data.freqtrade_exporter import _restore_cleared_volume
from scripts.collect_daily_snapshot import _merge_feather


def frame(volume):
    return pd.DataFrame(
        {
            "date": pd.to_datetime(["2026-10-23"], utc=True).as_unit("ns"),
            "open": [10.0],
            "high": [12.0],
            "low": [9.0],
            "close": [11.0],
            "volume": [volume],
        }
    )


@pytest.mark.parametrize("incoming", [0.0, 1.0, 99.0])
def test_daily_merge_clears_old_and_incoming_volume_without_changing_prices(tmp_path, incoming):
    path = tmp_path / "BTC_USDC_USDC-1h-futures.feather"
    old = frame(123.0)
    old.to_feather(path)
    _merge_feather(frame(incoming), path)
    result = pd.read_feather(path)
    assert result.volume.eq(0.0).all()
    assert result.drop(columns="volume").equals(old.drop(columns="volume"))


def test_tick_volume_cannot_enter_candle_column(tmp_path):
    path = tmp_path / "BTC_USDC_USDC-1h-futures.feather"
    old = frame(123.0)
    old.to_feather(path)
    ticks = pd.DataFrame(
        {"symbol": ["BTC"], "date": old.date, "volume": [500.0], "volume_usd": [5000.0]}
    )
    apply_volume_to_candles(ticks, tmp_path, "1h")
    result = pd.read_feather(path)
    assert result.volume.eq(0.0).all()
    assert result.drop(columns="volume").equals(old.drop(columns="volume"))
    assert ticks.volume.iloc[0] == 500.0


def test_export_merge_never_restores_nonzero_volume():
    old = pl.from_pandas(frame(123.0))
    result = _restore_cleared_volume(pl.from_pandas(frame(0.0)), old)
    assert result["volume"].to_list() == [0.0]
    assert result.drop("volume").equals(old.drop("volume"))


def test_storage_audit_and_repair_preserve_original_and_other_columns(tmp_path):
    from gmx_historical_data.zero_candle_volume import enforce_zero_volume

    root = tmp_path / "futures"
    root.mkdir()
    path = root / "BTC_USDC_USDC-1h-futures.feather"
    old = frame(float("nan"))
    old.to_feather(path)
    original = path.read_bytes()
    audit = enforce_zero_volume(root)
    assert audit["nonzero_files"] == [path.name]
    assert path.read_bytes() == original
    backup = tmp_path / "backup"
    repaired = enforce_zero_volume(root, backup)
    assert repaired["repaired"] == [path.name] and not repaired["failures"]
    preimage = pd.read_feather(backup / (path.name + ".original-volume.feather"))
    assert preimage.equals(old[["date", "volume"]])
    result = pd.read_feather(path)
    assert result.volume.eq(0.0).all()
    assert result.drop(columns="volume").equals(old.drop(columns="volume"))
    assert not enforce_zero_volume(root)["nonzero_files"]


@pytest.mark.parametrize("values", [[pd.NA], [0.0, pd.NA]])
def test_nullable_null_volume_is_rejected_and_repaired(tmp_path, values):
    from gmx_historical_data.zero_candle_volume import enforce_zero_volume

    root = tmp_path / "futures"
    root.mkdir()
    path = root / "BTC_USDC_USDC-1h-futures.feather"
    old = pd.concat([frame(0.0)] * len(values), ignore_index=True)
    old["volume"] = pd.Series(values, dtype="Float64")
    old.to_feather(path)
    assert enforce_zero_volume(root)["nonzero_files"] == [path.name]
    result = enforce_zero_volume(root, tmp_path / "backup")
    assert result["repaired"] == [path.name] and not result["failures"]
    assert pd.read_feather(path).volume.tolist() == [0.0] * len(values)
    saved = tmp_path / "backup" / (path.name + ".original-volume.feather")
    assert pd.read_feather(saved).equals(old[["date", "volume"]])


def test_preimage_and_journal_are_durable_before_source_replace(tmp_path, monkeypatch):
    from gmx_historical_data.zero_candle_volume import enforce_zero_volume

    root = tmp_path / "futures"
    root.mkdir()
    path = root / "BTC_USDC_USDC-1h-futures.feather"
    frame(123.0).to_feather(path)
    backup = tmp_path / "new-parent" / "backup"
    synced = []
    real_sync, real_replace = os.fsync, os.replace

    def sync(fd):
        synced.append(Path(os.readlink(f"/proc/self/fd/{fd}")))
        real_sync(fd)

    def replace(source, target):
        if Path(target) == path:
            assert backup / (path.name + ".original-volume.feather.tmp") in synced
            assert backup / (path.name + ".json.tmp") in synced
            assert backup in synced and backup.parent in synced and tmp_path in synced
            # Both file commits must have persisted their directory entries.
            journal_sync = synced.index(backup / (path.name + ".json.tmp"))
            assert backup in synced[journal_sync + 1 :]
        real_replace(source, target)

    monkeypatch.setattr(os, "fsync", sync)
    monkeypatch.setattr(os, "replace", replace)
    result = enforce_zero_volume(root, backup)
    assert result["repaired"] == [path.name] and not result["failures"]


def test_failed_preimage_sync_keeps_source_unchanged(tmp_path, monkeypatch):
    from gmx_historical_data.zero_candle_volume import enforce_zero_volume

    root = tmp_path / "futures"
    root.mkdir()
    path = root / "BTC_USDC_USDC-1h-futures.feather"
    frame(123.0).to_feather(path)
    original = path.read_bytes()
    real_sync = os.fsync

    def sync(fd):
        if "original-volume.feather.tmp" in os.readlink(f"/proc/self/fd/{fd}"):
            raise OSError("injected backup sync failure")
        real_sync(fd)

    monkeypatch.setattr(os, "fsync", sync)
    result = enforce_zero_volume(root, tmp_path / "backup")
    assert result["failures"] and not result["repaired"]
    assert path.read_bytes() == original
