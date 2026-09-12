"""Tests for the daily tick-collection phase.

This phase is the only part of the snapshot that needs HyperSync and an RPC.
Every other phase runs off the public REST API, so a missing key or a
HyperSync outage must degrade to "no volume today" rather than failing the
release -- candles, snapshots, tickers and APY are worth shipping without it.
"""

import importlib
import json
import sys
from types import ModuleType


def _import_snapshot_without_hypersync(monkeypatch) -> ModuleType:
    """Import the snapshot entry point with ``hypersync`` unimportable.

    Reproduces the runner environment that broke the daily release: the
    package is simply absent, so every ``import hypersync`` raises. A ``None``
    entry in ``sys.modules`` is CPython's own way of poisoning a name, and it
    survives the fresh import below.

    :param monkeypatch: pytest's monkeypatch fixture, which restores
        ``sys.modules`` afterwards.
    :returns: A freshly executed ``scripts.collect_daily_snapshot`` module.
    """
    poisoned = [
        name
        for name in list(sys.modules)
        if name == "hypersync"
        or name.startswith("hypersync.")
        or name.startswith("gmx_historical_data.trade_tick_collector")
        or name.startswith("gmx_historical_data.hypersync")
        or name == "scripts.collect_daily_snapshot"
    ]
    for name in poisoned:
        monkeypatch.delitem(sys.modules, name, raising=False)

    monkeypatch.setitem(sys.modules, "hypersync", None)
    return importlib.import_module("scripts.collect_daily_snapshot")


class TestHyperSyncIsOptional:
    """The collector must survive HyperSync being *uninstalled*, not just
    unreachable. Everything outside the tick phase runs off the keyless REST
    API and is worth publishing without volume."""

    def test_snapshot_imports_without_hypersync_installed(self, monkeypatch):
        """The entry point died at import time on 2026-09-11 and 2026-09-12,
        before a single market was fetched, because a module-level import
        chain reached ``hypersync``."""
        module = _import_snapshot_without_hypersync(monkeypatch)

        assert module.DEFAULT_MAX_BLOCKS > 0

    def test_tick_phase_degrades_when_hypersync_is_missing(self, tmp_path, monkeypatch):
        """With credentials present but the package absent, the phase reports
        a skip with a reason -- the same soft failure as a HyperSync outage."""
        module = _import_snapshot_without_hypersync(monkeypatch)

        monkeypatch.setenv("HYPERSYNC_API_TOKEN", "test-token")
        monkeypatch.setenv("ARBITRUM_RPC_URL", "https://example.invalid")

        checkpoint = tmp_path / "cp.json"
        module._write_tick_checkpoint(checkpoint, 1000)

        result = module.collect_and_save_ticks(
            markets=[],
            date_str="2026-09-12",
            ticks_dir=tmp_path / "ticks",
            tick_volume_dir=tmp_path / "tick_volume",
            futures_dir=tmp_path / "futures",
            checkpoint_path=checkpoint,
        )

        assert result["ticks"] == 0
        assert result["skipped"] is True
        assert "hypersync" in result["reason"].lower()
        # An unscanned range must stay unscanned, not be checkpointed past.
        assert module._read_tick_checkpoint(checkpoint) == 1000


class TestTickCheckpoint:
    def test_missing_checkpoint_reads_as_none(self, tmp_path):
        from scripts.collect_daily_snapshot import _read_tick_checkpoint

        assert _read_tick_checkpoint(tmp_path / "nope.json") is None

    def test_roundtrips_last_scanned_block(self, tmp_path):
        from scripts.collect_daily_snapshot import (
            _read_tick_checkpoint,
            _write_tick_checkpoint,
        )

        path = tmp_path / "checkpoints" / "trade_ticks.json"
        _write_tick_checkpoint(path, 503_686_637)

        assert _read_tick_checkpoint(path) == 503_686_637

    def test_corrupt_checkpoint_reads_as_none_not_crash(self, tmp_path):
        """A truncated write must cost one wide re-scan, not a failed run."""
        from scripts.collect_daily_snapshot import _read_tick_checkpoint

        path = tmp_path / "trade_ticks.json"
        path.write_text("{not json", encoding="utf-8")

        assert _read_tick_checkpoint(path) is None

    def test_checkpoint_advances_only_forward(self, tmp_path):
        """A stale in-flight run must not rewind a newer checkpoint and cause
        the same range to be re-scanned and volume double-counted."""
        from scripts.collect_daily_snapshot import (
            _read_tick_checkpoint,
            _write_tick_checkpoint,
        )

        path = tmp_path / "trade_ticks.json"
        _write_tick_checkpoint(path, 500)
        _write_tick_checkpoint(path, 400)

        assert _read_tick_checkpoint(path) == 500

    def test_checkpoint_file_is_readable_json(self, tmp_path):
        from scripts.collect_daily_snapshot import _write_tick_checkpoint

        path = tmp_path / "trade_ticks.json"
        _write_tick_checkpoint(path, 123)

        assert json.loads(path.read_text())["last_scanned_block"] == 123


class TestFailSoft:
    def test_missing_hypersync_token_skips_phase(self, tmp_path, monkeypatch, capsys):
        from scripts.collect_daily_snapshot import collect_and_save_ticks

        monkeypatch.delenv("HYPERSYNC_API_TOKEN", raising=False)
        monkeypatch.delenv("ARBITRUM_RPC_URL", raising=False)
        monkeypatch.delenv("ARBITRUM_CHAIN_JSON_RPC", raising=False)
        monkeypatch.delenv("JSON_RPC_ARBITRUM", raising=False)

        result = collect_and_save_ticks(
            markets=[],
            date_str="2026-09-10",
            ticks_dir=tmp_path / "ticks",
            tick_volume_dir=tmp_path / "tick_volume",
            futures_dir=tmp_path / "futures",
            checkpoint_path=tmp_path / "cp.json",
        )

        assert result["ticks"] == 0
        assert result["skipped"] is True

    def test_non_arbitrum_chain_skips_rather_than_scanning_wrong_contract(
        self, tmp_path, monkeypatch
    ):
        """trade_tick_collector hardcodes the Arbitrum EventEmitter address
        and this phase's RPC env vars are Arbitrum-only. Scanning another
        chain with that config would silently return an empty, successful
        scan and advance the checkpoint past blocks queried against the
        wrong contract entirely -- reject it up front instead."""
        from scripts.collect_daily_snapshot import collect_and_save_ticks

        monkeypatch.setenv("HYPERSYNC_API_TOKEN", "test-token")
        monkeypatch.setenv("ARBITRUM_RPC_URL", "https://example.invalid")

        result = collect_and_save_ticks(
            markets=[],
            date_str="2026-09-10",
            ticks_dir=tmp_path / "ticks",
            tick_volume_dir=tmp_path / "tick_volume",
            futures_dir=tmp_path / "futures",
            checkpoint_path=tmp_path / "cp.json",
            chain="avalanche",
        )

        assert result["ticks"] == 0
        assert result["skipped"] is True
        assert "avalanche" in result["reason"]

    def test_collection_failure_is_swallowed(self, tmp_path, monkeypatch):
        """A HyperSync 5xx mid-run must not abort the whole snapshot."""
        import scripts.collect_daily_snapshot as mod

        monkeypatch.setenv("HYPERSYNC_API_TOKEN", "test-token")
        monkeypatch.setenv("ARBITRUM_RPC_URL", "https://example.invalid")

        def boom(*args, **kwargs):
            raise RuntimeError("hypersync exploded")

        monkeypatch.setattr(mod, "_collect_ticks_sync", boom)

        result = mod.collect_and_save_ticks(
            markets=[],
            date_str="2026-09-10",
            ticks_dir=tmp_path / "ticks",
            tick_volume_dir=tmp_path / "tick_volume",
            futures_dir=tmp_path / "futures",
            checkpoint_path=tmp_path / "cp.json",
        )

        assert result["ticks"] == 0
        assert result["skipped"] is True

    def test_failure_does_not_advance_the_checkpoint(self, tmp_path, monkeypatch):
        """Advancing past a range that was never decoded would lose that
        window's volume permanently."""
        import scripts.collect_daily_snapshot as mod

        monkeypatch.setenv("HYPERSYNC_API_TOKEN", "test-token")
        monkeypatch.setenv("ARBITRUM_RPC_URL", "https://example.invalid")
        monkeypatch.setattr(
            mod, "_collect_ticks_sync", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("x"))
        )

        checkpoint = tmp_path / "cp.json"
        mod._write_tick_checkpoint(checkpoint, 1000)

        mod.collect_and_save_ticks(
            markets=[],
            date_str="2026-09-10",
            ticks_dir=tmp_path / "ticks",
            tick_volume_dir=tmp_path / "tick_volume",
            futures_dir=tmp_path / "futures",
            checkpoint_path=checkpoint,
        )

        assert mod._read_tick_checkpoint(checkpoint) == 1000
