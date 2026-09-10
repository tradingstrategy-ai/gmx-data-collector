"""Tests for the daily tick-collection phase.

This phase is the only part of the snapshot that needs HyperSync and an RPC.
Every other phase runs off the public REST API, so a missing key or a
HyperSync outage must degrade to "no volume today" rather than failing the
release -- candles, snapshots, tickers and APY are worth shipping without it.
"""

import json


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
