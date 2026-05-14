# tests/test_coverage_gate.py
"""Tests for the shared coverage gate."""

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest


class TestSkipDecision:
    def test_dataclass_is_frozen(self):
        from gmx_historical_data.coverage_gate import SkipDecision

        d = SkipDecision(skip=True, reason="current", existing_rows=10, expected_min_rows=5)
        with pytest.raises(Exception):
            d.skip = False  # type: ignore[misc]


class TestIsCurrent:
    """is_current() — parquet metadata check for daily-stamped files."""

    def _write_parquet(self, path: Path, rows: int) -> None:
        pl.DataFrame({"x": list(range(rows))}).write_parquet(str(path))

    def test_missing_file(self, tmp_path):
        from gmx_historical_data.coverage_gate import is_current

        decision = is_current(tmp_path / "nope.parquet", expected_min_rows=10)
        assert decision.skip is False
        assert decision.reason == "missing"
        assert decision.existing_rows == 0
        assert decision.expected_min_rows == 10

    def test_too_small(self, tmp_path):
        from gmx_historical_data.coverage_gate import is_current

        p = tmp_path / "small.parquet"
        self._write_parquet(p, rows=5)
        decision = is_current(p, expected_min_rows=10)
        assert decision.skip is False
        assert decision.reason == "too_small"
        assert decision.existing_rows == 5

    def test_exactly_min(self, tmp_path):
        from gmx_historical_data.coverage_gate import is_current

        p = tmp_path / "exact.parquet"
        self._write_parquet(p, rows=10)
        decision = is_current(p, expected_min_rows=10)
        assert decision.skip is True
        assert decision.reason == "current"

    def test_current(self, tmp_path):
        from gmx_historical_data.coverage_gate import is_current

        p = tmp_path / "ok.parquet"
        self._write_parquet(p, rows=135)
        decision = is_current(p, expected_min_rows=100)
        assert decision.skip is True
        assert decision.reason == "current"
        assert decision.existing_rows == 135

    def test_forced(self, tmp_path):
        from gmx_historical_data.coverage_gate import is_current

        p = tmp_path / "ok.parquet"
        self._write_parquet(p, rows=135)
        decision = is_current(p, expected_min_rows=100, force=True)
        assert decision.skip is False
        assert decision.reason == "forced"
        assert decision.existing_rows == 135

    def test_corrupt_file(self, tmp_path, caplog):
        import logging

        from gmx_historical_data.coverage_gate import is_current

        p = tmp_path / "corrupt.parquet"
        p.write_bytes(b"this is not parquet")
        with caplog.at_level(logging.WARNING, logger="gmx_historical_data.coverage_gate"):
            decision = is_current(p, expected_min_rows=10)
        assert decision.skip is False
        assert decision.reason == "missing"
        assert decision.existing_rows == 0
        assert any("failed to read" in r.message for r in caplog.records)
