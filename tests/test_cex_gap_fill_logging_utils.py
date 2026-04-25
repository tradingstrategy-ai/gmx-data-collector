"""Tests for cex_gap_fill.logging_utils."""

import json
from pathlib import Path

from gmx_historical_data.cex_gap_fill.logging_utils import (
    RunSummary,
    make_run_id,
    write_summary_json,
)


def test_make_run_id_format():
    rid = make_run_id()
    assert len(rid) == len("YYYYMMDD_HHMMSS")
    assert rid[8] == "_"
    assert rid[:8].isdigit()
    assert rid[9:].isdigit()


def test_write_summary_json_schema(tmp_path: Path):
    s = RunSummary(
        run_id="20260424_101503",
        started_at="2026-04-24T10:15:03Z",
        finished_at="2026-04-24T10:20:00Z",
    )
    s.symbols_processed = 5
    s.symbols_skipped_no_cex = ["FART"]
    s.totals["full_replaced"] = 3
    out = tmp_path / "summary.json"
    write_summary_json(s, out)
    data = json.loads(out.read_text())
    assert data["run_id"] == "20260424_101503"
    assert data["symbols_processed"] == 5
    assert data["symbols_skipped_no_cex"] == ["FART"]
    assert data["totals"]["full_replaced"] == 3


def test_write_summary_json_creates_parent_dirs(tmp_path: Path):
    out = tmp_path / "nested" / "deep" / "summary.json"
    s = RunSummary(run_id="x", started_at="2026-01-01")
    write_summary_json(s, out)
    assert out.exists()
