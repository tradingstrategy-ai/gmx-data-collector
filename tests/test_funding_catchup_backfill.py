"""``funding-catchup.yml`` ``datastore_backfill``: the opt-in genesis -> epoch DataStore backfill (#47)."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import yaml

from gmx_historical_data import datastore_tail, funding_catchup
from gmx_historical_data.funding_state import funding_network_dir

CATCHUP = Path(".github/workflows/funding-catchup.yml")
NOW = datetime(2026, 10, 19, 12, 30, tzinfo=UTC)


def _doc() -> dict:
    return yaml.safe_load(CATCHUP.read_text())


def _steps() -> list[dict]:
    return _doc()["jobs"]["catchup"]["steps"]


def _step(name: str) -> dict:
    (match,) = [s for s in _steps() if s.get("name") == name]
    return match


# ---------------------------------------------------------------------------
# Workflow shape
# ---------------------------------------------------------------------------


def test_dispatch_has_an_optional_boolean_backfill_input() -> None:
    inputs = _doc()[True]["workflow_dispatch"]["inputs"]
    assert set(inputs) == {"datastore_backfill"}
    assert inputs["datastore_backfill"]["type"] == "boolean"
    assert inputs["datastore_backfill"]["default"] is False
    assert not inputs["datastore_backfill"].get("required")


def test_backfill_runs_only_when_asked_and_after_the_hypersync_catchup() -> None:
    step = _step("Backfill DataStore history")
    assert "datastore_backfill" in step["if"] and "'true'" in step["if"]
    names = [s.get("name") for s in _steps()]
    assert (
        names.index("Extract funding to chain head")
        < names.index("Backfill DataStore history")
        < names.index("Report catch-up")
        < names.index("Upload advanced funding state")
    )
    assert set(step["env"]) == {"JSON_RPC_ARBITRUM"}


def test_backfill_is_resumable_and_stops_ahead_of_the_job_cap() -> None:
    run = re.sub(r"\s+", " ", _step("Backfill DataStore history")["run"].replace("\\\n", " "))
    assert "extract_funding_datastore.py --hourly-backfill --resume" in run
    assert "--output-dir user_data/data/gmx/funding" in run
    # 325 minutes after the job began: 35 of the 360 stay free for the report and upload.
    assert '--deadline-epoch "$((JOB_START_EPOCH + 325 * 60))"' in run
    assert _doc()["jobs"]["catchup"]["timeout-minutes"] == 360
    assert _step("Backfill DataStore history")["timeout-minutes"] < 360
    start = _step("Record job start")
    assert "JOB_START_EPOCH" in start["run"]
    assert _steps()[0]["name"] == "Record job start"


def test_backfill_always_merges_and_then_returns_the_extractors_exit_code() -> None:
    run = _step("Backfill DataStore history")["run"]
    assert "|| rc=$?" in run
    assert run.index("--hourly-backfill") < run.index("--merge-only") < run.index('exit "${rc}"')


def test_existing_steps_are_untouched() -> None:
    """Another change edits Report/upload; this one adds only inputs and steps."""
    extract = _step("Extract funding to chain head")
    assert extract["timeout-minutes"] == 330 and extract["continue-on-error"] is True
    report = _step("Report catch-up")
    assert report["if"] == "always()" and report["id"] == "catchup_report"
    upload = _step("Upload advanced funding state")
    assert "steps.catchup_report.outputs.advanced == 'true'" in upload["if"]


# ---------------------------------------------------------------------------
# The report counts backfill progress as progress
# ---------------------------------------------------------------------------


def _gmx_root(tmp_path: Path, factor: int = 100, fee: int = 100) -> Path:
    cp = funding_network_dir(tmp_path) / "checkpoints"
    cp.mkdir(parents=True)
    for name, symbol, block in (
        ("funding_factor_checkpoint.json", "funding_factor_all", factor),
        ("fee_per_size_checkpoint.json", "fee_per_size_all", fee),
    ):
        (cp / name).write_text(
            json.dumps(
                {
                    "symbol": symbol,
                    "last_block": block,
                    "last_timestamp": int(NOW.timestamp()),
                }
            )
        )
    return tmp_path


def _backfill_to(gmx_root: Path, last_hour: int, last_block: int, target: int = 10_000 * 3600):
    datastore_tail.write_backfill_checkpoint(
        funding_network_dir(gmx_root) / "checkpoints",
        first_hour=0,
        last_hour=last_hour,
        last_block=last_block,
        target_hour=target,
    )


def test_a_first_backfill_chunk_is_an_advance_even_if_hypersync_did_not_move(
    tmp_path: Path,
) -> None:
    gmx_root = _gmx_root(tmp_path)
    before = funding_catchup.snapshot(gmx_root)
    assert "datastore_backfill" not in before

    _backfill_to(gmx_root, 500 * 3600, 5_000)

    after = funding_catchup.snapshot(gmx_root)
    assert after["datastore_backfill"]["last_block"] == 5_000
    assert funding_catchup.has_advanced(before, after)


def test_a_later_chunk_advances_and_a_regression_is_refused(tmp_path: Path) -> None:
    gmx_root = _gmx_root(tmp_path)
    _backfill_to(gmx_root, 500 * 3600, 5_000)
    first = funding_catchup.snapshot(gmx_root)

    _backfill_to(gmx_root, 900 * 3600, 9_000)
    second = funding_catchup.snapshot(gmx_root)
    assert funding_catchup.has_advanced(first, second)

    assert not funding_catchup.has_advanced(second, first)  # never upload an older position
    assert not funding_catchup.has_advanced(second, second)  # nothing moved


def test_a_vanished_backfill_checkpoint_is_not_a_newer_state(tmp_path: Path) -> None:
    gmx_root = _gmx_root(tmp_path)
    _backfill_to(gmx_root, 500 * 3600, 5_000)
    before = funding_catchup.snapshot(gmx_root)
    (funding_network_dir(gmx_root) / "checkpoints" / datastore_tail.BACKFILL_CHECKPOINT).unlink()

    assert not funding_catchup.has_advanced(before, funding_catchup.snapshot(gmx_root))


def test_hypersync_progress_alone_still_counts(tmp_path: Path) -> None:
    gmx_root = _gmx_root(tmp_path)
    before = funding_catchup.snapshot(gmx_root)
    _gmx_root_advance = _gmx_root  # noqa: F841 - clarity: rewrite the factor checkpoint
    cp = funding_network_dir(gmx_root) / "checkpoints" / "funding_factor_checkpoint.json"
    data = json.loads(cp.read_text())
    data["last_block"] = 200
    cp.write_text(json.dumps(data))

    assert funding_catchup.has_advanced(before, funding_catchup.snapshot(gmx_root))


def test_the_report_says_where_the_backfill_is(tmp_path: Path) -> None:
    gmx_root = _gmx_root(tmp_path)
    before = funding_catchup.snapshot(gmx_root)
    _backfill_to(gmx_root, 500 * 3600, 5_000)

    report = funding_catchup.build_report(before, gmx_root, now=NOW, extract_outcome="success")

    assert report.advanced
    assert "datastore_backfill" in report.markdown
    assert "DataStore backfill: in progress" in report.markdown

    _backfill_to(gmx_root, 10_000 * 3600, 99_999)
    done = funding_catchup.build_report(before, gmx_root, now=NOW, extract_outcome="success")
    assert "DataStore backfill: **complete**" in done.markdown


def test_a_run_without_a_backfill_reports_exactly_as_before(tmp_path: Path) -> None:
    gmx_root = _gmx_root(tmp_path)
    before = funding_catchup.snapshot(gmx_root)

    report = funding_catchup.build_report(before, gmx_root, now=NOW, extract_outcome="success")

    assert "datastore_backfill" not in report.markdown
    assert "backfill" not in report.markdown.lower()
