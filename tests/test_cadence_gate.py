"""Behavioural tests for the release-time cadence gate.

These used to be YAML-substring assertions against ``release-data.yml``,
which is why the gate could ship a bug that suppressed exactly the defect
it exists to catch (a fresh outage in the newest 24h of data).  The logic
now lives in an importable module and is tested against real manifests.
"""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from gmx_historical_data.cadence_gate import (
    EXIT_ERROR,
    EXIT_OK,
    EXIT_REGRESSION,
    build_cadence_manifest,
    find_cadence_regressions,
    find_issue_number_for_marker,
    main,
    regression_marker,
)


def _entry(
    *,
    breaks: list[tuple[str, str, int]] = (),
    first: str = "2025-06-01T00:00:00+00:00",
    last: str = "2025-06-05T00:00:00+00:00",
    truncated: bool = False,
    breaks_total: int | None = None,
) -> dict:
    """Build one manifest entry.

    :param breaks: ``(before, after, missing_bars)`` triples.
    :param first: Earliest timestamp in the file.
    :param last: Latest timestamp in the file.
    :param truncated: Whether the ``breaks`` list is capped.
    :param breaks_total: Override for the exact break count (truncated files).
    :returns: A manifest entry dict.
    """
    listed = [{"before": b, "after": a, "missing_bars": n} for b, a, n in breaks]
    return {
        "timeframe": "4h",
        "expected_interval_seconds": 14400,
        "rows": 100,
        "first": first,
        "last": last,
        "breaks_total": len(listed) if breaks_total is None else breaks_total,
        "missing_bars_total": sum(n for _, _, n in breaks),
        "truncated": truncated,
        "breaks": listed,
    }


def _manifest(**files: dict) -> dict:
    return {"generated_at": "2026-09-06T00:00:00+00:00", "files": files}


NAME = "BTC_USDC_USDC-4h-futures.feather"


def test_fresh_outage_after_the_baseline_tail_is_a_regression():
    """The #29 scenario, and the bug the first cut of this gate shipped.

    Releases run daily, so the baseline's ``last`` is always ~yesterday.
    Suppressing any gap at or after it suppressed *every* fresh outage --
    precisely the class of defect the gate exists to catch.
    """
    baseline = _manifest(**{NAME: _entry(last="2025-06-05T00:00:00+00:00")})
    current = _manifest(
        **{
            NAME: _entry(
                breaks=[("2025-06-05T04:00:00+00:00", "2025-06-05T16:00:00+00:00", 2)],
                last="2025-06-06T00:00:00+00:00",
            )
        }
    )

    regressions = find_cadence_regressions(baseline, current)

    assert [r.file for r in regressions] == [NAME]
    assert "2 bar(s) missing" in regressions[0].detail


def test_gap_that_appears_inside_already_published_history_is_a_regression():
    """History that already shipped contiguous must not sprout a hole."""
    baseline = _manifest(**{NAME: _entry()})
    current = _manifest(
        **{NAME: _entry(breaks=[("2025-06-02T16:00:00+00:00", "2025-06-03T00:00:00+00:00", 1)])}
    )

    assert len(find_cadence_regressions(baseline, current)) == 1


def test_inherited_break_is_not_a_regression():
    """82% of shipped files carry one; re-reporting them every night would
    make the gate meaningless."""
    gap = ("2025-06-02T16:00:00+00:00", "2025-06-03T00:00:00+00:00", 1)
    baseline = _manifest(**{NAME: _entry(breaks=[gap])})
    current = _manifest(**{NAME: _entry(breaks=[gap], last="2025-06-06T00:00:00+00:00")})

    assert find_cadence_regressions(baseline, current) == []


def test_first_ever_file_is_skipped():
    """A file the previous release never carried has no baseline to regress
    against -- including on the first run after rollout."""
    current = _manifest(
        **{NAME: _entry(breaks=[("2025-06-02T16:00:00+00:00", "2025-06-03T00:00:00+00:00", 1)])}
    )

    assert find_cadence_regressions({"files": {}}, current) == []
    assert find_cadence_regressions({}, current) == []


def test_baseline_entry_missing_keys_does_not_crash():
    """A malformed or legacy baseline entry must degrade to 'nothing to
    compare', never take the whole release down: the publish steps have no
    ``if: always()``, so a crash here would block the tarball too."""
    baseline = _manifest(**{NAME: {"rows": 100}})
    current = _manifest(
        **{NAME: _entry(breaks=[("2025-06-02T16:00:00+00:00", "2025-06-03T00:00:00+00:00", 1)])}
    )

    assert find_cadence_regressions(baseline, current) == []


def test_null_last_baseline_is_handled():
    baseline = _manifest(**{NAME: _entry(last=None)})
    current = _manifest(
        **{NAME: _entry(breaks=[("2025-06-02T16:00:00+00:00", "2025-06-03T00:00:00+00:00", 1)])}
    )

    assert len(find_cadence_regressions(baseline, current)) == 1


def test_current_entry_missing_breaks_is_treated_as_clean():
    baseline = _manifest(**{NAME: _entry()})
    current = _manifest(**{NAME: {"breaks_total": 0}})

    assert find_cadence_regressions(baseline, current) == []


@pytest.mark.parametrize("side", ["baseline", "current", "both"])
def test_truncated_files_use_the_same_no_growth_rule(side):
    """Both branches enforce one rule -- the break population must not grow.
    A truncated file can only check the count, because its list is capped,
    but the verdict must not differ from an untruncated file's."""
    gap = ("2025-06-02T16:00:00+00:00", "2025-06-03T00:00:00+00:00", 1)
    baseline_entry = _entry(breaks=[gap], truncated=side in {"baseline", "both"}, breaks_total=1)
    grown = _entry(breaks=[gap], truncated=side in {"current", "both"}, breaks_total=2)
    same = _entry(breaks=[gap], truncated=side in {"current", "both"}, breaks_total=1)

    assert (
        len(
            find_cadence_regressions(
                _manifest(**{NAME: baseline_entry}), _manifest(**{NAME: grown})
            )
        )
        == 1
    )
    assert (
        find_cadence_regressions(_manifest(**{NAME: baseline_entry}), _manifest(**{NAME: same}))
        == []
    )


def test_check_command_exit_codes_and_regression_file(tmp_path: Path):
    baseline_path = tmp_path / "baseline.json"
    manifest_path = tmp_path / "_cadence_manifest.json"
    regressions_path = tmp_path / "regressions.json"

    baseline_path.write_text(json.dumps(_manifest(**{NAME: _entry()})), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            _manifest(
                **{
                    NAME: _entry(
                        breaks=[("2025-06-05T04:00:00+00:00", "2025-06-05T16:00:00+00:00", 2)],
                        last="2025-06-06T00:00:00+00:00",
                    )
                }
            )
        ),
        encoding="utf-8",
    )

    argv = [
        "check",
        "--baseline",
        str(baseline_path),
        "--manifest",
        str(manifest_path),
        "--regressions",
        str(regressions_path),
    ]
    assert main(argv) == EXIT_REGRESSION
    recorded = json.loads(regressions_path.read_text(encoding="utf-8"))
    assert [r["file"] for r in recorded] == [NAME]

    # Same manifest as its own baseline: nothing new.
    assert (
        main(
            [
                "check",
                "--baseline",
                str(manifest_path),
                "--manifest",
                str(manifest_path),
                "--regressions",
                str(regressions_path),
            ]
        )
        == EXIT_OK
    )


def test_check_command_reports_infrastructure_failure_distinctly(tmp_path: Path):
    """Exit 1 (cannot run) must stay distinguishable from exit 2 (real
    regression): the workflow retries the former and ships-and-alerts on
    the latter."""
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps({"files": {}}), encoding="utf-8")

    assert (
        main(
            [
                "check",
                "--baseline",
                str(baseline_path),
                "--manifest",
                str(tmp_path / "missing.json"),
                "--regressions",
                str(tmp_path / "regressions.json"),
            ]
        )
        == EXIT_ERROR
    )


def test_annotate_command_stamps_regressed_from(tmp_path: Path):
    manifest_path = tmp_path / "_cadence_manifest.json"
    regressions_path = tmp_path / "regressions.json"
    manifest_path.write_text(json.dumps(_manifest(**{NAME: _entry()})), encoding="utf-8")
    regressions_path.write_text(
        json.dumps([{"file": NAME, "detail": "..."}, {"file": "GONE.feather", "detail": "..."}]),
        encoding="utf-8",
    )

    assert (
        main(
            [
                "annotate",
                "--manifest",
                str(manifest_path),
                "--regressions",
                str(regressions_path),
                "--previous-tag",
                "data-2026-09-05",
            ]
        )
        == EXIT_OK
    )

    files = json.loads(manifest_path.read_text(encoding="utf-8"))["files"]
    assert files[NAME]["regressed_from"] == "data-2026-09-05"
    assert "GONE.feather" not in files  # an entry that vanished is skipped, not invented


def test_issue_dedupe_matches_the_exact_marker_not_shared_tokens():
    """GitHub's search tokenises, so `4h`/`futures`/`feather`/`usdc` are
    shared across every symbol and relevance ranking could hand back another
    symbol's ticket. Matching the exact HTML marker cannot cross-contaminate."""
    issues = [
        {
            "number": 41,
            "body": f"unrelated {regression_marker('ETH_USDC_USDC-4h-futures.feather')}",
        },
        {"number": 42, "body": f"here it is {regression_marker(NAME)}"},
    ]

    assert find_issue_number_for_marker(issues, regression_marker(NAME)) == 42
    assert (
        find_issue_number_for_marker(issues, regression_marker("SOL_USDC_USDC-1h-futures.feather"))
        is None
    )
    assert find_issue_number_for_marker([{"number": 1}], regression_marker(NAME)) is None


def _write_feather(path: Path, hour_offsets: list[int]) -> None:
    base = datetime(2025, 6, 2, tzinfo=UTC)
    dates = [base + timedelta(hours=h) for h in hour_offsets]
    n = len(dates)
    pl.DataFrame(
        {
            "date": pl.Series("date", dates, dtype=pl.Datetime("ns", "UTC")),
            "open": pl.Series("open", [1.0] * n, dtype=pl.Float64),
            "high": pl.Series("high", [1.0] * n, dtype=pl.Float64),
            "low": pl.Series("low", [1.0] * n, dtype=pl.Float64),
            "close": pl.Series("close", [1.0] * n, dtype=pl.Float64),
            "volume": pl.Series("volume", [0.0] * n, dtype=pl.Float64),
        }
    ).write_ipc(path)


def test_build_cadence_manifest_reads_every_futures_feather(tmp_path: Path):
    _write_feather(tmp_path / "BTC_USDC_USDC-4h-futures.feather", [0, 4, 12, 16])
    _write_feather(tmp_path / "ETH_USDC_USDC-4h-futures.feather", [0, 4, 8])
    _write_feather(tmp_path / "ETH_USDC_USDC-4h-mark.feather", [0, 8])  # not a candle file

    entries = build_cadence_manifest(tmp_path)

    assert set(entries) == {
        "BTC_USDC_USDC-4h-futures.feather",
        "ETH_USDC_USDC-4h-futures.feather",
    }
    assert entries["BTC_USDC_USDC-4h-futures.feather"]["breaks_total"] == 1
    assert entries["ETH_USDC_USDC-4h-futures.feather"]["breaks_total"] == 0


def test_build_cadence_manifest_skips_an_unreadable_feather(tmp_path: Path):
    """One corrupt or legacy file must not abort manifest generation for the
    other 700 -- 'record, don't reject', same as the export path."""
    _write_feather(tmp_path / "BTC_USDC_USDC-4h-futures.feather", [0, 4, 12])
    (tmp_path / "BAD_USDC_USDC-4h-futures.feather").write_bytes(b"not a feather")

    entries = build_cadence_manifest(tmp_path)

    assert set(entries) == {"BTC_USDC_USDC-4h-futures.feather"}
