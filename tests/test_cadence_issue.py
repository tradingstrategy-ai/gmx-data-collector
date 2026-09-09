"""One GitHub issue per cadence-regression incident, not one per file.

Issues #33–#42 (2026-09-09) were the same 108-file fleet outage filed ten
times, each body listing 50 other markets. The public seam is
``build_incident_issue`` / ``incident_marker`` / ``find_issue_number_for_marker``.
"""

from __future__ import annotations

import json
from pathlib import Path

from gmx_historical_data.cadence_issue import (
    build_incident_issue,
    find_issue_number_for_marker,
    incident_marker,
    main,
)

FILE_A = "0G_USDC_USDC-1m-futures.feather"
FILE_B = "AAVE_USDC_USDC-1m-futures.feather"
FILE_Z = "ZRO_USDC_USDC-1m-futures.feather"


def _regression(filename: str, missing_bars: int, before: str, after: str) -> dict:
    return {
        "file": filename,
        "detail": (
            f"{filename}: new cadence break - {missing_bars} bar(s) "
            f"missing between {before} and {after}"
        ),
    }


def _fleet(n: int = 108) -> list[dict]:
    """Alphabetically-sorted fake fleet, one break each — matches 2026-09-09."""
    names = [FILE_A, FILE_B] + [f"M{i:03d}_USDC_USDC-1m-futures.feather" for i in range(n - 3)]
    names.append(FILE_Z)
    assert len(names) == n
    return [
        _regression(name, 2, "2026-09-08T02:17:00+00:00", "2026-09-08T02:20:00+00:00")
        for name in names
    ]


def test_incident_marker_is_date_scoped_not_per_file() -> None:
    marker = incident_marker("2026-09-09")
    assert marker == "<!-- cadence-regression-incident:2026-09-09 -->"
    assert FILE_A not in marker
    assert "cadence-regression:" + FILE_A not in marker


def test_one_incident_one_title_with_file_count() -> None:
    issue = build_incident_issue(
        _fleet(108),
        previous_tag="data-2026-09-08",
        run_url="https://example.test/run/1",
        date="2026-09-09",
    )
    assert issue.title == "cadence regression — 2026-09-09 (108 files)"
    assert issue.file_count == 108
    assert issue.marker == incident_marker("2026-09-09")
    assert issue.marker in issue.body


def test_body_names_the_fleet_not_a_single_file_as_the_subject() -> None:
    issue = build_incident_issue(
        _fleet(108),
        previous_tag="data-2026-09-08",
        run_url="https://example.test/run/1",
        date="2026-09-09",
    )
    assert "108 files" in issue.body
    assert "`data-2026-09-08`" in issue.body
    assert "https://example.test/run/1" in issue.body
    assert "this file's data is unchanged" not in issue.body
    assert f"<!-- cadence-regression:{FILE_A} -->" not in issue.body


def test_body_lists_every_file_when_under_the_detail_cap() -> None:
    issue = build_incident_issue(
        [
            _regression(FILE_A, 2, "2026-09-08T02:17:00+00:00", "2026-09-08T02:20:00+00:00"),
            _regression(FILE_B, 2, "2026-09-08T02:17:00+00:00", "2026-09-08T02:20:00+00:00"),
        ],
        previous_tag="data-2026-09-08",
        run_url="https://example.test/run/1",
        date="2026-09-09",
    )
    assert FILE_A in issue.body
    assert FILE_B in issue.body
    assert "and " not in issue.body or "and 0 more" not in issue.body
    assert "more files" not in issue.body


def test_truncated_details_are_signposted() -> None:
    """A 108-file outage must not silently drop files 51–108."""
    issue = build_incident_issue(
        _fleet(108),
        previous_tag="data-2026-09-08",
        run_url="https://example.test/run/1",
        date="2026-09-09",
        max_detail_lines=50,
    )
    assert FILE_A in issue.body
    assert "and 58 more files" in issue.body
    assert FILE_Z not in issue.body  # sorted last; beyond the cap


def test_dedupe_matches_incident_marker_not_a_per_file_marker() -> None:
    marker = incident_marker("2026-09-09")
    issues = [
        {
            "number": 33,
            "body": f"cadence regression: {FILE_A}\n<!-- cadence-regression:{FILE_A} -->",
        },
        {"number": 99, "body": f"fleet outage\n{marker}"},
    ]
    assert find_issue_number_for_marker(issues, marker) == 99
    assert find_issue_number_for_marker(issues, incident_marker("2026-09-10")) is None


def test_format_cli_writes_title_body_marker(tmp_path: Path) -> None:
    regressions = tmp_path / "regressions.json"
    out = tmp_path / "incident.json"
    regressions.write_text(json.dumps(_fleet(3)), encoding="utf-8")

    assert (
        main(
            [
                "format",
                "--regressions",
                str(regressions),
                "--previous-tag",
                "data-2026-09-08",
                "--run-url",
                "https://example.test/run/1",
                "--date",
                "2026-09-09",
                "--out",
                str(out),
            ]
        )
        == 0
    )
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["title"] == "cadence regression — 2026-09-09 (3 files)"
    assert payload["marker"] == incident_marker("2026-09-09")
    assert payload["file_count"] == 3
    assert FILE_A in payload["body"]
