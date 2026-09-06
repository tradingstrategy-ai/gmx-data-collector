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
    load_exempt_symbols,
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

    entries = build_cadence_manifest(tmp_path).entries

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

    entries = build_cadence_manifest(tmp_path).entries

    assert set(entries) == {"BTC_USDC_USDC-4h-futures.feather"}


# --------------------------------------------------------------------------
# Delisted / relisted markets
# --------------------------------------------------------------------------

MEGA = "MEGA_USDC_USDC-1h-futures.feather"


def test_relisting_seam_on_a_delisted_market_is_not_a_regression():
    """A delisted market's history freezes; when it relists, collection
    resumes and leaves one huge legitimate seam (MEGA 1h, 213 bars -- see
    PR #23, which deliberately retains delisted market history). The
    existing `Validate futures candle file integrity` step already exempts
    these files via delisted_markets.json; the cadence gate must use the
    same roster and the same symbol key rather than filing a false alarm."""
    baseline = _manifest(**{MEGA: _entry(last="2025-06-05T00:00:00+00:00")})
    current = _manifest(
        **{
            MEGA: _entry(
                breaks=[("2025-06-05T00:00:00+00:00", "2025-06-13T21:00:00+00:00", 213)],
                last="2025-06-14T00:00:00+00:00",
            )
        }
    )

    assert find_cadence_regressions(baseline, current, exempt_symbols={"MEGA"}) == []
    # Without the exemption it is a regression -- the rule itself is unchanged.
    assert len(find_cadence_regressions(baseline, current)) == 1


def test_exempt_symbols_match_the_roster_case_and_key():
    """The roster stores bare symbols; the manifest is keyed by filename.
    Lookup must extract the symbol the same way the integrity step does."""
    baseline = _manifest(**{MEGA: _entry()})
    current = _manifest(
        **{MEGA: _entry(breaks=[("2025-06-02T16:00:00+00:00", "2025-06-03T00:00:00+00:00", 1)])}
    )

    assert find_cadence_regressions(baseline, current, exempt_symbols={"mega"}) == []
    assert len(find_cadence_regressions(baseline, current, exempt_symbols={"BTC"})) == 1


def test_load_exempt_symbols_unions_every_roster(tmp_path: Path):
    """The live roster is rewritten BEFORE collection, and a relisted symbol
    drops off it at that moment -- which is precisely the run whose seam gap
    needs exempting. The previous release's roster is unioned in so the
    relist day is covered exactly once."""
    live = tmp_path / "delisted_markets.json"
    live.write_text(json.dumps({"symbols": ["OM"], "updated": "2026-09-06"}), encoding="utf-8")
    baseline = tmp_path / "delisted-baseline.json"
    baseline.write_text(json.dumps({"symbols": ["mega"]}), encoding="utf-8")

    assert load_exempt_symbols([live, baseline, tmp_path / "absent.json"]) == {"OM", "MEGA"}


def test_load_exempt_symbols_survives_a_malformed_roster(tmp_path: Path):
    bad = tmp_path / "delisted_markets.json"
    bad.write_text("{not json", encoding="utf-8")

    assert load_exempt_symbols([bad]) == set()


def test_check_command_honours_the_delisted_roster(tmp_path: Path):
    baseline_path = tmp_path / "baseline.json"
    manifest_path = tmp_path / "_cadence_manifest.json"
    roster_path = tmp_path / "delisted_markets.json"

    baseline_path.write_text(json.dumps(_manifest(**{MEGA: _entry()})), encoding="utf-8")
    manifest_path.write_text(
        json.dumps(
            _manifest(
                **{
                    MEGA: _entry(
                        breaks=[("2025-06-05T00:00:00+00:00", "2025-06-13T21:00:00+00:00", 213)],
                        last="2025-06-14T00:00:00+00:00",
                    )
                }
            )
        ),
        encoding="utf-8",
    )
    roster_path.write_text(json.dumps({"symbols": ["MEGA"]}), encoding="utf-8")

    argv = [
        "check",
        "--baseline",
        str(baseline_path),
        "--manifest",
        str(manifest_path),
        "--regressions",
        str(tmp_path / "regressions.json"),
        "--delisted-roster",
        str(roster_path),
    ]
    assert main(argv) == EXIT_OK
    assert json.loads((tmp_path / "regressions.json").read_text(encoding="utf-8")) == []


# --------------------------------------------------------------------------
# Timestamp normalisation
# --------------------------------------------------------------------------


def test_the_same_instant_serialised_differently_is_not_a_regression():
    """Raw ISO-string equality would mass-false-positive every inherited
    break at once if the serialisation ever changed (`Z` vs `+00:00`, or a
    precision change), and there is no second filter left to catch that."""
    baseline = _manifest(
        **{NAME: _entry(breaks=[("2025-06-02T16:00:00+00:00", "2025-06-03T00:00:00+00:00", 1)])}
    )
    current = _manifest(
        **{NAME: _entry(breaks=[("2025-06-02T16:00:00Z", "2025-06-03T00:00:00.000000Z", 1)])}
    )

    assert find_cadence_regressions(baseline, current) == []


def test_unparseable_timestamps_fall_back_to_string_comparison():
    baseline = _manifest(**{NAME: _entry(breaks=[("not-a-date", "also-not", 1)])})
    current = _manifest(**{NAME: _entry(breaks=[("not-a-date", "also-not", 1)])})

    assert find_cadence_regressions(baseline, current) == []


# --------------------------------------------------------------------------
# Stale-entry hygiene
# --------------------------------------------------------------------------


def test_an_unreadable_feather_is_dropped_from_the_manifest_not_carried_forward(tmp_path: Path):
    """write_cadence_manifest merges rather than replaces, so a skipped file
    would otherwise keep yesterday's entry under today's `generated_at` --
    claiming "checked today" for a file nobody checked. The README's
    contract is that absence means not-checked, so the entry is dropped."""
    from gmx_historical_data.freqtrade_exporter import CADENCE_MANIFEST_NAME

    _write_feather(tmp_path / "BTC_USDC_USDC-4h-futures.feather", [0, 4, 8])
    _write_feather(tmp_path / "BAD_USDC_USDC-4h-futures.feather", [0, 4, 8])

    assert main(["build", "--futures-dir", str(tmp_path)]) == EXIT_OK
    manifest_path = tmp_path / CADENCE_MANIFEST_NAME
    assert (
        "BAD_USDC_USDC-4h-futures.feather"
        in json.loads(manifest_path.read_text(encoding="utf-8"))["files"]
    )

    # Now it goes corrupt: the stale entry must not survive as if fresh.
    (tmp_path / "BAD_USDC_USDC-4h-futures.feather").write_bytes(b"not a feather")
    assert main(["build", "--futures-dir", str(tmp_path)]) == EXIT_OK

    files = json.loads(manifest_path.read_text(encoding="utf-8"))["files"]
    assert "BTC_USDC_USDC-4h-futures.feather" in files
    assert "BAD_USDC_USDC-4h-futures.feather" not in files


def test_build_reports_which_files_it_skipped(tmp_path: Path):
    _write_feather(tmp_path / "BTC_USDC_USDC-4h-futures.feather", [0, 4])
    (tmp_path / "BAD_USDC_USDC-4h-futures.feather").write_bytes(b"not a feather")

    scan = build_cadence_manifest(tmp_path)

    assert set(scan.entries) == {"BTC_USDC_USDC-4h-futures.feather"}
    assert scan.skipped == {"BAD_USDC_USDC-4h-futures.feather"}
