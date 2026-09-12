"""Every data type the release ships must be accounted for, every run.

Two incidents in one week came from the same blind spot: a data type quietly
stopped being produced and nothing said so.

- The volume feature made ``hypersync`` a hard import, the release died, and
  two days shipped nothing (fixed in #48).
- Funding exports were retired as a variant, the canonical replacement was
  never backfilled, and the funding lake went 108 days stale while the release
  carried the old files forward verbatim (issue #47).

In both cases the release kept "succeeding". What was missing was an explicit
statement of what the bundle is *supposed* to contain, checked against what it
actually contains. That is what this module is.
"""

from datetime import UTC, datetime

import pytest

from gmx_historical_data.data_coverage import (
    DAILY_STAMPED_TYPES,
    CoverageStatus,
    assess_coverage,
    failing_entries,
    format_coverage_report,
)


def _stamp(root, data_type: str, date: str) -> None:
    """Create a daily-stamped parquet for a data type.

    :param root: ``user_data/data/gmx`` root.
    :param data_type: Directory name, e.g. ``snapshots``.
    :param date: ISO date used as the file stem.
    """
    d = root / data_type
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{date}.parquet").write_bytes(b"x")


def _all_current(root, today: str) -> None:
    """Populate every daily-stamped type with today's file."""
    for spec in DAILY_STAMPED_TYPES:
        _stamp(root, spec.name, today)


class TestAssessCoverage:
    def test_every_type_present_for_today_is_fresh(self, tmp_path):
        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")

        entries = assess_coverage(tmp_path, now=today)

        assert {e.name for e in entries} == {s.name for s in DAILY_STAMPED_TYPES}
        assert all(e.status is CoverageStatus.FRESH for e in entries)

    def test_a_type_with_no_directory_is_missing(self, tmp_path):
        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")
        for p in (tmp_path / "apy").iterdir():
            p.unlink()
        (tmp_path / "apy").rmdir()

        entries = {e.name: e for e in assess_coverage(tmp_path, now=today)}

        assert entries["apy"].status is CoverageStatus.MISSING
        assert entries["apy"].latest is None

    def test_an_empty_directory_is_missing_not_fresh(self, tmp_path):
        """A phase that created its output directory but wrote nothing is the
        exact shape of a silent failure -- it must not read as present."""
        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")
        for p in (tmp_path / "tickers").iterdir():
            p.unlink()

        entries = {e.name: e for e in assess_coverage(tmp_path, now=today)}

        assert entries["tickers"].status is CoverageStatus.MISSING

    def test_a_type_that_stopped_updating_is_stale(self, tmp_path):
        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")
        for p in (tmp_path / "volumes").iterdir():
            p.unlink()
        _stamp(tmp_path, "volumes", "2026-09-01")

        entries = {e.name: e for e in assess_coverage(tmp_path, now=today)}

        assert entries["volumes"].status is CoverageStatus.STALE
        assert entries["volumes"].age_days == 11

    def test_yesterday_is_fresh_for_ticks_but_stale_for_rest_phases(self, tmp_path):
        """The two kinds of type tolerate lag differently, and conflating them
        is what would let a missed day through.

        Tick outputs are keyed by the UTC date each *fill* happened and the
        02:00 window straddles midnight, so yesterday is normal. The REST
        phases are stamped ``date_str = now()`` taken when the run starts, so
        yesterday means today's write never happened -- and accepting it would
        only catch the second consecutive missed day.
        """
        today = datetime(2026, 9, 12, 2, 17, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-11")

        entries = {e.name: e for e in assess_coverage(tmp_path, now=today)}

        assert entries["ticks"].status is CoverageStatus.FRESH
        assert entries["tick_volume"].status is CoverageStatus.FRESH
        for name in ("snapshots", "tickers", "apy"):
            assert entries[name].status is CoverageStatus.STALE, name

    def test_a_single_missed_rest_day_blocks(self, tmp_path):
        """The whole point of the tighter tolerance: catch the *first* miss."""
        from gmx_historical_data.data_coverage import blocking_entries

        today = datetime(2026, 9, 12, 2, 17, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")
        for p in (tmp_path / "snapshots").iterdir():
            p.unlink()
        _stamp(tmp_path, "snapshots", "2026-09-11")

        blocking = blocking_entries(assess_coverage(tmp_path, now=today))

        assert [e.name for e in blocking] == ["snapshots"]

    def test_todays_stamp_is_fresh_for_every_type(self, tmp_path):
        today = datetime(2026, 9, 12, 2, 17, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")

        entries = assess_coverage(tmp_path, now=today)

        assert all(e.status is CoverageStatus.FRESH for e in entries)

    def test_unparseable_filenames_are_ignored_not_crashed_on(self, tmp_path):
        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")
        (tmp_path / "apy" / "_manifest.parquet").write_bytes(b"x")
        (tmp_path / "apy" / "not-a-date.parquet").write_bytes(b"x")

        entries = {e.name: e for e in assess_coverage(tmp_path, now=today)}

        assert entries["apy"].status is CoverageStatus.FRESH

    def test_macos_sidecars_are_ignored(self, tmp_path):
        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")
        (tmp_path / "apy" / "._2026-09-30.parquet").write_bytes(b"x")

        entries = {e.name: e for e in assess_coverage(tmp_path, now=today)}

        assert entries["apy"].latest.isoformat() == "2026-09-12"


class TestFormatCoverageReport:
    def test_report_names_every_type_and_its_state(self, tmp_path):
        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")

        text = format_coverage_report(assess_coverage(tmp_path, now=today))

        for spec in DAILY_STAMPED_TYPES:
            assert spec.name in text
        assert "FRESH" in text

    def test_report_flags_a_missing_type_loudly(self, tmp_path):
        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")
        for p in (tmp_path / "ticks").iterdir():
            p.unlink()

        text = format_coverage_report(assess_coverage(tmp_path, now=today))

        assert "MISSING" in text
        assert "ticks" in text


@pytest.mark.parametrize("spec", DAILY_STAMPED_TYPES, ids=lambda s: s.name)
def test_every_spec_describes_why_it_matters(spec):
    """A bare directory name is not enough for whoever reads a failing gate at
    02:00 UTC -- each type has to say what depends on it."""
    assert spec.description
    assert spec.max_age_days >= 0


class TestBlockingVsReported:
    """A type that is allowed to be absent must still be reported, but must
    not fail the release -- otherwise a HyperSync outage would undo the
    fail-soft contract the trade-tick phase depends on."""

    def test_degradable_type_is_reported_but_does_not_block(self, tmp_path):
        from gmx_historical_data.data_coverage import blocking_entries

        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")
        for p in (tmp_path / "ticks").iterdir():
            p.unlink()
        for p in (tmp_path / "tick_volume").iterdir():
            p.unlink()

        entries = assess_coverage(tmp_path, now=today)

        reported = {e.name for e in failing_entries(entries)}
        assert reported == {"ticks", "tick_volume"}
        assert blocking_entries(entries) == []

    def test_a_core_rest_phase_going_missing_does_block(self, tmp_path):
        from gmx_historical_data.data_coverage import blocking_entries

        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")
        for p in (tmp_path / "snapshots").iterdir():
            p.unlink()

        blocking = blocking_entries(assess_coverage(tmp_path, now=today))

        assert [e.name for e in blocking] == ["snapshots"]


class TestFutureStamps:
    """A date-stamped file from the future must not certify a type as fresh.

    Clock skew on a runner, or a hand-copied file, would otherwise let one
    bogus stamp mask months of genuinely stale data -- the exact silent
    staleness this module exists to catch.
    """

    def test_a_future_stamp_does_not_mask_stale_data(self, tmp_path):
        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-01-01")
        _stamp(tmp_path, "snapshots", "2099-01-01")

        entries = {e.name: e for e in assess_coverage(tmp_path, now=today)}

        assert entries["snapshots"].status is CoverageStatus.STALE
        assert entries["snapshots"].latest.isoformat() == "2026-01-01"
        assert entries["snapshots"].age_days == 254

    def test_only_future_stamps_reads_as_missing(self, tmp_path):
        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")
        for p in (tmp_path / "apy").iterdir():
            p.unlink()
        _stamp(tmp_path, "apy", "2099-01-01")

        entries = {e.name: e for e in assess_coverage(tmp_path, now=today)}

        assert entries["apy"].status is CoverageStatus.MISSING

    def test_future_stamps_are_counted_so_the_anomaly_is_visible(self, tmp_path):
        today = datetime(2026, 9, 12, tzinfo=UTC)
        _all_current(tmp_path, "2026-09-12")
        _stamp(tmp_path, "tickers", "2099-01-01")

        entries = {e.name: e for e in assess_coverage(tmp_path, now=today)}

        assert entries["tickers"].future_stamps == 1
        assert entries["tickers"].status is CoverageStatus.FRESH

    def test_a_naive_now_is_treated_as_utc(self, tmp_path):
        _all_current(tmp_path, "2026-09-12")

        entries = assess_coverage(tmp_path, now=datetime(2026, 9, 12))

        assert all(e.status is CoverageStatus.FRESH for e in entries)
