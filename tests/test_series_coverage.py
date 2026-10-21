"""Per-pair hourly funding/mark coverage gate (listing/delisting bounds,
unknown direction, per-market stale tail, set-level liveness)."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

from gmx_historical_data.series_coverage import (
    FUNDING_EPOCH,
    SeriesThresholds,
    TailStatus,
    assess_series_coverage,
)
from tests._funding_release_fixtures import (
    HOUR,
    hours,
    write_checkpoint,
    write_companion,
    write_futures,
)

NOW = datetime(2026, 10, 18, 2, 20, tzinfo=UTC)
CUTOFF = datetime(2026, 10, 18, 1, 0, tzinfo=UTC)  # floor(now) - 1h
LONG_AGO = datetime(2025, 6, 1, tzinfo=UTC)


class Lake:
    """A futures dir, a staged companion dir and checkpoints, built per test."""

    def __init__(self, tmp: Path) -> None:
        self.futures = tmp / "futures"
        self.stage = tmp / "stage"
        self.cp = tmp / "cp"
        self.export_report: dict = {"failures": [], "reconciliations": {}}
        write_checkpoint(self.cp, "funding_factor_checkpoint.json", "funding_factor_all", NOW)
        write_checkpoint(self.cp, "fee_per_size_checkpoint.json", "fee_per_size_all", NOW)

    def pair(
        self,
        symbol: str,
        *,
        futures_start: datetime = LONG_AGO,
        futures_end: datetime = CUTOFF,
        funding_start: datetime | None = None,
        funding_end: datetime | None = CUTOFF,
        funding_skip: tuple[datetime, ...] = (),
        mark: bool = True,
    ) -> None:
        count = int((futures_end - futures_start) / HOUR) + 1
        stamps = write_futures(self.futures, symbol, futures_start, count)
        if mark:
            write_companion(self.stage, symbol, "mark", stamps)
        if funding_end is not None:
            start = funding_start or max(futures_start, FUNDING_EPOCH)
            f_stamps = [
                s
                for s in hours(start, int((funding_end - start) / HOUR) + 1)
                if s not in funding_skip
            ]
            write_companion(self.stage, symbol, "funding_rate", f_stamps)

    def assess(self, **overrides):
        kwargs = dict(
            futures_dir=self.futures,
            companion_dir=self.stage,
            checkpoint_dir=self.cp,
            delisted=set(),
            now=NOW,
            state="ready",
            extract_outcome="success",
            export_report=self.export_report,
            previous=None,
            thresholds=SeriesThresholds(),
        )
        kwargs.update(overrides)
        return assess_series_coverage(**kwargs)


def by_symbol(report):
    return {p.symbol: p for p in report.pairs}


def codes(pair) -> set[str]:
    return {b.split(":")[0] for b in pair.blockers}


def test_clean_lake_passes(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC")
    report = lake.assess()
    assert report.passed, report.set_blockers
    pair = by_symbol(report)["BTC"]
    assert pair.tail is TailStatus.CURRENT
    assert pair.blockers == ()
    assert pair.pre_epoch  # candles start before the funding epoch


def test_liquid_market_cannot_mask_a_stale_quiet_one(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC")
    lake.pair("MOODENG", funding_end=CUTOFF - timedelta(days=23))
    lake.pair("WIF", funding_end=CUTOFF - timedelta(hours=20))
    report = lake.assess()
    pairs = by_symbol(report)
    assert pairs["BTC"].tail is TailStatus.CURRENT
    assert pairs["WIF"].tail is TailStatus.LAGGING
    assert pairs["MOODENG"].tail is TailStatus.STALE
    assert pairs["MOODENG"].tail_age_hours == 23 * 24
    # v1: stale tails are loud but do not withhold the release.
    assert report.passed


def test_stale_tail_blocks_when_the_switch_is_on(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC")
    lake.pair("MOODENG", funding_end=CUTOFF - timedelta(days=23))
    report = lake.assess(thresholds=SeriesThresholds(stale_tail_blocks=True))
    assert not report.passed
    assert "B7" in codes(by_symbol(report)["MOODENG"])
    assert by_symbol(report)["BTC"].blockers == ()


def test_pair_that_goes_stale_since_the_previous_release_is_flagged(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC")
    lake.pair("OP", funding_end=CUTOFF - timedelta(days=5))
    lake.pair("MOODENG", funding_end=CUTOFF - timedelta(days=23))
    previous = {"pairs": {"OP": {"tail": "CURRENT"}, "MOODENG": {"tail": "STALE"}}}
    pairs = by_symbol(lake.assess(previous=previous))
    assert pairs["OP"].newly_stale
    assert not pairs["MOODENG"].newly_stale


def test_missing_funding_file_blocks_an_active_pair(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC")
    lake.pair("ETH", funding_end=None)
    report = lake.assess()
    assert not report.passed
    assert "B1" in codes(by_symbol(report)["ETH"])


def test_fresh_listing_is_given_a_grace_period(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC")
    lake.pair("NEW", futures_start=CUTOFF - timedelta(days=3), funding_end=None)
    report = lake.assess()
    assert report.passed
    assert by_symbol(report)["NEW"].pending


def test_funding_that_starts_long_after_listing_blocks(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    listed = datetime(2026, 8, 1, tzinfo=UTC)
    lake.pair("LATE", futures_start=listed, funding_start=listed + timedelta(days=20))
    report = lake.assess()
    assert "B2" in codes(by_symbol(report)["LATE"])


def test_funding_starting_at_the_listing_date_passes_despite_earlier_candles(
    tmp_path: Path,
) -> None:
    # AR-like: GMX's price API serves candles from before the market is listed.
    lake = Lake(tmp_path)
    listed = datetime(2026, 1, 9, tzinfo=UTC)
    lake.pair(
        "AR",
        futures_start=datetime(2025, 12, 3, tzinfo=UTC),
        funding_start=datetime(2026, 1, 9, 10, tzinfo=UTC),
    )
    assert "B2" in codes(by_symbol(lake.assess())["AR"])  # the first-candle bound alone fails
    pair = by_symbol(lake.assess(listing_dates={"AR": listed}))["AR"]
    assert pair.blockers == ()
    assert pair.listing_date == listed


def test_funding_within_grace_of_a_recent_listing_passes(tmp_path: Path) -> None:
    # QQQ-like: listed 2026-08-21, funding from 2026-08-25 09:00 (inside the 7d grace).
    lake = Lake(tmp_path)
    listed = datetime(2026, 8, 21, tzinfo=UTC)
    lake.pair(
        "QQQ",
        futures_start=datetime(2026, 8, 1, tzinfo=UTC),
        funding_start=datetime(2026, 8, 25, 9, tzinfo=UTC),
    )
    report = lake.assess(listing_dates={"QQQ": listed})
    assert report.passed, report.reasons


def test_funding_late_after_the_listing_date_still_blocks(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    listed = datetime(2026, 8, 1, tzinfo=UTC)
    lake.pair(
        "LATE",
        futures_start=datetime(2026, 7, 1, tzinfo=UTC),
        funding_start=listed + timedelta(days=7, hours=1),
    )
    pair = by_symbol(lake.assess(listing_dates={"LATE": listed}))["LATE"]
    assert "B2" in codes(pair)
    assert "listing date 2026-08-01" in " ".join(pair.blockers)


def test_without_a_listing_date_the_first_futures_bar_is_the_bound(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    start = datetime(2026, 8, 1, tzinfo=UTC)
    lake.pair("NOLIST", futures_start=start, funding_start=start + timedelta(days=20))
    # Unrelated and unparseable-empty entries do not leak into this pair.
    pair = by_symbol(lake.assess(listing_dates={"OTHER": datetime(2026, 8, 20, tzinfo=UTC)}))[
        "NOLIST"
    ]
    assert "B2" in codes(pair)
    assert pair.listing_date is None
    assert "first futures bar" in " ".join(pair.blockers)


def test_listing_symbols_match_case_insensitively(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    listed = datetime(2026, 1, 9, tzinfo=UTC)
    lake.pair(
        "kPEPE",
        futures_start=datetime(2025, 12, 3, tzinfo=UTC),
        funding_start=datetime(2026, 1, 9, 10, tzinfo=UTC),
    )
    assert lake.assess(listing_dates={"KPEPE": listed}).passed


def test_a_listing_date_before_the_epoch_is_bounded_by_the_epoch(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair(
        "OLD",
        futures_start=datetime(2024, 1, 1, tzinfo=UTC),
        funding_start=FUNDING_EPOCH + timedelta(days=8),
    )
    pair = by_symbol(lake.assess(listing_dates={"OLD": datetime(2024, 1, 1, tzinfo=UTC)}))["OLD"]
    assert "B2" in codes(pair)


def test_listing_date_governs_the_recently_listed_grace(tmp_path: Path) -> None:
    # Old candles, but GMX listed the market 3 days ago and no funding exists yet.
    lake = Lake(tmp_path)
    lake.pair("BTC")
    lake.pair("NEW", futures_start=CUTOFF - timedelta(days=30), funding_end=None)
    assert "B1" in codes(by_symbol(lake.assess())["NEW"])
    report = lake.assess(listing_dates={"NEW": CUTOFF - timedelta(days=3)})
    assert report.passed, report.reasons
    assert by_symbol(report)["NEW"].pending


def test_funding_may_start_at_the_epoch_for_older_listings(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC", futures_start=datetime(2023, 1, 1, tzinfo=UTC))
    report = lake.assess()
    assert report.passed
    assert by_symbol(report)["BTC"].first_bar == FUNDING_EPOCH


def test_recent_interior_hole_blocks_but_an_old_one_is_only_reported(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("RECENT", funding_skip=(CUTOFF - timedelta(days=3),))
    lake.pair("OLD", funding_skip=(datetime(2025, 12, 1, 5, tzinfo=UTC),))
    pairs = by_symbol(lake.assess())
    assert "B3" in codes(pairs["RECENT"])
    assert pairs["RECENT"].holes_recent == 1
    assert pairs["OLD"].blockers == ()
    assert pairs["OLD"].holes_older == 1


def test_unknown_direction_hours_in_the_recent_window_block(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("OP")
    stamp = (CUTOFF - timedelta(days=2)).isoformat()
    lake.export_report["reconciliations"]["OP"] = {
        "missing_hours": [stamp],
        "unknown_direction_hours": [stamp],
        "null_rate_hours": [],
    }
    assert "B3" in codes(by_symbol(lake.assess())["OP"])


def test_non_finite_funding_value_blocks(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC")
    stamps = hours(max(LONG_AGO, FUNDING_EPOCH), int((CUTOFF - FUNDING_EPOCH) / HOUR) + 1)
    write_companion(lake.stage, "BTC", "funding_rate", stamps, value=float("nan"))
    assert "B6" in codes(by_symbol(lake.assess())["BTC"])


def test_mark_must_match_the_futures_candles_row_for_row(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("NOMARK", mark=False)
    lake.pair("SHORT")
    stamps = hours(LONG_AGO, 100)  # truncated mark
    write_companion(lake.stage, "SHORT", "mark", stamps)
    pairs = by_symbol(lake.assess())
    assert "B5" in codes(pairs["NOMARK"])
    assert "B5" in codes(pairs["SHORT"])


def test_delisted_pair_never_blocks_on_funding_but_is_listed(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC")
    lake.pair(
        "OM",
        futures_end=CUTOFF - timedelta(days=30),
        funding_end=CUTOFF - timedelta(days=40),
        funding_skip=(CUTOFF - timedelta(days=60),),
    )
    report = lake.assess(delisted={"OM"})
    om = by_symbol(report)["OM"]
    assert report.passed
    assert om.delisted and om.blockers == () and om.tail is None


def test_missing_checkpoint_or_lagging_head_blocks_the_set(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC")
    write_checkpoint(
        lake.cp, "fee_per_size_checkpoint.json", "fee_per_size_all", NOW - timedelta(hours=9)
    )
    report = lake.assess()
    assert not report.passed
    assert any(b.startswith("B4") for b in report.set_blockers)

    (lake.cp / "funding_factor_checkpoint.json").unlink()
    assert any("funding_factor" in b for b in lake.assess().set_blockers)


def test_failed_extraction_state_or_export_blocks_the_set(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC")
    assert any(b.startswith("B4") for b in lake.assess(extract_outcome="failure").set_blockers)
    assert any(b.startswith("B0") for b in lake.assess(state="absent").set_blockers)
    lake.export_report["failures"] = [{"symbol": "BTC", "timeframe": "1h", "reason": "OSError"}]
    assert "B1" in codes(by_symbol(lake.assess())["BTC"])


def test_mark_export_failure_is_reported_as_b5(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC")
    lake.export_report["mark_failures"] = [
        {"symbol": "BTC", "timeframe": "1h", "reason": "OSError"}
    ]

    pair = by_symbol(lake.assess())["BTC"]
    assert "B5" in codes(pair)
    assert "B1" not in codes(pair)


def test_newest_funding_bar_far_behind_cutoff_blocks_the_set(tmp_path: Path) -> None:
    lake = Lake(tmp_path)
    lake.pair("BTC", funding_end=CUTOFF - timedelta(days=2))
    report = lake.assess()
    assert any(b.startswith("B4") and "newest" in b for b in report.set_blockers)
