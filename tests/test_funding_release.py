"""Stage -> gate -> promote for the daily funding + mark companions."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from gmx_historical_data import funding_release
from gmx_historical_data.funding_state import (
    FundingState,
    assess_funding_state,
    seed_funding_state,
)
from gmx_historical_data.series_coverage import SeriesThresholds
from tests._funding_release_fixtures import (
    HOUR,
    hours,
    write_checkpoint,
    write_companion,
    write_futures,
    write_seed_source,
)

NOW = datetime(2026, 10, 18, 2, 20, tzinfo=UTC)
CUTOFF = datetime(2026, 10, 18, 1, 0, tzinfo=UTC)
EPOCH = datetime(2025, 8, 19, 12, tzinfo=UTC)


def _write_rates(gmx_root: Path, symbol: str, end: datetime, start: datetime = EPOCH) -> None:
    rates = gmx_root / "funding" / "arbitrum" / "rates" / symbol
    rates.mkdir(parents=True, exist_ok=True)
    stamps = hours(start, int((end - start) / HOUR) + 1)
    pl.DataFrame(
        {
            "timestamp": stamps,
            "funding_rate_hourly": [1e-6] * len(stamps),
            "longs_pay_shorts": [True] * len(stamps),
        }
    ).with_columns(pl.col("timestamp").dt.replace_time_zone("UTC")).write_parquet(
        rates / "1h.parquet"
    )


def _lake(tmp_path: Path, symbols=("BTC", "ETH")) -> Path:
    gmx_root = tmp_path / "gmx"
    futures = gmx_root / "futures"
    for symbol in symbols:
        n = int((CUTOFF - datetime(2026, 1, 1, tzinfo=UTC)) / HOUR) + 1
        write_futures(futures, symbol, datetime(2026, 1, 1, tzinfo=UTC), n)
        _write_rates(gmx_root, symbol, CUTOFF)
    # An alt-pool market and a deprecated alias: funding sources, not futures pairs.
    _write_rates(gmx_root, "AAVE_ETH-USDC", CUTOFF)
    _write_rates(gmx_root, "XAUT_(deprecated)", CUTOFF)
    cp = gmx_root / "funding" / "arbitrum" / "checkpoints"
    write_checkpoint(cp, "funding_factor_checkpoint.json", "funding_factor_all", NOW)
    write_checkpoint(cp, "fee_per_size_checkpoint.json", "fee_per_size_all", NOW)
    return gmx_root


def _tree_digest(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(root.rglob("*"))
        if p.is_file()
    }


def _gate(gmx_root: Path, stage: Path, **kw):
    kwargs = dict(state="ready", extract_outcome="success", now=NOW)
    kwargs.update(kw)
    return funding_release.run_gate(gmx_root, stage, **kwargs)


def test_stage_exports_only_futures_backed_symbols_and_leaves_futures_untouched(
    tmp_path: Path,
) -> None:
    gmx_root = _lake(tmp_path)
    before = _tree_digest(gmx_root / "futures")

    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    staged = sorted(p.name for p in (tmp_path / "stage" / "gmx" / "futures").glob("*.feather"))
    assert staged == [
        "BTC_USDC_USDC-1h-funding_rate.feather",
        "BTC_USDC_USDC-1h-mark.feather",
        "ETH_USDC_USDC-1h-funding_rate.feather",
        "ETH_USDC_USDC-1h-mark.feather",
    ]
    assert _tree_digest(gmx_root / "futures") == before


def test_stage_starts_from_the_published_companions_to_preserve_history(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path)
    older = hours(datetime(2025, 1, 1, tzinfo=UTC), 24)
    write_companion(gmx_root / "futures", "BTC", "funding_rate", older, value=0.5)

    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    staged = pl.read_ipc(
        tmp_path / "stage" / "gmx" / "futures" / "BTC_USDC_USDC-1h-funding_rate.feather"
    )
    assert staged["date"].min() == older[0]
    assert staged["date"].max() == CUTOFF


def test_passing_gate_promotes_staged_companions_and_writes_coverage(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    coverage = _gate(gmx_root, tmp_path / "stage")

    assert coverage["status"] == "PASS"
    assert coverage["refreshed"] is True
    futures = gmx_root / "futures"
    assert (futures / "BTC_USDC_USDC-1h-funding_rate.feather").exists()
    assert (futures / "ETH_USDC_USDC-1h-mark.feather").exists()
    on_disk = json.loads((gmx_root / "funding_coverage.json").read_text())
    assert on_disk["status"] == "PASS"
    assert on_disk["pairs"]["BTC"]["tail"] == "CURRENT"
    assert on_disk["pairs"]["BTC"]["shipped_funding_last_bar"] == CUTOFF.isoformat()


def test_withheld_keeps_last_known_good_companions_and_funding_state_exactly(
    tmp_path: Path,
) -> None:
    gmx_root = _lake(tmp_path)
    old = hours(datetime(2026, 10, 10, tzinfo=UTC), 24)
    write_companion(gmx_root / "futures", "BTC", "funding_rate", old)
    write_companion(gmx_root / "futures", "BTC", "mark", old)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")
    futures_before = _tree_digest(gmx_root / "futures")
    state_before = _tree_digest(gmx_root / "funding")

    coverage = _gate(gmx_root, tmp_path / "stage", extract_outcome="failure")

    assert coverage["status"] == "WITHHELD"
    assert coverage["refreshed"] is False
    assert any("B4" in reason for reason in coverage["reasons"])
    assert _tree_digest(gmx_root / "futures") == futures_before
    assert _tree_digest(gmx_root / "funding") == state_before
    btc = coverage["pairs"]["BTC"]
    assert btc["shipped_funding_last_bar"] == old[-1].isoformat()
    assert btc["shipped_mark_last_bar"] == old[-1].isoformat()
    assert coverage["pairs"]["ETH"]["shipped_funding_last_bar"] is None


def test_first_ever_run_that_withholds_ships_nothing(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    coverage = _gate(gmx_root, tmp_path / "stage", state="absent", extract_outcome="skipped")

    assert coverage["status"] == "WITHHELD"
    assert not list((gmx_root / "futures").glob("*-funding_rate.feather"))
    assert not list((gmx_root / "futures").glob("*-mark.feather"))


def test_gate_crash_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    gmx_root = _lake(tmp_path)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    def boom(**_kw):
        raise RuntimeError("kaput")

    monkeypatch.setattr(funding_release, "assess_series_coverage", boom)
    coverage = _gate(gmx_root, tmp_path / "stage")

    assert coverage["status"] == "WITHHELD"
    assert any("kaput" in reason for reason in coverage["reasons"])
    assert not list((gmx_root / "futures").glob("*-funding_rate.feather"))


def test_stale_pairs_are_listed_in_report_notes_and_warnings(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, symbols=("BTC", "MOODENG", "WIF"))
    _write_rates(gmx_root, "MOODENG", CUTOFF - timedelta(days=23))
    _write_rates(gmx_root, "WIF", CUTOFF - timedelta(hours=20))
    funding_release.stage_companions(gmx_root, tmp_path / "stage")
    report_path = tmp_path / "data_report.txt"
    report_path.write_text("existing report\n")

    coverage = _gate(gmx_root, tmp_path / "stage", report_path=report_path)

    assert coverage["status"] == "PASS"
    text = report_path.read_text()
    assert text.startswith("existing report")
    assert "MOODENG" in text and "STALE" in text and "LAGGING" in text and "WIF" in text
    notes = funding_release.format_release_notes(coverage)
    assert "MOODENG" in notes and "WIF" in notes and "BTC |" not in notes
    warnings = funding_release.github_warnings(coverage)
    assert any(w.startswith("::warning::") and "MOODENG" in w for w in warnings)
    assert not any("BTC" in w for w in warnings)

    # Re-running replaces the section rather than appending a second one.
    _gate(gmx_root, tmp_path / "stage", report_path=report_path)
    assert report_path.read_text().count("Funding & Mark Companions") == 1


def test_withheld_notes_say_funding_was_not_refreshed(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")
    coverage = _gate(gmx_root, tmp_path / "stage", extract_outcome="failure")

    notes = funding_release.format_release_notes(coverage)

    assert "NOT refreshed" in notes
    assert "WITHHELD" in notes


def test_stale_tail_switch_is_plumbed_through_the_gate(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, symbols=("BTC", "MOODENG"))
    _write_rates(gmx_root, "MOODENG", CUTOFF - timedelta(days=23))
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    coverage = _gate(
        gmx_root, tmp_path / "stage", thresholds=SeriesThresholds(stale_tail_blocks=True)
    )

    assert coverage["status"] == "WITHHELD"


def test_seed_then_one_incremental_day_end_to_end(tmp_path: Path) -> None:
    """Restored seed + one incremental day: old hours survive, the release is current."""
    source = write_seed_source(tmp_path / "src", symbols=("BTC",))
    seed_end = CUTOFF - 24 * HOUR
    stamps = hours(EPOCH, int((seed_end - EPOCH) / HOUR) + 1)
    pl.DataFrame(
        {
            "timestamp": stamps,
            "funding_rate_hourly": [1e-6] * len(stamps),
            "longs_pay_shorts": [True] * len(stamps),
        }
    ).with_columns(pl.col("timestamp").dt.replace_time_zone("UTC")).write_parquet(
        source / "rates" / "BTC" / "1h.parquet"
    )
    gmx_root = tmp_path / "gmx"
    n = int((CUTOFF - datetime(2026, 1, 1, tzinfo=UTC)) / HOUR) + 1
    write_futures(gmx_root / "futures", "BTC", datetime(2026, 1, 1, tzinfo=UTC), n)

    seed_funding_state(source, gmx_root)
    assert assess_funding_state(gmx_root) is FundingState.READY

    # The seed's synthesized checkpoints are ~April: before the extraction has
    # advanced them the gate must not call the pipeline current.
    funding_release.stage_companions(gmx_root, tmp_path / "stage1")
    stalled = _gate(gmx_root, tmp_path / "stage1")
    assert stalled["status"] == "WITHHELD"
    assert any("checkpoint" in r for r in stalled["reasons"])

    # The incremental day: extraction appends a day of rates and advances both checkpoints.
    _write_rates(gmx_root, "BTC", CUTOFF)
    cp = gmx_root / "funding" / "arbitrum" / "checkpoints"
    write_checkpoint(cp, "funding_factor_checkpoint.json", "funding_factor_all", NOW)
    write_checkpoint(cp, "fee_per_size_checkpoint.json", "fee_per_size_all", NOW)
    funding_release.stage_companions(gmx_root, tmp_path / "stage2")
    coverage = _gate(gmx_root, tmp_path / "stage2")

    assert coverage["status"] == "PASS"
    shipped = pl.read_ipc(gmx_root / "futures" / "BTC_USDC_USDC-1h-funding_rate.feather")
    assert shipped["date"].min() == EPOCH  # old timestamps survived
    assert shipped["date"].max() == CUTOFF  # and the new day is there
    assert (gmx_root / "futures" / "BTC_USDC_USDC-1h-mark.feather").exists()


def test_restored_history_at_hours_the_source_rejects_is_never_published(tmp_path: Path) -> None:
    """A restored companion value must not survive at an hour the current source
    rejects for unknown direction, however old that hour is."""
    gmx_root = _lake(tmp_path, symbols=("BTC",))
    bad = datetime(2026, 1, 1, tzinfo=UTC)
    rates_path = gmx_root / "funding" / "arbitrum" / "rates" / "BTC" / "1h.parquet"
    rates = pl.read_parquet(rates_path).with_columns(
        pl.when(pl.col("timestamp") == bad)
        .then(3e-5)
        .otherwise(pl.col("funding_rate_hourly"))
        .alias("funding_rate_hourly"),
        pl.when(pl.col("timestamp") == bad)
        .then(None)
        .otherwise(pl.col("longs_pay_shorts"))
        .alias("longs_pay_shorts"),
    )
    rates.write_parquet(rates_path)
    restored = hours(bad - 6 * HOUR, 13)
    write_companion(gmx_root / "futures", "BTC", "funding_rate", restored, value=0.5)

    funding_release.stage_companions(gmx_root, tmp_path / "stage")
    coverage = _gate(gmx_root, tmp_path / "stage")

    shipped = pl.read_ipc(gmx_root / "futures" / "BTC_USDC_USDC-1h-funding_rate.feather")
    assert bad not in shipped["date"].to_list()
    assert 0.5 not in shipped["open"].to_list() or shipped.filter(pl.col("date") == bad).is_empty()
    # Neighbouring hours carry the current, signed values -- not the restored 0.5.
    assert shipped.filter(pl.col("date") == bad - HOUR)["open"].to_list() == [pytest.approx(1e-6)]
    # Too old to block (nothing can repair it), but accounted for.
    assert coverage["status"] == "PASS"
    assert coverage["pairs"]["BTC"]["rejected_older"] == 1


def test_a_recent_rejected_hour_still_blocks(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, symbols=("BTC",))
    bad = CUTOFF - timedelta(days=2)
    rates_path = gmx_root / "funding" / "arbitrum" / "rates" / "BTC" / "1h.parquet"
    pl.read_parquet(rates_path).with_columns(
        pl.when(pl.col("timestamp") == bad)
        .then(None)
        .otherwise(pl.col("longs_pay_shorts"))
        .alias("longs_pay_shorts")
    ).write_parquet(rates_path)

    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    assert _gate(gmx_root, tmp_path / "stage")["status"] == "WITHHELD"


def _write_snapshot(gmx_root: Path, name: str, rows: list[tuple[str, str]]) -> None:
    snapshots = gmx_root / "snapshots"
    snapshots.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"symbol": [r[0] for r in rows], "listing_date": [r[1] for r in rows]}
    ).write_parquet(snapshots / f"{name}.parquet")


def test_load_listing_dates_takes_the_earliest_market_per_symbol(tmp_path: Path) -> None:
    gmx_root = tmp_path / "gmx"
    _write_snapshot(gmx_root, "2026-10-17", [("ZEC", "2020-01-01")])  # older snapshot: ignored
    _write_snapshot(
        gmx_root,
        "2026-10-18",
        [
            ("ZEC", "2026-10-16T00:00:00Z"),  # the USDG market
            ("zec", "2025-12-22"),
            ("", "2025-01-01"),  # swap-only pool
            ("BAD", "not-a-date"),
            ("EMPTY", ""),
        ],
    )
    assert funding_release.load_listing_dates(gmx_root / "snapshots") == {
        "ZEC": datetime(2025, 12, 22, tzinfo=UTC)
    }
    assert funding_release.load_listing_dates(tmp_path / "nowhere") == {}


def test_gate_bounds_funding_start_by_the_snapshot_listing_date(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, symbols=("BTC",))
    futures = gmx_root / "futures"
    listed = datetime(2026, 1, 9, tzinfo=UTC)
    # Candles pre-date the listing by five weeks; funding starts at the listing.
    start = datetime(2025, 12, 3, tzinfo=UTC)
    n = int((CUTOFF - start) / HOUR) + 1
    write_futures(futures, "AR", start, n)
    _write_rates(gmx_root, "AR", CUTOFF, start=listed + timedelta(hours=10))
    _write_snapshot(gmx_root, "2026-10-18", [("AR", "2026-01-09"), ("BTC", "2020-01-01")])
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    coverage = _gate(gmx_root, tmp_path / "stage")

    assert coverage["status"] == "PASS", coverage["reasons"]
    assert coverage["pairs"]["AR"]["listing_date"] == listed.isoformat()
    assert coverage["pairs"]["BTC"]["listing_date"] == "2020-01-01T00:00:00+00:00"


def test_gate_without_a_snapshot_falls_back_to_the_first_futures_bar(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, symbols=("BTC",))
    start = datetime(2025, 12, 3, tzinfo=UTC)
    write_futures(gmx_root / "futures", "AR", start, int((CUTOFF - start) / HOUR) + 1)
    _write_rates(gmx_root, "AR", CUTOFF, start=datetime(2026, 1, 9, 10, tzinfo=UTC))
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    coverage = _gate(gmx_root, tmp_path / "stage")

    assert coverage["status"] != "PASS"
    assert any("B2" in r and "first futures bar 2025-12-03" in r for r in coverage["reasons"])
    assert coverage["pairs"]["AR"]["listing_date"] is None
