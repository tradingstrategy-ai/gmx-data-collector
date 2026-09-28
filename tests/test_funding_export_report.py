"""The data report must say something about funding exports.

Issue #47 went unnoticed for ~19 hours of compute partly because the published
``data_report.txt`` never mentions funding at all -- 86 KB of per-asset OHLCV
coverage tables and not one funding line. Funding files ride along inside
``gmx-full.tar.gz`` without being regenerated or reported, so a variant going
stale, orphaned or missing is invisible by construction.

These tests pin a funding section into the report: how far the canonical
export reaches, and which variants are present that nothing produces any more.
"""

from datetime import UTC, datetime, timedelta

import pandas as pd
import pyarrow.feather as feather

from scripts.collect_daily_snapshot import _funding_export_summary, _funding_export_warning


def _write_funding_export(futures_dir, pair: str, variant: str, last: datetime, rows: int = 5):
    """Write a funding-rate export feather in the Freqtrade schema.

    :param futures_dir: Export directory.
    :param pair: Pair stem, e.g. ``BTC_USDC_USDC``.
    :param variant: Variant token, e.g. ``1h`` or ``1h_datastore``.
    :param last: Timestamp of the final bar.
    :param rows: Number of hourly bars to write.
    """
    futures_dir.mkdir(parents=True, exist_ok=True)
    dates = pd.to_datetime(
        [last - timedelta(hours=i) for i in reversed(range(rows))], utc=True
    ).as_unit("ns")
    frame = pd.DataFrame(
        {
            "date": dates,
            "open": [1e-6] * rows,
            "high": [0.0] * rows,
            "low": [0.0] * rows,
            "close": [0.0] * rows,
            "volume": [0.0] * rows,
        }
    )
    feather.write_feather(frame, futures_dir / f"{pair}-{variant}-funding_rate.feather")


class TestFundingExportSummary:
    def test_missing_directory_summarises_as_empty(self, tmp_path):
        summary = _funding_export_summary(tmp_path / "nope")

        assert summary["canonical_pairs"] == 0
        assert summary["canonical_latest"] is None
        assert summary["orphans"] == {}

    def test_counts_canonical_pairs_and_newest_bar(self, tmp_path):
        last = datetime(2026, 6, 12, 5, tzinfo=UTC)
        _write_funding_export(tmp_path, "BTC_USDC_USDC", "1h", last)
        _write_funding_export(tmp_path, "ETH_USDC_USDC", "1h", last - timedelta(hours=2))

        summary = _funding_export_summary(tmp_path)

        assert summary["canonical_pairs"] == 2
        assert summary["canonical_latest"] == pd.Timestamp(last)
        assert summary["orphans"] == {}

    def test_reports_variants_nothing_produces_any_more(self, tmp_path):
        last = datetime(2026, 6, 12, 5, tzinfo=UTC)
        _write_funding_export(tmp_path, "BTC_USDC_USDC", "1h", last)
        _write_funding_export(tmp_path, "BTC_USDC_USDC", "1h_datastore", last)
        _write_funding_export(tmp_path, "ETH_USDC_USDC", "1h_datastore", last)
        _write_funding_export(tmp_path, "ETH_USDC_USDC", "8h", last)

        summary = _funding_export_summary(tmp_path)

        assert summary["canonical_pairs"] == 1
        assert summary["orphans"] == {"1h_datastore": 2, "8h": 1}

    def test_ohlcv_feathers_are_not_mistaken_for_funding(self, tmp_path):
        tmp_path.mkdir(parents=True, exist_ok=True)
        (tmp_path / "BTC_USDC_USDC-1h-futures.feather").write_bytes(b"")

        summary = _funding_export_summary(tmp_path)

        assert summary["canonical_pairs"] == 0
        assert summary["orphans"] == {}

    def test_unreadable_export_does_not_break_the_report(self, tmp_path):
        """A corrupt feather must cost its own date, not the whole report --
        the report runs at the end of a release that already succeeded."""
        last = datetime(2026, 6, 12, 5, tzinfo=UTC)
        _write_funding_export(tmp_path, "BTC_USDC_USDC", "1h", last)
        (tmp_path / "ETH_USDC_USDC-1h-funding_rate.feather").write_bytes(b"not a feather")

        summary = _funding_export_summary(tmp_path)

        assert summary["canonical_pairs"] == 2
        assert summary["canonical_latest"] == pd.Timestamp(last)
        assert summary["unreadable"] == 1

    def test_parquet_exports_are_counted(self, tmp_path):
        last = datetime(2026, 6, 12, 5, tzinfo=UTC)
        frame = pd.DataFrame(
            {"date": pd.date_range(last - timedelta(hours=2), last, freq="h", tz="UTC")}
        )
        frame.to_parquet(tmp_path / "BTC_USDC_USDC-1h-funding_rate.parquet")

        summary = _funding_export_summary(tmp_path)

        assert summary["canonical_pairs"] == 1
        assert summary["canonical_latest"] == pd.Timestamp(last)

    def test_future_dated_export_does_not_certify_freshness(self, tmp_path):
        future = datetime.now(UTC) + timedelta(days=30)
        _write_funding_export(tmp_path, "BTC_USDC_USDC", "1h", future)

        summary = _funding_export_summary(tmp_path)

        assert summary["canonical_pairs"] == 1
        assert summary["canonical_latest"] is None


class TestTimezoneRobustness:
    """A legacy export written without a timezone must not take down the
    report -- `generate_report` runs at the end of an otherwise successful
    release, so a TypeError here fails a run whose data was already good."""

    def _write_naive(self, futures_dir, pair: str, last: datetime, rows: int = 3):
        futures_dir.mkdir(parents=True, exist_ok=True)
        dates = pd.to_datetime([last - timedelta(hours=i) for i in reversed(range(rows))]).as_unit(
            "ns"
        )
        feather.write_feather(
            pd.DataFrame(
                {
                    "date": dates,
                    "open": [1e-6] * rows,
                    "high": [0.0] * rows,
                    "low": [0.0] * rows,
                    "close": [0.0] * rows,
                    "volume": [0.0] * rows,
                }
            ),
            futures_dir / f"{pair}-1h-funding_rate.feather",
        )

    def test_naive_and_aware_exports_mix_without_raising(self, tmp_path):
        last = datetime(2026, 6, 12, 5, tzinfo=UTC)
        _write_funding_export(tmp_path, "AAA_USDC_USDC", "1h", last)
        self._write_naive(tmp_path, "BBB_USDC_USDC", last.replace(tzinfo=None))

        summary = _funding_export_summary(tmp_path)

        assert summary["canonical_pairs"] == 2
        assert summary["canonical_latest"] is not None
        # Whatever it picked must be comparable against an aware "now".
        assert (pd.Timestamp(datetime.now(UTC)) - summary["canonical_latest"]).days >= 0

    def test_naive_timestamps_are_read_as_utc(self, tmp_path):
        naive_last = datetime(2026, 6, 12, 5)
        self._write_naive(tmp_path, "BBB_USDC_USDC", naive_last)

        summary = _funding_export_summary(tmp_path)

        assert summary["canonical_latest"] == pd.Timestamp(naive_last, tz="UTC")

    def test_undated_export_is_counted_unreadable_not_raised(self, tmp_path):
        """A file whose date column cannot become timestamps at all."""
        tmp_path.mkdir(parents=True, exist_ok=True)
        feather.write_feather(
            pd.DataFrame({"date": ["not-a-date"], "open": [1.0]}),
            tmp_path / "CCC_USDC_USDC-1h-funding_rate.feather",
        )

        summary = _funding_export_summary(tmp_path)

        assert summary["canonical_latest"] is None
        assert summary["unreadable"] == 1


class TestFundingExportWarning:
    """Issue #47 is still open: the canonical export has 0 pairs today, and
    that fact must surface as a GitHub Actions ``::warning::`` in the Actions
    UI rather than silently sitting in the report body only -- the whole
    reason the incident went unnoticed for 19 hours."""

    def test_zero_canonical_pairs_produces_a_warning(self):
        funding = {
            "canonical_pairs": 0,
            "canonical_latest": None,
            "orphans": {},
            "unreadable": 0,
        }

        warning = _funding_export_warning(funding)

        assert warning is not None
        assert warning.startswith("::warning::")
        assert "0 pairs" in warning

    def test_no_readable_newest_bar_produces_a_warning_even_with_pairs(self):
        """A canonical export can exist but have no bar this run could read
        as current (e.g. every date failed to parse) -- also a real gap."""
        funding = {
            "canonical_pairs": 3,
            "canonical_latest": None,
            "orphans": {},
            "unreadable": 3,
        }

        warning = _funding_export_warning(funding)

        assert warning is not None
        assert warning.startswith("::warning::")

    def test_healthy_export_produces_no_warning(self):
        funding = {
            "canonical_pairs": 42,
            "canonical_latest": pd.Timestamp(datetime(2026, 9, 27, tzinfo=UTC)),
            "orphans": {},
            "unreadable": 0,
        }

        assert _funding_export_warning(funding) is None


class TestFundingWarningEmittedFromReport:
    """The warning is only useful printed to stdout as a plain, unstyled
    line inside GitHub Actions -- the rich console would otherwise wrap or
    style the ``::warning::`` marker and GitHub would never recognise it."""

    def test_warning_printed_in_github_actions_when_canonical_is_empty(
        self, tmp_path, capsys, monkeypatch
    ):
        from scripts.collect_daily_snapshot import generate_report

        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        markets_df = pd.DataFrame(
            {
                "name": ["BTC/USD"],
                "is_swap_only": [False],
                "is_listed": [True],
                "open_interest_long": ["0"],
                "open_interest_short": ["0"],
                "market_token": ["0x0"],
                "symbol": ["BTC"],
            }
        )
        generate_report(
            date_str="2026-09-27",
            markets_df=markets_df,
            candle_count=0,
            failed_symbols=[],
            ticker_count=0,
            apy_count=0,
            volume_count=0,
            volume_data={},
            futures_dir=tmp_path,
            snapshots_dir=tmp_path,
            tickers_dir=tmp_path,
            apy_dir=tmp_path,
            volumes_dir=tmp_path,
            report_path=tmp_path / "report.txt",
            ohlcv_coverage=None,
            skipped=None,
        )

        out = capsys.readouterr().out
        assert "::warning::" in out

    def test_warning_not_printed_outside_github_actions(self, tmp_path, capsys, monkeypatch):
        from scripts.collect_daily_snapshot import generate_report

        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
        markets_df = pd.DataFrame(
            {
                "name": ["BTC/USD"],
                "is_swap_only": [False],
                "is_listed": [True],
                "open_interest_long": ["0"],
                "open_interest_short": ["0"],
                "market_token": ["0x0"],
                "symbol": ["BTC"],
            }
        )
        generate_report(
            date_str="2026-09-27",
            markets_df=markets_df,
            candle_count=0,
            failed_symbols=[],
            ticker_count=0,
            apy_count=0,
            volume_count=0,
            volume_data={},
            futures_dir=tmp_path,
            snapshots_dir=tmp_path,
            tickers_dir=tmp_path,
            apy_dir=tmp_path,
            volumes_dir=tmp_path,
            report_path=tmp_path / "report.txt",
            ohlcv_coverage=None,
            skipped=None,
        )

        out = capsys.readouterr().out
        assert "::warning::funding" not in out
