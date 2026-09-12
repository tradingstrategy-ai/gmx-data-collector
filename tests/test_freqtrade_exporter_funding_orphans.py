"""A funding variant that stops being exportable must not vanish silently.

Issue #47: a downstream consumer found every
``*-1h_datastore-funding_rate.feather`` missing from its checkout and spent
~19 hours of compute before noticing. The exporter had done nothing wrong --
``1h_datastore`` was deliberately retired as an exportable timeframe by
``df6d042`` -- but it also said nothing, so "this symbol never had this
variant", "this variant was retired on purpose" and "the export broke" were
indistinguishable from the output directory alone.

These tests pin the issue's own minimum ask: warn loudly when a variant is
sitting in the output directory that this run can no longer produce.
"""

import logging

import polars as pl
import pytest

from gmx_historical_data.freqtrade_exporter import (
    FreqtradeExporter,
    find_orphaned_funding_variants,
)


def _write_funding_source(data_dir, symbol: str, variant: str = "1h") -> None:
    """Write a minimal funding-rate parquet into the source lake.

    :param data_dir: Exporter ``data_dir`` root.
    :param symbol: Token symbol.
    :param variant: Source file stem, e.g. ``1h`` or ``1h_datastore``.
    """
    d = data_dir / "funding" / "arbitrum" / "rates" / symbol
    d.mkdir(parents=True, exist_ok=True)
    ts = pl.datetime_range(
        pl.datetime(2026, 1, 1), pl.datetime(2026, 1, 1, 5), "1h", eager=True
    ).alias("timestamp")
    pl.DataFrame(
        {
            "timestamp": ts.dt.replace_time_zone("UTC"),
            "funding_rate": [1e-6] * len(ts),
            "funding_rate_hourly": [1e-6] * len(ts),
        }
    ).write_parquet(d / f"{variant}.parquet")


def _touch_export(gmx_dir, pair: str, variant: str) -> None:
    """Create an exported funding file for a variant.

    :param gmx_dir: ``{output}/gmx/futures`` directory.
    :param pair: Freqtrade pair stem, e.g. ``BTC_USDC_USDC``.
    :param variant: Variant token, e.g. ``1h_datastore``.
    """
    gmx_dir.mkdir(parents=True, exist_ok=True)
    (gmx_dir / f"{pair}-{variant}-funding_rate.feather").write_bytes(b"")


class TestFindOrphanedFundingVariants:
    def test_variant_present_but_not_produced_is_reported(self, tmp_path):
        _touch_export(tmp_path, "BTC_USDC_USDC", "1h_datastore")
        _touch_export(tmp_path, "ETH_USDC_USDC", "1h_datastore")
        _touch_export(tmp_path, "BTC_USDC_USDC", "1h")

        assert find_orphaned_funding_variants(tmp_path, {"1h"}) == {"1h_datastore": 2}

    def test_produced_variants_are_never_orphans(self, tmp_path):
        _touch_export(tmp_path, "BTC_USDC_USDC", "1h")

        assert find_orphaned_funding_variants(tmp_path, {"1h"}) == {}

    def test_several_orphan_variants_are_counted_separately(self, tmp_path):
        for pair in ("BTC_USDC_USDC", "ETH_USDC_USDC"):
            _touch_export(tmp_path, pair, "8h")
            _touch_export(tmp_path, pair, "1h_factor")
        _touch_export(tmp_path, "BTC_USDC_USDC", "1h_datastore")

        assert find_orphaned_funding_variants(tmp_path, {"1h"}) == {
            "8h": 2,
            "1h_factor": 2,
            "1h_datastore": 1,
        }

    def test_missing_directory_is_not_an_error(self, tmp_path):
        assert find_orphaned_funding_variants(tmp_path / "nope", {"1h"}) == {}

    def test_candle_and_mark_files_are_ignored(self, tmp_path):
        """Only ``-funding_rate`` files are in scope -- an OHLCV or mark-price
        feather sharing the directory must not be read as a funding variant."""
        tmp_path.mkdir(parents=True, exist_ok=True)
        (tmp_path / "BTC_USDC_USDC-1h-futures.feather").write_bytes(b"")
        (tmp_path / "BTC_USDC_USDC-1h-mark.feather").write_bytes(b"")

        assert find_orphaned_funding_variants(tmp_path, {"1h"}) == {}

    def test_macos_sidecars_are_ignored(self, tmp_path):
        """The external drive is exFAT, so it carries ``._`` AppleDouble
        sidecars beside every real file."""
        _touch_export(tmp_path, "BTC_USDC_USDC", "1h_datastore")
        (tmp_path / "._BTC_USDC_USDC-1h_datastore-funding_rate.feather").write_bytes(b"")

        assert find_orphaned_funding_variants(tmp_path, {"1h"}) == {"1h_datastore": 1}


class TestExportFundingWarnsAboutOrphans:
    def test_export_warns_about_a_retired_variant_on_disk(self, tmp_path, caplog):
        """The #47 scenario end to end: the source lake still holds
        ``1h_datastore.parquet``, the output dir still holds last run's
        ``1h_datastore-funding_rate`` export, but the variant is no longer
        exportable -- so the run must say so rather than quietly skip it."""
        data_dir = tmp_path / "data"
        out_dir = tmp_path / "out"
        _write_funding_source(data_dir, "BTC", "1h")
        _write_funding_source(data_dir, "BTC", "1h_datastore")
        _touch_export(out_dir / "gmx" / "futures", "BTC_USDC_USDC", "1h_datastore")

        exporter = FreqtradeExporter(data_dir, out_dir)
        with caplog.at_level(logging.WARNING):
            exporter.export_funding()

        assert "1h_datastore" in caplog.text
        assert "no longer exported" in caplog.text.lower()

    def test_clean_export_warns_about_nothing(self, tmp_path, caplog):
        data_dir = tmp_path / "data"
        out_dir = tmp_path / "out"
        _write_funding_source(data_dir, "BTC", "1h")

        exporter = FreqtradeExporter(data_dir, out_dir)
        with caplog.at_level(logging.WARNING):
            exporter.export_funding()

        assert "no longer exported" not in caplog.text.lower()


@pytest.mark.parametrize(
    "variant", ["1h_datastore", "1h_factor", "1h_short_borrow", "1h_borrow_rate"]
)
def test_companion_products_are_never_treated_as_exportable(tmp_path, variant):
    """Guards the ``df6d042`` decision itself: whatever else changes, these
    suffixed companion products must not come back as Freqtrade timeframes."""
    data_dir = tmp_path / "data"
    _write_funding_source(data_dir, "BTC", "1h")
    _write_funding_source(data_dir, "BTC", variant)

    exporter = FreqtradeExporter(data_dir, tmp_path / "out")

    assert exporter.list_funding_timeframes("BTC") == ["1h"]


def test_a_genuine_timeframe_source_is_exportable(tmp_path):
    """``8h`` is timeframe-shaped, so it is *not* filtered -- it would export
    if the source lake held an ``8h.parquet``.

    It does not: the lake carries only ``1h`` plus suffixed companions, which
    is precisely why the ~141 ``*-8h-funding_rate`` files on the production
    drive are orphans. Nothing produces them, so they are frozen wherever an
    older pipeline left them, and
    :func:`find_orphaned_funding_variants` is what surfaces that."""
    data_dir = tmp_path / "data"
    _write_funding_source(data_dir, "BTC", "1h")
    _write_funding_source(data_dir, "BTC", "8h")

    exporter = FreqtradeExporter(data_dir, tmp_path / "out")

    assert exporter.list_funding_timeframes("BTC") == ["1h", "8h"]


class TestWarningTracksAvailabilityNotSelection:
    """The warning must mean "nothing produces this any more", not "this run
    happened not to select it". A `--timeframe` or `--symbol` filter narrows
    what a run writes; it does not retire a variant."""

    def test_a_filtered_out_but_available_variant_is_not_warned(self, tmp_path, caplog):
        data_dir = tmp_path / "data"
        out_dir = tmp_path / "out"
        _write_funding_source(data_dir, "BTC", "1h")
        _write_funding_source(data_dir, "BTC", "8h")
        _touch_export(out_dir / "gmx" / "futures", "BTC_USDC_USDC", "8h")

        exporter = FreqtradeExporter(data_dir, out_dir)
        with caplog.at_level(logging.WARNING):
            exporter.export_funding(timeframes=["1h"])

        assert "8h" not in caplog.text

    def test_a_variant_absent_from_another_symbol_is_not_warned(self, tmp_path, caplog):
        """`--symbol BTC` must not declare ETH's variants retired."""
        data_dir = tmp_path / "data"
        out_dir = tmp_path / "out"
        _write_funding_source(data_dir, "BTC", "1h")
        _write_funding_source(data_dir, "ETH", "1h")
        _write_funding_source(data_dir, "ETH", "8h")
        _touch_export(out_dir / "gmx" / "futures", "ETH_USDC_USDC", "8h")

        exporter = FreqtradeExporter(data_dir, out_dir)
        with caplog.at_level(logging.WARNING):
            exporter.export_funding(symbols=["BTC"])

        assert "8h" not in caplog.text

    def test_a_genuinely_retired_variant_is_still_warned_under_a_filter(self, tmp_path, caplog):
        """Narrowing the run must not suppress a real orphan."""
        data_dir = tmp_path / "data"
        out_dir = tmp_path / "out"
        _write_funding_source(data_dir, "BTC", "1h")
        _touch_export(out_dir / "gmx" / "futures", "BTC_USDC_USDC", "1h_datastore")

        exporter = FreqtradeExporter(data_dir, out_dir)
        with caplog.at_level(logging.WARNING):
            exporter.export_funding(timeframes=["1h"])

        assert "1h_datastore" in caplog.text
