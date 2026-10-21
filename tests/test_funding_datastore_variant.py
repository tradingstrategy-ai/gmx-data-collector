"""``{PAIR}-1h_datastore-funding_rate.feather``: the DataStore series as an exported variant (#47)."""

from __future__ import annotations

import logging
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from gmx_historical_data.freqtrade_exporter import (
    FreqtradeExporter,
    find_orphaned_funding_variants,
    find_stale_orphaned_funding_variants,
)

START = datetime(2024, 3, 12, 11, tzinfo=UTC)
RATES = [-4.0e-6, 0.0, 3.5e-6, 0.0, -1.0e-7, 2.0e-6]  # signed hourly fractions, as read on chain


def _stamps(n: int, start: datetime = START) -> list[datetime]:
    return [start + timedelta(hours=i) for i in range(n)]


def _write_source(
    data_dir: Path,
    symbol: str,
    variant: str,
    rates: list[float],
    *,
    start: datetime = START,
    with_direction: bool = True,
) -> None:
    folder = data_dir / "funding" / "arbitrum" / "rates" / symbol
    folder.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "timestamp": _stamps(len(rates), start),
            "funding_rate": [r / 3600 for r in rates],
            "funding_rate_hourly": rates,
            "source": ["datastore"] * len(rates),
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    )
    if with_direction:
        # as written by aggregate_hourly_rates: direction is just ``rate > 0``
        frame = frame.with_columns((pl.col("funding_rate") > 0).alias("longs_pay_shorts"))
    frame.write_parquet(folder / f"{variant}.parquet")


def _read_variant(out: Path, symbol: str = "BTC", variant: str = "1h_datastore") -> pl.DataFrame:
    return pl.read_ipc(
        out / "gmx" / "futures" / f"{symbol}_USDC_USDC-{variant}-funding_rate.feather"
    )


def test_the_datastore_variant_is_exported_with_signs_exactly_as_read(tmp_path: Path) -> None:
    _write_source(tmp_path / "data", "BTC", "1h_datastore", RATES)
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")

    results, failed, failures = exporter.export_funding(timeframes=["1h"])

    assert failed == [] and failures == []
    frame = _read_variant(tmp_path / "out")
    assert frame["date"].to_list() == _stamps(len(RATES))
    assert frame["open"].to_list() == RATES
    # A genuine zero stays +0.0: the sign is the chain's, never re-derived.
    zeros = [v for v in frame["open"].to_list() if v == 0.0]
    assert zeros and all(math.copysign(1.0, v) == 1.0 for v in zeros)
    assert (frame["high"] == 0).all() and (frame["volume"] == 0).all()
    assert results["BTC"]["funding_files"] == 1


def test_the_release_call_for_1h_exports_the_canonical_file_and_its_datastore_variant(
    tmp_path: Path,
) -> None:
    _write_source(tmp_path / "data", "BTC", "1h", [1e-6] * 4, with_direction=False)
    _write_source(tmp_path / "data", "BTC", "1h_datastore", RATES)
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")

    exporter.export_funding(symbols=["BTC"], timeframes=["1h"])

    names = sorted(p.name for p in (tmp_path / "out" / "gmx" / "futures").glob("*.feather"))
    assert names == [
        "BTC_USDC_USDC-1h-funding_rate.feather",
        "BTC_USDC_USDC-1h_datastore-funding_rate.feather",
    ]


def test_an_unrelated_timeframe_selection_does_not_export_the_variant(tmp_path: Path) -> None:
    _write_source(tmp_path / "data", "BTC", "1h", [1e-6] * 4, with_direction=False)
    _write_source(tmp_path / "data", "BTC", "8h", [1e-6] * 4, with_direction=False)
    _write_source(tmp_path / "data", "BTC", "1h_datastore", RATES)
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")

    exporter.export_funding(timeframes=["8h"])

    names = [p.name for p in (tmp_path / "out" / "gmx" / "futures").glob("*.feather")]
    assert names == ["BTC_USDC_USDC-8h-funding_rate.feather"]


def test_a_symbol_with_only_a_datastore_source_still_exports(tmp_path: Path) -> None:
    """A market delisted before the V2.2 epoch has no canonical ``1h`` source, only DataStore reads."""
    _write_source(tmp_path / "data", "LUNA", "1h_datastore", RATES)
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")

    results, _, _ = exporter.export_funding()

    assert results["LUNA"]["funding_files"] == 1
    assert _read_variant(tmp_path / "out", "LUNA")["open"].to_list() == RATES


def test_it_is_still_not_a_timeframe(tmp_path: Path) -> None:
    _write_source(tmp_path / "data", "BTC", "1h", [1e-6], with_direction=False)
    _write_source(tmp_path / "data", "BTC", "1h_datastore", RATES)
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")
    assert exporter.list_funding_timeframes("BTC") == ["1h"]
    assert exporter.list_datastore_variants("BTC") == ["1h_datastore"]


def test_the_guarded_merge_keeps_history_already_published(tmp_path: Path) -> None:
    out_futures = tmp_path / "out" / "gmx" / "futures"
    out_futures.mkdir(parents=True)
    older = _stamps(3, START - timedelta(days=400))
    pl.DataFrame(
        {
            "date": older,
            "open": [-1e-6, 2e-6, 0.0],
            "high": [0.0] * 3,
            "low": [0.0] * 3,
            "close": [0.0] * 3,
            "volume": [0.0] * 3,
        }
    ).with_columns(pl.col("date").dt.replace_time_zone("UTC").dt.cast_time_unit("ns")).write_ipc(
        out_futures / "BTC_USDC_USDC-1h_datastore-funding_rate.feather"
    )
    _write_source(tmp_path / "data", "BTC", "1h_datastore", RATES)

    FreqtradeExporter(tmp_path / "data", tmp_path / "out").export_funding(timeframes=["1h"])

    frame = _read_variant(tmp_path / "out")
    assert frame["date"].min() == older[0]
    assert frame["open"].to_list()[:3] == [-1e-6, 2e-6, 0.0]
    assert frame["open"].to_list()[3:] == RATES


def test_the_variant_is_reconciled_like_any_funding_file(tmp_path: Path) -> None:
    _write_source(tmp_path / "data", "BTC", "1h_datastore", RATES)
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")

    exporter.export_funding(timeframes=["1h"])

    (rec,) = [r for r in exporter.funding_reconciliations if r.timeframe == "1h_datastore"]
    assert rec.clean and rec.source_hours == len(RATES) == rec.exported_hours


def test_a_null_rate_is_rejected_and_never_exported_as_zero(tmp_path: Path) -> None:
    _write_source(tmp_path / "data", "BTC", "1h_datastore", [1e-6, 2e-6, 3e-6])
    path = tmp_path / "data" / "funding" / "arbitrum" / "rates" / "BTC" / "1h_datastore.parquet"
    frame = pl.read_parquet(path).with_columns(
        pl.when(pl.col("timestamp") == START + timedelta(hours=1))
        .then(None)
        .otherwise(pl.col("funding_rate_hourly"))
        .alias("funding_rate_hourly")
    )
    frame.write_parquet(path)
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")

    exporter.export_funding(timeframes=["1h"])

    assert _read_variant(tmp_path / "out")["date"].to_list() == [START, START + timedelta(hours=2)]
    (rec,) = [r for r in exporter.funding_reconciliations if r.timeframe == "1h_datastore"]
    assert rec.null_rate_hours == (START + timedelta(hours=1),)


# ---- the #49 orphan detection now sees 1h_datastore as produced ------------------------


def _touch(gmx_dir: Path, variant: str) -> None:
    gmx_dir.mkdir(parents=True, exist_ok=True)
    (gmx_dir / f"BTC_USDC_USDC-{variant}-funding_rate.feather").write_bytes(b"")


def test_a_produced_datastore_variant_is_not_an_orphan(tmp_path: Path) -> None:
    _touch(tmp_path, "1h_datastore")
    assert find_orphaned_funding_variants(tmp_path, {"1h", "1h_datastore"}) == {}
    assert find_orphaned_funding_variants(tmp_path, {"1h"}) == {"1h_datastore": 1}


def test_export_does_not_warn_about_the_datastore_variant_it_writes(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _write_source(tmp_path / "data", "BTC", "1h", [1e-6] * 4, with_direction=False)
    _write_source(tmp_path / "data", "BTC", "1h_datastore", RATES)
    _touch(tmp_path / "out" / "gmx" / "futures", "1h_datastore")
    exporter = FreqtradeExporter(tmp_path / "data", tmp_path / "out")

    with caplog.at_level(logging.WARNING):
        exporter.export_funding()

    assert "not produced by this exporter" not in caplog.text.lower()


def test_a_stale_datastore_export_with_no_source_anywhere_is_still_reported(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    gmx = tmp_path / "out" / "gmx" / "futures"
    _touch(gmx, "1h_datastore")
    assert find_stale_orphaned_funding_variants(gmx, {"1h"}) == {"1h_datastore": 1}
