"""Both funding export entry points must publish exactly the same file (issue #60).

``make export-funding`` runs :meth:`FreqtradeExporter.export_funding`;
``make funding-feather`` runs ``scripts/extract_unified_funding.py::export_feather``.
The second used to select ``funding_rate_hourly`` raw, ignoring
``longs_pay_shorts``, and replaced the destination outright. These tests drive
both over the same source lake and require identical output, plus the
loss-accounting from issue #65: every finite source hour is reconciled against
the published file, and rows that cannot be exported are counted per symbol
with their times.
"""

from __future__ import annotations

import importlib.util
import logging
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter

T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _load_script():
    path = Path(__file__).resolve().parents[1] / "scripts" / "extract_unified_funding.py"
    spec = importlib.util.spec_from_file_location("extract_unified_funding_parity", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


script = _load_script()


def _hour(i: int) -> datetime:
    return T0 + timedelta(hours=i)


def _ts(n: int, start: int = 0) -> pl.Series:
    return pl.Series("timestamp", [_hour(start + i) for i in range(n)]).dt.replace_time_zone("UTC")


def _lake(tmp_path: Path, frame: pl.DataFrame, symbol: str = "ETH") -> Path:
    """Write ``frame`` as the unified 1h parquet; return the network dir."""
    network_dir = tmp_path / "data" / "funding" / "arbitrum"
    rates = network_dir / "rates" / symbol
    rates.mkdir(parents=True)
    frame.write_parquet(rates / "1h.parquet")
    return network_dir


def _run_main(network_dir: Path, out: Path, symbol: str = "ETH") -> FreqtradeExporter:
    data_dir = network_dir.parent.parent
    exporter = FreqtradeExporter(data_dir, out)
    exporter.export_funding(symbols=[symbol], timeframes=["1h"])
    return exporter


def _run_legacy(network_dir: Path, out: Path) -> None:
    script.export_feather(network_dir, out)


def _main_file(out: Path, symbol: str = "ETH") -> Path:
    return out / "gmx" / "futures" / f"{symbol}_USDC_USDC-1h-funding_rate.feather"


def _legacy_file(out: Path, symbol: str = "ETH") -> Path:
    return out / "data" / "gmx" / "futures" / f"{symbol}_USDC_USDC-1h-funding_rate.feather"


def _both(tmp_path: Path, frame: pl.DataFrame):
    network_dir = _lake(tmp_path, frame)
    main_out, legacy_out = tmp_path / "main", tmp_path / "legacy"
    _run_main(network_dir, main_out)
    _run_legacy(network_dir, legacy_out)
    return pl.read_ipc(_main_file(main_out)), pl.read_ipc(_legacy_file(legacy_out))


def test_parity_long_payer_short_payer_zero(tmp_path):
    """Direction decides the sign in both writers; a genuine zero stays zero."""
    frame = pl.DataFrame(
        {
            "timestamp": _ts(3),
            "funding_rate": [1e-10, 2e-10, 0.0],
            "funding_rate_hourly": [3.6e-7, 3.6e-2, 0.0],
            "longs_pay_shorts": [True, False, False],
        }
    )

    main, legacy = _both(tmp_path, frame)

    assert main["open"].to_list() == [3.6e-7, -3.6e-2, 0.0]
    assert legacy.equals(main)
    assert legacy.columns == ["date", "open", "high", "low", "close", "volume"]


def test_parity_signed_datastore_rows(tmp_path):
    """DataStore rows (signed, no direction) are exported as stored by both writers."""
    frame = pl.DataFrame(
        {
            "timestamp": _ts(3),
            "funding_rate_hourly": [-1e-6, 2e-6, 0.0],
            "longs_pay_shorts": pl.Series([None, None, None], dtype=pl.Boolean),
            "source": ["datastore"] * 3,
        }
    )

    main, legacy = _both(tmp_path, frame)

    assert main["open"].to_list() == [-1e-6, 2e-6, 0.0]
    assert legacy.equals(main)


def test_parity_unknown_direction_is_not_exported_positive(tmp_path):
    """An unsigned magnitude with unknown direction is excluded by both writers."""
    frame = pl.DataFrame(
        {
            "timestamp": _ts(3),
            "funding_rate_hourly": [4e-6, 5e-6, 0.0],
            "longs_pay_shorts": pl.Series([None, True, None], dtype=pl.Boolean),
            "source": ["hypersync"] * 3,
        }
    )

    main, legacy = _both(tmp_path, frame)

    assert main["open"].to_list() == [5e-6, 0.0]
    assert legacy.equals(main)


def test_parity_per_second_fallback_is_scaled_to_hourly(tmp_path):
    """Without ``funding_rate_hourly`` both writers export ``funding_rate`` x 3600."""
    frame = pl.DataFrame(
        {
            "timestamp": _ts(2),
            "funding_rate": [1e-5, 1e-5],
            "longs_pay_shorts": [True, False],
        }
    )

    main, legacy = _both(tmp_path, frame)

    assert main["open"].to_list() == pytest.approx([0.036, -0.036])
    assert legacy.equals(main)


def test_legacy_writer_keeps_longer_destination_history(tmp_path):
    """A destination covering more history than the source is merged, never truncated."""
    frame = pl.DataFrame(
        {
            "timestamp": _ts(3, start=10),
            "funding_rate_hourly": [1e-6, 2e-6, 3e-6],
            "longs_pay_shorts": [True, True, True],
        }
    )
    network_dir = _lake(tmp_path, frame)
    out = tmp_path / "out"
    older = pl.DataFrame(
        {
            "date": pl.Series([_hour(i) for i in range(10)])
            .dt.replace_time_zone("UTC")
            .dt.cast_time_unit("ns"),
            "open": [9e-6] * 10,
            "high": [0.0] * 10,
            "low": [0.0] * 10,
            "close": [0.0] * 10,
            "volume": [0.0] * 10,
        }
    )
    dest = _legacy_file(out)
    dest.parent.mkdir(parents=True)
    older.write_ipc(dest)

    _run_legacy(network_dir, out)

    written = pl.read_ipc(dest)
    assert written.height == 13
    assert written["date"].min() == older["date"].min()
    assert written["open"].to_list()[:10] == [9e-6] * 10
    assert written["open"].to_list()[10:] == [1e-6, 2e-6, 3e-6]


def test_stale_feather_with_newer_mtime_never_shadows_parquet(tmp_path):
    """Parquet is the one source for both writers, even beside a newer-mtime stale feather."""
    fresh = pl.DataFrame(
        {
            "timestamp": _ts(4),
            "funding_rate_hourly": [1e-6] * 4,
            "longs_pay_shorts": [True] * 4,
        }
    )
    network_dir = _lake(tmp_path, fresh)
    stale_path = network_dir / "rates" / "ETH" / "1h.feather"
    fresh.head(2).write_ipc(stale_path)
    parquet_path = network_dir / "rates" / "ETH" / "1h.parquet"
    newer = parquet_path.stat().st_mtime_ns + 10_000_000_000
    os.utime(stale_path, ns=(newer, newer))
    assert stale_path.stat().st_mtime_ns > parquet_path.stat().st_mtime_ns

    main_out, legacy_out = tmp_path / "main", tmp_path / "legacy"
    _run_main(network_dir, main_out)
    _run_legacy(network_dir, legacy_out)

    main, legacy = pl.read_ipc(_main_file(main_out)), pl.read_ipc(_legacy_file(legacy_out))
    assert main.height == 4
    assert legacy.equals(main)


def test_legacy_writer_falls_back_to_feather_only_lake(tmp_path, capsys):
    """A lake with only ``1h.feather`` still exports, with a regenerate-parquet warning."""
    frame = pl.DataFrame(
        {
            "timestamp": _ts(3),
            "funding_rate_hourly": [1e-6, 2e-6, 3e-6],
            "longs_pay_shorts": [True, False, True],
        }
    )
    network_dir = _lake(tmp_path, frame)
    parquet_path = network_dir / "rates" / "ETH" / "1h.parquet"
    frame.write_ipc(parquet_path.with_suffix(".feather"))
    parquet_path.unlink()

    _run_legacy(network_dir, tmp_path / "out")

    assert pl.read_ipc(_legacy_file(tmp_path / "out"))["open"].to_list() == [1e-6, -2e-6, 3e-6]
    assert "regenerate parquet" in capsys.readouterr().out


def test_live_row_survives_a_rebuild(tmp_path):
    """A live row appended past the source tail is kept by the next rebuild."""
    frame = pl.DataFrame(
        {
            "timestamp": _ts(3),
            "funding_rate_hourly": [1e-6] * 3,
            "longs_pay_shorts": [True] * 3,
        }
    )
    network_dir = _lake(tmp_path, frame)
    out = tmp_path / "out"
    _run_legacy(network_dir, out)
    dest = _legacy_file(out)
    live = pl.DataFrame(
        {
            "date": pl.Series([_hour(30)]).dt.replace_time_zone("UTC").dt.cast_time_unit("ns"),
            "open": [-7e-6],
            "high": [0.0],
            "low": [0.0],
            "close": [0.0],
            "volume": [0.0],
        }
    )
    pl.concat([pl.read_ipc(dest), live]).write_ipc(dest)

    _run_legacy(network_dir, out)

    written = pl.read_ipc(dest)
    assert written["date"].to_list()[-1] == live["date"][0]
    assert written["open"].to_list()[-1] == -7e-6
    assert written.height == 4


def test_reconciliation_op_like_interior_hours_with_null_direction(tmp_path, caplog):
    """Finite negative interior hours with null direction survive, first and incrementally."""
    n = 6
    frame = pl.DataFrame(
        {
            "timestamp": _ts(n),
            "funding_rate_hourly": [-1e-6, -2e-6, -3e-6, -4e-6, 5e-6, 6e-6],
            "longs_pay_shorts": pl.Series([None, None, None, None, True, True], dtype=pl.Boolean),
            "update_count": pl.Series([1, 0, 0, 0, 1, 1], dtype=pl.UInt32),
            "source": ["hypersync"] * n,
        }
    )
    network_dir = _lake(tmp_path, frame)
    out = tmp_path / "out"

    with caplog.at_level(logging.WARNING):
        exporter = _run_main(network_dir, out)
    first = pl.read_ipc(_main_file(out))
    assert first["open"].to_list() == [-1e-6, -2e-6, -3e-6, -4e-6, 5e-6, 6e-6]
    (rec,) = exporter.funding_reconciliations
    assert rec.clean
    assert rec.source_hours == n and rec.exported_hours == n
    assert "reconciliation failed" not in caplog.text

    # incremental run into the existing file keeps every interior hour
    exporter = _run_main(network_dir, out)
    assert pl.read_ipc(_main_file(out)).equals(first)
    assert exporter.funding_reconciliations[0].clean


def test_reconciliation_counts_null_and_unknown_rows_with_times(tmp_path, caplog):
    """Rejected null-rate and excluded unknown-direction rows are reported per symbol/time."""
    frame = pl.DataFrame(
        {
            "timestamp": _ts(5),
            "funding_rate_hourly": [1e-6, None, 3e-6, 4e-6, 5e-6],
            "longs_pay_shorts": pl.Series([True, True, None, None, True], dtype=pl.Boolean),
            "source": ["hypersync"] * 5,
        }
    )
    network_dir = _lake(tmp_path, frame)

    with caplog.at_level(logging.WARNING):
        exporter = _run_main(network_dir, tmp_path / "out")

    (rec,) = exporter.funding_reconciliations
    assert rec.symbol == "ETH" and rec.timeframe == "1h"
    assert rec.null_rate_hours == (_hour(1),)
    assert rec.unknown_direction_hours == (_hour(2), _hour(3))
    assert rec.source_hours == 4  # the null-rate row is not a finite source hour
    assert rec.missing_hours == (_hour(2), _hour(3))
    assert not rec.clean
    assert "ETH/1h" in caplog.text
    assert "2026-01-01T01:00Z" in caplog.text  # null-rate time
    assert "2026-01-01T02:00Z" in caplog.text  # unknown-direction time
    assert "unknown direction" in caplog.text


def test_reconciliation_flags_a_finite_source_hour_missing_from_output(tmp_path, caplog):
    """A finite source hour absent from the published file is reported as a failure."""
    frame = pl.DataFrame(
        {
            "timestamp": _ts(3),
            "funding_rate_hourly": [1e-6] * 3,
            "longs_pay_shorts": [True] * 3,
        }
    )
    network_dir = _lake(tmp_path, frame)
    exporter = FreqtradeExporter(network_dir.parent.parent, tmp_path / "out")
    report = exporter._transform_funding_rate_with_report(frame)[1]
    published = exporter._transform_funding_rate(frame).slice(0, 2)  # drop the last hour

    with caplog.at_level(logging.WARNING):
        rec = exporter._record_reconciliation("ETH", "1h", report, published=published)

    assert rec.missing_hours == (_hour(2),)
    assert "reconciliation failed" in caplog.text
    assert "2026-01-01T02:00Z" in caplog.text
