"""``FreqtradeExporter.export_mark_from_futures``: the thin mark writer."""

from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
from tests._funding_release_fixtures import write_futures

START = datetime(2026, 9, 1, tzinfo=UTC)


def _exporter(tmp_path: Path) -> FreqtradeExporter:
    return FreqtradeExporter(data_dir=tmp_path / "data", output_dir=tmp_path / "out")


def test_mark_is_derived_from_the_futures_feather(tmp_path: Path) -> None:
    futures = tmp_path / "futures"
    stamps = write_futures(futures, "BTC", START, 48)
    out = tmp_path / "stage"

    written, failures = _exporter(tmp_path).export_mark_from_futures(futures, out_dir=out)

    assert failures == []
    assert written == {"BTC": 1}
    mark = pl.read_ipc(out / "BTC_USDC_USDC-1h-mark.feather")
    futures_frame = pl.read_ipc(futures / "BTC_USDC_USDC-1h-futures.feather")
    assert mark["date"].to_list() == stamps
    # Same OHLC proxy as ``export_candles`` writes; volume is always 0.
    assert mark.select("open", "high", "low", "close").equals(
        futures_frame.select("open", "high", "low", "close")
    )
    assert mark["volume"].to_list() == [0.0] * 48


def test_mark_matches_the_transform_export_candles_uses(tmp_path: Path) -> None:
    futures = tmp_path / "futures"
    write_futures(futures, "ETH", START, 10)
    exporter = _exporter(tmp_path)
    exporter.export_mark_from_futures(futures, out_dir=tmp_path / "stage")

    source = pl.read_ipc(futures / "ETH_USDC_USDC-1h-futures.feather").rename({"date": "timestamp"})
    expected = exporter._transform_mark_price(source)
    got = pl.read_ipc(tmp_path / "stage" / "ETH_USDC_USDC-1h-mark.feather")
    assert got.equals(expected)


def test_rerun_is_idempotent_and_preserves_history(tmp_path: Path) -> None:
    futures = tmp_path / "futures"
    write_futures(futures, "BTC", START, 24)
    out = tmp_path / "stage"
    exporter = _exporter(tmp_path)
    exporter.export_mark_from_futures(futures, out_dir=out)
    first = (out / "BTC_USDC_USDC-1h-mark.feather").read_bytes()
    exporter.export_mark_from_futures(futures, out_dir=out)
    assert (out / "BTC_USDC_USDC-1h-mark.feather").read_bytes() == first

    # Futures now covers fewer bars (e.g. truncated): the earlier history stays.
    write_futures(futures, "BTC", START, 12)
    exporter.export_mark_from_futures(futures, out_dir=out)
    assert len(pl.read_ipc(out / "BTC_USDC_USDC-1h-mark.feather")) == 24


def test_only_requested_timeframes_and_symbols_are_written(tmp_path: Path) -> None:
    futures = tmp_path / "futures"
    write_futures(futures, "BTC", START, 24)
    write_futures(futures, "BTC", START, 24, tf="4h")
    write_futures(futures, "ETH", START, 24)
    out = tmp_path / "stage"

    written, _ = _exporter(tmp_path).export_mark_from_futures(futures, symbols=["BTC"], out_dir=out)

    assert written == {"BTC": 1}
    assert sorted(p.name for p in out.glob("*.feather")) == ["BTC_USDC_USDC-1h-mark.feather"]


def test_a_defective_futures_file_is_reported_not_raised(tmp_path: Path) -> None:
    futures = tmp_path / "futures"
    write_futures(futures, "BTC", START, 24)
    (futures / "ETH_USDC_USDC-1h-futures.feather").write_bytes(b"not arrow")

    written, failures = _exporter(tmp_path).export_mark_from_futures(
        futures, out_dir=tmp_path / "stage"
    )

    assert written == {"BTC": 1}
    assert [(f.symbol, f.timeframe) for f in failures] == [("ETH", "1h")]
