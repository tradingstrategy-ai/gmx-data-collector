"""Tests for the cadence manifest shipped alongside exported candle feathers."""

import json
from pathlib import Path

import pandas as pd

from gmx_historical_data.freqtrade_exporter import (
    CADENCE_MANIFEST_NAME,
    FreqtradeExporter,
)
from gmx_historical_data.storage import ParquetStorage


def _candles(symbol: str, hour_offsets: list[int]) -> pd.DataFrame:
    """Build a candle frame at explicit hour offsets so a hole can be seeded.

    :param symbol: Token symbol.
    :param hour_offsets: Hour offsets from 2024-01-01 00:00 UTC.
    :returns: pandas DataFrame with the columns ``save_candles`` requires.
    """
    base = pd.Timestamp("2024-01-01", tz="UTC")
    timestamps = [base + pd.Timedelta(hours=h) for h in hour_offsets]
    n = len(timestamps)
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "open": [1.0] * n,
            "high": [1.0] * n,
            "low": [1.0] * n,
            "close": [1.0] * n,
            "symbol": [symbol] * n,
        }
    )


def _manifest(output_dir: Path) -> dict:
    path = output_dir / "gmx" / "futures" / CADENCE_MANIFEST_NAME
    return json.loads(path.read_text(encoding="utf-8"))


def test_manifest_records_a_gap_and_marks_clean_files(tmp_path: Path):
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    storage.save_candles(_candles("AAA", [0, 1, 2, 3]), "1h", "AAA")  # contiguous
    storage.save_candles(_candles("BBB", [0, 1, 3]), "1h", "BBB")  # 02:00 missing

    output_dir = tmp_path / "output"
    FreqtradeExporter(data_dir, output_dir).export_candles(
        symbols=["AAA", "BBB"], timeframes=["1h"]
    )

    manifest = _manifest(output_dir)
    assert "generated_at" in manifest

    clean = manifest["files"]["AAA_USDC_USDC-1h-futures.feather"]
    assert clean["breaks_total"] == 0
    assert clean["missing_bars_total"] == 0
    assert clean["breaks"] == []
    assert clean["timeframe"] == "1h"
    assert clean["expected_interval_seconds"] == 3600
    assert clean["rows"] == 4

    gapped = manifest["files"]["BBB_USDC_USDC-1h-futures.feather"]
    assert gapped["breaks_total"] == 1
    assert gapped["missing_bars_total"] == 1
    assert gapped["breaks"][0]["missing_bars"] == 1
    assert gapped["breaks"][0]["before"] == "2024-01-01T01:00:00+00:00"
    assert gapped["breaks"][0]["after"] == "2024-01-01T03:00:00+00:00"


def test_manifest_records_every_clean_file_so_absence_is_meaningful(tmp_path: Path):
    """A consumer must be able to tell 'checked, contiguous' from 'not checked'.
    Recording only gapped files would make those two indistinguishable."""
    data_dir = tmp_path / "data"
    ParquetStorage(data_dir).save_candles(_candles("AAA", [0, 1, 2]), "1h", "AAA")

    output_dir = tmp_path / "output"
    FreqtradeExporter(data_dir, output_dir).export_candles(symbols=["AAA"], timeframes=["1h"])

    assert "AAA_USDC_USDC-1h-futures.feather" in _manifest(output_dir)["files"]


def test_partial_export_merges_into_existing_manifest(tmp_path: Path):
    """A `--symbol BBB` run must not erase AAA's entry."""
    data_dir = tmp_path / "data"
    storage = ParquetStorage(data_dir)
    storage.save_candles(_candles("AAA", [0, 1, 2]), "1h", "AAA")
    storage.save_candles(_candles("BBB", [0, 1, 3]), "1h", "BBB")

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    exporter.export_candles(symbols=["AAA", "BBB"], timeframes=["1h"])
    exporter.export_candles(symbols=["BBB"], timeframes=["1h"])

    files = _manifest(output_dir)["files"]
    assert "AAA_USDC_USDC-1h-futures.feather" in files
    assert "BBB_USDC_USDC-1h-futures.feather" in files


def test_manifest_excludes_funding_and_mark_files(tmp_path: Path):
    """Funding frames are deliberately not cadence-checked (drop_nulls by
    design, free-form timeframes); mark/index files duplicate the candle
    series and would double every entry."""
    data_dir = tmp_path / "data"
    ParquetStorage(data_dir).save_candles(_candles("AAA", [0, 1, 3]), "1h", "AAA")

    output_dir = tmp_path / "output"
    FreqtradeExporter(data_dir, output_dir).export_candles(symbols=["AAA"], timeframes=["1h"])

    names = set(_manifest(output_dir)["files"])
    assert names == {"AAA_USDC_USDC-1h-futures.feather"}


def test_manifest_truncates_pathological_break_lists(tmp_path: Path):
    """1m files carry thousands of breaks; the manifest must stay small."""
    from gmx_historical_data.freqtrade_exporter import MAX_BREAKS_PER_FILE

    # Every other bar present -> one break per present-pair.
    offsets = list(range(0, (MAX_BREAKS_PER_FILE + 20) * 2, 2))
    data_dir = tmp_path / "data"
    ParquetStorage(data_dir).save_candles(_candles("AAA", offsets), "1h", "AAA")

    output_dir = tmp_path / "output"
    FreqtradeExporter(data_dir, output_dir).export_candles(symbols=["AAA"], timeframes=["1h"])

    entry = _manifest(output_dir)["files"]["AAA_USDC_USDC-1h-futures.feather"]
    assert entry["truncated"] is True
    assert len(entry["breaks"]) == MAX_BREAKS_PER_FILE
    assert entry["breaks_total"] > MAX_BREAKS_PER_FILE


def test_manifest_is_not_mistaken_for_a_data_file(tmp_path: Path):
    """The manifest lives in the futures dir; a second export must not try to
    read it as a feather."""
    data_dir = tmp_path / "data"
    ParquetStorage(data_dir).save_candles(_candles("AAA", [0, 1, 2]), "1h", "AAA")

    output_dir = tmp_path / "output"
    exporter = FreqtradeExporter(data_dir, output_dir)
    exporter.export_candles(symbols=["AAA"], timeframes=["1h"])
    results, failed_symbols, failures = exporter.export_candles(symbols=["AAA"], timeframes=["1h"])

    assert failed_symbols == []
    assert failures == []
    assert "AAA" in results
