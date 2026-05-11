"""Static-analysis isolation tests for OI / pool-liquidity / funding extractors.

These tests grep extractor scripts for filesystem writes that escape their
expected output directory.  They will fail loudly if a future PR couples
one pipeline to another.

Context: the 2026-05-11 incident showed that cross-pipeline writes (the
freqtrade exporter handling OHLCV *and* funding in one method) can let a
flag intended for one data type silently corrupt another.  These tests
prevent that pattern from creeping back in.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def _read(path: str) -> str:
    return (REPO / path).read_text()


def test_oi_extractor_does_not_write_outside_oi_dir():
    """``extract_open_interest.py`` must only write to its own output dir."""
    src = _read("scripts/extract_open_interest.py")
    forbidden = ("/candles/", "/funding/", "gmx/futures/")
    for needle in forbidden:
        assert needle not in src, (
            f"extract_open_interest.py writes to '{needle}' — must stay in OI dir."
        )


def test_pool_liquidity_does_not_write_outside_its_dir():
    """``extract_pool_liquidity.py`` must only write to its own output dir."""
    src = _read("scripts/extract_pool_liquidity.py")
    forbidden = ("/candles/", "/funding/", "gmx/futures/")
    for needle in forbidden:
        assert needle not in src, (
            f"extract_pool_liquidity.py writes to '{needle}' — must stay in pool dir."
        )


def test_unified_funding_does_not_write_to_candles():
    """``extract_unified_funding.py`` must never write into ``candles/``."""
    src = _read("scripts/extract_unified_funding.py")
    candle_writes = re.findall(r"\.write_(?:parquet|ipc)\([^)]*candles", src)
    assert not candle_writes, (
        "extract_unified_funding.py writes into a candles/ path — broken isolation."
    )


def test_freqtrade_exporter_has_split_methods():
    """The exporter exposes isolated ``export_candles`` and ``export_funding``."""
    src = _read("src/gmx_historical_data/freqtrade_exporter.py")
    assert "def export_candles(" in src, (
        "FreqtradeExporter must expose export_candles() — required for path isolation."
    )
    assert "def export_funding(" in src, (
        "FreqtradeExporter must expose export_funding() — required for path isolation."
    )
    # The cleanup helper must remain candle-only.
    assert "def _cleanup_candle_source(" in src, (
        "Cleanup helper must be named _cleanup_candle_source to keep its scope explicit."
    )
    assert "def _cleanup_source_parquet(" not in src, (
        "Legacy _cleanup_source_parquet name must be removed (renamed to "
        "_cleanup_candle_source for explicit scope)."
    )


def test_freqtrade_exporter_funding_path_never_calls_cleanup():
    """``export_funding`` must not invoke any cleanup that touches candle paths."""
    src = _read("src/gmx_historical_data/freqtrade_exporter.py")

    # Split on top-level (4-space-indented) method defs and grab the
    # export_funding chunk only.
    chunks = re.split(r"\n    def ", src)
    funding_chunks = [c for c in chunks if c.startswith("export_funding(")]
    assert funding_chunks, "Could not locate export_funding method body"
    body = funding_chunks[0]
    assert "_cleanup_" not in body, (
        "export_funding contains a _cleanup_* call — must never delete other pipelines' files."
    )
