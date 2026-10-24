"""Oracle candle volume stays zero while separate trade-volume data is retained."""

import pandas as pd
import pyarrow.feather as feather


def _write_candles(path, dates, volumes=None):
    """Write a candle feather with the locked 6-column export schema."""
    volumes = [0.0] * len(dates) if volumes is None else volumes
    frame = pd.DataFrame(
        {
            "date": pd.to_datetime(dates, utc=True).as_unit("ns"),
            "open": [100.0] * len(dates),
            "high": [101.0] * len(dates),
            "low": [99.0] * len(dates),
            "close": [100.5] * len(dates),
            "volume": volumes,
        }
    )
    feather.write_feather(frame, path)
    return frame


class TestMergeEnforcesZeroVolume:
    def test_refetch_clears_legacy_candle_volume(self, tmp_path):
        """The exact daily-cron sequence: volume gets computed, then the next
        run re-fetches the same bars from an API that reports no volume."""
        from scripts.collect_daily_snapshot import _merge_feather

        path = tmp_path / "BTC_USDC_USDC-1h-futures.feather"
        _write_candles(path, ["2026-09-10T00:00", "2026-09-10T01:00"], volumes=[12.5, 8.25])

        refetched = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-09-10T00:00", "2026-09-10T01:00"], utc=True).as_unit(
                    "ns"
                ),
                "open": [100.0, 100.0],
                "high": [101.0, 101.0],
                "low": [99.0, 99.0],
                "close": [100.5, 100.5],
                "volume": [0.0, 0.0],
            }
        )
        _merge_feather(refetched, path)

        result = pd.read_feather(path)
        assert result["volume"].tolist() == [0.0, 0.0]

    def test_nonzero_incoming_volume_is_cleared(self, tmp_path):
        """Neither existing nor incoming traded size may enter oracle candles."""
        from scripts.collect_daily_snapshot import _merge_feather

        path = tmp_path / "BTC_USDC_USDC-1h-futures.feather"
        _write_candles(path, ["2026-09-10T00:00"], volumes=[12.5])

        updated = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-09-10T00:00"], utc=True).as_unit("ns"),
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [99.0],
            }
        )
        _merge_feather(updated, path)

        assert pd.read_feather(path)["volume"].tolist() == [0.0]

    def test_ohlc_is_still_refreshed_by_the_merge(self, tmp_path):
        """Zero-volume normalisation must still refresh price columns."""
        from scripts.collect_daily_snapshot import _merge_feather

        path = tmp_path / "BTC_USDC_USDC-1h-futures.feather"
        _write_candles(path, ["2026-09-10T00:00"], volumes=[12.5])

        corrected = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-09-10T00:00"], utc=True).as_unit("ns"),
                "open": [200.0],
                "high": [201.0],
                "low": [199.0],
                "close": [200.5],
                "volume": [0.0],
            }
        )
        _merge_feather(corrected, path)

        result = pd.read_feather(path)
        assert result["close"].tolist() == [200.5]
        assert result["volume"].tolist() == [0.0]


class TestApplyVolumeToCandles:
    def _volume_frame(self, dates, volumes, usd, symbol="BTC"):
        return pd.DataFrame(
            {
                "symbol": [symbol] * len(dates),
                "date": pd.to_datetime(dates, utc=True),
                "volume": volumes,
                "volume_usd": usd,
                "trades": [1] * len(dates),
            }
        )

    def test_writes_volume_onto_matching_bars_only(self, tmp_path):
        from gmx_historical_data.candle_volume import apply_volume_to_candles

        path = tmp_path / "BTC_USDC_USDC-1h-futures.feather"
        _write_candles(path, ["2026-09-10T00:00", "2026-09-10T01:00", "2026-09-10T02:00"])

        volume = self._volume_frame(
            ["2026-09-10T00:00", "2026-09-10T02:00"], [5.0, 7.0], [400.0, 560.0]
        )
        updated = apply_volume_to_candles(volume, tmp_path, timeframe="1h")

        result = pd.read_feather(path)
        assert result["volume"].tolist() == [0.0, 0.0, 0.0]
        assert result["close"].tolist() == [100.5, 100.5, 100.5]
        assert updated == 1

    def test_missing_symbol_file_is_skipped_quietly(self, tmp_path):
        from gmx_historical_data.candle_volume import apply_volume_to_candles

        volume = self._volume_frame(["2026-09-10T00:00"], [5.0], [400.0], symbol="NOSUCH")

        assert apply_volume_to_candles(volume, tmp_path, timeframe="1h") == 0

    def test_empty_volume_frame_is_a_noop(self, tmp_path):
        from gmx_historical_data.candle_volume import apply_volume_to_candles

        path = tmp_path / "BTC_USDC_USDC-1h-futures.feather"
        _write_candles(path, ["2026-09-10T00:00"], volumes=[3.0])

        empty = pd.DataFrame(
            {
                "symbol": [],
                "date": pd.to_datetime([], utc=True),
                "volume": [],
                "volume_usd": [],
                "trades": [],
            }
        )
        assert apply_volume_to_candles(empty, tmp_path, timeframe="1h") == 0
        assert pd.read_feather(path)["volume"].tolist() == [3.0]

    def test_schema_stays_six_columns(self, tmp_path):
        """EXPORT_COLUMNS is what Freqtrade reads; volume_usd lives in the
        sidecar file, never in the feather."""
        from gmx_historical_data.candle_volume import apply_volume_to_candles
        from gmx_historical_data.ohlcv_validation import EXPORT_COLUMNS

        path = tmp_path / "BTC_USDC_USDC-1h-futures.feather"
        _write_candles(path, ["2026-09-10T00:00"])

        apply_volume_to_candles(
            self._volume_frame(["2026-09-10T00:00"], [5.0], [400.0]), tmp_path, timeframe="1h"
        )

        assert tuple(pd.read_feather(path).columns) == EXPORT_COLUMNS
