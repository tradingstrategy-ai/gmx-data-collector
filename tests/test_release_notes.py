"""Tests for the release-notes coverage table (Asset/Timeframe/From/To/Candles)."""

import pandas as pd
import pyarrow.feather as feather


def _write_feather(path, dates):
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(dates, utc=True).as_unit("ns"),
            "open": [1.0] * len(dates),
            "high": [1.0] * len(dates),
            "low": [1.0] * len(dates),
            "close": [1.0] * len(dates),
            "volume": [0.0] * len(dates),
        }
    )
    feather.write_feather(df, path)


class TestCollectCoverageRows:
    def test_reads_asset_timeframe_range_and_count(self, tmp_path):
        from gmx_historical_data.release_notes import collect_coverage_rows

        _write_feather(
            tmp_path / "BTC_USDC_USDC-1m-futures.feather",
            ["2026-03-05", "2026-03-06", "2026-09-09"],
        )

        rows = collect_coverage_rows(tmp_path)

        assert len(rows) == 1
        row = rows[0]
        assert row.asset == "BTC_USDC_USDC"
        assert row.timeframe == "1m"
        assert row.first == "2026-03-05"
        assert row.last == "2026-09-09"
        assert row.candles == 3

    def test_collects_one_row_per_asset_per_timeframe(self, tmp_path):
        from gmx_historical_data.release_notes import collect_coverage_rows

        _write_feather(tmp_path / "BTC_USDC_USDC-1m-futures.feather", ["2026-03-05"])
        _write_feather(tmp_path / "BTC_USDC_USDC-1h-futures.feather", ["2025-09-13"])
        _write_feather(tmp_path / "ETH_USDC_USDC-1m-futures.feather", ["2026-03-05"])

        rows = collect_coverage_rows(tmp_path)

        assert {(r.asset, r.timeframe) for r in rows} == {
            ("BTC_USDC_USDC", "1m"),
            ("BTC_USDC_USDC", "1h"),
            ("ETH_USDC_USDC", "1m"),
        }

    def test_ignores_non_candle_files(self, tmp_path):
        """The cadence manifest and delisted roster live alongside the
        feathers but are not candle data."""
        from gmx_historical_data.release_notes import collect_coverage_rows

        (tmp_path / "_cadence_manifest.json").write_text("{}", encoding="utf-8")
        _write_feather(tmp_path / "BTC_USDC_USDC-1m-futures.feather", ["2026-03-05"])

        rows = collect_coverage_rows(tmp_path)

        assert len(rows) == 1
        assert rows[0].asset == "BTC_USDC_USDC"

    def test_skips_empty_feather(self, tmp_path):
        from gmx_historical_data.release_notes import collect_coverage_rows

        empty = pd.DataFrame({"date": pd.Series([], dtype="datetime64[ns, UTC]")})
        feather.write_feather(empty, tmp_path / "EMPTY_USDC_USDC-1m-futures.feather")

        rows = collect_coverage_rows(tmp_path)

        assert rows == []

    def test_missing_dir_returns_empty(self, tmp_path):
        from gmx_historical_data.release_notes import collect_coverage_rows

        rows = collect_coverage_rows(tmp_path / "does-not-exist")

        assert rows == []


class TestRenderCoverageTable:
    def test_matches_apex_style_markdown_table(self):
        from gmx_historical_data.release_notes import CoverageRow, render_coverage_table

        rows = [
            CoverageRow("ETH_USDC_USDC", "1m", "2026-03-05", "2026-09-09", 256993),
            CoverageRow("BTC_USDC_USDC", "1h", "2025-09-13", "2026-09-09", 8809),
            CoverageRow("BTC_USDC_USDC", "1m", "2026-03-05", "2026-09-09", 256992),
        ]

        table = render_coverage_table(rows)

        assert "## Coverage" in table
        assert "| Asset | Timeframe | From | To | Candles |" in table
        assert "|---|---|---|---|---:|" in table
        # Thousands separator, matching the Apex reference release.
        assert "| BTC_USDC_USDC | 1m | 2026-03-05 | 2026-09-09 | 256,992 |" in table
        # Sorted by asset, then timeframe -- BTC before ETH, 1h before 1m.
        btc_1h = table.index("BTC_USDC_USDC | 1h")
        btc_1m = table.index("BTC_USDC_USDC | 1m")
        eth_1m = table.index("ETH_USDC_USDC | 1m")
        assert btc_1h < btc_1m < eth_1m

    def test_empty_rows_does_not_render_a_bare_header(self):
        from gmx_historical_data.release_notes import render_coverage_table

        table = render_coverage_table([])

        assert "## Coverage" in table
        assert "| Asset | Timeframe" not in table


class TestMain:
    def test_build_writes_table_to_out_file(self, tmp_path):
        from gmx_historical_data.release_notes import main

        futures_dir = tmp_path / "futures"
        futures_dir.mkdir()
        _write_feather(
            futures_dir / "BTC_USDC_USDC-1m-futures.feather", ["2026-03-05", "2026-03-06"]
        )
        out = tmp_path / "coverage.md"

        rc = main(["build", "--futures-dir", str(futures_dir), "--out", str(out)])

        assert rc == 0
        text = out.read_text(encoding="utf-8")
        assert "## Coverage" in text
        assert "BTC_USDC_USDC | 1m | 2026-03-05 | 2026-03-06 | 2 |" in text
