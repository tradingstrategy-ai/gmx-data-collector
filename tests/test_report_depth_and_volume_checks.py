"""I/O glue for the per-symbol depth-inversion and zero-volume report checks.

``gmx_historical_data.depth_inversion`` and ``.zero_volume_check`` are pure
and tested on their own (see ``test_depth_inversion.py`` and
``test_zero_volume_check.py``). These tests cover the loaders in
``scripts/collect_daily_snapshot.py`` that feed them from real feathers and
parquet files, and the end-to-end wiring into ``generate_report``: the new
report sections, and the ``::warning::`` annotations that must fire only
inside GitHub Actions and only for a genuine truncation/bug signature.
"""

from datetime import UTC, datetime, timedelta

import pandas as pd
import pyarrow.feather as feather

from scripts.collect_daily_snapshot import (
    _earliest_dates_by_tf,
    _open_interest_usd_by_symbol,
    _zero_volume_shares,
    generate_report,
)


def _write_candles(futures_dir, pair: str, tf: str, dates: list[datetime], volume=None):
    futures_dir.mkdir(parents=True, exist_ok=True)
    idx = pd.to_datetime(dates, utc=True).as_unit("ns")
    n = len(idx)
    frame = pd.DataFrame(
        {
            "date": idx,
            "open": [1.0] * n,
            "high": [1.0] * n,
            "low": [1.0] * n,
            "close": [1.0] * n,
            "volume": volume if volume is not None else [0.0] * n,
        }
    )
    feather.write_feather(frame, futures_dir / f"{pair}-{tf}-futures.feather")


class TestEarliestDatesByTf:
    def test_missing_directory_is_empty(self, tmp_path):
        result = _earliest_dates_by_tf(tmp_path / "nope")

        assert result == {tf: {} for tf in result}

    def test_reads_earliest_date_per_pair_per_timeframe(self, tmp_path):
        _write_candles(
            tmp_path,
            "BTC_USDC_USDC",
            "4h",
            [datetime(2023, 7, 20, tzinfo=UTC), datetime(2023, 7, 21, tzinfo=UTC)],
        )
        _write_candles(
            tmp_path,
            "BTC_USDC_USDC",
            "1d",
            [datetime(2026, 3, 8, tzinfo=UTC)],
        )

        result = _earliest_dates_by_tf(tmp_path)

        assert result["4h"]["BTC_USDC_USDC"] == datetime(2023, 7, 20, tzinfo=UTC).date()
        assert result["1d"]["BTC_USDC_USDC"] == datetime(2026, 3, 8, tzinfo=UTC).date()

    def test_unreadable_file_is_skipped(self, tmp_path):
        tmp_path.mkdir(parents=True, exist_ok=True)
        (tmp_path / "BAD_USDC_USDC-1h-futures.feather").write_bytes(b"not a feather")

        result = _earliest_dates_by_tf(tmp_path)

        assert result["1h"] == {}


class TestOpenInterestUsdBySymbol:
    def test_sums_long_and_short_converted_from_gmx_precision(self):
        markets_df = pd.DataFrame(
            {
                "symbol": ["BTC"],
                "is_swap_only": [False],
                "open_interest_long": [str(500_000 * int(1e30))],
                "open_interest_short": [str(500_000 * int(1e30))],
            }
        )

        result = _open_interest_usd_by_symbol(markets_df)

        assert result["BTC"] == 1_000_000.0

    def test_swap_only_rows_are_excluded(self):
        markets_df = pd.DataFrame(
            {
                "symbol": [""],
                "is_swap_only": [True],
                "open_interest_long": ["0"],
                "open_interest_short": ["0"],
            }
        )

        assert _open_interest_usd_by_symbol(markets_df) == {}

    def test_unparsable_oi_is_skipped_not_raised(self):
        markets_df = pd.DataFrame(
            {
                "symbol": ["BOME"],
                "is_swap_only": [False],
                "open_interest_long": ["not-a-number"],
                "open_interest_short": ["0"],
            }
        )

        assert _open_interest_usd_by_symbol(markets_df) == {}


class TestZeroVolumeShares:
    def test_missing_directory_is_empty(self, tmp_path):
        assert _zero_volume_shares(tmp_path / "nope", 30, pd.Timestamp.now(tz="UTC")) == {}

    def test_computes_share_over_the_lookback_window(self, tmp_path):
        now = pd.Timestamp(datetime(2026, 9, 27, tzinfo=UTC))
        dates = [now - timedelta(hours=i) for i in range(10)]
        volumes = [0.0] * 8 + [5.0, 5.0]  # 8/10 zero
        _write_candles(tmp_path, "BOME_USDC_USDC", "1h", dates, volume=volumes)

        result = _zero_volume_shares(tmp_path, lookback_days=30, now=now)

        share, count = result["BOME_USDC_USDC"]
        assert count == 10
        assert share == 0.8

    def test_candles_outside_the_window_are_excluded(self, tmp_path):
        now = pd.Timestamp(datetime(2026, 9, 27, tzinfo=UTC))
        recent = [now - timedelta(hours=i) for i in range(5)]
        old = [now - timedelta(days=200)]
        _write_candles(
            tmp_path,
            "AAA_USDC_USDC",
            "1h",
            old + recent,
            volume=[9.0] + [0.0] * 5,
        )

        result = _zero_volume_shares(tmp_path, lookback_days=30, now=now)

        _share, count = result["AAA_USDC_USDC"]
        assert count == 5  # the old, non-zero candle must not dilute the window

    def test_non_1h_files_are_ignored(self, tmp_path):
        now = pd.Timestamp(datetime(2026, 9, 27, tzinfo=UTC))
        _write_candles(tmp_path, "AAA_USDC_USDC", "4h", [now])

        assert _zero_volume_shares(tmp_path, 30, now) == {}


class TestReportDepthAndVolumeSections:
    def _base_markets_df(self, symbols_and_oi: dict[str, float]) -> pd.DataFrame:
        rows = []
        for i, (sym, oi) in enumerate(symbols_and_oi.items()):
            oi_units = str(int(oi * 1e30))
            rows.append(
                {
                    "name": f"{sym}/USD",
                    "symbol": sym,
                    "is_swap_only": False,
                    "is_listed": True,
                    "market_token": f"0x{i:040x}",
                    "open_interest_long": oi_units,
                    "open_interest_short": "0",
                }
            )
        columns = [
            "name",
            "symbol",
            "is_swap_only",
            "is_listed",
            "market_token",
            "open_interest_long",
            "open_interest_short",
        ]
        return pd.DataFrame(rows, columns=columns)

    def _generate(self, tmp_path, markets_df, futures_dir, gmx_root, date_str="2026-09-27"):
        report_path = tmp_path / "report.txt"
        generate_report(
            date_str=date_str,
            markets_df=markets_df,
            candle_count=0,
            failed_symbols=[],
            ticker_count=0,
            apy_count=0,
            volume_count=0,
            volume_data={},
            futures_dir=futures_dir,
            snapshots_dir=tmp_path / "snapshots",
            tickers_dir=tmp_path / "tickers",
            apy_dir=tmp_path / "apy",
            volumes_dir=tmp_path / "volumes",
            report_path=report_path,
            ohlcv_coverage=None,
            skipped=None,
        )
        return report_path.read_text()

    def test_depth_inversion_warns_for_a_systemic_lag(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        futures_dir = tmp_path / "gmx" / "futures"
        pairs = [f"SYM{i}_USDC_USDC" for i in range(25)]
        for pair in pairs:
            _write_candles(futures_dir, pair, "4h", [datetime(2023, 7, 20, tzinfo=UTC)])
            _write_candles(futures_dir, pair, "1d", [datetime(2026, 3, 8, tzinfo=UTC)])

        markets_df = self._base_markets_df({})
        content = self._generate(tmp_path, markets_df, futures_dir, tmp_path / "gmx")

        assert "## Per-Symbol Candle Depth" in content
        assert "1d" in content and "2026-03-08" in content
        out = capsys.readouterr().out
        assert "::warning::" in out
        assert "depth inversion" in out.lower() or "1d" in out

    def test_v2_launch_cohort_alone_does_not_warn(self, tmp_path, capsys, monkeypatch):
        """Many pairs legitimately sharing one earliest date at every
        timeframe (no lag between finer/coarser siblings) must not warn."""
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        futures_dir = tmp_path / "gmx" / "futures"
        launch = datetime(2023, 7, 20, tzinfo=UTC)
        pairs = [f"CL{i}_USDC_USDC" for i in range(30)]
        for pair in pairs:
            _write_candles(futures_dir, pair, "1h", [launch])
            _write_candles(futures_dir, pair, "4h", [launch])

        markets_df = self._base_markets_df({})
        content = self._generate(tmp_path, markets_df, futures_dir, tmp_path / "gmx")

        assert "## Per-Symbol Candle Depth" in content
        out = capsys.readouterr().out
        assert "depth inversion" not in out.lower()

    def test_zero_volume_negligible_oi_is_listed_expected_without_warning(
        self, tmp_path, capsys, monkeypatch
    ):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        futures_dir = tmp_path / "gmx" / "futures"
        now = datetime(2026, 9, 27, tzinfo=UTC)
        dates = [now - timedelta(hours=i) for i in range(720)]
        _write_candles(futures_dir, "BOME_USDC_USDC", "1h", dates, volume=[0.0] * 720)

        markets_df = self._base_markets_df({"BOME": 0.0})
        content = self._generate(
            tmp_path, markets_df, futures_dir, tmp_path / "gmx", date_str="2026-09-27"
        )

        assert "## Per-Symbol Zero-Volume Check" in content
        assert "BOME" in content
        out = capsys.readouterr().out
        assert "zero-volume with open interest" not in out.lower()

    def test_zero_volume_with_real_oi_warns(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        futures_dir = tmp_path / "gmx" / "futures"
        now = datetime(2026, 9, 27, tzinfo=UTC)
        dates = [now - timedelta(hours=i) for i in range(720)]
        _write_candles(futures_dir, "BUGGY_USDC_USDC", "1h", dates, volume=[0.0] * 720)

        markets_df = self._base_markets_df({"BUGGY": 5_000_000.0})
        content = self._generate(
            tmp_path, markets_df, futures_dir, tmp_path / "gmx", date_str="2026-09-27"
        )

        assert "BUGGY" in content
        out = capsys.readouterr().out
        assert "::warning::" in out
        assert "BUGGY" in out

    def test_delisted_pairs_are_excluded_from_zero_volume_check(
        self, tmp_path, capsys, monkeypatch
    ):
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        gmx_root = tmp_path / "gmx"
        futures_dir = gmx_root / "futures"
        now = datetime(2026, 9, 27, tzinfo=UTC)
        dates = [now - timedelta(hours=i) for i in range(720)]
        _write_candles(futures_dir, "DEAD_USDC_USDC", "1h", dates, volume=[0.0] * 720)
        gmx_root.mkdir(parents=True, exist_ok=True)
        (gmx_root / "delisted_markets.json").write_text('{"symbols": ["DEAD"]}', encoding="utf-8")

        markets_df = self._base_markets_df({"DEAD": 5_000_000.0})
        content = self._generate(tmp_path, markets_df, futures_dir, gmx_root, date_str="2026-09-27")

        assert "DEAD" not in content.split("## Per-Symbol Zero-Volume Check")[1].split("##")[0]
        out = capsys.readouterr().out
        assert "DEAD" not in out

    def test_checks_never_abort_the_report_on_bad_data(self, tmp_path, monkeypatch):
        """Fail-soft: a corrupt feather in the depth/volume checks must not
        take down report generation, which runs at the end of an otherwise
        successful release."""
        monkeypatch.setenv("GITHUB_ACTIONS", "true")
        futures_dir = tmp_path / "gmx" / "futures"
        futures_dir.mkdir(parents=True)
        (futures_dir / "BAD_USDC_USDC-1h-futures.feather").write_bytes(b"not a feather")
        (futures_dir / "BAD_USDC_USDC-4h-futures.feather").write_bytes(b"not a feather")

        markets_df = self._base_markets_df({})
        # Must not raise.
        self._generate(tmp_path, markets_df, futures_dir, tmp_path / "gmx")
