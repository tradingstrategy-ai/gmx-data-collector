"""Tests for daily snapshot collection helpers."""

import pandas as pd
import pyarrow.feather as feather
import pytest


def _make_ohlcv(dates, close_values):
    """Build a minimal OHLCV DataFrame for testing."""
    return pd.DataFrame(
        {
            "date": pd.to_datetime(dates, utc=True).as_unit("ns"),
            "open": close_values,
            "high": close_values,
            "low": close_values,
            "close": close_values,
            "volume": 0.0,
        }
    )


class TestMergeFeather:
    """Tests for the _merge_feather helper."""

    def test_new_file_created(self, tmp_path):
        """First run creates the file from scratch."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        new_df = _make_ohlcv(["2026-03-10", "2026-03-11"], [100.0, 110.0])

        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 2
        assert list(result["close"]) == [100.0, 110.0]

    def test_append_no_overlap(self, tmp_path):
        """New rows are appended when no overlap exists."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        existing = _make_ohlcv(["2026-03-10"], [100.0])
        feather.write_feather(existing, filepath)

        new_df = _make_ohlcv(["2026-03-11", "2026-03-12"], [110.0, 120.0])
        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 3
        assert list(result["close"]) == [100.0, 110.0, 120.0]

    def test_overlap_keeps_new(self, tmp_path):
        """Overlapping timestamps use new data, existing data preserved."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        existing = _make_ohlcv(["2026-03-10", "2026-03-11"], [100.0, 110.0])
        feather.write_feather(existing, filepath)

        new_df = _make_ohlcv(["2026-03-11", "2026-03-12"], [115.0, 120.0])
        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 3
        assert list(result["close"]) == [100.0, 115.0, 120.0]

    def test_existing_data_never_lost(self, tmp_path):
        """Historical data not in new fetch is always preserved."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        existing = _make_ohlcv(["2026-03-08", "2026-03-09", "2026-03-10"], [80.0, 90.0, 100.0])
        feather.write_feather(existing, filepath)

        new_df = _make_ohlcv(["2026-03-11"], [110.0])
        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert len(result) == 4
        assert list(result["close"]) == [80.0, 90.0, 100.0, 110.0]

    def test_sorted_output(self, tmp_path):
        """Output is always sorted by date regardless of input order."""
        from scripts.collect_daily_snapshot import _merge_feather

        filepath = tmp_path / "TEST_USDC_USDC-1d-futures.feather"
        new_df = _make_ohlcv(["2026-03-12", "2026-03-10", "2026-03-11"], [120.0, 100.0, 110.0])

        _merge_feather(new_df, filepath)

        result = pd.read_feather(filepath)
        assert list(result["close"]) == [100.0, 110.0, 120.0]


class TestExtractSymbols:
    """Tests for the _extract_symbols helper."""

    def test_extracts_unique_listed_perp_symbols(self):
        from scripts.collect_daily_snapshot import _extract_symbols

        markets = [
            {"name": "ETH/USD [ETH-USDC]", "isListed": True},
            {"name": "ETH/USD [ETH-ETH]", "isListed": True},
            {"name": "BTC/USD [WBTC.b-USDC]", "isListed": True},
            {"name": "USDC-USDT", "isListed": True},  # swap-only, no "/"
            {"name": "DOGE/USD [ETH-USDC]", "isListed": False},  # unlisted
        ]
        symbols = _extract_symbols(markets)
        assert symbols == ["BTC", "ETH"]

    def test_empty_markets(self):
        from scripts.collect_daily_snapshot import _extract_symbols

        assert _extract_symbols([]) == []


class TestCollectApy:
    """Tests for the APY collection function."""

    def test_flatten_apy_data(self):
        """APY response is flattened with market_token and period columns."""
        from scripts.collect_daily_snapshot import _flatten_apy

        raw_apy = {
            "markets": {
                "0xabc": {"apy": 0.05, "baseApy": 0.04, "bonusApr": 0.01},
                "0xdef": {"apy": 0.10, "baseApy": 0.10, "bonusApr": 0.0},
            },
            "glvs": {
                "0x111": {"apy": 0.03, "baseApy": 0.03, "bonusApr": 0.0},
            },
        }
        df = _flatten_apy(raw_apy, "30d", "2026-03-12")
        assert len(df) == 3  # 2 markets + 1 glv
        assert "market_token" in df.columns
        assert "period" in df.columns
        assert "apy" in df.columns
        assert "type" in df.columns
        assert set(df["type"]) == {"market", "glv"}
        assert all(df["period"] == "30d")

    def test_flatten_apy_empty(self):
        """Empty APY response returns empty DataFrame."""
        from scripts.collect_daily_snapshot import _flatten_apy

        df = _flatten_apy({}, "30d", "2026-03-12")
        assert len(df) == 0


class TestReleaseGuardrails:
    """Tests for release-abort guardrails in the daily collector."""

    def test_abort_on_failed_ohlcv_fetches_allows_success(self):
        from scripts.collect_daily_snapshot import _abort_on_failed_ohlcv_fetches

        _abort_on_failed_ohlcv_fetches([])

    def test_abort_on_failed_ohlcv_fetches_fails_on_partial_run(self):
        from scripts.collect_daily_snapshot import _abort_on_failed_ohlcv_fetches

        with pytest.raises(SystemExit) as excinfo:
            _abort_on_failed_ohlcv_fetches(["BTC/1m", "ETH/5m"])

        assert excinfo.value.code == 1


class TestOhlcvRetry:
    """Tests for OHLCV retry handling in the daily collector."""

    def test_fetch_candles_with_retry_recovers_from_transient_error(self, monkeypatch):
        from scripts.collect_daily_snapshot import _fetch_candles_with_retry

        sleeps: list[float] = []

        class FakeApi:
            def __init__(self):
                self.calls = 0

            def get_candlesticks_dataframe(self, symbol, period, limit):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("temporary outage")
                return _make_ohlcv(["2026-03-10"], [100.0]).rename(columns={"date": "timestamp"})

        monkeypatch.setattr(
            "scripts.collect_daily_snapshot.time.sleep", lambda seconds: sleeps.append(seconds)
        )

        df = _fetch_candles_with_retry(
            FakeApi(), "BTC", "1m", 10000, max_retries=5, initial_backoff=2.0
        )

        assert len(df) == 1
        assert sleeps == [2.0]

    def test_fetch_candles_with_retry_fails_after_exhausting_attempts(self, monkeypatch):
        from scripts.collect_daily_snapshot import _fetch_candles_with_retry

        sleeps: list[float] = []

        class FakeApi:
            def get_candlesticks_dataframe(self, symbol, period, limit):
                raise RuntimeError("temporary outage")

        monkeypatch.setattr(
            "scripts.collect_daily_snapshot.time.sleep", lambda seconds: sleeps.append(seconds)
        )

        with pytest.raises(
            RuntimeError, match="Failed to fetch candles for BTC/1m after 3 attempts"
        ):
            _fetch_candles_with_retry(
                FakeApi(), "BTC", "1m", 10000, max_retries=3, initial_backoff=2.0
            )

        assert sleeps == [2.0, 4.0]

    def test_fetch_candles_with_retry_treats_empty_response_as_failure(self, monkeypatch):
        """Empty DataFrame must trigger a retry, not be returned as a successful empty result."""
        import pandas as pd

        from scripts.collect_daily_snapshot import _fetch_candles_with_retry

        sleeps: list[float] = []

        class FakeApi:
            def __init__(self):
                self.calls = 0

            def get_candlesticks_dataframe(self, symbol, period, limit):
                self.calls += 1
                if self.calls == 1:
                    return pd.DataFrame()  # transient empty response
                return _make_ohlcv(["2026-03-10"], [100.0]).rename(columns={"date": "timestamp"})

        monkeypatch.setattr(
            "scripts.collect_daily_snapshot.time.sleep", lambda seconds: sleeps.append(seconds)
        )

        df = _fetch_candles_with_retry(
            FakeApi(), "BTC", "1m", 10000, max_retries=5, initial_backoff=2.0
        )

        assert len(df) == 1, "retry should have produced non-empty data"
        assert sleeps == [2.0], "exactly one backoff sleep expected before the recovery call"

    def test_fetch_candles_with_retry_fails_when_responses_stay_empty(self, monkeypatch):
        """Empty responses on every attempt must raise after exhausting retries (not silently succeed)."""
        import pandas as pd

        from scripts.collect_daily_snapshot import _fetch_candles_with_retry

        sleeps: list[float] = []

        class FakeApi:
            def get_candlesticks_dataframe(self, symbol, period, limit):
                return pd.DataFrame()

        monkeypatch.setattr(
            "scripts.collect_daily_snapshot.time.sleep", lambda seconds: sleeps.append(seconds)
        )

        with pytest.raises(
            RuntimeError, match="Failed to fetch candles for BTC/1m after 3 attempts"
        ):
            _fetch_candles_with_retry(
                FakeApi(), "BTC", "1m", 10000, max_retries=3, initial_backoff=2.0
            )

        assert sleeps == [2.0, 4.0]


class TestCollectTickers:
    """Tests for the ticker collection function."""

    def test_flatten_ticker_data(self):
        """Ticker list is flattened to a DataFrame with correct columns."""
        from scripts.collect_daily_snapshot import _flatten_tickers

        raw_tickers = [
            {
                "tokenAddress": "0xabc",
                "tokenSymbol": "ETH",
                "minPrice": "330000000000",
                "maxPrice": "331000000000",
                "updatedAt": 1773308683207,
                "timestamp": 1773308682,
            },
            {
                "tokenAddress": "0xdef",
                "tokenSymbol": "BTC",
                "minPrice": "8300000000000",
                "maxPrice": "8310000000000",
                "updatedAt": 1773308683207,
                "timestamp": 1773308682,
            },
        ]
        df = _flatten_tickers(raw_tickers, "2026-03-12")
        assert len(df) == 2
        assert "token_symbol" in df.columns
        assert "min_price" in df.columns
        assert "max_price" in df.columns
        assert "date" in df.columns
        assert list(df["token_symbol"]) == ["ETH", "BTC"]


class TestExpectedLastBar:
    """_expected_last_bar — derives latest fully-closed bar for a timeframe."""

    def test_today_1h(self):
        from scripts.collect_daily_snapshot import _expected_last_bar

        now = pd.Timestamp("2026-05-14 17:42", tz="UTC")
        got = _expected_last_bar("1h", target_date="2026-05-14", now=now)
        assert got == pd.Timestamp("2026-05-14 17:00", tz="UTC")

    def test_today_15m(self):
        from scripts.collect_daily_snapshot import _expected_last_bar

        now = pd.Timestamp("2026-05-14 17:42", tz="UTC")
        got = _expected_last_bar("15m", target_date="2026-05-14", now=now)
        assert got == pd.Timestamp("2026-05-14 17:30", tz="UTC")

    def test_today_1d(self):
        from scripts.collect_daily_snapshot import _expected_last_bar

        now = pd.Timestamp("2026-05-14 17:42", tz="UTC")
        got = _expected_last_bar("1d", target_date="2026-05-14", now=now)
        assert got == pd.Timestamp("2026-05-14", tz="UTC")

    def test_past_date_1h(self):
        """Backfill — clamps to end-of-day for the target date."""
        from scripts.collect_daily_snapshot import _expected_last_bar

        now = pd.Timestamp("2026-05-14 17:42", tz="UTC")
        got = _expected_last_bar("1h", target_date="2026-03-10", now=now)
        assert got == pd.Timestamp("2026-03-10 23:00", tz="UTC")

    def test_past_date_1d(self):
        from scripts.collect_daily_snapshot import _expected_last_bar

        now = pd.Timestamp("2026-05-14 17:42", tz="UTC")
        got = _expected_last_bar("1d", target_date="2026-03-10", now=now)
        assert got == pd.Timestamp("2026-03-10", tz="UTC")


class TestRowCount:
    def test_existing_parquet(self, tmp_path):
        import polars as pl

        from scripts.collect_daily_snapshot import _row_count

        p = tmp_path / "x.parquet"
        pl.DataFrame({"a": [1, 2, 3, 4]}).write_parquet(str(p))
        assert _row_count(p) == 4

    def test_missing_returns_zero(self, tmp_path):
        from scripts.collect_daily_snapshot import _row_count

        assert _row_count(tmp_path / "nope.parquet") == 0

    def test_corrupt_returns_zero(self, tmp_path):
        from scripts.collect_daily_snapshot import _row_count

        p = tmp_path / "corrupt.parquet"
        p.write_bytes(b"junk")
        assert _row_count(p) == 0


class TestOhlcvGateSkip:
    """collect_and_save_ohlcv — feather-already-current → SKIPPED."""

    def test_skipped_status_when_feather_current(self, tmp_path, monkeypatch):
        """A feather whose max date >= expected_last → status='SKIPPED', no API call."""
        import pyarrow.feather as feather

        from scripts import collect_daily_snapshot as cds

        symbol = "ZZZ"
        # Build a feather whose latest bar is far in the future.
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2099-12-31"], utc=True).as_unit("ns"),
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [0.0],
            }
        )
        feather.write_feather(df, tmp_path / f"{symbol}_USDC_USDC-1d-futures.feather")

        class FailingAPI:
            def get_candlesticks_dataframe(self, *args, **kwargs):
                raise AssertionError("API must not be called when gate fires")

        markets = [{"name": f"{symbol}/USD", "isListed": True}]
        saved, failed, coverage = cds.collect_and_save_ohlcv(
            FailingAPI(),
            markets,
            tmp_path,
            timeframes=["1d"],
            target_date="2026-05-14",
        )
        assert coverage[(symbol, "1d")]["status"] == "SKIPPED"
        assert coverage[(symbol, "1d")]["api_slice"] is None
        # Saved counts fetches, not skips:
        assert saved == 0
        assert failed == []


class TestOhlcvIncrementalLookback:
    """collect_and_save_ohlcv — incremental 1m fetch must overlap last stored bar."""

    def test_late_daily_run_does_not_leave_a_1m_hole(self, tmp_path, monkeypatch):
        """A later daily run must not drop the minutes between last stored bar and now-1440.

        Incident 2026-09-09: previous 1m last bar sat 1443 minutes behind request-time
        ``now``. GMX ``get_candlesticks_dataframe(limit=N)`` returns the N most-recent
        candles. The hardcoded 1m limit of 1440 therefore started at ``now-1440`` and
        the merge left ``now-1442`` / ``now-1441`` missing. The on-disk series must
        stay contiguous through those minutes.
        """
        from scripts import collect_daily_snapshot as cds

        monkeypatch.setattr(cds.time, "sleep", lambda seconds: None)

        now = pd.Timestamp.now(tz="UTC").floor("min")
        last_closed = now - pd.Timedelta(minutes=1)
        last_stored = now - pd.Timedelta(minutes=1443)
        hole = (
            now - pd.Timedelta(minutes=1442),
            now - pd.Timedelta(minutes=1441),
        )
        world = pd.date_range(end=last_closed, periods=1500, freq="min", tz="UTC")

        existing_dates = world[world <= last_stored][-5:]
        assert last_stored in existing_dates
        assert hole[0] not in existing_dates

        filepath = tmp_path / "ETH_USDC_USDC-1m-futures.feather"
        feather.write_feather(_make_ohlcv(existing_dates, [1.0] * len(existing_dates)), filepath)

        class RollingWindowAPI:
            """GMX-shaped API: ``limit`` most-recent candles of ``world``."""

            def get_candlesticks_dataframe(self, symbol, period, limit):
                slice_dates = world[-int(limit) :]
                return pd.DataFrame(
                    {
                        "timestamp": slice_dates,
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                    }
                )

        saved, failed, coverage = cds.collect_and_save_ohlcv(
            RollingWindowAPI(),
            [{"name": "ETH/USD", "isListed": True}],
            tmp_path,
            timeframes=["1m"],
        )

        assert failed == []
        assert saved == 1
        assert coverage[("ETH", "1m")]["status"] in {"OK", "FLAT"}

        merged = pd.read_feather(filepath)
        dates = set(pd.to_datetime(merged["date"], utc=True).dt.floor("min"))
        assert hole[0] in dates, f"missing {hole[0]} (the 1440-bar lookback hole)"
        assert hole[1] in dates, f"missing {hole[1]} (the 1440-bar lookback hole)"
        assert last_stored in dates

    def test_limit_covers_the_2026_09_09_run_start_drift(self):
        """Incident numbers: 1443 min late → 1m limit must exceed the 1440 floor."""
        from scripts.collect_daily_snapshot import incremental_candle_limit

        last = pd.Timestamp("2026-09-08 02:17", tz="UTC")
        now = pd.Timestamp("2026-09-09 02:20", tz="UTC")
        limit = incremental_candle_limit("1m", last, now)
        assert limit >= 1503
        assert limit <= 10000

    def test_limit_keeps_1m_floor_for_a_short_gap(self):
        """A few missing minutes still request the 72h 1m floor (not 24h)."""
        from scripts.collect_daily_snapshot import incremental_candle_limit

        last = pd.Timestamp("2026-09-09 02:10", tz="UTC")
        now = pd.Timestamp("2026-09-09 02:20", tz="UTC")
        assert incremental_candle_limit("1m", last, now) == 4320

    def test_limit_is_max_when_no_stored_bar(self):
        from scripts.collect_daily_snapshot import MAX_CANDLE_LIMIT, incremental_candle_limit

        assert incremental_candle_limit("1m", None) == MAX_CANDLE_LIMIT

    def test_daily_1m_floor_fills_the_2026_09_08_interior_hole(self, tmp_path, monkeypatch):
        """The 02:17 hole is ~36h behind a same-day afternoon last bar.

        15m already fetches 72h; 1m fetched 24h and missed it. The daily
        1m floor must be 72h so the cron heals this class of hole without
        ``repair=True``.
        """
        from scripts import collect_daily_snapshot as cds

        monkeypatch.setattr(cds.time, "sleep", lambda seconds: None)

        now = pd.Timestamp("2026-09-09 18:00", tz="UTC")
        last_closed = now - pd.Timedelta(minutes=1)
        hole = pd.Timestamp("2026-09-08 02:18", tz="UTC")
        world = pd.date_range(end=last_closed, periods=5000, freq="min", tz="UTC")
        assert hole in world

        existing = world[world != hole]
        filepath = tmp_path / "ETH_USDC_USDC-1m-futures.feather"
        feather.write_feather(_make_ohlcv(existing, [1.0] * len(existing)), filepath)

        class RollingWindowAPI:
            def get_candlesticks_dataframe(self, symbol, period, limit):
                slice_dates = world[-int(limit) :]
                return pd.DataFrame(
                    {
                        "timestamp": slice_dates,
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                    }
                )

        saved, failed, _coverage = cds.collect_and_save_ohlcv(
            RollingWindowAPI(),
            [{"name": "ETH/USD", "isListed": True}],
            tmp_path,
            timeframes=["1m"],
            now=now,
        )
        assert failed == []
        assert saved == 1
        dates = set(pd.to_datetime(pd.read_feather(filepath)["date"], utc=True))
        assert hole in dates

    def test_repair_fills_an_interior_1m_hole_default_lookback_cannot_reach(
        self, tmp_path, monkeypatch
    ):
        """Holes older than the 72h 1m floor still need ``repair=True``."""
        from scripts import collect_daily_snapshot as cds

        monkeypatch.setattr(cds.time, "sleep", lambda seconds: None)

        now = pd.Timestamp("2026-09-09 18:00", tz="UTC")
        last_closed = now - pd.Timedelta(minutes=1)
        hole = now - pd.Timedelta(days=5)
        world = pd.date_range(end=last_closed, periods=8000, freq="min", tz="UTC")
        assert hole in world

        existing = world[world != hole]
        filepath = tmp_path / "ETH_USDC_USDC-1m-futures.feather"
        feather.write_feather(_make_ohlcv(existing, [1.0] * len(existing)), filepath)

        class RollingWindowAPI:
            def get_candlesticks_dataframe(self, symbol, period, limit):
                slice_dates = world[-int(limit) :]
                return pd.DataFrame(
                    {
                        "timestamp": slice_dates,
                        "open": 1.0,
                        "high": 1.0,
                        "low": 1.0,
                        "close": 1.0,
                    }
                )

        saved, failed, _coverage = cds.collect_and_save_ohlcv(
            RollingWindowAPI(),
            [{"name": "ETH/USD", "isListed": True}],
            tmp_path,
            timeframes=["1m"],
            now=now,
        )
        assert failed == []
        assert saved == 1
        default_dates = set(pd.to_datetime(pd.read_feather(filepath)["date"], utc=True))
        assert hole not in default_dates

        saved, failed, _coverage = cds.collect_and_save_ohlcv(
            RollingWindowAPI(),
            [{"name": "ETH/USD", "isListed": True}],
            tmp_path,
            timeframes=["1m"],
            repair=True,
            now=now,
        )
        assert failed == []
        assert saved == 1
        repaired = set(pd.to_datetime(pd.read_feather(filepath)["date"], utc=True))
        assert hole in repaired


class TestReportSkippedSection:
    def test_section_present_when_skips_recorded(self, tmp_path):
        from gmx_historical_data.coverage_gate import SkipDecision
        from scripts.collect_daily_snapshot import generate_report

        markets_df = pd.DataFrame(
            {
                "name": ["BTC/USD"],
                "is_swap_only": [False],
                "is_listed": [True],
                "open_interest_long": ["0"],
                "open_interest_short": ["0"],
                "market_token": ["0x0"],
            }
        )
        out = tmp_path / "report.txt"
        skipped = {
            "markets": SkipDecision(True, "current", 135, 100),
            "apy": SkipDecision(True, "current", 945, 700),
        }
        generate_report(
            date_str="2026-05-14",
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
            report_path=out,
            ohlcv_coverage=None,
            skipped=skipped,
        )
        content = out.read_text()
        assert "## Skipped (already current)" in content
        assert "Markets snapshots: existing 135 rows ≥ 100" in content
        assert "APY: existing 945 rows ≥ 700" in content

    def test_section_absent_when_no_skips(self, tmp_path):
        from scripts.collect_daily_snapshot import generate_report

        markets_df = pd.DataFrame(
            {
                "name": ["BTC/USD"],
                "is_swap_only": [False],
                "is_listed": [True],
                "open_interest_long": ["0"],
                "open_interest_short": ["0"],
                "market_token": ["0x0"],
            }
        )
        out = tmp_path / "report.txt"
        generate_report(
            date_str="2026-05-14",
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
            report_path=out,
            ohlcv_coverage=None,
            skipped=None,
        )
        assert "## Skipped (already current)" not in out.read_text()
