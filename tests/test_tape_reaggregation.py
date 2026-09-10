"""Regression tests for the two defects that shipped past a green suite.

B1: the daily phase scanned only `checkpoint+1 -> tip` and *replaced* a bar's
volume with that partial sum. The 02:00 UTC cron window straddles midnight, so
every 1d bar lost its 00:00-02:00 fills -- roughly 2h in 24, every day. The old
tests only ever aggregated one scan, so nothing caught it.

R1: HyperSync's `Query.to_block` is exclusive, and it was passed an inclusive
end, silently dropping the last block of every range.
"""

import pandas as pd
import pyarrow.feather as feather

from gmx_historical_data.candle_volume import (
    apply_volume_from_tapes,
    write_tick_tapes,
)
from gmx_historical_data.gmx_trade_ticks import TradeTick


def _candles(path, dates):
    feather.write_feather(
        pd.DataFrame(
            {
                "date": pd.to_datetime(dates, utc=True).as_unit("ns"),
                "open": [1.0] * len(dates),
                "high": [1.0] * len(dates),
                "low": [1.0] * len(dates),
                "close": [1.0] * len(dates),
                "volume": [0.0] * len(dates),
            }
        ),
        path,
    )


def _tick(timestamp, size, tx, kind="perp", symbol="BTC"):
    return TradeTick(
        timestamp=timestamp,
        block_number=1,
        transaction_hash=tx,
        log_index=0,
        event_name="PositionIncrease",
        kind=kind,
        symbol=symbol,
        market="0xm",
        price_usd=100.0,
        size_tokens=size,
        size_usd=size * 100.0,
        is_long=True,
        price_impact_usd=0.0,
        side=None,
    )


# 2026-09-09 01:00 and 03:00 UTC -- one before the 02:00 cron, one after.
BEFORE_CRON = 1788915600
AFTER_CRON = 1788922800

# A pair straddling UTC midnight: 2026-09-09 23:40 and 2026-09-10 00:20.
LATE_IN_DAY = 1788997200
EARLY_NEXT_DAY = 1788999600


class TestTwoScansSplittingOneBar:
    def test_second_scan_adds_to_the_bar_instead_of_replacing_it(self, tmp_path):
        """The exact daily-cron shape: yesterday's 1d bar is filled by two
        different runs, because the scan window straddles midnight."""
        futures = tmp_path / "futures"
        futures.mkdir()
        _candles(futures / "BTC_USDC_USDC-1d-futures.feather", ["2026-09-09"])
        ticks_dir = tmp_path / "ticks"
        sidecar = tmp_path / "tick_volume"

        # Run A sees only the fills before the cron boundary.
        dates = write_tick_tapes([_tick(BEFORE_CRON, 10.0, "0xa")], ticks_dir)
        apply_volume_from_tapes(ticks_dir, sidecar, futures, ["1d"], dates=sorted(dates))
        assert pd.read_feather(futures / "BTC_USDC_USDC-1d-futures.feather")["volume"].tolist() == [
            10.0
        ]

        # Run B, the next day, sees the rest of that same UTC day.
        dates = write_tick_tapes([_tick(AFTER_CRON, 90.0, "0xb")], ticks_dir)
        apply_volume_from_tapes(ticks_dir, sidecar, futures, ["1d"], dates=sorted(dates))

        volume = pd.read_feather(futures / "BTC_USDC_USDC-1d-futures.feather")["volume"].tolist()
        assert volume == [100.0], "second scan replaced the bar instead of completing it"

    def test_rerunning_the_same_scan_does_not_inflate_the_bar(self, tmp_path):
        """Re-aggregating from the tape must be idempotent, or a retried run
        would double the day's volume."""
        futures = tmp_path / "futures"
        futures.mkdir()
        _candles(futures / "BTC_USDC_USDC-1d-futures.feather", ["2026-09-09"])
        ticks_dir = tmp_path / "ticks"
        sidecar = tmp_path / "tick_volume"

        ticks = [_tick(BEFORE_CRON, 10.0, "0xa"), _tick(AFTER_CRON, 90.0, "0xb")]
        for _ in range(3):
            dates = write_tick_tapes(ticks, ticks_dir)
            apply_volume_from_tapes(ticks_dir, sidecar, futures, ["1d"], dates=sorted(dates))

        assert pd.read_feather(futures / "BTC_USDC_USDC-1d-futures.feather")["volume"].tolist() == [
            100.0
        ]

    def test_tapes_are_keyed_by_utc_fill_date_not_run_date(self, tmp_path):
        """A single scan straddles midnight. Keying the tape by run date would
        file both days' fills together and let the next run overwrite them."""
        ticks_dir = tmp_path / "ticks"

        dates = write_tick_tapes(
            [_tick(LATE_IN_DAY, 5.0, "0xa"), _tick(EARLY_NEXT_DAY, 7.0, "0xb")], ticks_dir
        )

        assert dates == {"2026-09-09", "2026-09-10"}
        assert {p.stem for p in ticks_dir.glob("*.parquet")} == dates

    def test_earlier_slice_survives_a_later_write_to_the_same_day(self, tmp_path):
        """Daily used overwrite semantics while the backfill merged, so a
        same-day re-run silently dropped the earlier slice."""
        ticks_dir = tmp_path / "ticks"

        write_tick_tapes([_tick(BEFORE_CRON, 10.0, "0xa")], ticks_dir)
        write_tick_tapes([_tick(AFTER_CRON, 90.0, "0xb")], ticks_dir)

        stored = pd.read_parquet(ticks_dir / "2026-09-09.parquet")
        assert len(stored) == 2
        assert stored["size_tokens"].sum() == 100.0

    def test_sidecar_reflects_the_whole_day_not_the_last_slice(self, tmp_path):
        futures = tmp_path / "futures"
        futures.mkdir()
        ticks_dir = tmp_path / "ticks"
        sidecar = tmp_path / "tick_volume"

        write_tick_tapes([_tick(BEFORE_CRON, 10.0, "0xa")], ticks_dir)
        dates = write_tick_tapes([_tick(AFTER_CRON, 90.0, "0xb")], ticks_dir)
        apply_volume_from_tapes(ticks_dir, sidecar, futures, ["1d"], dates=sorted(dates))

        rows = pd.read_parquet(sidecar / "2026-09-09.parquet")
        perp = rows[(rows.kind == "perp") & (rows.timeframe == "1d")]
        assert perp["volume"].sum() == 100.0


class TestExclusiveToBlock:
    def test_query_end_is_made_exclusive(self):
        """Verified live: query [b, b] returns 0 logs, [b, b+1] returns 1.
        Passing the inclusive end drops the last block of every range."""
        from gmx_historical_data.trade_tick_collector import build_trade_query

        query = build_trade_query(100, 200)

        assert query.from_block == 100
        assert query.to_block == 201, "to_block must be exclusive to cover block 200"

    def test_single_block_range_is_not_empty(self):
        from gmx_historical_data.trade_tick_collector import build_trade_query

        query = build_trade_query(500, 500)

        assert query.to_block == 501
