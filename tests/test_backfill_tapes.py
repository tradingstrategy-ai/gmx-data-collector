"""Tests for the per-UTC-day tick tape writer.

Shared by the daily phase and the backfill, so it has to be right for both.
The tape is append-and-merge: a block chunk straddles midnight, and a range
may be re-scanned after an interruption. Both mean the writer has to be
idempotent without collapsing distinct fills.
"""

import pandas as pd

from gmx_historical_data.gmx_trade_ticks import TradeTick


def _tick(symbol, kind, tx="0xaa", log_index=7, timestamp=1788_000_000, size=1.0):
    return TradeTick(
        timestamp=timestamp,
        block_number=500_000_000,
        transaction_hash=tx,
        log_index=log_index,
        event_name="SwapInfo" if kind == "swap" else "PositionIncrease",
        kind=kind,
        symbol=symbol,
        market="0xmarket",
        price_usd=100.0,
        size_tokens=size,
        size_usd=size * 100.0,
        is_long=None if kind == "swap" else True,
        price_impact_usd=0.0,
    )


class TestWriteTickTapes:
    def test_both_sides_of_a_swap_survive(self, tmp_path):
        """One SwapInfo log yields two ticks that share a transaction hash
        AND a log index -- they differ only by token. Deduping on
        (tx, log_index) alone would silently discard one side of every swap
        in the dataset."""
        from gmx_historical_data.candle_volume import write_tick_tapes

        ticks = [_tick("USDC", "swap"), _tick("ETH", "swap")]
        write_tick_tapes(ticks, tmp_path)

        stored = pd.read_parquet(next(tmp_path.glob("*.parquet")))
        assert len(stored) == 2
        assert set(stored["symbol"]) == {"USDC", "ETH"}

    def test_rescanning_the_same_range_does_not_double_count(self, tmp_path):
        """An interrupted backfill re-scans its last chunk on resume."""
        from gmx_historical_data.candle_volume import write_tick_tapes

        ticks = [_tick("USDC", "swap"), _tick("ETH", "swap"), _tick("BTC", "perp", log_index=9)]
        write_tick_tapes(ticks, tmp_path)
        write_tick_tapes(ticks, tmp_path)

        stored = pd.read_parquet(next(tmp_path.glob("*.parquet")))
        assert len(stored) == 3
        assert stored["size_tokens"].sum() == 3.0

    def test_distinct_logs_in_one_transaction_are_kept(self, tmp_path):
        """A single tx routinely emits several fills at different log indices."""
        from gmx_historical_data.candle_volume import write_tick_tapes

        ticks = [
            _tick("BTC", "perp", log_index=1),
            _tick("BTC", "perp", log_index=2),
            _tick("BTC", "perp", log_index=3),
        ]
        write_tick_tapes(ticks, tmp_path)

        assert len(pd.read_parquet(next(tmp_path.glob("*.parquet")))) == 3

    def test_ticks_are_split_across_day_files(self, tmp_path):
        """Chunks straddle midnight; each fill belongs to its own UTC day."""
        from gmx_historical_data.candle_volume import write_tick_tapes

        before_midnight = 1788_133_800  # 2026-08-30T23:50:00Z
        after_midnight = 1788_135_000  # 2026-08-31T00:10:00Z
        touched = write_tick_tapes(
            [
                _tick("BTC", "perp", tx="0x1", timestamp=before_midnight),
                _tick("BTC", "perp", tx="0x2", timestamp=after_midnight),
            ],
            tmp_path,
        )

        assert len(touched) == 2
        assert {p.stem for p in tmp_path.glob("*.parquet")} == touched
        for path in tmp_path.glob("*.parquet"):
            assert len(pd.read_parquet(path)) == 1

    def test_merging_preserves_earlier_days_rows(self, tmp_path):
        from gmx_historical_data.candle_volume import write_tick_tapes

        write_tick_tapes([_tick("BTC", "perp", tx="0x1")], tmp_path)
        write_tick_tapes([_tick("ETH", "perp", tx="0x2")], tmp_path)

        stored = pd.read_parquet(next(tmp_path.glob("*.parquet")))
        assert set(stored["symbol"]) == {"BTC", "ETH"}

    def test_empty_input_writes_nothing(self, tmp_path):
        from gmx_historical_data.candle_volume import write_tick_tapes

        assert write_tick_tapes([], tmp_path) == set()
        assert list(tmp_path.glob("*.parquet")) == []
