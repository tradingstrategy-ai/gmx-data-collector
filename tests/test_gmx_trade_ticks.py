"""Tests for GMX trade-tick decoding and volume aggregation.

The scaling constants here are checked against events pulled live from
Arbitrum on 2026-09-10 (block ~503678428): a USDC->WETH ``SwapInfo`` whose
``tokenOutPrice`` scales to $2468.96 against an oracle candle of $2465, and
a ``PositionIncrease`` whose ``sizeInUsd``/``sizeInTokens`` ratio matches its
``executionPrice``. If these formulas drift the published volume is wrong by
orders of magnitude, so they are pinned.
"""

import pandas as pd
import pytest
from eth_defi.gmx.events import GMXEventData

from gmx_historical_data.gmx_trade_ticks import (
    TokenMeta,
    aggregate_tick_volume,
    ticks_from_event_data,
)

USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
BTC_MARKET = "0x47c031236e19d024b42f8AE6780E44A573170703"

TOKENS = {
    USDC: TokenMeta(symbol="USDC", decimals=6),
    WETH: TokenMeta(symbol="ETH", decimals=18),
}

#: Market token address -> (index token symbol, index token decimals).
MARKETS = {BTC_MARKET: TokenMeta(symbol="BTC", decimals=8)}


def _position_event(name: str, *, size_usd: int, size_tokens: int, is_long: bool) -> GMXEventData:
    """Build a decoded position event with GMX's raw fixed-point encoding."""
    return GMXEventData(
        event_name=name,
        address_items={"market": BTC_MARKET, "account": "0x" + "11" * 20},
        uint_items={
            "sizeDeltaUsd": size_usd,
            "sizeDeltaInTokens": size_tokens,
            "executionPrice": 778219175818792227960498191,
        },
        int_items={"priceImpactUsd": 0},
        bool_items={"isLong": is_long},
    )


def _swap_event() -> GMXEventData:
    """Build the real USDC->WETH SwapInfo observed at block 503678428."""
    return GMXEventData(
        event_name="SwapInfo",
        address_items={"market": WETH, "tokenIn": USDC, "tokenOut": WETH},
        uint_items={
            "tokenInPrice": 999869800000000000000000,
            "tokenOutPrice": 2468955797890000,
            "amountIn": 1259620,
            "amountOut": 498523596802860,
        },
        int_items={"priceImpactUsd": -285658710327564583684837200},
        bool_items={},
    )


class TestTicksFromEventData:
    def test_position_increase_scales_usd_and_tokens(self):
        # $77,821.90 of BTC at ~$77,821 -> ~1 BTC (8 decimals).
        event = _position_event(
            "PositionIncrease",
            size_usd=77_821 * 10**30,
            size_tokens=1 * 10**8,
            is_long=True,
        )

        ticks = ticks_from_event_data(
            event,
            timestamp=1789036497,
            block_number=1,
            tx_hash="0xabc",
            log_index=3,
            markets=MARKETS,
            tokens=TOKENS,
        )

        assert len(ticks) == 1
        tick = ticks[0]
        assert tick.symbol == "BTC"
        assert tick.kind == "perp"
        assert tick.event_name == "PositionIncrease"
        assert tick.size_usd == pytest.approx(77_821.0)
        assert tick.size_tokens == pytest.approx(1.0)
        assert tick.is_long is True

    def test_position_decrease_is_counted_as_volume(self):
        """A close is a fill too -- exchange volume counts both sides."""
        event = _position_event(
            "PositionDecrease", size_usd=5 * 10**30, size_tokens=10**7, is_long=False
        )

        ticks = ticks_from_event_data(
            event,
            timestamp=1,
            block_number=1,
            tx_hash="0xa",
            log_index=0,
            markets=MARKETS,
            tokens=TOKENS,
        )

        assert len(ticks) == 1
        assert ticks[0].kind == "perp"
        assert ticks[0].size_usd == pytest.approx(5.0)
        assert ticks[0].size_tokens == pytest.approx(0.1)

    def test_swap_emits_one_tick_per_side_with_own_decimals(self):
        """USDC (6dp) and WETH (18dp) scale differently; a swap is real flow
        in both tokens, so each side gets its own tick."""
        ticks = ticks_from_event_data(
            _swap_event(),
            timestamp=1789036305,
            block_number=503678428,
            tx_hash="0x2fa52a",
            log_index=1,
            markets=MARKETS,
            tokens=TOKENS,
        )

        assert len(ticks) == 2
        by_symbol = {t.symbol: t for t in ticks}

        assert by_symbol["USDC"].size_tokens == pytest.approx(1.25962)
        assert by_symbol["ETH"].size_tokens == pytest.approx(0.00049852359680286)
        # Verified live: tokenOutPrice scales to $2468.96 vs a $2465 oracle candle.
        assert by_symbol["ETH"].price_usd == pytest.approx(2468.95579789)
        assert by_symbol["USDC"].price_usd == pytest.approx(0.9998698)
        assert all(t.kind == "swap" for t in ticks)
        # Each side is valued at its own token's price. The two notionals are
        # close but not equal -- the gap is the swap fee (amountIn 1.25962 USDC
        # vs amountInAfterFees 1.23128), so neither side may be derived from
        # the other.
        assert by_symbol["USDC"].size_usd == pytest.approx(1.2594, rel=1e-3)
        assert by_symbol["ETH"].size_usd == pytest.approx(1.2308, rel=1e-3)

    def test_unknown_event_yields_nothing(self):
        event = GMXEventData(event_name="OrderCancelled", address_items={}, uint_items={})

        assert (
            ticks_from_event_data(
                event,
                timestamp=1,
                block_number=1,
                tx_hash="0x",
                log_index=0,
                markets=MARKETS,
                tokens=TOKENS,
            )
            == []
        )

    def test_unmapped_market_is_skipped_not_guessed(self):
        """An unknown market means unknown index-token decimals. Guessing 18
        would inflate volume by 10^10 for an 8-decimal token, so skip."""
        event = _position_event(
            "PositionIncrease", size_usd=10**30, size_tokens=10**8, is_long=True
        )
        event.address_items["market"] = "0x" + "99" * 20

        assert (
            ticks_from_event_data(
                event,
                timestamp=1,
                block_number=1,
                tx_hash="0x",
                log_index=0,
                markets=MARKETS,
                tokens=TOKENS,
            )
            == []
        )

    def test_unmapped_swap_token_skips_only_that_side(self):
        event = _swap_event()
        event.address_items["tokenIn"] = "0x" + "88" * 20

        ticks = ticks_from_event_data(
            event,
            timestamp=1,
            block_number=1,
            tx_hash="0x",
            log_index=0,
            markets=MARKETS,
            tokens=TOKENS,
        )

        assert [t.symbol for t in ticks] == ["ETH"]


class TestAggregateTickVolume:
    def _ticks(self):
        event = _position_event(
            "PositionIncrease", size_usd=10 * 10**30, size_tokens=10**8, is_long=True
        )
        out = []
        for ts in (1789036200, 1789036260, 1789036320):
            out += ticks_from_event_data(
                event,
                timestamp=ts,
                block_number=1,
                tx_hash="0x",
                log_index=0,
                markets=MARKETS,
                tokens=TOKENS,
            )
        return out

    def test_sums_per_symbol_per_bucket(self):
        df = aggregate_tick_volume(self._ticks(), timeframe="1m")

        assert list(df.columns) == ["symbol", "date", "volume", "volume_usd", "trades"]
        assert len(df) == 3
        assert df["volume"].tolist() == pytest.approx([1.0, 1.0, 1.0])
        assert df["volume_usd"].tolist() == pytest.approx([10.0, 10.0, 10.0])
        assert df["trades"].tolist() == [1, 1, 1]

    def test_buckets_collapse_at_coarser_timeframe(self):
        df = aggregate_tick_volume(self._ticks(), timeframe="1h")

        assert len(df) == 1
        assert df["volume"].iloc[0] == pytest.approx(3.0)
        assert df["trades"].iloc[0] == 3

    def test_kind_filter_selects_which_flow_counts(self):
        """Whether GM-pool swaps count toward candle volume is a policy
        choice; the aggregator must not hardcode it."""
        ticks = self._ticks() + ticks_from_event_data(
            _swap_event(),
            timestamp=1789036200,
            block_number=1,
            tx_hash="0x",
            log_index=0,
            markets=MARKETS,
            tokens=TOKENS,
        )

        perp_only = aggregate_tick_volume(ticks, timeframe="1h", kinds=("perp",))
        both = aggregate_tick_volume(ticks, timeframe="1h", kinds=("perp", "swap"))

        assert set(perp_only["symbol"]) == {"BTC"}
        assert set(both["symbol"]) == {"BTC", "ETH", "USDC"}

    def test_empty_input_returns_empty_frame_with_schema(self):
        df = aggregate_tick_volume([], timeframe="1m")

        assert df.empty
        assert list(df.columns) == ["symbol", "date", "volume", "volume_usd", "trades"]

    def test_bucket_timestamps_are_utc_floored(self):
        df = aggregate_tick_volume(self._ticks(), timeframe="1m")

        assert str(df["date"].dt.tz) == "UTC"
        assert (df["date"].dt.second == 0).all()
        assert df["date"].is_monotonic_increasing


class TestTicksToFrame:
    def test_frame_roundtrips_for_parquet_storage(self):
        from gmx_historical_data.gmx_trade_ticks import ticks_to_frame

        ticks = ticks_from_event_data(
            _swap_event(),
            timestamp=1789036305,
            block_number=503678428,
            tx_hash="0x2fa52a",
            log_index=1,
            markets=MARKETS,
            tokens=TOKENS,
        )
        df = ticks_to_frame(ticks)

        assert len(df) == 2
        assert str(df["date"].dt.tz) == "UTC"
        assert {"symbol", "kind", "event_name", "price_usd", "size_tokens", "size_usd"} <= set(
            df.columns
        )
        assert df["block_number"].dtype.kind in "iu"

    def test_empty_ticks_still_produce_typed_frame(self):
        from gmx_historical_data.gmx_trade_ticks import ticks_to_frame

        df = ticks_to_frame([])

        assert df.empty
        assert "symbol" in df.columns
        assert isinstance(df, pd.DataFrame)


class TestSwapSideLabelling:
    def test_each_swap_side_is_labelled_in_or_out(self):
        """Both sides carry the full notional, so a protocol-wide sum over
        swap rows double-counts by exactly 2x -- measured against GMX's own
        swapVolumeUsd for 2026-09-01 (ratio 1.999). The label lets a consumer
        filter to one side and recover the true total; per-symbol sums stay
        correct either way."""
        ticks = ticks_from_event_data(
            _swap_event(),
            timestamp=1789036305,
            block_number=503678428,
            tx_hash="0x2fa52a",
            log_index=1,
            markets=MARKETS,
            tokens=TOKENS,
        )

        assert {t.side for t in ticks} == {"in", "out"}
        assert next(t for t in ticks if t.symbol == "USDC").side == "in"
        assert next(t for t in ticks if t.symbol == "ETH").side == "out"

    def test_perp_fills_have_no_side(self):
        event = _position_event(
            "PositionIncrease", size_usd=10**30, size_tokens=10**8, is_long=True
        )

        ticks = ticks_from_event_data(
            event,
            timestamp=1,
            block_number=1,
            tx_hash="0x",
            log_index=0,
            markets=MARKETS,
            tokens=TOKENS,
        )

        assert ticks[0].side is None

    def test_side_survives_into_the_stored_frame(self):
        from gmx_historical_data.gmx_trade_ticks import ticks_to_frame

        ticks = ticks_from_event_data(
            _swap_event(),
            timestamp=1789036305,
            block_number=503678428,
            tx_hash="0x2fa52a",
            log_index=1,
            markets=MARKETS,
            tokens=TOKENS,
        )
        df = ticks_to_frame(ticks)

        assert set(df["side"]) == {"in", "out"}
