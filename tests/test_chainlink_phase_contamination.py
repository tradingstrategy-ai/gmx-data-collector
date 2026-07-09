"""Tests for Chainlink phase-continuity validation (repurposed-proxy contamination).

These tests exercise :meth:`ChainlinkRPCCollector._filter_contaminated_phases`
and its helper :meth:`_sample_phase_representative_price` with synthetic phase
samples, so no RPC access is required.  They prove the two required behaviours:

1. A repurposed proxy (PEPE-like: phase-1 serves a ~$15k asset, phase-2 serves
   PEPE at ~9e-7) has its contaminated older phase **dropped**.
2. A legitimate same-asset aggregator rotation (ETH-like: phase-1 and phase-2
   both ~$1.5k–$2k) keeps **all** phases.
"""

from unittest.mock import MagicMock

from gmx_historical_data.chainlink_rpc_collector import (
    PHASE_SCALE_DISCONTINUITY_RATIO,
    ChainlinkRound,
    ChainlinkRPCCollector,
)

# Chainlink USD feeds use 8 decimals; raw answer = price * 10**8.
DECIMALS = 8


def _raw(price: float) -> int:
    """Encode a human price into a raw 8-decimal Chainlink answer.

    :param price: Human-readable price.
    :returns: Raw int256 answer as Chainlink would store it.
    """
    return int(round(price * (10**DECIMALS)))


def _make_collector(round_prices: dict[int, float]) -> ChainlinkRPCCollector:
    """Build a collector whose ``get_round_data`` returns canned prices.

    :param round_prices: Map of round_id -> human price to serve.
    :returns: Collector instance with a stubbed ``get_round_data``.
    """
    collector = ChainlinkRPCCollector.__new__(ChainlinkRPCCollector)
    collector.web3 = MagicMock()

    def fake_get_round_data(feed_address: str, round_id: int) -> ChainlinkRound | None:
        if round_id not in round_prices:
            return None
        return ChainlinkRound(
            round_id=round_id,
            answer=_raw(round_prices[round_id]),
            started_at=1_600_000_000,
            updated_at=1_600_000_000,
            answered_in_round=round_id,
        )

    collector.get_round_data = fake_get_round_data  # type: ignore[method-assign]
    return collector


# Round-ID space: roundId = (phaseId << 64) + aggregatorRoundId
P1 = 1 << 64
P2 = 2 << 64


def test_drops_repurposed_proxy_phase():
    """PEPE-like: phase-1 (~$15k foreign asset) is dropped, phase-2 (PEPE) kept."""
    # Phase 1: a different ~$15,000 asset. Phase 2: PEPE at ~9e-7.
    prices = {
        P1 + 1: 15_000.0,
        P1 + 50: 15_100.0,
        P1 + 100: 14_900.0,
        P2 + 1: 9.0e-7,
        P2 + 500: 1.1e-6,
        P2 + 1000: 3.0e-5,
    }
    collector = _make_collector(prices)

    phases = [
        (1, P1 + 1, P1 + 100),
        (2, P2 + 1, P2 + 1000),
    ]
    kept = collector._filter_contaminated_phases("0xFEED", phases)

    kept_phase_ids = [p[0] for p in kept]
    assert kept_phase_ids == [2], f"expected only phase 2 kept, got {kept_phase_ids}"


def test_keeps_legit_same_asset_rotation():
    """ETH-like: both phases on the same ~$1.5k–$2k scale — nothing dropped."""
    prices = {
        P1 + 1: 1_500.0,
        P1 + 50: 1_600.0,
        P1 + 100: 1_450.0,
        P2 + 1: 1_800.0,
        P2 + 500: 2_000.0,
        P2 + 1000: 1_900.0,
    }
    collector = _make_collector(prices)

    phases = [
        (1, P1 + 1, P1 + 100),
        (2, P2 + 1, P2 + 1000),
    ]
    kept = collector._filter_contaminated_phases("0xFEED", phases)

    kept_phase_ids = [p[0] for p in kept]
    assert kept_phase_ids == [1, 2], f"expected both phases kept, got {kept_phase_ids}"


def test_drops_all_phases_older_than_discontinuity():
    """A three-phase feed drops every phase at/below the first discontinuity."""
    # Phase 1 & 2: foreign ~$15k asset. Phase 3: PEPE at ~1e-6.
    prices = {
        P1 + 1: 15_000.0,
        P1 + 100: 15_000.0,
        P2 + 1: 12_000.0,
        P2 + 100: 12_000.0,
        (3 << 64) + 1: 1.0e-6,
        (3 << 64) + 500: 1.2e-6,
        (3 << 64) + 1000: 2.0e-6,
    }
    collector = _make_collector(prices)

    phases = [
        (1, P1 + 1, P1 + 100),
        (2, P2 + 1, P2 + 100),
        (3, (3 << 64) + 1, (3 << 64) + 1000),
    ]
    kept = collector._filter_contaminated_phases("0xFEED", phases)

    kept_phase_ids = [p[0] for p in kept]
    assert kept_phase_ids == [3], f"expected only phase 3 kept, got {kept_phase_ids}"


def test_single_phase_is_untouched():
    """A single-phase list is returned unchanged (no comparison possible)."""
    collector = _make_collector({P2 + 1: 100.0})
    phases = [(2, P2 + 1, P2 + 1000)]
    assert collector._filter_contaminated_phases("0xFEED", phases) == phases


def test_representative_price_uses_median():
    """The representative price is the median of sampled valid answers."""
    # first/mid/last of phase 2; one zero answer should be ignored.
    prices = {P2 + 1: 0.0, P2 + 5: 2.0e-6, P2 + 10: 4.0e-6}
    collector = _make_collector(prices)
    price = collector._sample_phase_representative_price("0xFEED", P2 + 1, P2 + 10)
    # Zero (P2+1) dropped -> median of {2e-6, 4e-6} = 3e-6.
    assert price is not None
    assert abs(price - 3.0e-6) < 1e-12


def test_threshold_is_two_orders_of_magnitude():
    """Sanity check on the configured threshold constant."""
    assert PHASE_SCALE_DISCONTINUITY_RATIO == 100.0
