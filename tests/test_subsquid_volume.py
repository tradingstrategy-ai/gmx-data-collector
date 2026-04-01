"""Integration tests for Subsquid volume fetcher.

These tests hit the live Subsquid GraphQL endpoint — they require
internet access and may be flaky if the endpoint is temporarily down.
"""

from decimal import Decimal

import pytest

from gmx_historical_data.subsquid_volume import fetch_daily_volumes, fetch_volume_history

# --- fetch_daily_volumes ---


class TestFetchDailyVolumes:
    """Tests for per-market 24h volume fetching."""

    def test_returns_dict_of_checksummed_addresses(self):
        """Volume keys must be checksummed Ethereum addresses."""
        volumes = fetch_daily_volumes()
        assert isinstance(volumes, dict)
        assert len(volumes) > 0
        for addr in volumes:
            # EIP-55 checksum: starts with 0x, 42 chars, mixed case
            assert addr.startswith("0x")
            assert len(addr) == 42
            assert addr != addr.lower(), f"Address not checksummed: {addr}"

    def test_values_are_decimal_usd(self):
        """Volume values must be non-negative Decimal USD amounts."""
        volumes = fetch_daily_volumes()
        for addr, vol in volumes.items():
            assert isinstance(vol, Decimal), f"{addr}: expected Decimal, got {type(vol)}"
            assert vol >= 0, f"{addr}: negative volume {vol}"

    def test_has_active_markets(self):
        """At least some markets should have non-zero volume."""
        volumes = fetch_daily_volumes()
        nonzero = [v for v in volumes.values() if v > 0]
        # GMX typically has 50+ active markets
        assert len(nonzero) >= 10, f"Only {len(nonzero)} markets with volume — expected at least 10"

    def test_total_volume_sanity(self):
        """Total 24h volume should be at least $1M (GMX is a major DEX)."""
        volumes = fetch_daily_volumes()
        total = sum(volumes.values())
        assert total > 1_000_000, f"Total 24h volume ${total:,.0f} seems too low"

    def test_avalanche_chain(self):
        """Avalanche chain should also return volume data."""
        volumes = fetch_daily_volumes(chain="avalanche")
        assert isinstance(volumes, dict)
        # Avalanche may have fewer markets or even zero, just check it doesn't crash

    def test_unsupported_chain_raises(self):
        """Unsupported chain name should raise KeyError."""
        with pytest.raises(KeyError):
            fetch_daily_volumes(chain="solana")


# --- fetch_volume_history ---


class TestFetchVolumeHistory:
    """Tests for historical daily aggregate volume."""

    def test_returns_list_of_dicts(self):
        """History entries must be dicts with expected keys."""
        history = fetch_volume_history(days=7)
        assert isinstance(history, list)
        assert len(history) > 0
        for entry in history:
            assert "timestamp" in entry
            assert "volume_usd" in entry
            assert "margin_volume_usd" in entry
            assert "swap_volume_usd" in entry

    def test_values_are_decimal(self):
        """All USD values in history must be Decimal."""
        history = fetch_volume_history(days=3)
        for entry in history:
            assert isinstance(entry["volume_usd"], Decimal)
            assert isinstance(entry["margin_volume_usd"], Decimal)
            assert isinstance(entry["swap_volume_usd"], Decimal)

    def test_timestamps_are_recent(self):
        """Returned timestamps should be within the last 30 days."""
        import time

        history = fetch_volume_history(days=7)
        now = int(time.time())
        thirty_days_ago = now - (30 * 86400)
        for entry in history:
            assert entry["timestamp"] > thirty_days_ago, (
                f"Timestamp {entry['timestamp']} is too old"
            )

    def test_margin_plus_swap_approximates_total(self):
        """Margin + swap volume should roughly equal total volume."""
        history = fetch_volume_history(days=3)
        for entry in history:
            parts = entry["margin_volume_usd"] + entry["swap_volume_usd"]
            total = entry["volume_usd"]
            if total > 0:
                # Allow some tolerance for deposit/withdrawal volume
                assert parts <= total * Decimal("1.1"), (
                    f"Parts {parts} exceed total {total} by >10%"
                )
