"""Tests for live GMX market registry helpers."""

from gmx_historical_data.daemon.config import DaemonConfig
from gmx_historical_data.market_registry import (
    _MIN_LIVE_MARKETS_FOR_ABSENCE_FILTER,
    _build_registry,
    get_disabled_market_symbols,
)


def _make_live_registry(count: int, disabled_symbols: set[str] = frozenset()) -> dict[str, dict]:
    """Build a synthetic ``fetch_markets``-shaped registry for tests.

    :param count: Number of synthetic markets to generate (``SYM0``..``SYM{count-1}``).
    :param disabled_symbols: Symbols that should carry ``isDisabled=True``.
    :returns: Dict keyed by fake address, matching the shape returned by
        :func:`gmx_historical_data.market_registry.fetch_markets`.
    """
    registry = {}
    for i in range(count):
        symbol = f"SYM{i}"
        registry[f"0x{i}"] = {
            "symbol": f"{symbol}/USD",
            "indexToken": symbol,
            "isDisabled": symbol in disabled_symbols,
        }
    return registry


def test_build_registry_preserves_is_disabled() -> None:
    registry = _build_registry(
        [
            {
                "marketToken": "0x123",
                "name": "OM/USD",
                "indexToken": "0x456",
                "listingDate": "2025-01-01",
                "isListed": True,
                "isDisabled": True,
            }
        ]
    )

    entry = registry["0x123"]
    assert entry["isDisabled"] is True
    assert entry["isListed"] is True


def test_get_disabled_market_symbols_uses_collector_symbol_and_refreshes(monkeypatch) -> None:
    calls = []

    def fake_fetch(*args, **kwargs):
        calls.append(kwargs)
        return {
            "0x1": {"symbol": "OM/USD", "indexToken": "OM", "isDisabled": True},
            "0x2": {"symbol": "BONK/USD", "indexToken": "BONK", "isDisabled": False},
        }

    monkeypatch.setattr(
        "gmx_historical_data.market_registry.fetch_markets",
        fake_fetch,
    )

    assert get_disabled_market_symbols() == {"OM"}
    assert calls == [{"chain": "arbitrum", "cache_dir": None, "force_refresh": True}]


def test_get_disabled_market_symbols_marks_requested_removed_market_unavailable(monkeypatch) -> None:
    # Registry must look healthy (>= threshold markets) for absence-based
    # filtering to fire — see the partial-registry test below for the guard.
    registry = _make_live_registry(_MIN_LIVE_MARKETS_FOR_ABSENCE_FILTER)
    registry["0x1"] = {"symbol": "BONK/USD", "indexToken": "BONK", "isDisabled": False}
    monkeypatch.setattr(
        "gmx_historical_data.market_registry.fetch_markets",
        lambda *args, **kwargs: registry,
    )

    assert get_disabled_market_symbols(candidate_symbols=["OM", "BONK"]) == {"OM"}


def test_get_disabled_market_symbols_excludes_absent_candidate_when_registry_healthy(monkeypatch) -> None:
    registry = _make_live_registry(_MIN_LIVE_MARKETS_FOR_ABSENCE_FILTER, disabled_symbols={"SYM0"})
    monkeypatch.setattr(
        "gmx_historical_data.market_registry.fetch_markets",
        lambda *args, **kwargs: registry,
    )

    result = get_disabled_market_symbols(candidate_symbols=["SYM0", "SYM1", "OM"])

    # SYM0 is explicitly disabled (branch a), OM is absent from a healthy
    # registry (branch b). SYM1 is present and not disabled, so it stays.
    assert result == {"SYM0", "OM"}


def test_get_disabled_market_symbols_keeps_absent_candidate_when_registry_partial(monkeypatch, caplog) -> None:
    partial_count = _MIN_LIVE_MARKETS_FOR_ABSENCE_FILTER - 1
    registry = _make_live_registry(partial_count, disabled_symbols={"SYM0"})
    monkeypatch.setattr(
        "gmx_historical_data.market_registry.fetch_markets",
        lambda *args, **kwargs: registry,
    )

    with caplog.at_level("WARNING"):
        result = get_disabled_market_symbols(candidate_symbols=["SYM0", "SYM1", "OM"])

    # SYM0 is explicitly disabled (branch a, fail-safe, unaffected by the
    # guard). OM is absent but the registry is partial, so absence-based
    # filtering (branch b) must be skipped — OM is NOT excluded.
    assert result == {"SYM0"}
    assert any("absence-based" in record.message for record in caplog.records)


def test_daemon_config_filters_disabled_collection_symbols(monkeypatch) -> None:
    monkeypatch.setenv("JSON_RPC_ARBITRUM", "http://localhost:8545")
    monkeypatch.setenv("COLLECTION_SYMBOLS", "OM,BONK")
    monkeypatch.setenv("TIMEFRAME_CONCURRENCY", "1")
    monkeypatch.setattr(
        "gmx_historical_data.daemon.config.get_disabled_market_symbols",
        lambda *args, **kwargs: {"OM"},
    )

    config = DaemonConfig.from_env()

    assert config.collection_symbols == ["BONK"]
