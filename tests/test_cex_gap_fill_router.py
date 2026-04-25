"""Tests for cex_gap_fill.router."""

import json
from pathlib import Path

from gmx_historical_data.cex_gap_fill.router import (
    Route,
    RoutingTable,
    load_routing,
    save_routing,
)

# ── Task 7 tests ──────────────────────────────────────────────────────────────


def test_load_routing_returns_defaults_for_empty_file(tmp_path: Path):
    p = tmp_path / "r.json"
    p.write_text(json.dumps({
        "version": 1,
        "defaults": {"primary": "binance", "fallback": "bybit", "skip_unresolved": True},
        "overrides": {},
        "auto": {},
    }))
    table = load_routing(p)
    assert table.defaults.primary == "binance"
    assert table.defaults.fallback == "bybit"
    assert table.defaults.skip_unresolved is True


def test_load_routing_overrides_precedence(tmp_path: Path):
    p = tmp_path / "r.json"
    p.write_text(json.dumps({
        "version": 1,
        "defaults": {"primary": "binance", "fallback": "bybit", "skip_unresolved": True},
        "overrides": {"BTC": {"exchange": "binance", "pair": "BTC/USDT:USDT"}},
        "auto": {"BTC": {"exchange": "bybit", "pair": "BTC/USDT:USDT", "resolved_at": "2026-01-01"}},
    }))
    table = load_routing(p)
    route = table.resolve("BTC")
    assert route.exchange == "binance"  # override wins over auto


def test_load_routing_auto_cache_hit(tmp_path: Path):
    p = tmp_path / "r.json"
    p.write_text(json.dumps({
        "version": 1,
        "defaults": {"primary": "binance", "fallback": "bybit", "skip_unresolved": True},
        "overrides": {},
        "auto": {"ETH": {"exchange": "bybit", "pair": "ETH/USDT:USDT", "resolved_at": "2026-01-01"}},
    }))
    table = load_routing(p)
    route = table.resolve("ETH")
    assert route.exchange == "bybit"
    assert route.pair == "ETH/USDT:USDT"


def test_load_routing_unresolved_returns_none():
    table = RoutingTable.empty()
    assert table.resolve("UNKNOWN") is None


def test_route_skip_sentinel(tmp_path: Path):
    p = tmp_path / "r.json"
    p.write_text(json.dumps({
        "version": 1,
        "defaults": {"primary": "binance", "fallback": "bybit", "skip_unresolved": True},
        "overrides": {"FART": {"exchange": "skip"}},
        "auto": {},
    }))
    table = load_routing(p)
    route = table.resolve("FART")
    assert route.exchange == "skip"
    assert route.is_skip is True


# ── Task 8 tests ──────────────────────────────────────────────────────────────


def test_record_auto_roundtrips_through_disk(tmp_path: Path):
    p = tmp_path / "r.json"
    p.write_text(json.dumps({
        "version": 1,
        "defaults": {"primary": "binance", "fallback": "bybit", "skip_unresolved": True},
        "overrides": {},
        "auto": {},
    }))
    table = load_routing(p)
    table.record_auto("SUI", Route(exchange="bybit", pair="SUI/USDT:USDT", resolved_at="2026-04-24"))
    save_routing(table, p)

    reloaded = load_routing(p)
    route = reloaded.resolve("SUI")
    assert route.exchange == "bybit"
    assert route.pair == "SUI/USDT:USDT"
    assert route.resolved_at == "2026-04-24"
