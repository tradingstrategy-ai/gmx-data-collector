"""Routing table for GMX symbol → CEX (exchange, pair) resolution.

Backed by ``configs/cex_routing.json``. Overrides beat auto-cache.
Static Python mappings for 1000x tokens live in :mod:`symbols`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True, slots=True)
class Defaults:
    """Default exchange resolution settings.

    :param primary: First CEX to probe, e.g. ``"binance"``.
    :param fallback: Fallback CEX if primary lacks the symbol.
    :param skip_unresolved: If True, unresolved symbols are silently skipped.
    """

    primary: str
    fallback: str
    skip_unresolved: bool


@dataclass(frozen=True, slots=True)
class Route:
    """Resolved CEX route for one GMX symbol.

    :param exchange: Exchange name, e.g. ``"binance"``, ``"bybit"``, or ``"skip"``.
    :param pair: Freqtrade pair string, e.g. ``"BTC/USDT:USDT"``. Empty for skip routes.
    :param resolved_at: ISO date string when auto-resolved. Empty for manual overrides.
    """

    exchange: str
    pair: str = ""
    resolved_at: str = ""

    @property
    def is_skip(self) -> bool:
        """Return True if this route indicates the symbol should be skipped."""
        return self.exchange == "skip"


@dataclass
class RoutingTable:
    """In-memory routing state backed by a JSON config file.

    :param defaults: Default probe order and skip behaviour.
    :param overrides: Manual per-symbol overrides; always take precedence.
    :param auto: Automatically resolved symbol cache.
    """

    defaults: Defaults
    overrides: dict[str, Route] = field(default_factory=dict)
    auto: dict[str, Route] = field(default_factory=dict)

    @classmethod
    def empty(cls) -> RoutingTable:
        """Return an empty routing table with sensible defaults."""
        return cls(defaults=Defaults("binance", "bybit", True))

    def resolve(self, gmx_symbol: str) -> Route | None:
        """Resolve a GMX symbol to a CEX route.

        :param gmx_symbol: GMX-side symbol, e.g. ``"BTC"``.
        :returns: :class:`Route` if found in overrides or auto cache; ``None`` otherwise.
        """
        if gmx_symbol in self.overrides:
            return self.overrides[gmx_symbol]
        if gmx_symbol in self.auto:
            return self.auto[gmx_symbol]
        return None

    def record_auto(self, gmx_symbol: str, route: Route) -> None:
        """Store an auto-resolved route in the cache.

        :param gmx_symbol: GMX-side symbol.
        :param route: Resolved :class:`Route` to cache.
        """
        self.auto[gmx_symbol] = route


def load_routing(path: Path) -> RoutingTable:
    """Load routing table from a JSON file.

    If ``path`` does not exist, returns an empty :class:`RoutingTable` with
    default settings. Copy ``cex_routing.example.json`` to create the file.

    :param path: Path to the JSON config file.
    :returns: Populated :class:`RoutingTable`.
    """
    if not path.exists():
        return RoutingTable.empty()
    data = json.loads(path.read_text())
    d = data["defaults"]
    defaults = Defaults(
        primary=d["primary"],
        fallback=d["fallback"],
        skip_unresolved=d["skip_unresolved"],
    )
    overrides = {k: Route(**v) for k, v in data.get("overrides", {}).items()}
    auto = {k: Route(**v) for k, v in data.get("auto", {}).items()}
    return RoutingTable(defaults=defaults, overrides=overrides, auto=auto)


def save_routing(table: RoutingTable, path: Path) -> None:
    """Persist routing table to disk.

    The ``auto`` section is fully regenerated. Overrides are preserved as written.

    :param table: Routing table to persist.
    :param path: Destination JSON file path.
    """

    def _route_dict(r: Route) -> dict:
        return {k: v for k, v in {"exchange": r.exchange, "pair": r.pair, "resolved_at": r.resolved_at}.items() if v != ""}

    payload = {
        "version": 1,
        "defaults": {
            "primary": table.defaults.primary,
            "fallback": table.defaults.fallback,
            "skip_unresolved": table.defaults.skip_unresolved,
        },
        "overrides": {k: _route_dict(v) for k, v in table.overrides.items()},
        "auto": {k: _route_dict(v) for k, v in table.auto.items()},
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
