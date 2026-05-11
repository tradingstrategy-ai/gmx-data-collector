# CEX Gap-Fill Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an additive pipeline stage that fills GMX OHLCV price gaps and zero-volume bars using Binance and Bybit data downloaded via the `./freqtrade-gmx` wrapper, without modifying current `collect` / `export-freqtrade` behaviour.

**Architecture:** A new module `src/gmx_historical_data/cex_gap_fill/` runs between GMX parquet write and feather export. It detects gap ranges, resolves GMX symbols to CEX pairs via a JSON routing file + static Python mappings, subprocesses `./freqtrade-gmx download-data`, reconciles price-scaled CEX bars into the existing parquet, logs replacements. Triggered only via a new `fill-gaps-cex` CLI command and new Makefile targets (`refresh-data-cex`, `full-data-cex`, `full-data-nn-cex`); legacy targets are untouched.

**Tech Stack:** Python 3.11+, polars, typer, stdlib (`json`, `subprocess`, `logging`, `pathlib`). No new Python deps. Subprocess to freqtrade venv via `./freqtrade-gmx`.

**Spec:** See `docs/superpowers/specs/2026-04-24-cex-gap-fill-design.md` for the full design. This plan implements that spec.

**Branch:** `feat/cex-gap-fill` — user creates it manually (project rule: no git commands without explicit ask). Commits below assume the branch is checked out.

---

## Chunk 1: Scaffold + Symbols + Detector

### Task 1: Create module scaffold

**Files:**
- Create: `src/gmx_historical_data/cex_gap_fill/__init__.py`

- [ ] **Step 1: Create package dir + empty init**

```python
"""CEX gap-fill pipeline stage.

Additive post-processing that fills GMX OHLCV price gaps and zero-volume bars
using Binance/Bybit data downloaded via the ./freqtrade-gmx wrapper.

Entry point: :func:`fill_gaps_from_cex`. Never imported by legacy code paths.
"""

__all__ = ["fill_gaps_from_cex"]


def fill_gaps_from_cex(*args, **kwargs):
    """Placeholder — filled in Task 16."""
    raise NotImplementedError
```

- [ ] **Step 2: Verify package imports cleanly**

Run: `poetry run python -c "from gmx_historical_data import cex_gap_fill; print(cex_gap_fill.__all__)"`
Expected: `['fill_gaps_from_cex']`

- [ ] **Step 3: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/__init__.py
git commit -m "feat(cex-gap-fill): add module scaffold"
```

---

### Task 2: Symbols module — K-prefix normalization

**Files:**
- Create: `src/gmx_historical_data/cex_gap_fill/symbols.py`
- Create: `tests/test_cex_gap_fill_symbols.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_cex_gap_fill_symbols.py
"""Tests for cex_gap_fill.symbols module."""

from gmx_historical_data.cex_gap_fill.symbols import normalize_k_prefix


def test_normalize_k_prefix_lowercase_k_prepended_uppercase_base():
    assert normalize_k_prefix("kPEPE") == "KPEPE"


def test_normalize_k_prefix_already_uppercase_unchanged():
    assert normalize_k_prefix("KPEPE") == "KPEPE"


def test_normalize_k_prefix_non_k_prefix_unchanged():
    assert normalize_k_prefix("BTC") == "BTC"


def test_normalize_k_prefix_lowercase_k_lowercase_base_unchanged():
    assert normalize_k_prefix("kitty") == "kitty"


def test_normalize_k_prefix_empty_string_unchanged():
    assert normalize_k_prefix("") == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `poetry run pytest tests/test_cex_gap_fill_symbols.py -v`
Expected: `ImportError: cannot import name 'normalize_k_prefix'` (5 errors)

- [ ] **Step 3: Implement minimal symbols module**

```python
# src/gmx_historical_data/cex_gap_fill/symbols.py
"""Static GMX <-> CEX symbol mappings.

Copied verbatim from ``gmx-strategies/scripts/merge_gmx_binance.py`` and
``gmx-strategies/plugins/pairlist/HistoricalVolumePairList.py``. Keep in sync
manually when those change.
"""

GMX_TO_BINANCE_NAME: dict[str, str] = {
    "BONK": "1000BONK",
    "FLOKI": "1000FLOKI",
    "PEPE": "1000PEPE",
    "SHIB": "1000SHIB",
    "SATS": "1000SATS",
}

PRICE_DIVISORS: dict[str, int] = {
    "BONK": 1000,
    "FLOKI": 1000,
    "PEPE": 1000,
    "SHIB": 1000,
    "SATS": 1000,
}


def normalize_k_prefix(ticker: str) -> str:
    """Normalize Hyperliquid-style k-prefix to uppercase K-prefix.

    :param ticker: Raw ticker, e.g. ``kPEPE``.
    :returns: Normalized ticker, e.g. ``KPEPE``.
    """
    if len(ticker) > 1 and ticker[0] == "k" and ticker[1].isupper():
        return "K" + ticker[1:]
    return ticker
```

- [ ] **Step 4: Run test to verify it passes**

Run: `poetry run pytest tests/test_cex_gap_fill_symbols.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/symbols.py tests/test_cex_gap_fill_symbols.py
git commit -m "feat(cex-gap-fill): add symbols module with K-prefix normalization"
```

---

### Task 3: Symbols module — GMX→CEX pair builder

**Files:**
- Modify: `src/gmx_historical_data/cex_gap_fill/symbols.py`
- Modify: `tests/test_cex_gap_fill_symbols.py`

- [ ] **Step 1: Append failing tests**

```python
# Append to tests/test_cex_gap_fill_symbols.py
from gmx_historical_data.cex_gap_fill.symbols import (
    gmx_symbol_to_cex_pair,
    gmx_symbol_to_cex_base,
    price_scale_for,
)


def test_gmx_symbol_to_cex_base_plain_symbol_unchanged():
    assert gmx_symbol_to_cex_base("BTC") == "BTC"


def test_gmx_symbol_to_cex_base_bonk_becomes_1000bonk():
    assert gmx_symbol_to_cex_base("BONK") == "1000BONK"


def test_gmx_symbol_to_cex_base_applies_k_prefix_normalization():
    assert gmx_symbol_to_cex_base("kPEPE") == "1000PEPE"  # KPEPE → PEPE in GMX_TO_BINANCE_NAME is absent; K-prefix only


def test_gmx_symbol_to_cex_pair_format():
    assert gmx_symbol_to_cex_pair("BTC") == "BTC/USDT:USDT"


def test_gmx_symbol_to_cex_pair_for_1000_prefixed_symbol():
    assert gmx_symbol_to_cex_pair("BONK") == "1000BONK/USDT:USDT"


def test_price_scale_for_plain_symbol_is_one():
    assert price_scale_for("BTC") == 1.0


def test_price_scale_for_bonk_is_thousandth():
    assert price_scale_for("BONK") == 1 / 1000
```

Note: Third test revised — K-prefix normalization fires before `GMX_TO_BINANCE_NAME` lookup. `kPEPE` → `KPEPE`; `KPEPE` is not in `GMX_TO_BINANCE_NAME` (only bare `PEPE` is). Decision: `gmx_symbol_to_cex_base` applies K-prefix normalize, then 1000× mapping. If the normalized ticker (minus the `K`) is a known 1000× mapping key, use it. Otherwise pass through. See implementation.

- [ ] **Step 2: Run tests — expect 3 failing imports + new test failures**

Run: `poetry run pytest tests/test_cex_gap_fill_symbols.py -v`
Expected: ImportError on the three new names.

- [ ] **Step 3: Implement**

Append to `src/gmx_historical_data/cex_gap_fill/symbols.py`:

```python
CEX_QUOTE_SETTLE = "USDT"  # Both Binance & Bybit linear perps.


def gmx_symbol_to_cex_base(gmx_symbol: str) -> str:
    """Resolve GMX symbol to CEX base (no quote/settle).

    Pipeline: K-prefix normalization, then 1000x remap.
    If neither applies, the symbol is returned unchanged.

    :param gmx_symbol: e.g. ``BONK``, ``kPEPE``, ``BTC``.
    :returns: e.g. ``1000BONK``, ``KPEPE``, ``BTC``.
    """
    normalized = normalize_k_prefix(gmx_symbol)
    # Strip leading K if present and look up bare name for 1000x mapping.
    bare = normalized[1:] if normalized.startswith("K") and len(normalized) > 1 and normalized[1].isupper() else normalized
    if bare in GMX_TO_BINANCE_NAME:
        return GMX_TO_BINANCE_NAME[bare]
    return normalized


def gmx_symbol_to_cex_pair(gmx_symbol: str) -> str:
    """Build CEX linear-perp pair string, e.g. ``BTC/USDT:USDT``."""
    base = gmx_symbol_to_cex_base(gmx_symbol)
    return f"{base}/{CEX_QUOTE_SETTLE}:{CEX_QUOTE_SETTLE}"


def price_scale_for(gmx_symbol: str) -> float:
    """Multiplier to apply to CEX OHLC columns so they line up with GMX prices.

    For 1000x-prefixed tokens: CEX price × (1 / 1000) = GMX price.
    Volume gets multiplied by the divisor in the reconciler.
    """
    normalized = normalize_k_prefix(gmx_symbol)
    bare = normalized[1:] if normalized.startswith("K") and len(normalized) > 1 and normalized[1].isupper() else normalized
    if bare in PRICE_DIVISORS:
        return 1.0 / PRICE_DIVISORS[bare]
    return 1.0
```

Also revise test comment — `kPEPE` normalized is `KPEPE`, bare is `PEPE`, `PEPE` IS in `GMX_TO_BINANCE_NAME` so result is `1000PEPE`. Keep the test assertion as `1000PEPE`.

- [ ] **Step 4: Run tests to verify pass**

Run: `poetry run pytest tests/test_cex_gap_fill_symbols.py -v`
Expected: 12 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/symbols.py tests/test_cex_gap_fill_symbols.py
git commit -m "feat(cex-gap-fill): add GMX→CEX pair builder and price scale"
```

---

### Task 4: Detector — data types and price-jump mask

**Files:**
- Create: `src/gmx_historical_data/cex_gap_fill/detector.py`
- Create: `tests/test_cex_gap_fill_detector.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_cex_gap_fill_detector.py
"""Tests for cex_gap_fill.detector."""

import polars as pl
from datetime import datetime, timedelta, UTC

from gmx_historical_data.cex_gap_fill.detector import (
    price_jump_mask,
    DetectorConfig,
)


def _build_df(prices: list[float], volumes: list[float] | None = None, tf_minutes: int = 60) -> pl.DataFrame:
    assert volumes is None or len(prices) == len(volumes)
    start = datetime(2026, 1, 1, tzinfo=UTC)
    ts = [start + timedelta(minutes=tf_minutes * i) for i in range(len(prices))]
    return pl.DataFrame(
        {
            "timestamp": ts,
            "open": prices,
            "high": prices,
            "low": prices,
            "close": prices,
            "volume": volumes if volumes is not None else [1.0] * len(prices),
        }
    )


def test_price_jump_mask_detects_100pct_jump():
    df = _build_df([100, 100, 200, 200, 200])  # +100% at index 2
    mask = price_jump_mask(df, threshold=0.20)
    assert mask.to_list() == [False, False, True, False, False]


def test_price_jump_mask_first_bar_is_false():
    df = _build_df([100, 100])
    mask = price_jump_mask(df, threshold=0.20)
    assert mask[0] is False or mask.to_list()[0] is False


def test_price_jump_mask_small_move_below_threshold():
    df = _build_df([100, 110])  # +10% < 20%
    mask = price_jump_mask(df, threshold=0.20)
    assert mask.to_list() == [False, False]


def test_price_jump_mask_negative_jump_detected():
    df = _build_df([200, 50])  # -75%
    mask = price_jump_mask(df, threshold=0.20)
    assert mask.to_list() == [False, True]
```

- [ ] **Step 2: Run — expect ImportError**

Run: `poetry run pytest tests/test_cex_gap_fill_detector.py -v`
Expected: ImportError

- [ ] **Step 3: Implement minimal detector**

```python
# src/gmx_historical_data/cex_gap_fill/detector.py
"""Gap detection for GMX OHLCV data.

Detects contiguous ranges where GMX data is suspected missing or stale based
on price pct-change, zero-volume runs, and missing rows against the expected
timeframe grid.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import polars as pl


@dataclass(frozen=True, slots=True)
class DetectorConfig:
    """Tunable thresholds; defaults match the design doc."""

    gap_pct_threshold: float = 0.20
    merge_gap_bars: int = 2
    min_range_bars: int = 1


def price_jump_mask(df: pl.DataFrame, threshold: float) -> pl.Series:
    """Return a boolean Series marking bars where ``|pct_change(close)| > threshold``.

    The first bar is always ``False`` because pct-change is undefined.
    """
    pct = df["close"].pct_change().abs()
    return (pct > threshold).fill_null(False)
```

- [ ] **Step 4: Run tests — expect pass**

Run: `poetry run pytest tests/test_cex_gap_fill_detector.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/detector.py tests/test_cex_gap_fill_detector.py
git commit -m "feat(cex-gap-fill): add price-jump detection"
```

---

### Task 5: Detector — zero-volume and missing-row masks

**Files:**
- Modify: `src/gmx_historical_data/cex_gap_fill/detector.py`
- Modify: `tests/test_cex_gap_fill_detector.py`

- [ ] **Step 1: Append failing tests**

```python
# Append to tests/test_cex_gap_fill_detector.py
from gmx_historical_data.cex_gap_fill.detector import (
    zero_volume_mask,
    reindex_and_mark_missing,
)


def test_zero_volume_mask_flags_zeros():
    df = _build_df([100, 100, 100], volumes=[10, 0, 5])
    mask = zero_volume_mask(df)
    assert mask.to_list() == [False, True, False]


def test_reindex_and_mark_missing_inserts_rows_for_gaps():
    # 1h timeframe, drop index 2
    start = datetime(2026, 1, 1, tzinfo=UTC)
    df = pl.DataFrame(
        {
            "timestamp": [start, start + timedelta(hours=1), start + timedelta(hours=3)],
            "open": [100.0, 101.0, 103.0],
            "high": [100.0, 101.0, 103.0],
            "low": [100.0, 101.0, 103.0],
            "close": [100.0, 101.0, 103.0],
            "volume": [1.0, 1.0, 1.0],
        }
    )
    result, missing_mask = reindex_and_mark_missing(df, tf_minutes=60)
    assert result.height == 4
    assert missing_mask.to_list() == [False, False, True, False]
    # Filled row has null OHLCV (or zero; we expect null — reconciler replaces wholesale)
    assert result["close"][2] is None
```

- [ ] **Step 2: Run tests — expect ImportError**

Run: `poetry run pytest tests/test_cex_gap_fill_detector.py -v -k "zero_volume or reindex"`
Expected: ImportError

- [ ] **Step 3: Implement**

Append to `src/gmx_historical_data/cex_gap_fill/detector.py`:

```python
def zero_volume_mask(df: pl.DataFrame) -> pl.Series:
    """Boolean Series marking bars with zero volume."""
    return df["volume"] == 0


# Mapping from our human timeframe strings to minutes.
_TF_TO_MINUTES: dict[str, int] = {
    "1min": 1, "5min": 5, "15min": 15, "1h": 60, "4h": 240, "1d": 1440,
}


def reindex_and_mark_missing(df: pl.DataFrame, tf_minutes: int) -> tuple[pl.DataFrame, pl.Series]:
    """Reindex ``df`` onto a regular ``tf_minutes``-spaced grid.

    :returns: ``(reindexed_df, missing_mask)`` where ``missing_mask[i]`` is True
        iff the corresponding timestamp was absent from the original frame.
    """
    if df.is_empty():
        return df, pl.Series("missing", [], dtype=pl.Boolean)

    sorted_df = df.sort("timestamp")
    start = sorted_df["timestamp"][0]
    end = sorted_df["timestamp"][-1]
    grid = pl.datetime_range(start, end, interval=f"{tf_minutes}m", time_zone="UTC", eager=True).alias("timestamp")
    grid_df = pl.DataFrame({"timestamp": grid})
    merged = grid_df.join(sorted_df, on="timestamp", how="left")
    missing = merged["close"].is_null()
    return merged, missing


def minutes_for_timeframe(tf: str) -> int:
    """Return the grid interval in minutes for a timeframe string."""
    if tf not in _TF_TO_MINUTES:
        raise ValueError(f"unknown timeframe: {tf!r}")
    return _TF_TO_MINUTES[tf]
```

- [ ] **Step 4: Run tests — expect pass**

Run: `poetry run pytest tests/test_cex_gap_fill_detector.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/detector.py tests/test_cex_gap_fill_detector.py
git commit -m "feat(cex-gap-fill): add zero-volume mask and missing-row detection"
```

---

### Task 6: Detector — range clustering

**Files:**
- Modify: `src/gmx_historical_data/cex_gap_fill/detector.py`
- Modify: `tests/test_cex_gap_fill_detector.py`

- [ ] **Step 1: Append failing tests**

```python
# Append to tests/test_cex_gap_fill_detector.py
from gmx_historical_data.cex_gap_fill.detector import (
    cluster_ranges,
    detect_gaps,
    GapRange,
    RangeKind,
)


def test_cluster_ranges_merges_contiguous_trues():
    mask = pl.Series("m", [False, True, True, False, True, False])
    ranges = cluster_ranges(mask, merge_gap_bars=0, min_range_bars=1)
    assert ranges == [(1, 2), (4, 4)]


def test_cluster_ranges_merges_short_false_gaps():
    mask = pl.Series("m", [True, False, True, False, False, False, True])
    # One False between → merged; three Falses → not merged
    ranges = cluster_ranges(mask, merge_gap_bars=1, min_range_bars=1)
    assert ranges == [(0, 2), (6, 6)]


def test_cluster_ranges_drops_below_min_size():
    mask = pl.Series("m", [True, False, True, True, True])
    ranges = cluster_ranges(mask, merge_gap_bars=0, min_range_bars=2)
    assert ranges == [(2, 4)]


def test_detect_gaps_full_pipeline():
    # 1h bars; index 2 jumps +100%, index 4 is zero volume with sane price.
    df = _build_df(
        prices=[100, 100, 200, 200, 200, 200],
        volumes=[1, 1, 1, 1, 0, 1],
    )
    config = DetectorConfig(gap_pct_threshold=0.20, merge_gap_bars=0, min_range_bars=1)
    result = detect_gaps(df, tf="1h", config=config)
    assert len(result.full_ranges) == 1
    assert result.full_ranges[0].kind == RangeKind.FULL
    assert len(result.volume_ranges) == 1
    assert result.volume_ranges[0].kind == RangeKind.VOLUME_ONLY
```

- [ ] **Step 2: Run — expect ImportError**

Run: `poetry run pytest tests/test_cex_gap_fill_detector.py -v -k "cluster or detect_gaps"`
Expected: ImportError

- [ ] **Step 3: Implement**

Append to `src/gmx_historical_data/cex_gap_fill/detector.py`:

```python
from enum import Enum


class RangeKind(Enum):
    FULL = "full"
    VOLUME_ONLY = "volume_only"


@dataclass(frozen=True, slots=True)
class GapRange:
    """Half-open range [start_idx, end_idx] into the reindexed DataFrame."""

    start_idx: int
    end_idx: int
    kind: RangeKind

    def __len__(self) -> int:
        return self.end_idx - self.start_idx + 1


@dataclass(frozen=True, slots=True)
class DetectionResult:
    reindexed_df: pl.DataFrame
    full_ranges: list[GapRange]
    volume_ranges: list[GapRange]


def cluster_ranges(mask: pl.Series, merge_gap_bars: int, min_range_bars: int) -> list[tuple[int, int]]:
    """Cluster a boolean Series into ``[start, end]`` inclusive index pairs.

    Runs of True separated by at most ``merge_gap_bars`` False entries are
    merged. Final ranges shorter than ``min_range_bars`` are dropped.
    """
    result: list[tuple[int, int]] = []
    vals = mask.to_list()
    i = 0
    n = len(vals)
    while i < n:
        if not vals[i]:
            i += 1
            continue
        start = i
        end = i
        while end + 1 < n:
            # peek up to merge_gap_bars ahead for continuation
            look = end + 1
            skipped = 0
            while look < n and not vals[look] and skipped < merge_gap_bars:
                look += 1
                skipped += 1
            if look < n and vals[look]:
                end = look
            else:
                break
        if (end - start + 1) >= min_range_bars:
            result.append((start, end))
        i = end + 1
    return result


def detect_gaps(df: pl.DataFrame, tf: str, config: DetectorConfig) -> DetectionResult:
    """Run the full detection pipeline on a single (symbol, timeframe) frame."""
    tf_minutes = minutes_for_timeframe(tf)
    reindexed, missing = reindex_and_mark_missing(df, tf_minutes=tf_minutes)

    # price_bad: pct-change jump OR missing row
    price_jump = price_jump_mask(reindexed, threshold=config.gap_pct_threshold)
    price_bad = price_jump | missing

    # vol_bad: zero volume OR missing row
    vol_null_or_zero = reindexed["volume"].is_null() | (reindexed["volume"] == 0)
    vol_bad = vol_null_or_zero

    # vol-only candidates = vol_bad and not price_bad
    vol_only = vol_bad & ~price_bad

    full_ranges = [
        GapRange(s, e, RangeKind.FULL)
        for s, e in cluster_ranges(price_bad, config.merge_gap_bars, config.min_range_bars)
    ]
    volume_ranges = [
        GapRange(s, e, RangeKind.VOLUME_ONLY)
        for s, e in cluster_ranges(vol_only, config.merge_gap_bars, config.min_range_bars)
    ]
    return DetectionResult(
        reindexed_df=reindexed,
        full_ranges=full_ranges,
        volume_ranges=volume_ranges,
    )
```

- [ ] **Step 4: Run tests**

Run: `poetry run pytest tests/test_cex_gap_fill_detector.py -v`
Expected: 10 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/detector.py tests/test_cex_gap_fill_detector.py
git commit -m "feat(cex-gap-fill): add range clustering and full detection pipeline"
```

---

## Chunk 2: Router + Freqtrade Runner

### Task 7: Router — load routing JSON

**Files:**
- Create: `src/gmx_historical_data/cex_gap_fill/router.py`
- Create: `tests/test_cex_gap_fill_router.py`
- Create: `configs/cex_routing.json`

- [ ] **Step 1: Seed a minimal routing file**

```json
{
  "version": 1,
  "defaults": {
    "primary": "binance",
    "fallback": "bybit",
    "skip_unresolved": true
  },
  "overrides": {},
  "auto": {}
}
```

Write to `configs/cex_routing.json`.

- [ ] **Step 2: Write failing test**

```python
# tests/test_cex_gap_fill_router.py
"""Tests for cex_gap_fill.router."""

import json
from pathlib import Path

import pytest

from gmx_historical_data.cex_gap_fill.router import (
    RoutingTable,
    load_routing,
    Route,
)


def test_load_routing_returns_defaults_for_empty_file(tmp_path: Path):
    p = tmp_path / "r.json"
    p.write_text(json.dumps({"version": 1, "defaults": {"primary": "binance", "fallback": "bybit", "skip_unresolved": True}, "overrides": {}, "auto": {}}))
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
    # override must win
    assert route.exchange == "binance"


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
    # Empty table → unknown symbol resolves to None (caller decides probe or skip).
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
```

- [ ] **Step 3: Run — expect ImportError**

Run: `poetry run pytest tests/test_cex_gap_fill_router.py -v`
Expected: ImportError

- [ ] **Step 4: Implement**

```python
# src/gmx_historical_data/cex_gap_fill/router.py
"""Routing table for GMX symbol → CEX (exchange, pair) resolution.

Backed by ``configs/cex_routing.json``. Overrides beat auto-cache beats
probe. Static Python mappings live in :mod:`symbols`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Self


@dataclass(frozen=True, slots=True)
class Defaults:
    primary: str
    fallback: str
    skip_unresolved: bool


@dataclass(frozen=True, slots=True)
class Route:
    exchange: str
    pair: str = ""
    resolved_at: str = ""

    @property
    def is_skip(self) -> bool:
        return self.exchange == "skip"


@dataclass
class RoutingTable:
    defaults: Defaults
    overrides: dict[str, Route] = field(default_factory=dict)
    auto: dict[str, Route] = field(default_factory=dict)

    @classmethod
    def empty(cls) -> "RoutingTable":
        return cls(defaults=Defaults("binance", "bybit", True))

    def resolve(self, gmx_symbol: str) -> Route | None:
        if gmx_symbol in self.overrides:
            return self.overrides[gmx_symbol]
        if gmx_symbol in self.auto:
            return self.auto[gmx_symbol]
        return None

    def record_auto(self, gmx_symbol: str, route: Route) -> None:
        self.auto[gmx_symbol] = route


def load_routing(path: Path) -> RoutingTable:
    """Load routing table from disk. Missing file raises FileNotFoundError."""
    data = json.loads(path.read_text())
    d = data["defaults"]
    defaults = Defaults(primary=d["primary"], fallback=d["fallback"], skip_unresolved=d["skip_unresolved"])
    overrides = {k: Route(**v) for k, v in data.get("overrides", {}).items()}
    auto = {k: Route(**v) for k, v in data.get("auto", {}).items()}
    return RoutingTable(defaults=defaults, overrides=overrides, auto=auto)


def save_routing(table: RoutingTable, path: Path) -> None:
    """Persist routing table. Preserves user-supplied overrides; auto section is regenerated."""
    payload = {
        "version": 1,
        "defaults": {
            "primary": table.defaults.primary,
            "fallback": table.defaults.fallback,
            "skip_unresolved": table.defaults.skip_unresolved,
        },
        "overrides": {k: {kk: vv for kk, vv in v.__dict__.items() if vv != ""} for k, v in table.overrides.items()},
        "auto": {k: {kk: vv for kk, vv in v.__dict__.items() if vv != ""} for k, v in table.auto.items()},
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
```

- [ ] **Step 5: Run tests**

Run: `poetry run pytest tests/test_cex_gap_fill_router.py -v`
Expected: 5 passed

- [ ] **Step 6: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/router.py tests/test_cex_gap_fill_router.py configs/cex_routing.json
git commit -m "feat(cex-gap-fill): add routing table with overrides/auto precedence"
```

---

### Task 8: Router — auto-record round trip

**Files:**
- Modify: `tests/test_cex_gap_fill_router.py`

- [ ] **Step 1: Append test for round-trip through disk**

```python
# Append to tests/test_cex_gap_fill_router.py
from gmx_historical_data.cex_gap_fill.router import save_routing


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
```

- [ ] **Step 2: Run — expect pass (save_routing already implemented)**

Run: `poetry run pytest tests/test_cex_gap_fill_router.py -v`
Expected: 6 passed

- [ ] **Step 3: Commit**

```bash
git add tests/test_cex_gap_fill_router.py
git commit -m "test(cex-gap-fill): add router round-trip through disk"
```

---

### Task 9: Freqtrade runner — argv builder

**Files:**
- Create: `src/gmx_historical_data/cex_gap_fill/freqtrade_runner.py`
- Create: `tests/test_cex_gap_fill_freqtrade_runner.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_cex_gap_fill_freqtrade_runner.py
"""Tests for cex_gap_fill.freqtrade_runner."""

from pathlib import Path

from gmx_historical_data.cex_gap_fill.freqtrade_runner import build_download_argv


def test_build_download_argv_open_ended_timerange():
    argv = build_download_argv(
        exchange="binance",
        pairs=["BTC/USDT:USDT", "ETH/USDT:USDT"],
        timeframes=["1h", "4h"],
        timerange_start="20230801",
        datadir=None,
    )
    assert argv[0] == "./freqtrade-gmx"
    assert argv[1] == "download-data"
    assert "--exchange" in argv
    assert argv[argv.index("--exchange") + 1] == "binance"
    assert "--timerange" in argv
    assert argv[argv.index("--timerange") + 1] == "20230801-"
    assert "--data-format-ohlcv" in argv
    assert argv[argv.index("--data-format-ohlcv") + 1] == "feather"
    assert "--trading-mode" in argv
    assert argv[argv.index("--trading-mode") + 1] == "futures"


def test_build_download_argv_passes_all_pairs_and_timeframes():
    argv = build_download_argv(
        exchange="bybit",
        pairs=["SUI/USDT:USDT", "APT/USDT:USDT"],
        timeframes=["1min", "1h"],
        timerange_start="20240101",
        datadir=None,
    )
    pairs_idx = argv.index("--pairs")
    tf_idx = argv.index("--timeframes")
    # pairs list lives between --pairs and the next flag
    assert "SUI/USDT:USDT" in argv
    assert "APT/USDT:USDT" in argv
    assert "1min" in argv
    assert "1h" in argv
    assert pairs_idx < tf_idx  # order not strictly required but sanity


def test_build_download_argv_with_datadir(tmp_path: Path):
    argv = build_download_argv(
        exchange="binance",
        pairs=["BTC/USDT:USDT"],
        timeframes=["1h"],
        timerange_start="20230801",
        datadir=tmp_path,
    )
    assert "--datadir" in argv
    assert str(tmp_path) in argv


def test_build_download_argv_without_datadir_omits_flag():
    argv = build_download_argv(
        exchange="binance",
        pairs=["BTC/USDT:USDT"],
        timeframes=["1h"],
        timerange_start="20230801",
        datadir=None,
    )
    assert "--datadir" not in argv
```

- [ ] **Step 2: Run — expect ImportError**

Run: `poetry run pytest tests/test_cex_gap_fill_freqtrade_runner.py -v`
Expected: ImportError

- [ ] **Step 3: Implement argv builder**

```python
# src/gmx_historical_data/cex_gap_fill/freqtrade_runner.py
"""Subprocess wrapper for ``./freqtrade-gmx download-data``.

This is the only network boundary in the gap-fill stage. Freqtrade (inside its
own venv) handles ccxt, rate limits, retries.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


class CEXDownloadError(RuntimeError):
    """Raised when ``freqtrade download-data`` fails for an exchange call."""


def build_download_argv(
    exchange: str,
    pairs: list[str],
    timeframes: list[str],
    timerange_start: str,
    datadir: Path | None,
    trading_mode: str = "futures",
) -> list[str]:
    """Construct the subprocess argv for one ``./freqtrade-gmx download-data`` call.

    :param timerange_start: e.g. ``"20230801"``. Becomes ``--timerange 20230801-``
        so freqtrade fetches forward to "now".
    :param datadir: if ``None``, the freqtrade default datadir is used and the
        flag is omitted from argv entirely.
    """
    argv: list[str] = [
        "./freqtrade-gmx", "download-data",
        "--exchange", exchange,
        "--pairs", *pairs,
        "--timeframes", *timeframes,
        "--timerange", f"{timerange_start}-",
        "--data-format-ohlcv", "feather",
        "--trading-mode", trading_mode,
    ]
    if datadir is not None:
        argv += ["--datadir", str(datadir)]
    return argv
```

- [ ] **Step 4: Run tests**

Run: `poetry run pytest tests/test_cex_gap_fill_freqtrade_runner.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/freqtrade_runner.py tests/test_cex_gap_fill_freqtrade_runner.py
git commit -m "feat(cex-gap-fill): add freqtrade download argv builder"
```

---

### Task 10: Freqtrade runner — subprocess wrapper + error handling

**Files:**
- Modify: `src/gmx_historical_data/cex_gap_fill/freqtrade_runner.py`
- Modify: `tests/test_cex_gap_fill_freqtrade_runner.py`

- [ ] **Step 1: Append failing tests (mocked subprocess)**

```python
# Append to tests/test_cex_gap_fill_freqtrade_runner.py
import subprocess
from unittest.mock import patch, MagicMock

import pytest

from gmx_historical_data.cex_gap_fill.freqtrade_runner import (
    run_download,
    CEXDownloadError,
)


def test_run_download_invokes_subprocess_with_argv():
    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
        run_download(
            exchange="binance",
            pairs=["BTC/USDT:USDT"],
            timeframes=["1h"],
            timerange_start="20230801",
            datadir=None,
            cwd=Path("."),
            timeout=30,
        )
        assert mock_run.called
        args, kwargs = mock_run.call_args
        argv = args[0]
        assert argv[0] == "./freqtrade-gmx"
        assert kwargs["cwd"] == Path(".")
        assert kwargs["timeout"] == 30


def test_run_download_raises_on_nonzero_exit():
    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=2, stdout=b"", stderr=b"boom")
        with pytest.raises(CEXDownloadError, match="boom"):
            run_download(
                exchange="binance",
                pairs=["BTC/USDT:USDT"],
                timeframes=["1h"],
                timerange_start="20230801",
                datadir=None,
                cwd=Path("."),
                timeout=30,
            )


def test_run_download_timeout_propagates():
    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="x", timeout=1)
        with pytest.raises(CEXDownloadError, match="timed out"):
            run_download(
                exchange="binance",
                pairs=["BTC/USDT:USDT"],
                timeframes=["1h"],
                timerange_start="20230801",
                datadir=None,
                cwd=Path("."),
                timeout=1,
            )
```

- [ ] **Step 2: Run — expect ImportError / NameError**

Run: `poetry run pytest tests/test_cex_gap_fill_freqtrade_runner.py -v -k "run_download"`
Expected: 3 errors

- [ ] **Step 3: Implement subprocess wrapper**

Append to `freqtrade_runner.py`:

```python
import subprocess


def run_download(
    exchange: str,
    pairs: list[str],
    timeframes: list[str],
    timerange_start: str,
    datadir: Path | None,
    cwd: Path,
    timeout: int = 1800,
) -> subprocess.CompletedProcess[bytes]:
    """Invoke ``./freqtrade-gmx download-data`` for one exchange.

    :param cwd: working directory where the ``./freqtrade-gmx`` script lives.
    :param timeout: seconds before the subprocess is killed.
    :raises CEXDownloadError: on non-zero exit or timeout.
    """
    argv = build_download_argv(
        exchange=exchange,
        pairs=pairs,
        timeframes=timeframes,
        timerange_start=timerange_start,
        datadir=datadir,
    )
    try:
        result = subprocess.run(argv, cwd=cwd, timeout=timeout, capture_output=True)
    except subprocess.TimeoutExpired as err:
        raise CEXDownloadError(f"freqtrade download-data timed out after {timeout}s for {exchange}") from err
    if result.returncode != 0:
        tail = result.stderr.decode(errors="replace").strip().splitlines()[-10:]
        raise CEXDownloadError(f"freqtrade download-data exit={result.returncode} for {exchange}: " + "\n".join(tail))
    return result
```

- [ ] **Step 4: Run tests**

Run: `poetry run pytest tests/test_cex_gap_fill_freqtrade_runner.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/freqtrade_runner.py tests/test_cex_gap_fill_freqtrade_runner.py
git commit -m "feat(cex-gap-fill): add freqtrade subprocess wrapper with error handling"
```

---

### Task 11: Freqtrade runner — output feather path resolver

**Files:**
- Modify: `src/gmx_historical_data/cex_gap_fill/freqtrade_runner.py`
- Modify: `tests/test_cex_gap_fill_freqtrade_runner.py`

- [ ] **Step 1: Append failing test**

```python
# Append to tests/test_cex_gap_fill_freqtrade_runner.py
from gmx_historical_data.cex_gap_fill.freqtrade_runner import resolve_feather_path


def test_resolve_feather_path_futures_layout(tmp_path: Path):
    path = resolve_feather_path(
        datadir=tmp_path,
        exchange="binance",
        pair="BTC/USDT:USDT",
        timeframe="1h",
    )
    expected = tmp_path / "binance" / "futures" / "BTC_USDT_USDT-1h-futures.feather"
    assert path == expected


def test_resolve_feather_path_handles_1000_prefix(tmp_path: Path):
    path = resolve_feather_path(
        datadir=tmp_path,
        exchange="bybit",
        pair="1000BONK/USDT:USDT",
        timeframe="5min",
    )
    expected = tmp_path / "bybit" / "futures" / "1000BONK_USDT_USDT-5min-futures.feather"
    assert path == expected
```

- [ ] **Step 2: Run — expect ImportError**

Run: `poetry run pytest tests/test_cex_gap_fill_freqtrade_runner.py -v -k resolve_feather_path`
Expected: ImportError

- [ ] **Step 3: Implement**

Append to `freqtrade_runner.py`:

```python
def resolve_feather_path(datadir: Path, exchange: str, pair: str, timeframe: str) -> Path:
    """Build the expected freqtrade feather path for a given (exchange, pair, tf).

    Freqtrade naming: ``{BASE}_{QUOTE}_{SETTLE}-{tf}-futures.feather``.
    """
    base, rest = pair.split("/", 1)
    quote, settle = rest.split(":", 1)
    fname = f"{base}_{quote}_{settle}-{timeframe}-futures.feather"
    return datadir / exchange / "futures" / fname
```

- [ ] **Step 4: Run tests**

Run: `poetry run pytest tests/test_cex_gap_fill_freqtrade_runner.py -v`
Expected: 9 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/freqtrade_runner.py tests/test_cex_gap_fill_freqtrade_runner.py
git commit -m "feat(cex-gap-fill): add freqtrade feather path resolver"
```

---

## Chunk 3: Reconciler + Logging

### Task 12: Reconciler — price scaling

**Files:**
- Create: `src/gmx_historical_data/cex_gap_fill/reconciler.py`
- Create: `tests/test_cex_gap_fill_reconciler.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_cex_gap_fill_reconciler.py
"""Tests for cex_gap_fill.reconciler."""

from datetime import datetime, timedelta, UTC

import polars as pl

from gmx_historical_data.cex_gap_fill.reconciler import apply_price_scale


def _cex_df(prices: list[float], volumes: list[float]) -> pl.DataFrame:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    ts = [start + timedelta(hours=i) for i in range(len(prices))]
    return pl.DataFrame(
        {
            "timestamp": ts,
            "open": prices, "high": prices, "low": prices, "close": prices,
            "volume": volumes,
        }
    )


def test_apply_price_scale_for_btc_is_noop():
    df = _cex_df([50000.0, 51000.0], [10, 12])
    out = apply_price_scale(df, "BTC")
    assert out["close"].to_list() == [50000.0, 51000.0]
    assert out["volume"].to_list() == [10, 12]


def test_apply_price_scale_for_bonk_divides_price_multiplies_volume():
    # CEX 1000BONK price 0.050 → GMX BONK price 0.000050
    df = _cex_df([0.050, 0.051], [100.0, 200.0])
    out = apply_price_scale(df, "BONK")
    # price / 1000
    assert out["close"].to_list() == [0.050 / 1000, 0.051 / 1000]
    # volume * 1000
    assert out["volume"].to_list() == [100_000.0, 200_000.0]
```

- [ ] **Step 2: Run — expect ImportError**

Run: `poetry run pytest tests/test_cex_gap_fill_reconciler.py -v`
Expected: ImportError

- [ ] **Step 3: Implement**

```python
# src/gmx_historical_data/cex_gap_fill/reconciler.py
"""Merge CEX OHLCV into GMX parquet, honouring detector ranges and symbol scaling."""

from __future__ import annotations

import logging
from dataclasses import dataclass

import polars as pl

from .detector import DetectionResult, GapRange, RangeKind
from .symbols import PRICE_DIVISORS, normalize_k_prefix

log = logging.getLogger(__name__)


def apply_price_scale(cex_df: pl.DataFrame, gmx_symbol: str) -> pl.DataFrame:
    """Scale CEX OHLC and volume so they align with the GMX symbol's price regime.

    For 1000x tokens (e.g. BONK): OHLC / 1000, volume * 1000.
    For others: noop.
    """
    normalized = normalize_k_prefix(gmx_symbol)
    bare = normalized[1:] if normalized.startswith("K") and len(normalized) > 1 and normalized[1].isupper() else normalized
    divisor = PRICE_DIVISORS.get(bare)
    if divisor is None:
        return cex_df
    return cex_df.with_columns(
        (pl.col("open") / divisor).alias("open"),
        (pl.col("high") / divisor).alias("high"),
        (pl.col("low") / divisor).alias("low"),
        (pl.col("close") / divisor).alias("close"),
        (pl.col("volume") * divisor).alias("volume"),
    )
```

- [ ] **Step 4: Run tests**

Run: `poetry run pytest tests/test_cex_gap_fill_reconciler.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/reconciler.py tests/test_cex_gap_fill_reconciler.py
git commit -m "feat(cex-gap-fill): add CEX price scaling for 1000x tokens"
```

---

### Task 13: Reconciler — full-range replace with CEX confirmation

**Files:**
- Modify: `src/gmx_historical_data/cex_gap_fill/reconciler.py`
- Modify: `tests/test_cex_gap_fill_reconciler.py`

- [ ] **Step 1: Append failing tests**

```python
# Append to tests/test_cex_gap_fill_reconciler.py
from gmx_historical_data.cex_gap_fill.detector import DetectorConfig, detect_gaps
from gmx_historical_data.cex_gap_fill.reconciler import reconcile, ReconcileStats


def _gmx_df(prices: list[float], volumes: list[float]) -> pl.DataFrame:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    ts = [start + timedelta(hours=i) for i in range(len(prices))]
    return pl.DataFrame({
        "timestamp": ts,
        "open": prices, "high": prices, "low": prices, "close": prices,
        "volume": volumes,
    })


def test_reconcile_replaces_full_range_when_cex_disagrees():
    # GMX jumps +100% at idx 2 (missing oracle), CEX shows steady trend
    gmx = _gmx_df([100, 100, 200, 200], [1, 1, 1, 1])
    cex = _cex_df([100, 101, 102, 103], [10, 10, 10, 10])
    config = DetectorConfig(gap_pct_threshold=0.20, merge_gap_bars=0, min_range_bars=1)
    det = detect_gaps(gmx, tf="1h", config=config)
    out, stats = reconcile(gmx, cex, det, gmx_symbol="BTC", config=config)

    assert out["close"][2] == 102  # replaced with CEX
    assert stats.full_replaced >= 1


def test_reconcile_keeps_full_range_when_cex_confirms():
    # GMX +100% at idx 2; CEX also shows +100% — real event
    gmx = _gmx_df([100, 100, 200, 200], [1, 1, 1, 1])
    cex = _cex_df([100, 100, 200, 200], [10, 10, 10, 10])
    config = DetectorConfig(gap_pct_threshold=0.20, merge_gap_bars=0, min_range_bars=1)
    det = detect_gaps(gmx, tf="1h", config=config)
    out, stats = reconcile(gmx, cex, det, gmx_symbol="BTC", config=config)

    assert out["close"][2] == 200  # kept (GMX close)
    assert stats.kept >= 1


def test_reconcile_empty_cex_leaves_gmx_untouched():
    gmx = _gmx_df([100, 100, 200, 200], [1, 1, 1, 1])
    cex = _cex_df([], [])  # no CEX data
    config = DetectorConfig(gap_pct_threshold=0.20, merge_gap_bars=0, min_range_bars=1)
    det = detect_gaps(gmx, tf="1h", config=config)
    out, stats = reconcile(gmx, cex, det, gmx_symbol="BTC", config=config)
    assert out["close"].to_list() == [100, 100, 200, 200]
    assert stats.cex_missing >= 1
```

- [ ] **Step 2: Run — expect ImportError**

Run: `poetry run pytest tests/test_cex_gap_fill_reconciler.py -v -k reconcile`
Expected: ImportError

- [ ] **Step 3: Implement**

Append to `reconciler.py`:

```python
@dataclass
class ReconcileStats:
    full_replaced: int = 0
    volume_replaced: int = 0
    kept: int = 0
    cex_missing: int = 0


def _pct_change_at(df: pl.DataFrame, start_idx: int, end_idx: int) -> float | None:
    """Close-to-close pct change across an inclusive index range."""
    if start_idx == 0 or end_idx >= df.height:
        return None
    prev = df["close"][start_idx - 1]
    last = df["close"][end_idx]
    if prev is None or last is None or prev == 0:
        return None
    return (last - prev) / prev


def reconcile(
    gmx_df: pl.DataFrame,
    cex_df: pl.DataFrame,
    detection: DetectionResult,
    gmx_symbol: str,
    config,
) -> tuple[pl.DataFrame, ReconcileStats]:
    """Apply CEX OHLCV replacement ranges into the reindexed GMX frame.

    Returns a new DataFrame with the same schema as ``detection.reindexed_df``.
    """
    stats = ReconcileStats()
    scaled_cex = apply_price_scale(cex_df, gmx_symbol) if not cex_df.is_empty() else cex_df
    cex_by_ts = (
        {row["timestamp"]: row for row in scaled_cex.iter_rows(named=True)}
        if not scaled_cex.is_empty()
        else {}
    )

    df = detection.reindexed_df
    new_rows = df.to_dicts()  # editable list of dicts

    # Full-replace pass
    for gap in detection.full_ranges:
        ts_range = [new_rows[i]["timestamp"] for i in range(gap.start_idx, gap.end_idx + 1)]
        missing = [ts for ts in ts_range if ts not in cex_by_ts]
        if missing:
            log.info("cex missing for %s range [%s..%s]", gmx_symbol, ts_range[0], ts_range[-1])
            stats.cex_missing += 1
            continue
        gmx_pct = _pct_change_at(df, gap.start_idx, gap.end_idx)
        cex_slice = scaled_cex.filter(pl.col("timestamp").is_in(ts_range))
        if cex_slice.height < 2:
            cex_pct = None
        else:
            cex_prev = new_rows[gap.start_idx - 1]["close"] if gap.start_idx > 0 else None
            cex_last = cex_slice["close"][-1]
            cex_pct = (cex_last - cex_prev) / cex_prev if cex_prev not in (None, 0) else None
        if gmx_pct is not None and cex_pct is not None and abs(cex_pct - gmx_pct) < config.gap_pct_threshold / 2:
            stats.kept += 1
            continue
        # replace OHLCV
        for i in range(gap.start_idx, gap.end_idx + 1):
            ts = new_rows[i]["timestamp"]
            src = cex_by_ts[ts]
            for col in ("open", "high", "low", "close", "volume"):
                new_rows[i][col] = src[col]
        stats.full_replaced += 1

    # Volume-only pass
    for gap in detection.volume_ranges:
        for i in range(gap.start_idx, gap.end_idx + 1):
            ts = new_rows[i]["timestamp"]
            if ts in cex_by_ts:
                new_rows[i]["volume"] = cex_by_ts[ts]["volume"]
                stats.volume_replaced += 1

    return pl.DataFrame(new_rows, schema=df.schema), stats
```

- [ ] **Step 4: Run tests**

Run: `poetry run pytest tests/test_cex_gap_fill_reconciler.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/reconciler.py tests/test_cex_gap_fill_reconciler.py
git commit -m "feat(cex-gap-fill): add reconciler with CEX-confirm guard"
```

---

### Task 14: Reconciler — history-preservation guard + seam warning

**Files:**
- Modify: `src/gmx_historical_data/cex_gap_fill/reconciler.py`
- Modify: `tests/test_cex_gap_fill_reconciler.py`

- [ ] **Step 1: Append failing tests**

```python
# Append to tests/test_cex_gap_fill_reconciler.py
import pytest
from gmx_historical_data.cex_gap_fill.reconciler import assert_history_preserved, HistoryTruncationError


def test_assert_history_preserved_passes_when_earliest_equal():
    a = _gmx_df([100, 101, 102], [1, 1, 1])
    b = _gmx_df([100, 102, 103], [1, 1, 1])  # same earliest timestamp
    assert_history_preserved(original=a, corrected=b)  # no raise


def test_assert_history_preserved_raises_when_earliest_later():
    a = _gmx_df([100, 101, 102], [1, 1, 1])
    b = _gmx_df([101, 102], [1, 1])
    # drop first row in b by reconstructing with later start
    start = datetime(2026, 1, 1, 1, tzinfo=UTC)
    b = pl.DataFrame({
        "timestamp": [start, start + timedelta(hours=1)],
        "open": [101, 102], "high": [101, 102], "low": [101, 102], "close": [101, 102],
        "volume": [1, 1],
    })
    with pytest.raises(HistoryTruncationError):
        assert_history_preserved(original=a, corrected=b)
```

- [ ] **Step 2: Run — expect ImportError**

Run: `poetry run pytest tests/test_cex_gap_fill_reconciler.py -v -k history_preserved`
Expected: ImportError

- [ ] **Step 3: Implement**

Append to `reconciler.py`:

```python
class HistoryTruncationError(ValueError):
    """Reconciled frame begins later than the original — refuse to write."""


def assert_history_preserved(original: pl.DataFrame, corrected: pl.DataFrame) -> None:
    """Fatal guard: earliest timestamp of corrected must equal original."""
    if original.is_empty() and corrected.is_empty():
        return
    orig_first = original.sort("timestamp")["timestamp"][0]
    corr_first = corrected.sort("timestamp")["timestamp"][0]
    if corr_first > orig_first:
        raise HistoryTruncationError(
            f"corrected frame starts at {corr_first} but original starts at {orig_first}"
        )


def warn_seam_discontinuities(df: pl.DataFrame, threshold: float, gmx_symbol: str) -> int:
    """Log a warning for any residual pct_change > threshold after reconciliation. Never raises."""
    pct = df["close"].pct_change().abs()
    bad = pct > threshold
    n = int(bad.sum())
    if n > 0:
        log.warning("seam discontinuities after reconcile for %s: %d bars > %.2f", gmx_symbol, n, threshold)
    return n
```

- [ ] **Step 4: Run tests**

Run: `poetry run pytest tests/test_cex_gap_fill_reconciler.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/reconciler.py tests/test_cex_gap_fill_reconciler.py
git commit -m "feat(cex-gap-fill): add history-preservation guard and seam warning"
```

---

### Task 15: Logging utils

**Files:**
- Create: `src/gmx_historical_data/cex_gap_fill/logging_utils.py`
- Create: `tests/test_cex_gap_fill_logging_utils.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_cex_gap_fill_logging_utils.py
"""Tests for cex_gap_fill.logging_utils."""

import json
from pathlib import Path

from gmx_historical_data.cex_gap_fill.logging_utils import (
    RunSummary,
    make_run_id,
    write_summary_json,
)


def test_make_run_id_format():
    rid = make_run_id()
    assert len(rid) == len("YYYYMMDD_HHMMSS")
    assert rid[8] == "_"


def test_write_summary_json_schema(tmp_path: Path):
    s = RunSummary(run_id="20260424_101503", started_at="2026-04-24T10:15:03Z", finished_at="2026-04-24T10:20:00Z")
    s.symbols_processed = 5
    s.symbols_skipped_no_cex = ["FART"]
    s.totals["full_replaced"] = 3
    out = tmp_path / "summary.json"
    write_summary_json(s, out)
    data = json.loads(out.read_text())
    assert data["run_id"] == "20260424_101503"
    assert data["symbols_processed"] == 5
    assert data["symbols_skipped_no_cex"] == ["FART"]
    assert data["totals"]["full_replaced"] == 3
```

- [ ] **Step 2: Run — expect ImportError**

Run: `poetry run pytest tests/test_cex_gap_fill_logging_utils.py -v`
Expected: ImportError

- [ ] **Step 3: Implement**

```python
# src/gmx_historical_data/cex_gap_fill/logging_utils.py
"""Run log + JSON summary writers for the CEX gap-fill stage."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, UTC
from pathlib import Path


def make_run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


@dataclass
class RunSummary:
    run_id: str
    started_at: str
    finished_at: str = ""
    symbols_processed: int = 0
    symbols_skipped_no_cex: list[str] = field(default_factory=list)
    totals: dict[str, int] = field(default_factory=lambda: {"full_replaced": 0, "volume_replaced": 0, "kept": 0})
    errors: list[str] = field(default_factory=list)


def write_summary_json(summary: RunSummary, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary.__dict__, indent=2, default=str) + "\n")


def configure_run_logger(log_path: Path, level: str = "INFO") -> logging.Logger:
    """Configure the ``cex_gap_fill`` logger tree to write to ``log_path``."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("gmx_historical_data.cex_gap_fill")
    logger.setLevel(level)
    # remove any prior run handlers
    for h in list(logger.handlers):
        if getattr(h, "_cex_gap_fill", False):
            logger.removeHandler(h)
    fh = logging.FileHandler(log_path, mode="w")
    fh.setFormatter(logging.Formatter("[%(asctime)sZ] %(name)s %(levelname)s: %(message)s", datefmt="%Y-%m-%dT%H:%M:%S"))
    fh._cex_gap_fill = True  # type: ignore[attr-defined]
    logger.addHandler(fh)
    return logger
```

- [ ] **Step 4: Run tests**

Run: `poetry run pytest tests/test_cex_gap_fill_logging_utils.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/logging_utils.py tests/test_cex_gap_fill_logging_utils.py
git commit -m "feat(cex-gap-fill): add run logger and summary JSON writer"
```

---

## Chunk 4: Orchestrator + CLI + Makefile + Regression

### Task 16: Orchestrator — `fill_gaps_from_cex` entry point

**Files:**
- Modify: `src/gmx_historical_data/cex_gap_fill/__init__.py`
- Create: `src/gmx_historical_data/cex_gap_fill/orchestrator.py`
- Create: `tests/test_cex_gap_fill_orchestrator.py`

- [ ] **Step 1: Write failing end-to-end test with mocked subprocess**

```python
# tests/test_cex_gap_fill_orchestrator.py
"""End-to-end test for fill_gaps_from_cex with mocked freqtrade subprocess."""

from datetime import datetime, timedelta, UTC
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from gmx_historical_data.cex_gap_fill import fill_gaps_from_cex


def _write_gmx_parquet(path: Path, prices: list[float], volumes: list[float]) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    ts = [start + timedelta(hours=i) for i in range(len(prices))]
    df = pl.DataFrame({
        "timestamp": ts,
        "open": prices, "high": prices, "low": prices, "close": prices,
        "volume": volumes,
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def _write_cex_feather(path: Path, prices: list[float], volumes: list[float]) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    ts = [start + timedelta(hours=i) for i in range(len(prices))]
    df = pl.DataFrame({
        "date": ts,
        "open": prices, "high": prices, "low": prices, "close": prices,
        "volume": volumes,
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_ipc(path, compression=None)


def test_fill_gaps_from_cex_end_to_end(tmp_path: Path, monkeypatch):
    # Layout
    data_dir = tmp_path / "user_data"
    cex_datadir = tmp_path / "cex"
    parquet_path = data_dir / "data" / "candles" / "arbitrum" / "BTC" / "1h.parquet"
    _write_gmx_parquet(parquet_path, [100, 100, 200, 200, 200], [1, 1, 1, 1, 1])

    cex_feather = cex_datadir / "binance" / "futures" / "BTC_USDT_USDT-1h-futures.feather"
    _write_cex_feather(cex_feather, [100, 101, 102, 103, 104], [10, 10, 10, 10, 10])

    routing_file = tmp_path / "cex_routing.json"
    routing_file.write_text(
        '{"version":1,"defaults":{"primary":"binance","fallback":"bybit","skip_unresolved":true},'
        '"overrides":{"BTC":{"exchange":"binance","pair":"BTC/USDT:USDT"}},"auto":{}}'
    )

    # Patch subprocess so we do not actually spawn freqtrade.
    with patch("gmx_historical_data.cex_gap_fill.freqtrade_runner.subprocess.run") as mock_run:
        from unittest.mock import MagicMock
        mock_run.return_value = MagicMock(returncode=0, stdout=b"", stderr=b"")
        fill_gaps_from_cex(
            data_dir=data_dir,
            symbols=["BTC"],
            timeframes=["1h"],
            routing_file=routing_file,
            cex_datadir=cex_datadir,
            exchanges=["binance"],
            gap_threshold=0.20,
            merge_gap_bars=0,
            log_dir=tmp_path / "logs",
            dry_run=False,
        )

    # The GMX parquet should now have the replaced close at idx 2.
    out = pl.read_parquet(parquet_path)
    assert out["close"][2] == 102
```

- [ ] **Step 2: Run — expect NotImplementedError or ImportError**

Run: `poetry run pytest tests/test_cex_gap_fill_orchestrator.py -v`
Expected: fails

- [ ] **Step 3: Implement orchestrator**

```python
# src/gmx_historical_data/cex_gap_fill/orchestrator.py
"""Top-level orchestrator: iterates symbols/timeframes, invokes sub-modules, writes parquet."""

from __future__ import annotations

import logging
from datetime import datetime, UTC
from pathlib import Path

import polars as pl

from .detector import DetectorConfig, detect_gaps, minutes_for_timeframe
from .freqtrade_runner import resolve_feather_path, run_download, CEXDownloadError
from .logging_utils import RunSummary, configure_run_logger, make_run_id, write_summary_json
from .reconciler import assert_history_preserved, reconcile, warn_seam_discontinuities
from .router import RoutingTable, load_routing, save_routing
from .symbols import gmx_symbol_to_cex_pair

log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_TIMEFRAMES = ["1min", "5min", "15min", "1h", "4h", "1d"]


def fill_gaps_from_cex(
    data_dir: Path,
    symbols: list[str] | None,
    timeframes: list[str] | None,
    routing_file: Path,
    cex_datadir: Path | None,
    exchanges: list[str],
    gap_threshold: float,
    merge_gap_bars: int,
    log_dir: Path,
    dry_run: bool,
    skip_download: bool = False,
    download_timeout: int = 1800,
    download_start: str = "20230801",
    network: str = "arbitrum",
) -> RunSummary:
    """Run the full gap-fill stage.

    :param data_dir: root data dir (same as ``collect --output-dir``). Parquets live at
        ``{data_dir}/data/candles/{network}/{SYMBOL}/{tf}.parquet``.
    :param symbols: whitelist, None = all on disk.
    :param timeframes: whitelist, None = all six.
    :param routing_file: path to ``configs/cex_routing.json``.
    :param cex_datadir: override freqtrade datadir; None = freqtrade default.
    :param exchanges: which CEX venues to try, in order.
    :param dry_run: detect + log without writing parquet.
    :param skip_download: reuse on-disk CEX feathers, skip subprocess.
    """
    run_id = make_run_id()
    log_path = log_dir / f"cex_gap_fill_{run_id}.log"
    configure_run_logger(log_path)
    summary = RunSummary(run_id=run_id, started_at=datetime.now(UTC).isoformat())

    config = DetectorConfig(gap_pct_threshold=gap_threshold, merge_gap_bars=merge_gap_bars)
    tfs = timeframes or DEFAULT_TIMEFRAMES
    routing = load_routing(routing_file)

    candles_root = data_dir / "data" / "candles" / network
    if symbols is None:
        symbols = sorted(p.name for p in candles_root.iterdir() if p.is_dir())

    # 1. Build CEX download plan per exchange, honouring routing.
    plan = _build_download_plan(symbols, tfs, routing, exchanges)

    # 2. Invoke freqtrade per exchange.
    if not skip_download:
        for exch, pairs in plan.items():
            if not pairs:
                continue
            try:
                run_download(
                    exchange=exch,
                    pairs=sorted(pairs),
                    timeframes=tfs,
                    timerange_start=download_start,
                    datadir=cex_datadir,
                    cwd=REPO_ROOT,
                    timeout=download_timeout,
                )
            except CEXDownloadError as err:
                log.error("download failed for %s: %s", exch, err)
                summary.errors.append(f"{exch}: {err}")

    # 3. Per-symbol reconcile.
    for sym in symbols:
        route = routing.resolve(sym)
        if route is None or route.is_skip:
            summary.symbols_skipped_no_cex.append(sym)
            continue
        for tf in tfs:
            parquet_path = candles_root / sym / f"{tf}.parquet"
            if not parquet_path.exists():
                continue
            gmx_df = pl.read_parquet(parquet_path)
            cex_path = _resolve_cex_feather(cex_datadir, route.exchange, route.pair, tf)
            cex_df = pl.read_ipc(cex_path) if cex_path.exists() else pl.DataFrame(schema=gmx_df.schema)
            cex_df = _normalize_cex_schema(cex_df)

            det = detect_gaps(gmx_df, tf=tf, config=config)
            corrected, stats = reconcile(gmx_df, cex_df, det, gmx_symbol=sym, config=config)
            assert_history_preserved(original=gmx_df, corrected=corrected)
            warn_seam_discontinuities(corrected, threshold=gap_threshold, gmx_symbol=sym)

            summary.totals["full_replaced"] += stats.full_replaced
            summary.totals["volume_replaced"] += stats.volume_replaced
            summary.totals["kept"] += stats.kept

            if not dry_run:
                corrected.write_parquet(parquet_path)

        summary.symbols_processed += 1

    # 4. Persist routing (auto cache may have changed).
    save_routing(routing, routing_file)

    summary.finished_at = datetime.now(UTC).isoformat()
    write_summary_json(summary, log_dir / f"cex_gap_fill_{run_id}.summary.json")
    return summary


def _build_download_plan(symbols: list[str], tfs: list[str], routing: RoutingTable, exchanges: list[str]) -> dict[str, set[str]]:
    plan: dict[str, set[str]] = {e: set() for e in exchanges}
    for sym in symbols:
        route = routing.resolve(sym)
        if route and not route.is_skip and route.exchange in plan:
            plan[route.exchange].add(route.pair)
    return plan


def _resolve_cex_feather(datadir: Path | None, exchange: str, pair: str, tf: str) -> Path:
    if datadir is None:
        # Freqtrade default: repo-root relative user_data/data
        datadir = REPO_ROOT / "user_data" / "data"
    return resolve_feather_path(datadir=datadir, exchange=exchange, pair=pair, timeframe=tf)


def _normalize_cex_schema(cex_df: pl.DataFrame) -> pl.DataFrame:
    """Freqtrade feathers use 'date'; our code uses 'timestamp'."""
    if "date" in cex_df.columns and "timestamp" not in cex_df.columns:
        cex_df = cex_df.rename({"date": "timestamp"})
    return cex_df
```

Update `__init__.py`:

```python
"""CEX gap-fill pipeline stage."""

from .orchestrator import fill_gaps_from_cex  # noqa: F401

__all__ = ["fill_gaps_from_cex"]
```

- [ ] **Step 4: Run test**

Run: `poetry run pytest tests/test_cex_gap_fill_orchestrator.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/cex_gap_fill/orchestrator.py src/gmx_historical_data/cex_gap_fill/__init__.py tests/test_cex_gap_fill_orchestrator.py
git commit -m "feat(cex-gap-fill): add orchestrator entry point"
```

---

### Task 17: CLI command — `fill-gaps-cex`

**Files:**
- Modify: `src/gmx_historical_data/cli.py`
- Create: `tests/test_cli_fill_gaps_cex.py`

- [ ] **Step 1: Inspect current CLI registration pattern**

Run: `grep -n "app.command" src/gmx_historical_data/cli.py | head -5`
Expected: examples of `app.command(name="...")(fn)` pattern.

- [ ] **Step 2: Write failing CLI test**

```python
# tests/test_cli_fill_gaps_cex.py
"""Test for the fill-gaps-cex typer command."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from gmx_historical_data.cli import app


def test_fill_gaps_cex_invokes_orchestrator(tmp_path: Path):
    runner = CliRunner()
    with patch("gmx_historical_data.cli.fill_gaps_from_cex") as mock:
        mock.return_value = None
        result = runner.invoke(
            app,
            [
                "fill-gaps-cex",
                "--data-dir", str(tmp_path),
                "--symbol", "BTC,ETH",
                "--timeframe", "1h",
                "--gap-threshold", "0.25",
                "--merge-gap-bars", "3",
                "--exchanges", "binance,bybit",
                "--routing-file", str(tmp_path / "r.json"),
                "--log-dir", str(tmp_path / "logs"),
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, result.output
        assert mock.called
        kwargs = mock.call_args.kwargs
        assert kwargs["symbols"] == ["BTC", "ETH"]
        assert kwargs["timeframes"] == ["1h"]
        assert kwargs["gap_threshold"] == 0.25
        assert kwargs["merge_gap_bars"] == 3
        assert kwargs["exchanges"] == ["binance", "bybit"]
        assert kwargs["dry_run"] is True
```

- [ ] **Step 3: Run — expect failure (command not registered)**

Run: `poetry run pytest tests/test_cli_fill_gaps_cex.py -v`
Expected: fail

- [ ] **Step 4: Add command to `cli.py`**

Append to `src/gmx_historical_data/cli.py` (before the final `if __name__ == "__main__":` guard, matching existing `app.command(name=...)` pattern):

```python
from gmx_historical_data.cex_gap_fill import fill_gaps_from_cex


def fill_gaps_cex(
    data_dir: Path = typer.Option(Path("./user_data"), "--data-dir", help="Root data dir"),
    symbol: str = typer.Option("", "--symbol", help="Comma-separated whitelist; empty = all"),
    timeframe: str = typer.Option("", "--timeframe", help="Comma-separated whitelist; empty = all six"),
    gap_threshold: float = typer.Option(0.20, "--gap-threshold"),
    merge_gap_bars: int = typer.Option(2, "--merge-gap-bars"),
    cex_datadir: Optional[Path] = typer.Option(None, "--cex-datadir"),
    exchanges: str = typer.Option("binance,bybit", "--exchanges"),
    routing_file: Path = typer.Option(Path("configs/cex_routing.json"), "--routing-file"),
    skip_download: bool = typer.Option(False, "--skip-download"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    log_dir: Path = typer.Option(Path("./logs"), "--log-dir"),
    network: str = typer.Option("arbitrum", "--network"),
) -> None:
    """Fill GMX OHLCV price gaps using Binance/Bybit via freqtrade download-data."""
    symbols = [s.strip() for s in symbol.split(",") if s.strip()] or None
    tfs = [t.strip() for t in timeframe.split(",") if t.strip()] or None
    exch = [e.strip() for e in exchanges.split(",") if e.strip()]
    fill_gaps_from_cex(
        data_dir=data_dir,
        symbols=symbols,
        timeframes=tfs,
        routing_file=routing_file,
        cex_datadir=cex_datadir,
        exchanges=exch,
        gap_threshold=gap_threshold,
        merge_gap_bars=merge_gap_bars,
        log_dir=log_dir,
        dry_run=dry_run,
        skip_download=skip_download,
        network=network,
    )


app.command(name="fill-gaps-cex")(fill_gaps_cex)
```

Verify imports at top of file: add `from typing import Optional` if not present. Add `from pathlib import Path` if not present. These already exist in `cli.py` — confirm before editing.

- [ ] **Step 5: Run tests**

Run: `poetry run pytest tests/test_cli_fill_gaps_cex.py -v`
Expected: 1 passed

- [ ] **Step 6: Commit**

```bash
git add src/gmx_historical_data/cli.py tests/test_cli_fill_gaps_cex.py
git commit -m "feat(cex-gap-fill): register fill-gaps-cex CLI command"
```

---

### Task 18: Makefile targets

**Files:**
- Modify: `Makefile`

- [ ] **Step 1: Add knobs after line 53 (KEEP block)**

```makefile
# CEX gap-fill knobs (additive, optional)
GAP_THRESHOLD     ?= 0.20
MERGE_GAP_BARS    ?= 2
CEX_DATADIR       ?=
CEX_EXCHANGES     ?= binance,bybit
CEX_ROUTING_FILE  ?= configs/cex_routing.json
SKIP_DOWNLOAD     ?=
```

- [ ] **Step 2: Update the `.PHONY` line (top of Makefile, line 59–66)**

Add to the existing `.PHONY`:
```
        fill-gaps-cex refresh-data-cex full-data-cex full-data-nn-cex
```

- [ ] **Step 3: Append the four new targets at the end of the file**

```makefile
# ==============================================================================
# CEX Gap-Fill (additive — runs between collect and export-freqtrade)
# ==============================================================================

fill-gaps-cex:
	@echo "Filling GMX price gaps with CEX data..."
	@echo "  Data dir:    $(DATA_DIR)"
	@echo "  Threshold:   $(GAP_THRESHOLD)"
	@echo "  Exchanges:   $(CEX_EXCHANGES)"
	$(if $(SYMBOL),@echo "  Symbol:      $(SYMBOL)",)
	@mkdir -p "$(DATA_DIR)" "$(LOG_DIR)"
	$(NICE) poetry run python -m gmx_historical_data.cli fill-gaps-cex \
		--data-dir "$(DATA_DIR)" \
		--gap-threshold $(GAP_THRESHOLD) \
		--merge-gap-bars $(MERGE_GAP_BARS) \
		--exchanges $(CEX_EXCHANGES) \
		--routing-file "$(CEX_ROUTING_FILE)" \
		$(if $(CEX_DATADIR),--cex-datadir "$(CEX_DATADIR)",) \
		$(if $(SYMBOL),--symbol $(SYMBOL),) \
		$(SKIP_DOWNLOAD) \
		$(ARGS)

refresh-data-cex: collect-update funding-unified-resume extract-all-resume fill-gaps-cex export-freqtrade
	@echo ""
	@echo "Incremental refresh (with CEX gap-fill) complete"

full-data-cex: collect-full funding-unified extract-all fill-gaps-cex export-freqtrade
	@echo ""
	@echo "Full data download (with CEX gap-fill) complete"

full-data-nn-cex: collect-full-nn funding-unified extract-all fill-gaps-cex export-freqtrade
	@echo ""
	@echo "Full data download no-nice (with CEX gap-fill) complete"
```

- [ ] **Step 4: Update the `help:` text (inside the echo block around lines 71-106)**

Insert these lines under the "Recommended targets" section:
```
	@echo "  refresh-data-cex   Incremental + CEX gap-fill (Binance/Bybit) + export"
	@echo "  full-data-cex      Full historical + CEX gap-fill + export"
	@echo "  full-data-nn-cex   Full historical no-nice + CEX gap-fill + export"
```

And under "Individual targets":
```
	@echo "  fill-gaps-cex      Run CEX gap-fill stage on existing parquet"
```

- [ ] **Step 5: Verify Makefile parses**

Run: `make show-config`
Expected: clean output. No syntax errors.

- [ ] **Step 6: Dry-run the new target against a single symbol (skip-download, no network)**

Run: `make fill-gaps-cex SYMBOL=ETH SKIP_DOWNLOAD=--skip-download ARGS=--dry-run`
Expected: runs the CLI command, exits 0 (or logs "no symbols" if nothing on disk).

- [ ] **Step 7: Commit**

```bash
git add Makefile
git commit -m "feat(cex-gap-fill): add Makefile targets fill-gaps-cex, refresh-data-cex, full-data-cex, full-data-nn-cex"
```

---

### Task 19: Legacy flow regression test

**Files:**
- Create: `tests/test_cex_gap_fill_legacy_flow_unchanged.py`

- [ ] **Step 1: Write test that asserts legacy `export-freqtrade` output is byte-identical with and without `cex_gap_fill` imported**

```python
# tests/test_cex_gap_fill_legacy_flow_unchanged.py
"""Regression test: the legacy export-freqtrade flow is byte-identical when
the CEX gap-fill module is imported but not invoked.

The guarantee is that importing `cex_gap_fill` has zero side effects on the
existing pipeline. Feather bytes produced by the exporter must not change.
"""

from datetime import datetime, timedelta, UTC
from pathlib import Path
import hashlib

import polars as pl


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _seed_parquet(path: Path) -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    df = pl.DataFrame({
        "timestamp": [start + timedelta(hours=i) for i in range(10)],
        "open":   [100.0 + i for i in range(10)],
        "high":   [101.0 + i for i in range(10)],
        "low":    [ 99.0 + i for i in range(10)],
        "close":  [100.5 + i for i in range(10)],
        "volume": [float(i + 1) for i in range(10)],
    })
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def test_import_of_cex_gap_fill_does_not_change_legacy_exporter_output(tmp_path: Path):
    # Prepare a minimal candle parquet.
    parquet_path = tmp_path / "data" / "candles" / "arbitrum" / "BTC" / "1h.parquet"
    _seed_parquet(parquet_path)

    # Run the legacy exporter once.
    from gmx_historical_data.freqtrade_exporter import FreqtradeExporter
    out1 = tmp_path / "out1"
    FreqtradeExporter(data_dir=tmp_path, output_dir=out1).export()
    feather1 = out1 / "gmx" / "futures" / "BTC_USDC_USDC-1h-futures.feather"
    assert feather1.exists()
    sha_before = _sha(feather1)

    # Now import the cex_gap_fill module (without invoking).
    import gmx_historical_data.cex_gap_fill  # noqa: F401

    # Re-run legacy exporter into a fresh dir; bytes must be identical.
    out2 = tmp_path / "out2"
    FreqtradeExporter(data_dir=tmp_path, output_dir=out2).export()
    feather2 = out2 / "gmx" / "futures" / "BTC_USDC_USDC-1h-futures.feather"
    assert feather2.exists()
    sha_after = _sha(feather2)

    assert sha_before == sha_after
```

Note: `FreqtradeExporter` class name and constructor signature need verification — adjust to match `src/gmx_historical_data/freqtrade_exporter.py`. The assertion logic is what matters; call-site is the adapter.

- [ ] **Step 2: Adapt test to match real exporter signature**

Run: `grep -n "class FreqtradeExporter\|def export\|def __init__" src/gmx_historical_data/freqtrade_exporter.py | head -10`
Expected: true class/method names and signature.

Update the test if signature differs (e.g. `.run()` instead of `.export()`).

- [ ] **Step 3: Run test**

Run: `poetry run pytest tests/test_cex_gap_fill_legacy_flow_unchanged.py -v`
Expected: 1 passed

- [ ] **Step 4: Commit**

```bash
git add tests/test_cex_gap_fill_legacy_flow_unchanged.py
git commit -m "test(cex-gap-fill): legacy exporter byte-identical after cex_gap_fill import"
```

---

### Task 20: Manual integration check + final commit

**Files:**
- Modify: `docs/superpowers/specs/2026-04-24-cex-gap-fill-design.md` (add "Verified" banner)

- [ ] **Step 1: Run the complete unit test suite for the new module**

Run: `poetry run pytest tests/test_cex_gap_fill_*.py tests/test_cli_fill_gaps_cex.py -v`
Expected: all green.

- [ ] **Step 2: Run the existing test suite to confirm no regression**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run pytest -x`
Expected: same pass/fail baseline as before the branch; no new failures.

- [ ] **Step 3: Manual end-to-end against a small symbol set**

Requires an active freqtrade venv reachable via `./freqtrade-gmx`. Pick one symbol that exists both on GMX and Binance:

```bash
# Seed routing
cat > configs/cex_routing.json <<'EOF'
{
  "version": 1,
  "defaults": {"primary": "binance", "fallback": "bybit", "skip_unresolved": true},
  "overrides": {
    "ETH": {"exchange": "binance", "pair": "ETH/USDT:USDT"}
  },
  "auto": {}
}
EOF

# Run the full chain for ETH only
make full-data-cex SYMBOL=ETH
```

Inspect:
- `logs/cex_gap_fill_*.log` — per-symbol range list.
- `logs/cex_gap_fill_*.summary.json` — totals.
- `data/gmx/futures/ETH_USDC_USDC-1h-futures.feather` — file size changed vs. pre-fill; open in polars to spot-check replaced bars.

Compare against a non-CEX run (`make refresh-data SYMBOL=ETH`) for the same symbol to quantify the difference.

- [ ] **Step 4: Add a "Verified" banner to the spec**

Edit `docs/superpowers/specs/2026-04-24-cex-gap-fill-design.md`, change the `Status:` line at the top to:

```
**Status:** Implemented on branch `feat/cex-gap-fill` (YYYY-MM-DD)
```

- [ ] **Step 5: Commit**

```bash
git add docs/superpowers/specs/2026-04-24-cex-gap-fill-design.md
git commit -m "docs(cex-gap-fill): mark spec as implemented"
```

- [ ] **Step 6: Open a PR (when ready)**

User decides when. Suggested:

```bash
gh pr create --title "feat: CEX gap-fill stage for GMX feather export" --body-file docs/superpowers/specs/2026-04-24-cex-gap-fill-design.md
```

---

## Completion Checklist

- [ ] All 20 tasks committed individually.
- [ ] `poetry run pytest tests/test_cex_gap_fill_*.py` — all green.
- [ ] `make full-data-nn` (legacy target) still works and produces identical output to before the branch against a known fixture.
- [ ] `make refresh-data-cex SYMBOL=ETH` produces a filled dataset with a matching log and summary.
- [ ] `configs/cex_routing.json` committed with at least the `BTC` / `ETH` / `SOL` overrides seeded.
- [ ] Spec updated with the "Implemented" banner.
- [ ] Branch is ready to PR into `master`.
