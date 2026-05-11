# Funding Rate Hourly Gap-Fill — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Forward-fill silent hours in `1h.parquet` so the merged per-symbol funding-rate file has one row for every hour in `[first_event_hour, last_event_hour]`. Filled rows carry the most recent prior rate; they are flagged via a new `is_gap_filled` column. The raw event parquet and `1h_factor.parquet` stay event-only.

**Architecture:** One new pure-Polars helper `forward_fill_hourly_grid` in `scripts/extract_unified_funding.py`. One integration point in `merge_symbol` (call between reading `1h_factor.parquet` and `apply_direction_to_rates`). One schema additive change (`is_gap_filled: Boolean`).

**Tech Stack:** Polars `^1.x`, pytest. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-05-11-funding-gap-fill-design.md`

**Predecessor:** `docs/superpowers/plans/2026-05-11-funding-correctness-polars.md` (must be applied first — gap-fill depends on the TWAP aggregator and the direction forward-fill).

---

## Chunk 1: Add `forward_fill_hourly_grid` helper (TDD)

### Task 1: Write unit tests first

**Files:**
- Create: `tests/test_unified_gap_fill.py`

**Context for implementer:**

The function lives in `scripts/extract_unified_funding.py` (next to `apply_direction_to_rates`). Load the script as a module via `importlib.util` to match the existing test pattern in `tests/test_unified_merge_direction.py`.

The factor parquet schema after the predecessor plan: `timestamp`, `funding_rate`, `funding_rate_min`, `funding_rate_max`, `funding_rate_hourly`, `funding_rate_annualized`, `update_count`, `symbol`, `market`. (No `longs_pay_shorts` or signed-fee columns — those are added later by `apply_direction_to_rates`.)

- [ ] **Step 1.1: Create the test file.**

```python
"""Tests for forward_fill_hourly_grid in :mod:`scripts.extract_unified_funding`."""

from __future__ import annotations

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl


def _load_module():
    repo = Path(__file__).resolve().parents[1]
    path = repo / "scripts" / "extract_unified_funding.py"
    spec = importlib.util.spec_from_file_location("extract_unified_funding", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["extract_unified_funding"] = module
    spec.loader.exec_module(module)
    return module


_mod = _load_module()
forward_fill_hourly_grid = _mod.forward_fill_hourly_grid


def _factor_df(rows):
    """Build a factor-shaped frame from (timestamp, rate, update_count) tuples."""
    ts, rates, counts = zip(*rows)
    return pl.DataFrame(
        {
            "timestamp": list(ts),
            "funding_rate": list(rates),
            "funding_rate_min": list(rates),
            "funding_rate_max": list(rates),
            "funding_rate_hourly": [r * 3600 for r in rates],
            "funding_rate_annualized": [r * 3600 * 8760 for r in rates],
            "update_count": list(counts),
            "symbol": ["ETH"] * len(rows),
            "market": ["0xm"] * len(rows),
        },
        schema_overrides={
            "timestamp": pl.Datetime("ns", "UTC"),
            "update_count": pl.UInt32,
        },
    )


def test_fill_internal_gap_carries_rate_forward():
    """Two events with a 5-hour gap: filled rows take the earlier rate."""
    base = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    df = _factor_df(
        [
            (base, 1e-9, 3),
            (base + timedelta(hours=6), 2e-9, 2),
        ]
    )
    out = forward_fill_hourly_grid(df).sort("timestamp")

    # Grid covers 7 hours: 12:00..18:00 inclusive.
    assert out.height == 7
    rates = out["funding_rate"].to_list()
    filled = out["is_gap_filled"].to_list()
    counts = out["update_count"].to_list()

    # Hour 0: real event at 12:00 → rate 1e-9, not filled, count=3.
    assert rates[0] == 1e-9
    assert filled[0] is False
    assert counts[0] == 3
    # Hours 1-5 (13:00..17:00): forward-filled with 1e-9, count=0, filled=True.
    for i in range(1, 6):
        assert rates[i] == 1e-9
        assert filled[i] is True
        assert counts[i] == 0
    # Hour 6 (18:00): real event → rate 2e-9, not filled, count=2.
    assert rates[6] == 2e-9
    assert filled[6] is False
    assert counts[6] == 2


def test_fill_derived_columns_also_propagate():
    """funding_rate_hourly and friends must be filled, not left null."""
    base = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    df = _factor_df([(base, 1e-9, 1), (base + timedelta(hours=2), 2e-9, 1)])
    out = forward_fill_hourly_grid(df).sort("timestamp")

    # Middle hour (01:00): filled. funding_rate_hourly = 1e-9 * 3600 = 3.6e-6.
    middle = out.filter(pl.col("timestamp") == base + timedelta(hours=1))
    assert middle["funding_rate_hourly"].item() == 1e-9 * 3600
    assert middle["funding_rate_annualized"].item() == 1e-9 * 3600 * 8760
    assert middle["funding_rate_min"].item() == 1e-9
    assert middle["funding_rate_max"].item() == 1e-9


def test_fill_preserves_symbol_and_market():
    base = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    df = _factor_df([(base, 1e-9, 1), (base + timedelta(hours=2), 1e-9, 1)])
    out = forward_fill_hourly_grid(df).sort("timestamp")
    assert out["symbol"].to_list() == ["ETH"] * 3
    assert out["market"].to_list() == ["0xm"] * 3


def test_empty_input_returns_empty():
    df = pl.DataFrame(
        {
            "timestamp": [],
            "funding_rate": [],
            "update_count": [],
        },
        schema={"timestamp": pl.Datetime("ns", "UTC"), "funding_rate": pl.Float64, "update_count": pl.UInt32},
    )
    out = forward_fill_hourly_grid(df)
    assert out.is_empty()


def test_single_row_returns_one_hour():
    base = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    df = _factor_df([(base, 5e-9, 1)])
    out = forward_fill_hourly_grid(df)
    assert out.height == 1
    assert out["is_gap_filled"].to_list() == [False]
    assert out["funding_rate"].to_list() == [5e-9]


def test_fill_does_not_extend_beyond_last_event():
    """Grid stops at max(timestamp). No zombie-fill past the last on-chain event."""
    base = datetime(2026, 1, 1, 0, 0, tzinfo=UTC)
    df = _factor_df([(base, 1e-9, 1), (base + timedelta(hours=3), 2e-9, 1)])
    out = forward_fill_hourly_grid(df).sort("timestamp")
    assert out["timestamp"].max() == base + timedelta(hours=3)
    assert out.height == 4  # hours 0,1,2,3
```

- [ ] **Step 1.2: Run the test — confirm failure** (function doesn't exist yet).

```bash
poetry run python -m pytest tests/test_unified_gap_fill.py -v
```

Expected: `AttributeError: module 'extract_unified_funding' has no attribute 'forward_fill_hourly_grid'`.

### Task 2: Implement `forward_fill_hourly_grid`

**Files:**
- Modify: `scripts/extract_unified_funding.py` — add the helper near `apply_direction_to_rates`.

- [ ] **Step 2.1: Add the function.**

Insert after `apply_direction_to_rates`:

```python
def forward_fill_hourly_grid(rates: pl.DataFrame) -> pl.DataFrame:
    """Expand event-based hourly rates onto a contiguous hourly grid.

    GMX emits ``Funding`` events only on position updates, so low-volume
    markets have silent hours. The on-chain ``fundingFactorPerSecond`` is a
    contract state variable that persists between events — a silent hour
    means the rate is unchanged since the most recent prior event, not
    that the rate is unknown.

    This helper builds a continuous hourly grid spanning
    ``[min(timestamp), max(timestamp)]`` and forward-fills numeric and
    identifier columns. Filled rows are flagged via ``is_gap_filled = True``
    and ``update_count = 0``. The grid never extends past the last observed
    event (no backfill, no zombie-fill).

    :param rates: Hourly rate frame from ``aggregate_hourly_rates`` or
        ``rates/{SYM}/1h_factor.parquet``. Must contain at least
        ``timestamp`` and ``update_count``.
    :returns: Contiguous hourly frame with ``is_gap_filled`` column added.
        Empty input returns an empty frame (schema preserved).
    """
    if rates.is_empty():
        return rates.with_columns(pl.lit(False).alias("is_gap_filled"))

    start = rates["timestamp"].min()
    end = rates["timestamp"].max()
    grid = pl.DataFrame(
        {
            "timestamp": pl.datetime_range(
                start, end, interval="1h", time_zone="UTC", eager=True
            )
        }
    )

    # Left-join factor rows onto the grid; unmatched grid hours get nulls.
    merged = grid.join(rates, on="timestamp", how="left")

    # Flag filled rows BEFORE forward-fill (otherwise we lose the signal).
    merged = merged.with_columns(
        pl.col("update_count").is_null().alias("is_gap_filled"),
    )

    # Forward-fill rate columns and identifiers. update_count is filled with 0
    # (not forward-filled — a filled hour has zero events by definition).
    ff_cols = [
        "funding_rate",
        "funding_rate_min",
        "funding_rate_max",
        "funding_rate_hourly",
        "funding_rate_annualized",
        "symbol",
        "market",
    ]
    merged = merged.with_columns(
        [pl.col(c).forward_fill() for c in ff_cols if c in merged.columns]
    )
    merged = merged.with_columns(
        pl.col("update_count").fill_null(0).cast(pl.UInt32),
    )

    return merged.sort("timestamp")
```

- [ ] **Step 2.2: Run tests — expect all PASS.**

```bash
poetry run python -m pytest tests/test_unified_gap_fill.py -v
```

- [ ] **Step 2.3: Lint.**

```bash
poetry run ruff check scripts/extract_unified_funding.py tests/test_unified_gap_fill.py
```

---

## Chunk 2: Wire fill into `merge_symbol`

### Task 3: Call `forward_fill_hourly_grid` before direction merge

**Files:**
- Modify: `scripts/extract_unified_funding.py:merge_symbol` (around the factor-read block).

**Context:** After the previous plan, `merge_symbol` looks like:

```python
factor_path = rates_dir / symbol / "1h_factor.parquet"
if factor_path.exists():
    hs_df = pl.read_parquet(factor_path)
    direction_path = direction_dir / symbol / "1h.parquet"
    dir_df = pl.read_parquet(direction_path) if direction_path.exists() else None
    hs_df = apply_direction_to_rates(hs_df, dir_df)
    hs_df = hs_df.with_columns(pl.lit("hypersync").alias("source"))
    frames.append(hs_df)
```

- [ ] **Step 3.1: Add the fill call.**

Insert `hs_df = forward_fill_hourly_grid(hs_df)` between the parquet read and `apply_direction_to_rates`:

```python
factor_path = rates_dir / symbol / "1h_factor.parquet"
if factor_path.exists():
    hs_df = pl.read_parquet(factor_path)
    hs_df = forward_fill_hourly_grid(hs_df)  # ← NEW
    direction_path = direction_dir / symbol / "1h.parquet"
    dir_df = pl.read_parquet(direction_path) if direction_path.exists() else None
    hs_df = apply_direction_to_rates(hs_df, dir_df)
    hs_df = hs_df.with_columns(pl.lit("hypersync").alias("source"))
    frames.append(hs_df)
```

- [ ] **Step 3.2: Handle `is_gap_filled` on the DataStore side.**

After the `pl.concat([...], how="diagonal_relaxed")` call, DataStore rows have NULL for `is_gap_filled` (they're synthesised hourly from RPC reads, not event-based — never gap-filled). Set NULL → False:

```python
unified = pl.concat(frames, how="diagonal_relaxed")
unified = unified.with_columns(
    pl.col("is_gap_filled").fill_null(False),
)
unified = unified.unique(subset=["timestamp"], keep="last")
unified = unified.sort("timestamp")
```

- [ ] **Step 3.3: Run full test suite — confirm no regressions.**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run python -m pytest tests/ -q \
  --ignore=tests/test_hybrid_collection.py \
  --ignore=tests/test_gmx_event_collector.py \
  --ignore=tests/test_integration_event_based.py \
  --ignore=tests/test_integration_gmx_first.py \
  --ignore=tests/test_live_funding.py \
  --ignore=tests/test_gmx_market_mapper.py \
  --ignore=tests/test_gmx_token_discovery.py 2>&1 | tail -5
```

Expected pass count: same as previous plan + 6 new gap-fill tests (236 passed).

### Task 4: Integration test (real-data smoke)

- [ ] **Step 4.1: Re-merge a single symbol with known gap (BTC: 138 h gap around 2025-08-25).**

Run with `--merge-only` to re-derive `1h.parquet` from existing `1h_factor.parquet` + `1h.parquet` (direction) without re-extracting:

```bash
poetry run python scripts/extract_unified_funding.py \
  --network arbitrum \
  --output-dir "/Volumes/WD Blue 1tb/VMs/data/gmx/funding" \
  --output parquet \
  --merge-only \
  --market "BTC/USD"
```

- [ ] **Step 4.2: Verify the gap is filled.**

```bash
poetry run python - <<'EOF'
import polars as pl
df = pl.read_parquet("/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates/BTC/1h.parquet")
print(f"rows: {df.height}, time range: {df['timestamp'].min()} → {df['timestamp'].max()}")
expected = int((df['timestamp'].max() - df['timestamp'].min()).total_seconds() / 3600) + 1
print(f"expected hours: {expected}; coverage: {df.height/expected*100:.1f}%")
print(f"is_gap_filled True count: {df['is_gap_filled'].sum()}")
diffs = df.sort('timestamp').with_columns(pl.col('timestamp').diff().dt.total_hours().alias('dt'))
print(f"max gap (hours): {diffs['dt'].max()}")
EOF
```

Expected: coverage ≈ 100 %, `max gap = 1.0 h`, `is_gap_filled` True count ≈ 138 + earlier gaps.

- [ ] **Step 4.3: Verify carried-forward rate is correct.**

Spot-check one filled row in the V2.2 rollout gap (Aug 25 → Sep 1):

```bash
poetry run python - <<'EOF'
import polars as pl
df = pl.read_parquet("/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates/BTC/1h.parquet")
window = df.filter(pl.col('timestamp').is_between(
    "2025-08-24 06:00:00", "2025-09-01 12:00:00", closed="both"
)).select(['timestamp', 'funding_rate', 'update_count', 'is_gap_filled']).sort('timestamp')
print(window.head(20))
print("...")
print(window.tail(20))
EOF
```

Expected: the row at the gap's start has `is_gap_filled=False, update_count>=1`; subsequent rows have `is_gap_filled=True, update_count=0` with identical `funding_rate`; the row at gap's end has `is_gap_filled=False` and a (possibly different) `funding_rate`.

### Task 5: Full re-merge across all markets + feather refresh

- [ ] **Step 5.1: Re-merge all symbols.**

```bash
poetry run python scripts/extract_unified_funding.py \
  --network arbitrum \
  --output-dir "/Volumes/WD Blue 1tb/VMs/data/gmx/funding" \
  --output parquet \
  --merge-only
```

- [ ] **Step 5.2: Re-export feather.**

```bash
make export-freqtrade OVERWRITE=--overwrite KEEP=--keep
```

- [ ] **Step 5.3: Re-run the coverage audit.**

```bash
poetry run python - <<'EOF'
import polars as pl
from pathlib import Path
from datetime import datetime, timezone

rates_dir = Path("/Volumes/WD Blue 1tb/VMs/data/gmx/funding/arbitrum/rates")
total = filled_count = 0
bad = []
for d in sorted(rates_dir.iterdir()):
    p = d / "1h.parquet"
    if not p.exists() or p.name.startswith("._"):
        continue
    df = pl.read_parquet(p, columns=["timestamp", "is_gap_filled"]).sort("timestamp")
    if df.height < 2:
        continue
    diffs = df.with_columns(pl.col("timestamp").diff().dt.total_hours().alias("dt"))
    max_gap = diffs["dt"].max() or 0
    total += df.height
    filled_count += df["is_gap_filled"].sum()
    if max_gap > 1:
        bad.append((d.name, max_gap, df.height))

print(f"Total markets: {len(list(rates_dir.iterdir()))}")
print(f"Total rows: {total:,}; filled: {filled_count:,} ({filled_count/total*100:.1f}%)")
print(f"Markets with any gap >1h after fill: {len(bad)}")
for m, g, n in bad[:10]:
    print(f"  {m}: max_gap={g}h, rows={n}")
EOF
```

Expected: `Markets with any gap >1h after fill: 0`. If any market still has gaps, that's a bug — investigate.

---

## Chunk 3: Commit

- [ ] **Step 6.1: Show the user the final diff and verification output.**

- [ ] **Step 6.2: Get explicit OK before any git commit.**

Suggested commit (single):

```
feat(funding): forward-fill silent hours in merged 1h.parquet

GMX emits Funding events only on position updates, so low-volume markets
had multi-day gaps in their hourly rate parquet. The on-chain rate
persists between events, so a silent hour means "rate unchanged" — not
"unknown."

This change builds a contiguous hourly grid in merge_symbol and forward-
fills rate columns from the most recent prior event. Filled rows are
flagged via a new is_gap_filled boolean and update_count = 0. Raw event
parquet and 1h_factor.parquet are unchanged (still event-only).

Grid spans only [first_event_hour, last_event_hour] — no backward-fill,
no zombie-fill past the last on-chain event.

Spec: docs/superpowers/specs/2026-05-11-funding-gap-fill-design.md
Plan: docs/superpowers/plans/2026-05-11-funding-gap-fill.md
```
