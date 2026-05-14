# Coverage Gate for Daily Snapshot Collectors — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a shared coverage gate so `collect_daily_snapshot.py` skips fetching markets / OHLCV / tickers / APY when the on-disk data already covers the work, with a `--force-refresh` escape hatch and skips surfaced in `data_report.txt`.

**Architecture:** A new module `src/gmx_historical_data/coverage_gate.py` exposes `is_current()` (parquet metadata row-count check) and `has_ohlcv_through()` (feather max-date check). `scripts/collect_daily_snapshot.py` calls them inline before each fetch site. A `SkipDecision` dataclass carries the outcome; daily-stamped skips populate a `dict[str, SkipDecision]`; OHLCV gains a new `"SKIPPED"` status. `generate_report` renders a `## Skipped (already current)` section when any skips occurred.

**Tech Stack:** Python 3.11, polars, pyarrow, pandas, pytest. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-05-14-collectors-incremental-audit-design.md` (approved 2026-05-14).

**Branch:** Already on `spec/collectors-incremental-audit` (contains the spec). Implementation continues on the same branch.

---

## File Structure

```
src/gmx_historical_data/
└── coverage_gate.py                    [CREATE] SkipDecision + is_current + has_ohlcv_through

scripts/
└── collect_daily_snapshot.py           [MODIFY] gate calls + helpers + CLI flag + report

tests/
├── test_coverage_gate.py               [CREATE] unit tests for the two gate functions
├── test_daily_snapshot.py              [MODIFY] add _expected_last_bar + _row_count tests
└── test_daily_snapshot_gate.py         [CREATE] end-to-end skip behavior
```

Boundaries:
- `coverage_gate.py` knows nothing about the daily snapshot — pure file inspection. Easy to unit-test, easy to reuse for future collectors.
- `collect_daily_snapshot.py` owns the per-data-type policy (which `expected_min_rows`, which `_expected_last_bar`). The gate is the mechanism; this script is the policy.
- Tests split between gate (unit) and snapshot integration (end-to-end).

---

## Chunk 1: `coverage_gate` module

### Task 1: `SkipDecision` dataclass + `is_current` function

**Files:**
- Create: `src/gmx_historical_data/coverage_gate.py`
- Test: `tests/test_coverage_gate.py`

- [ ] **Step 1: Write failing tests for `SkipDecision` + `is_current`**

```python
# tests/test_coverage_gate.py
"""Tests for the shared coverage gate."""

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest


class TestSkipDecision:
    def test_dataclass_is_frozen(self):
        from gmx_historical_data.coverage_gate import SkipDecision

        d = SkipDecision(skip=True, reason="current", existing_rows=10, expected_min_rows=5)
        with pytest.raises(Exception):
            d.skip = False  # type: ignore[misc]


class TestIsCurrent:
    """is_current() — parquet metadata check for daily-stamped files."""

    def _write_parquet(self, path: Path, rows: int) -> None:
        pl.DataFrame({"x": list(range(rows))}).write_parquet(str(path))

    def test_missing_file(self, tmp_path):
        from gmx_historical_data.coverage_gate import is_current

        decision = is_current(tmp_path / "nope.parquet", expected_min_rows=10)
        assert decision.skip is False
        assert decision.reason == "missing"
        assert decision.existing_rows == 0
        assert decision.expected_min_rows == 10

    def test_too_small(self, tmp_path):
        from gmx_historical_data.coverage_gate import is_current

        p = tmp_path / "small.parquet"
        self._write_parquet(p, rows=5)
        decision = is_current(p, expected_min_rows=10)
        assert decision.skip is False
        assert decision.reason == "too_small"
        assert decision.existing_rows == 5

    def test_exactly_min(self, tmp_path):
        from gmx_historical_data.coverage_gate import is_current

        p = tmp_path / "exact.parquet"
        self._write_parquet(p, rows=10)
        decision = is_current(p, expected_min_rows=10)
        assert decision.skip is True
        assert decision.reason == "current"

    def test_current(self, tmp_path):
        from gmx_historical_data.coverage_gate import is_current

        p = tmp_path / "ok.parquet"
        self._write_parquet(p, rows=135)
        decision = is_current(p, expected_min_rows=100)
        assert decision.skip is True
        assert decision.reason == "current"
        assert decision.existing_rows == 135

    def test_forced(self, tmp_path):
        from gmx_historical_data.coverage_gate import is_current

        p = tmp_path / "ok.parquet"
        self._write_parquet(p, rows=135)
        decision = is_current(p, expected_min_rows=100, force=True)
        assert decision.skip is False
        assert decision.reason == "forced"
        assert decision.existing_rows == 135

    def test_corrupt_file(self, tmp_path, caplog):
        from gmx_historical_data.coverage_gate import is_current

        p = tmp_path / "corrupt.parquet"
        p.write_bytes(b"this is not parquet")
        decision = is_current(p, expected_min_rows=10)
        assert decision.skip is False
        assert decision.reason == "missing"
        assert decision.existing_rows == 0
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
poetry run pytest tests/test_coverage_gate.py -v
```

Expected: `ModuleNotFoundError: No module named 'gmx_historical_data.coverage_gate'`

- [ ] **Step 3: Implement `SkipDecision` + `is_current`**

```python
# src/gmx_historical_data/coverage_gate.py
"""Shared coverage gate for data collectors.

Each collector calls one of the two functions below before issuing a fetch.
If the on-disk file already covers the work, the collector can skip the
network call entirely.  See
``docs/superpowers/specs/2026-05-14-collectors-incremental-audit-design.md``
for the design.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SkipDecision:
    """Outcome of a coverage check at one collector site.

    :ivar skip: ``True`` when the collector should bypass its fetch.
    :ivar reason: One of ``"missing"``, ``"too_small"``, ``"stale"``,
        ``"forced"``, ``"current"``.
    :ivar existing_rows: Row count of the on-disk file (``0`` for both
        genuinely-missing and corrupt files).
    :ivar expected_min_rows: The threshold the caller asked to enforce.
    """

    skip: bool
    reason: str
    existing_rows: int
    expected_min_rows: int


def is_current(
    path: Path,
    expected_min_rows: int,
    *,
    force: bool = False,
    fmt: Literal["parquet", "feather"] = "parquet",
) -> SkipDecision:
    """Decide whether a daily-stamped parquet already covers today's work.

    Reads only the file footer (parquet metadata) — never opens row groups.

    :param path: Target output file.
    :param expected_min_rows: Floor — files with fewer rows are treated as
        incomplete and re-fetched.
    :param force: When ``True``, always returns ``skip=False, reason='forced'``.
    :param fmt: ``'parquet'`` (default) or ``'feather'``.  Only ``'parquet'``
        is currently used by daily-stamped data.
    """
    if force and path.exists():
        # Existing-but-forced: surface the row count for the report.
        try:
            existing = pq.read_metadata(str(path)).num_rows if fmt == "parquet" else 0
        except Exception:
            existing = 0
        return SkipDecision(False, "forced", existing, expected_min_rows)
    if force:
        return SkipDecision(False, "forced", 0, expected_min_rows)
    if not path.exists():
        return SkipDecision(False, "missing", 0, expected_min_rows)

    try:
        if fmt == "parquet":
            rows = pq.read_metadata(str(path)).num_rows
        else:
            # Feather metadata read — fall back to full read of the index col.
            import polars as pl
            rows = pl.read_ipc(str(path), columns=[]).height
    except Exception as exc:
        logger.warning("coverage_gate: failed to read %s (%s) — treating as missing", path, exc)
        return SkipDecision(False, "missing", 0, expected_min_rows)

    if rows < expected_min_rows:
        return SkipDecision(False, "too_small", rows, expected_min_rows)
    return SkipDecision(True, "current", rows, expected_min_rows)
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
poetry run pytest tests/test_coverage_gate.py -v -k "SkipDecision or IsCurrent"
```

Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/coverage_gate.py tests/test_coverage_gate.py
git commit -m "feat(coverage_gate): SkipDecision + is_current for daily-stamped files"
```

---

### Task 2: `has_ohlcv_through` function

**Files:**
- Modify: `src/gmx_historical_data/coverage_gate.py`
- Test: `tests/test_coverage_gate.py`

- [ ] **Step 1: Write failing tests**

```python
# Append to tests/test_coverage_gate.py
import pandas as pd
import pyarrow.feather as feather


class TestHasOhlcvThrough:
    """has_ohlcv_through() — feather max-date check for OHLCV per (symbol, tf)."""

    def _write_feather(self, path: Path, max_iso: str) -> None:
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2026-05-01", max_iso], utc=True).as_unit("ns"),
                "open": [1.0, 2.0],
                "high": [1.0, 2.0],
                "low": [1.0, 2.0],
                "close": [1.0, 2.0],
                "volume": [0.0, 0.0],
            }
        )
        feather.write_feather(df, path)

    def test_missing(self, tmp_path):
        from gmx_historical_data.coverage_gate import has_ohlcv_through

        d = has_ohlcv_through(
            tmp_path / "nope.feather",
            expected_max_date=pd.Timestamp("2026-05-14 16:00", tz="UTC"),
        )
        assert d.skip is False
        assert d.reason == "missing"

    def test_stale(self, tmp_path):
        from gmx_historical_data.coverage_gate import has_ohlcv_through

        p = tmp_path / "stale.feather"
        self._write_feather(p, max_iso="2026-05-14 12:00")
        d = has_ohlcv_through(
            p, expected_max_date=pd.Timestamp("2026-05-14 16:00", tz="UTC")
        )
        assert d.skip is False
        assert d.reason == "stale"

    def test_current(self, tmp_path):
        from gmx_historical_data.coverage_gate import has_ohlcv_through

        p = tmp_path / "ok.feather"
        self._write_feather(p, max_iso="2026-05-14 16:00")
        d = has_ohlcv_through(
            p, expected_max_date=pd.Timestamp("2026-05-14 16:00", tz="UTC")
        )
        assert d.skip is True
        assert d.reason == "current"

    def test_ahead(self, tmp_path):
        from gmx_historical_data.coverage_gate import has_ohlcv_through

        p = tmp_path / "ahead.feather"
        self._write_feather(p, max_iso="2026-05-14 18:00")
        d = has_ohlcv_through(
            p, expected_max_date=pd.Timestamp("2026-05-14 16:00", tz="UTC")
        )
        assert d.skip is True
        assert d.reason == "current"

    def test_forced(self, tmp_path):
        from gmx_historical_data.coverage_gate import has_ohlcv_through

        p = tmp_path / "ok.feather"
        self._write_feather(p, max_iso="2026-05-14 16:00")
        d = has_ohlcv_through(
            p,
            expected_max_date=pd.Timestamp("2026-05-14 16:00", tz="UTC"),
            force=True,
        )
        assert d.skip is False
        assert d.reason == "forced"

    def test_corrupt(self, tmp_path):
        from gmx_historical_data.coverage_gate import has_ohlcv_through

        p = tmp_path / "corrupt.feather"
        p.write_bytes(b"not a feather")
        d = has_ohlcv_through(
            p, expected_max_date=pd.Timestamp("2026-05-14 16:00", tz="UTC")
        )
        assert d.skip is False
        assert d.reason == "missing"
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
poetry run pytest tests/test_coverage_gate.py::TestHasOhlcvThrough -v
```

Expected: `ImportError: cannot import name 'has_ohlcv_through'`

- [ ] **Step 3: Implement `has_ohlcv_through`**

```python
# Append to src/gmx_historical_data/coverage_gate.py
def has_ohlcv_through(
    feather_path: Path,
    expected_max_date,  # pd.Timestamp, typed loosely to avoid module-level pandas import
    *,
    force: bool = False,
) -> SkipDecision:
    """Decide whether an OHLCV feather already extends through the latest bar.

    Reads only the ``date`` column.

    :param feather_path: Per-symbol feather (e.g.
        ``BTC_USDC_USDC-1h-futures.feather``).
    :param expected_max_date: Latest fully-closed bar for the timeframe
        (UTC ``pd.Timestamp``).
    :param force: When ``True``, always returns ``skip=False, reason='forced'``.
    """
    import polars as pl

    if force:
        return SkipDecision(False, "forced", 0, 0)
    if not feather_path.exists():
        return SkipDecision(False, "missing", 0, 0)

    try:
        df = pl.read_ipc(str(feather_path), columns=["date"])
    except Exception as exc:
        logger.warning(
            "coverage_gate: failed to read %s (%s) — treating as missing",
            feather_path,
            exc,
        )
        return SkipDecision(False, "missing", 0, 0)

    if df.height == 0:
        return SkipDecision(False, "missing", 0, 0)

    max_date = df["date"].max()
    # Normalise to tz-aware UTC for comparison.
    import pandas as pd

    max_ts = pd.Timestamp(max_date)
    if max_ts.tzinfo is None:
        max_ts = max_ts.tz_localize("UTC")
    else:
        max_ts = max_ts.tz_convert("UTC")
    expected = pd.Timestamp(expected_max_date)
    if expected.tzinfo is None:
        expected = expected.tz_localize("UTC")

    if max_ts < expected:
        return SkipDecision(False, "stale", df.height, 0)
    return SkipDecision(True, "current", df.height, 0)
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
poetry run pytest tests/test_coverage_gate.py -v
```

Expected: 12 passed (6 from Task 1 + 6 new).

- [ ] **Step 5: Commit**

```bash
git add src/gmx_historical_data/coverage_gate.py tests/test_coverage_gate.py
git commit -m "feat(coverage_gate): has_ohlcv_through for per-symbol feathers"
```

---

## Chunk 2: helpers in `collect_daily_snapshot.py`

### Task 3: `_expected_last_bar` helper

**Files:**
- Modify: `scripts/collect_daily_snapshot.py` (add helper near other private helpers, around line 70)
- Modify: `tests/test_daily_snapshot.py` (append test class)

- [ ] **Step 1: Write failing test**

```python
# Append to tests/test_daily_snapshot.py
import pandas as pd


class TestExpectedLastBar:
    """_expected_last_bar — derives latest fully-closed bar for a timeframe."""

    def test_today_1h(self):
        from scripts.collect_daily_snapshot import _expected_last_bar

        now = pd.Timestamp("2026-05-14 17:42", tz="UTC")
        got = _expected_last_bar("1h", target_date="2026-05-14", now=now)
        assert got == pd.Timestamp("2026-05-14 17:00", tz="UTC")

    def test_today_15m(self):
        from scripts.collect_daily_snapshot import _expected_last_bar

        now = pd.Timestamp("2026-05-14 17:42", tz="UTC")
        got = _expected_last_bar("15m", target_date="2026-05-14", now=now)
        assert got == pd.Timestamp("2026-05-14 17:30", tz="UTC")

    def test_today_1d(self):
        from scripts.collect_daily_snapshot import _expected_last_bar

        now = pd.Timestamp("2026-05-14 17:42", tz="UTC")
        got = _expected_last_bar("1d", target_date="2026-05-14", now=now)
        assert got == pd.Timestamp("2026-05-14", tz="UTC")

    def test_past_date_1h(self):
        """Backfill — clamps to end-of-day for the target date."""
        from scripts.collect_daily_snapshot import _expected_last_bar

        now = pd.Timestamp("2026-05-14 17:42", tz="UTC")
        got = _expected_last_bar("1h", target_date="2026-03-10", now=now)
        assert got == pd.Timestamp("2026-03-10 23:00", tz="UTC")

    def test_past_date_1d(self):
        from scripts.collect_daily_snapshot import _expected_last_bar

        now = pd.Timestamp("2026-05-14 17:42", tz="UTC")
        got = _expected_last_bar("1d", target_date="2026-03-10", now=now)
        assert got == pd.Timestamp("2026-03-10", tz="UTC")
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
poetry run pytest tests/test_daily_snapshot.py::TestExpectedLastBar -v
```

Expected: `ImportError: cannot import name '_expected_last_bar'`

- [ ] **Step 3: Implement `_expected_last_bar`**

Add near line 70 in `scripts/collect_daily_snapshot.py`, after `_merge_feather`:

```python
def _expected_last_bar(
    tf: str,
    target_date: str,
    *,
    now: pd.Timestamp | None = None,
) -> pd.Timestamp:
    """Latest fully-closed bar we expect on disk for ``target_date``.

    For *today's* date, returns the most recent closed bar of the timeframe
    (e.g. ``1h`` at 17:42 UTC → ``today 17:00``).  For *past* dates,
    returns the last bar of that calendar day (e.g. ``1h`` and
    ``--date 2026-03-10`` → ``2026-03-10 23:00``).

    :param tf: Timeframe (``1m``, ``5m``, ``15m``, ``1h``, ``4h``, ``1d``).
    :param target_date: ISO date string (``YYYY-MM-DD``).
    :param now: Override for the current UTC wall-clock; defaults to
        ``pd.Timestamp.now(tz='UTC')``.
    """
    target = pd.Timestamp(target_date, tz="UTC").normalize()
    now = now if now is not None else pd.Timestamp.now(tz="UTC")
    if target.date() < now.date():
        anchor = target + pd.Timedelta(hours=23, minutes=59)
    else:
        anchor = now
    floor_freq = {
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        "1h": "1h",
        "4h": "4h",
        "1d": "1D",
    }[tf]
    return anchor.floor(floor_freq)
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
poetry run pytest tests/test_daily_snapshot.py::TestExpectedLastBar -v
```

Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/collect_daily_snapshot.py tests/test_daily_snapshot.py
git commit -m "feat(daily_snapshot): _expected_last_bar helper for OHLCV gate"
```

---

### Task 4: `_row_count` helper

**Files:**
- Modify: `scripts/collect_daily_snapshot.py`
- Modify: `tests/test_daily_snapshot.py`

- [ ] **Step 1: Write failing test**

```python
# Append to tests/test_daily_snapshot.py
class TestRowCount:
    def test_existing_parquet(self, tmp_path):
        import polars as pl
        from scripts.collect_daily_snapshot import _row_count

        p = tmp_path / "x.parquet"
        pl.DataFrame({"a": [1, 2, 3, 4]}).write_parquet(str(p))
        assert _row_count(p) == 4

    def test_missing_returns_zero(self, tmp_path):
        from scripts.collect_daily_snapshot import _row_count

        assert _row_count(tmp_path / "nope.parquet") == 0

    def test_corrupt_returns_zero(self, tmp_path):
        from scripts.collect_daily_snapshot import _row_count

        p = tmp_path / "corrupt.parquet"
        p.write_bytes(b"junk")
        assert _row_count(p) == 0
```

- [ ] **Step 2: Run tests, verify they fail**

```bash
poetry run pytest tests/test_daily_snapshot.py::TestRowCount -v
```

Expected: `ImportError: cannot import name '_row_count'`

- [ ] **Step 3: Implement `_row_count`**

Add near other private helpers in `scripts/collect_daily_snapshot.py`:

```python
def _row_count(path: Path) -> int:
    """Return parquet row count via metadata; ``0`` if missing/corrupt.

    Used by the gate-skip code path to report "existing N rows" without
    reopening the file twice.
    """
    if not path.exists():
        return 0
    try:
        import pyarrow.parquet as pq

        return pq.read_metadata(str(path)).num_rows
    except Exception:
        return 0
```

- [ ] **Step 4: Run tests, verify they pass**

```bash
poetry run pytest tests/test_daily_snapshot.py::TestRowCount -v
```

Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/collect_daily_snapshot.py tests/test_daily_snapshot.py
git commit -m "feat(daily_snapshot): _row_count helper"
```

---

## Chunk 3: Wire the gate into each phase

### Task 5: Add `--force-refresh` CLI flag

**Files:**
- Modify: `scripts/collect_daily_snapshot.py` (argparse section in `main()`)

- [ ] **Step 1: Locate argparse section**

```bash
grep -n "add_argument" scripts/collect_daily_snapshot.py | head
```

The argparse setup lives inside `main()` around the `--quickstart-ref` argument.

- [ ] **Step 2: Add the flag**

After the `--quickstart-ref` block:

```python
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help=(
            "Ignore the coverage gate and re-fetch all data types even if "
            "the on-disk file is already current.  Default off; matches "
            "the conservative behaviour expected by the daily release "
            "workflow."
        ),
    )
```

- [ ] **Step 3: Verify it parses**

```bash
poetry run python scripts/collect_daily_snapshot.py --help 2>&1 | grep -A1 force-refresh
```

Expected: `--force-refresh` shows up in help output.

- [ ] **Step 4: Commit**

```bash
git add scripts/collect_daily_snapshot.py
git commit -m "feat(daily_snapshot): --force-refresh CLI flag"
```

---

### Task 6: Wire gate into Phase 1 (markets)

**Files:**
- Modify: `scripts/collect_daily_snapshot.py` (Phase 1 section in `main()`)

- [ ] **Step 1: Locate Phase 1**

```bash
grep -n "Phase 1\|collect_markets_snapshot" scripts/collect_daily_snapshot.py
```

- [ ] **Step 2: Refactor to gate the snapshot write**

Inside `main()`, replace the existing Phase 1 block:

```python
    # --- Phase 1: Markets snapshot (ALL markets: perp + swap-only + unlisted) ---
    console.print("\n[bold]Phase 1: Markets snapshot (OI, liquidity, rates)[/bold]")
    markets_path = snapshots_dir / f"{date_str}.parquet"
    markets_path.parent.mkdir(parents=True, exist_ok=True)

    from gmx_historical_data.coverage_gate import is_current

    skipped: dict[str, "SkipDecision"] = {}
    markets_decision = is_current(
        markets_path, expected_min_rows=100, force=args.force_refresh
    )
    if markets_decision.skip:
        skipped["markets"] = markets_decision
        markets_df = pd.read_parquet(markets_path)
        console.print(
            f"  [yellow]Skipped — existing {markets_decision.existing_rows} rows ≥ "
            f"{markets_decision.expected_min_rows} required[/yellow]"
        )
    else:
        markets_df = collect_markets_snapshot(all_markets, date_str)
        markets_df.to_parquet(markets_path, index=False)
        console.print(f"  Saved → {markets_path}")
    console.print()
```

Also add the import at the top of the module (alongside other `from gmx_historical_data...` imports), and import `SkipDecision` for the type hint:

```python
from gmx_historical_data.coverage_gate import SkipDecision, is_current
```

Then remove the duplicate inline `from gmx_historical_data.coverage_gate import is_current` inside `main()`.

- [ ] **Step 3: Smoke test — gate triggers when file exists**

```bash
mkdir -p /tmp/gate_smoke/data/gmx/snapshots
poetry run python -c "
import polars as pl
pl.DataFrame({'name': ['x'] * 150}).write_parquet('/tmp/gate_smoke/data/gmx/snapshots/2026-05-14.parquet')
"
poetry run python scripts/collect_daily_snapshot.py --output-dir /tmp/gate_smoke --date 2026-05-14 2>&1 | grep -i "phase 1\|skipped" | head
```

Expected output includes `Skipped — existing 150 rows ≥ 100 required`.

- [ ] **Step 4: Commit**

```bash
git add scripts/collect_daily_snapshot.py
git commit -m "feat(daily_snapshot): gate Phase 1 markets parquet write"
```

---

### Task 7: Wire gate into Phase 2 (OHLCV per symbol/tf)

**Files:**
- Modify: `scripts/collect_daily_snapshot.py` (`collect_and_save_ohlcv` signature + loop body)

- [ ] **Step 1: Locate `collect_and_save_ohlcv`**

```bash
grep -n "def collect_and_save_ohlcv" scripts/collect_daily_snapshot.py
```

- [ ] **Step 2: Add `force_refresh` param + gate logic in the loop**

Change the signature:

```python
def collect_and_save_ohlcv(
    api: GMXAPI,
    markets: list[dict],
    futures_dir: Path,
    timeframes: list[str] | None = None,
    *,
    force_refresh: bool = False,
    target_date: str | None = None,
) -> tuple[int, list[str], dict[tuple[str, str], dict]]:
```

Inside the per-`(symbol, tf)` loop, add the gate before the try/except that does the fetch:

```python
            filepath = futures_dir / f"{symbol}_USDC_USDC-{tf}-futures.feather"
            pre_stats = _feather_date_stats(filepath)

            # Coverage gate — skip fetch entirely if on-disk feather already
            # covers today's last expected bar.
            if target_date is not None:
                from gmx_historical_data.coverage_gate import has_ohlcv_through

                expected_last = _expected_last_bar(tf, target_date=target_date)
                gate = has_ohlcv_through(filepath, expected_last, force=force_refresh)
                if gate.skip:
                    coverage[(symbol, tf)] = {
                        "pre_merge": pre_stats,
                        "api_slice": None,
                        "post_merge": pre_stats,  # no write happened
                        "status": "SKIPPED",
                    }
                    continue

            entry: dict = {
                "pre_merge": pre_stats,
                "api_slice": None,
                "post_merge": None,
                "status": "FAILED",
            }
            coverage[(symbol, tf)] = entry
            # ... existing try/except fetch block unchanged ...
```

Update the call site in `main()`:

```python
    candle_count, failed_symbols, ohlcv_coverage = collect_and_save_ohlcv(
        api,
        all_markets,
        futures_dir,
        force_refresh=args.force_refresh,
        target_date=date_str,
    )
```

- [ ] **Step 3: Add unit test for the SKIPPED path**

Append to `tests/test_daily_snapshot.py`:

```python
class TestOhlcvGateSkip:
    """collect_and_save_ohlcv — feather-already-current → SKIPPED."""

    def test_skipped_status_when_feather_current(self, tmp_path, monkeypatch):
        """A feather whose max date >= expected_last → status='SKIPPED', no API call."""
        from scripts import collect_daily_snapshot as cds

        symbol = "ZZZ"
        # Build a feather whose latest bar is far in the future.
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(["2099-12-31"], utc=True).as_unit("ns"),
                "open": [1.0],
                "high": [1.0],
                "low": [1.0],
                "close": [1.0],
                "volume": [0.0],
            }
        )
        import pyarrow.feather as feather

        feather.write_feather(df, tmp_path / f"{symbol}_USDC_USDC-1d-futures.feather")

        class FailingAPI:
            def get_candlesticks_dataframe(self, *args, **kwargs):
                raise AssertionError("API must not be called when gate fires")

        markets = [{"name": f"{symbol}/USD", "isListed": True}]
        saved, failed, coverage = cds.collect_and_save_ohlcv(
            FailingAPI(),
            markets,
            tmp_path,
            timeframes=["1d"],
            target_date="2026-05-14",
        )
        assert coverage[(symbol, "1d")]["status"] == "SKIPPED"
        assert coverage[(symbol, "1d")]["api_slice"] is None
        # Saved counts fetches, not skips:
        assert saved == 0
        assert failed == []
```

- [ ] **Step 4: Run the new test**

```bash
poetry run pytest tests/test_daily_snapshot.py::TestOhlcvGateSkip -v
```

Expected: 1 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/collect_daily_snapshot.py tests/test_daily_snapshot.py
git commit -m "feat(daily_snapshot): gate OHLCV per (symbol, tf), new SKIPPED status"
```

---

### Task 8: Wire gate into Phase 4 (tickers)

**Files:**
- Modify: `scripts/collect_daily_snapshot.py` (Phase 4 in `main()`)

- [ ] **Step 1: Replace the Phase 4 block**

```python
    # --- Phase 4: Tickers (bid/ask prices) ---
    console.print("[bold]Phase 4: Tickers (bid/ask prices)[/bold]")
    ticker_path = tickers_dir / f"{date_str}.parquet"
    ticker_decision = is_current(
        ticker_path, expected_min_rows=100, force=args.force_refresh
    )
    if ticker_decision.skip:
        skipped["tickers"] = ticker_decision
        ticker_count = _row_count(ticker_path)
        console.print(
            f"  [yellow]Skipped — existing {ticker_decision.existing_rows} rows ≥ "
            f"{ticker_decision.expected_min_rows} required[/yellow]"
        )
    else:
        ticker_count = collect_and_save_tickers(api, date_str, tickers_dir)
    console.print()
```

- [ ] **Step 2: Smoke test**

```bash
mkdir -p /tmp/gate_smoke/data/gmx/tickers
poetry run python -c "
import polars as pl
pl.DataFrame({'token_symbol': ['x'] * 130}).write_parquet('/tmp/gate_smoke/data/gmx/tickers/2026-05-14.parquet')
"
# (Phase 1 file already from Task 6 smoke test)
poetry run python scripts/collect_daily_snapshot.py --output-dir /tmp/gate_smoke --date 2026-05-14 2>&1 | grep -i "phase 4\|skipped"
```

Expected: tickers shows skipped.

- [ ] **Step 3: Commit**

```bash
git add scripts/collect_daily_snapshot.py
git commit -m "feat(daily_snapshot): gate Phase 4 tickers"
```

---

### Task 9: Wire gate into Phase 5 (APY)

**Files:**
- Modify: `scripts/collect_daily_snapshot.py` (Phase 5 in `main()`)

- [ ] **Step 1: Replace the Phase 5 block**

```python
    # --- Phase 5: APY (all periods) ---
    console.print("[bold]Phase 5: APY (yield data)[/bold]")
    apy_path = apy_dir / f"{date_str}.parquet"
    apy_decision = is_current(
        apy_path, expected_min_rows=7 * 100, force=args.force_refresh
    )
    if apy_decision.skip:
        skipped["apy"] = apy_decision
        apy_count = _row_count(apy_path)
        console.print(
            f"  [yellow]Skipped — existing {apy_decision.existing_rows} rows ≥ "
            f"{apy_decision.expected_min_rows} required[/yellow]"
        )
    else:
        apy_count = collect_and_save_apy(api, date_str, apy_dir)
    console.print()
```

- [ ] **Step 2: Smoke test**

```bash
mkdir -p /tmp/gate_smoke/data/gmx/apy
poetry run python -c "
import polars as pl
pl.DataFrame({'date': ['2026-05-14'] * 945, 'period': ['1d'] * 945, 'apy': [0.0] * 945}).write_parquet('/tmp/gate_smoke/data/gmx/apy/2026-05-14.parquet')
"
poetry run python scripts/collect_daily_snapshot.py --output-dir /tmp/gate_smoke --date 2026-05-14 2>&1 | grep -i "phase 5\|skipped"
```

Expected: APY shows skipped.

- [ ] **Step 3: Commit**

```bash
git add scripts/collect_daily_snapshot.py
git commit -m "feat(daily_snapshot): gate Phase 5 APY"
```

---

## Chunk 4: Report integration

### Task 10: `generate_report` skipped section + thread `skipped` from `main()`

**Files:**
- Modify: `scripts/collect_daily_snapshot.py` (`generate_report` signature + body, `main()` call site)

- [ ] **Step 1: Extend `generate_report` signature**

Add the new param appended **after** `ohlcv_coverage`:

```python
def generate_report(
    date_str: str,
    markets_df: pd.DataFrame,
    candle_count: int,
    failed_symbols: list[str],
    ticker_count: int,
    apy_count: int,
    volume_count: int,
    volume_data: dict[str, "Decimal"],
    futures_dir: Path,
    snapshots_dir: Path,
    tickers_dir: Path,
    apy_dir: Path,
    volumes_dir: Path,
    report_path: Path,
    ohlcv_coverage: dict[tuple[str, str], dict] | None = None,
    skipped: dict[str, "SkipDecision"] | None = None,
) -> None:
```

- [ ] **Step 2: Add the `## Skipped` section in `generate_report`**

Insert after `## Collection Summary`, before `## Date Range Summary`. Only emit when at least one skip occurred (daily-stamped OR OHLCV):

```python
    skip_lines: list[str] = []
    if skipped:
        labels = {"markets": "Markets snapshots", "tickers": "Tickers", "apy": "APY", "volumes": "Volumes"}
        for key in ("markets", "tickers", "apy", "volumes"):
            d = skipped.get(key)
            if d is None:
                continue
            skip_lines.append(
                f"- {labels[key]}: existing {d.existing_rows} rows ≥ "
                f"{d.expected_min_rows} required (reason: {d.reason})"
            )
    if ohlcv_coverage:
        per_tf_skips: dict[str, int] = {}
        per_tf_total: dict[str, int] = {}
        for (sym, tf), entry in ohlcv_coverage.items():
            per_tf_total[tf] = per_tf_total.get(tf, 0) + 1
            if entry.get("status") == "SKIPPED":
                per_tf_skips[tf] = per_tf_skips.get(tf, 0) + 1
        for tf in TIMEFRAMES:
            if per_tf_skips.get(tf):
                skip_lines.append(
                    f"- OHLCV {tf}: {per_tf_skips[tf]}/{per_tf_total.get(tf, 0)} "
                    "symbols skipped (existing max ≥ expected last bar)"
                )
    if skip_lines:
        lines.append("")
        lines.append("## Skipped (already current)")
        lines.extend(skip_lines)
```

- [ ] **Step 3: Update `main()` to pass `skipped`**

```python
    generate_report(
        date_str=date_str,
        ...
        ohlcv_coverage=ohlcv_coverage,
        skipped=skipped,
    )
```

- [ ] **Step 4: Add unit test for the section**

Append to `tests/test_daily_snapshot.py`:

```python
class TestReportSkippedSection:
    def test_section_present_when_skips_recorded(self, tmp_path):
        from scripts.collect_daily_snapshot import generate_report
        from gmx_historical_data.coverage_gate import SkipDecision

        markets_df = pd.DataFrame(
            {
                "name": ["BTC/USD"],
                "is_swap_only": [False],
                "is_listed": [True],
                "open_interest_long": ["0"],
                "open_interest_short": ["0"],
                "market_token": ["0x0"],
            }
        )
        out = tmp_path / "report.txt"
        skipped = {
            "markets": SkipDecision(True, "current", 135, 100),
            "apy": SkipDecision(True, "current", 945, 700),
        }
        generate_report(
            date_str="2026-05-14",
            markets_df=markets_df,
            candle_count=0,
            failed_symbols=[],
            ticker_count=0,
            apy_count=0,
            volume_count=0,
            volume_data={},
            futures_dir=tmp_path,
            snapshots_dir=tmp_path,
            tickers_dir=tmp_path,
            apy_dir=tmp_path,
            volumes_dir=tmp_path,
            report_path=out,
            ohlcv_coverage=None,
            skipped=skipped,
        )
        content = out.read_text()
        assert "## Skipped (already current)" in content
        assert "Markets snapshots: existing 135 rows ≥ 100" in content
        assert "APY: existing 945 rows ≥ 700" in content

    def test_section_absent_when_no_skips(self, tmp_path):
        from scripts.collect_daily_snapshot import generate_report

        markets_df = pd.DataFrame(
            {
                "name": ["BTC/USD"],
                "is_swap_only": [False],
                "is_listed": [True],
                "open_interest_long": ["0"],
                "open_interest_short": ["0"],
                "market_token": ["0x0"],
            }
        )
        out = tmp_path / "report.txt"
        generate_report(
            date_str="2026-05-14",
            markets_df=markets_df,
            candle_count=0,
            failed_symbols=[],
            ticker_count=0,
            apy_count=0,
            volume_count=0,
            volume_data={},
            futures_dir=tmp_path,
            snapshots_dir=tmp_path,
            tickers_dir=tmp_path,
            apy_dir=tmp_path,
            volumes_dir=tmp_path,
            report_path=out,
            ohlcv_coverage=None,
            skipped=None,
        )
        assert "## Skipped (already current)" not in out.read_text()
```

- [ ] **Step 5: Run tests, verify they pass**

```bash
poetry run pytest tests/test_daily_snapshot.py::TestReportSkippedSection -v
```

Expected: 2 passed.

- [ ] **Step 6: Commit**

```bash
git add scripts/collect_daily_snapshot.py tests/test_daily_snapshot.py
git commit -m "feat(report): ## Skipped (already current) section in data_report.txt"
```

---

## Chunk 5: Integration test + verification

### Task 11: End-to-end integration test

**Files:**
- Create: `tests/test_daily_snapshot_gate.py`

- [ ] **Step 1: Write the test**

```python
"""End-to-end test: gate prevents API calls when daily files exist."""

from pathlib import Path
from unittest.mock import patch

import polars as pl
import pyarrow.feather as feather
import pandas as pd
import pytest


@pytest.fixture
def seeded_dir(tmp_path):
    """Seed all daily-stamped files for 2026-05-14 so the gate skips them."""
    gmx = tmp_path / "data" / "gmx"
    for sub, rows in (
        ("snapshots", 135),
        ("tickers", 126),
        ("apy", 945),
    ):
        d = gmx / sub
        d.mkdir(parents=True, exist_ok=True)
        cols = {"col": list(range(rows))}
        # snapshots needs columns referenced by generate_report
        if sub == "snapshots":
            cols = {
                "name": ["BTC/USD"] * rows,
                "is_swap_only": [False] * rows,
                "is_listed": [True] * rows,
                "market_token": ["0x0"] * rows,
                "open_interest_long": ["0"] * rows,
                "open_interest_short": ["0"] * rows,
                "index_token": ["0x0"] * rows,
                "long_token": ["0x0"] * rows,
                "short_token": ["0x0"] * rows,
                "listing_date": [""] * rows,
                "pool_amount_long": ["0"] * rows,
                "pool_amount_short": ["0"] * rows,
                "available_liquidity_long": ["0"] * rows,
                "available_liquidity_short": ["0"] * rows,
                "funding_rate_long": ["0"] * rows,
                "funding_rate_short": ["0"] * rows,
                "borrowing_rate_long": ["0"] * rows,
                "borrowing_rate_short": ["0"] * rows,
                "net_rate_long": ["0"] * rows,
                "net_rate_short": ["0"] * rows,
                "symbol": ["BTC"] * rows,
                "date": ["2026-05-14"] * rows,
            }
        pl.DataFrame(cols).write_parquet(str(d / "2026-05-14.parquet"))

    # Seed one OHLCV feather way in the future so it skips too
    fut = gmx / "futures"
    fut.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        {
            "date": pd.to_datetime(["2099-12-31"], utc=True).as_unit("ns"),
            "open": [1.0],
            "high": [1.0],
            "low": [1.0],
            "close": [1.0],
            "volume": [0.0],
        }
    )
    feather.write_feather(df, fut / "BTC_USDC_USDC-1d-futures.feather")
    return tmp_path


def test_gate_skips_tickers_and_apy_api(seeded_dir, capsys):
    """Verify get_tickers() and get_apy() are not called when files are current."""
    from scripts import collect_daily_snapshot as cds

    raise_called = lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("API must not be called when gate fires")
    )

    # Stub the API class so any candlestick call fails (it shouldn't be called for BTC/1d).
    class StubAPI:
        def __init__(self, chain): pass
        def get_markets_info(self):
            # Markets API is NOT gated — has to return something realistic.
            return {"markets": [{"name": "BTC/USD", "isListed": True}]}
        def get_tickers(self, use_cache=False): raise_called()
        def get_apy(self, period, use_cache=False): raise_called()
        def get_candlesticks_dataframe(self, *a, **k): raise_called()

    with patch("scripts.collect_daily_snapshot.GMXAPI", StubAPI):
        with patch.object(
            __import__("sys"), "argv",
            ["collect_daily_snapshot.py", "--output-dir", str(seeded_dir / "data" / ".."), "--date", "2026-05-14"],
        ):
            # The argparse + main() flow; if the gate fails to fire,
            # raise_called() bubbles up and fails the test.
            cds.main()

    # Confirm the report has the skipped section
    report = (seeded_dir / "data_report.txt").read_text()
    assert "## Skipped (already current)" in report
    assert "Markets snapshots:" in report
    assert "Tickers:" in report
    assert "APY:" in report
```

- [ ] **Step 2: Run the test**

```bash
poetry run pytest tests/test_daily_snapshot_gate.py -v
```

Expected: 1 passed. If a phase isn't gated and the stub raises, this surfaces immediately.

- [ ] **Step 3: Commit**

```bash
git add tests/test_daily_snapshot_gate.py
git commit -m "test(daily_snapshot): end-to-end gate skip integration test"
```

---

### Task 12: Live smoke run + report inspection

**Files:** None — manual verification.

- [ ] **Step 1: Run twice against a temp dir**

```bash
rm -rf /tmp/gate_live_test && mkdir -p /tmp/gate_live_test
# First run: cold path, populates files
poetry run python scripts/collect_daily_snapshot.py --output-dir /tmp/gate_live_test --date 2026-05-14 2>&1 | tail -20
# Second run: should skip
time poetry run python scripts/collect_daily_snapshot.py --output-dir /tmp/gate_live_test --date 2026-05-14 2>&1 | tail -40
```

- [ ] **Step 2: Verify second run is fast and report shows skips**

Expected on second run:
- Wall-clock time: under 30 seconds (vs ~3 minutes for cold)
- stdout shows `Skipped — existing N rows ≥ M required` for Phase 1/4/5
- `cat /tmp/data_report.txt | grep -A20 "## Skipped"` shows all three daily-stamped types plus per-tf OHLCV counts

- [ ] **Step 3: Verify `--force-refresh` overrides**

```bash
poetry run python scripts/collect_daily_snapshot.py --output-dir /tmp/gate_live_test --date 2026-05-14 --force-refresh 2>&1 | grep -i "skipped" || echo "No skips — force-refresh works"
```

Expected: `No skips — force-refresh works`.

- [ ] **Step 4: No commit (verification only)**

---

### Task 13: Push branch + open PR

**Files:** None.

- [ ] **Step 1: Run full test suite**

```bash
poetry run pytest tests/test_coverage_gate.py tests/test_daily_snapshot.py tests/test_daily_snapshot_gate.py -v
```

Expected: all pass.

- [ ] **Step 2: Push branch**

```bash
git push -u origin spec/collectors-incremental-audit
```

- [ ] **Step 3: Open PR**

```bash
gh pr create \
  --base master \
  --head spec/collectors-incremental-audit \
  --title "feat(daily_snapshot): coverage gate — skip fetches when data already current" \
  --body-file <(cat <<'BODY'
## Summary

Adds a shared coverage gate so the daily snapshot skips fetching markets / OHLCV / tickers / APY when the on-disk files already cover the work.

- New module: `src/gmx_historical_data/coverage_gate.py` (`SkipDecision`, `is_current`, `has_ohlcv_through`)
- `scripts/collect_daily_snapshot.py` calls the gate before each fetch site
- `--force-refresh` CLI flag bypasses the gate
- `## Skipped (already current)` section added to `data_report.txt`
- New OHLCV coverage status: `SKIPPED`

## Test plan
- [x] Unit tests for `is_current` (6 cases) + `has_ohlcv_through` (6 cases)
- [x] Helper tests: `_expected_last_bar`, `_row_count`
- [x] OHLCV gate test: API stub asserts no candle call when feather is current
- [x] Integration test: pre-seeded dir → all phases skip, report shows `## Skipped`
- [x] Live smoke run: same-day rerun ~30s vs ~3min cold

## Spec
`docs/superpowers/specs/2026-05-14-collectors-incremental-audit-design.md`
BODY
)
```

- [ ] **Step 4: Wait for CI green, then merge**

```bash
gh pr checks $(gh pr view --json number --jq .number)
gh pr merge --squash --delete-branch
```

---

## Plan complete

Plan saved to `docs/superpowers/plans/2026-05-14-collectors-incremental-audit.md`.
