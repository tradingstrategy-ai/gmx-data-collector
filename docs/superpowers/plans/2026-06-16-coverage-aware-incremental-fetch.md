# Coverage-aware incremental fetch + `--force` rewrite — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `collect --update` fetch only the data we don't already have (append by default), and add `--force` to re-fetch from genesis and overwrite.

**Architecture:** Inspect stored coverage before each historical backfill; clip the fetch range to the missing slice (or skip if covered); default writes merge/append, `--force` writes overwrite and bypasses coverage + checkpoint skip. Touches the gap analyzer, boundary calculator, CLI collect path, and storage flag pass-through. No schema or merge-algorithm changes.

**Tech Stack:** Python 3.11, Typer CLI, pandas/polars, pytest, poetry. Spec: `docs/superpowers/specs/2026-06-16-coverage-aware-incremental-fetch-design.md`.

**Project rules (from CLAUDE.md):** imports at top of file; Sphinx docstrings; run tests with `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC` first; manual review before merge; no irreversible changes.

---

## File Structure

- `src/gmx_historical_data/gap_analyzer.py` — backfill-range helpers. **Change:** return a bounded start + an explicit "needed" signal; never imply genesis when data is covered.
- `src/gmx_historical_data/fetch_boundary_calculator.py` — translates gap results into `FetchBoundaries`. **Change:** use bounded start values; don't route symbols-with-data into `NO_EXISTING_DATA`/full.
- `src/gmx_historical_data/cli.py` — `collect_symbol` / `collect_all_symbols` / `cli()`. **Change:** thread a `force` bool → overwrite saves + genesis range + bypass checkpoint skip.
- `src/gmx_historical_data/storage.py` — already supports `overwrite`; **Change:** none beyond receiving the flag (verify).
- `tests/test_gap_analyzer_coverage.py` (new), `tests/test_fetch_boundary_incremental.py` (new), `tests/test_cli_force_overwrite.py` (new), plus edits to any existing affected tests.

---

## Chunk 1: Reproduce the waste + bound the backfill range

### Task 1: Reproduction test — already-current symbol must not backfill from genesis

**Files:**
- Test: `tests/test_gap_analyzer_coverage.py` (create)

- [ ] **Step 1: Write the failing test**

```python
from datetime import UTC, datetime
import pandas as pd
from gmx_historical_data.gap_analyzer import GapAnalyzer  # adjust to real class name


def _df(start: str, periods: int, freq: str = "1h") -> pd.DataFrame:
    idx = pd.date_range(start, periods=periods, freq=freq, tz=UTC)
    return pd.DataFrame({"timestamp": idx, "open": 1.0, "high": 1.0,
                         "low": 1.0, "close": 1.0, "volume": 0.0})


def test_incremental_gap_no_backfill_when_history_covered():
    """If our stored data starts at/ before GMX earliest, no Chainlink backfill."""
    analyzer = GapAnalyzer(chainlink_available=True)
    gmx = _df("2026-01-01", 24)            # GMX earliest = 2026-01-01
    existing = _df("2021-07-13", 100)      # we already have data back to 2021
    start, end = analyzer._calculate_incremental_gap(gmx, existing)
    assert (start, end) == (None, None)    # nothing to fetch
```

- [ ] **Step 2: Run test to verify behavior**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run pytest tests/test_gap_analyzer_coverage.py::test_incremental_gap_no_backfill_when_history_covered -v`
Expected: PASS (documents current correct behavior for the covered case) — confirms the harness/imports work.

- [ ] **Step 3: Commit the characterization test**

```bash
git add tests/test_gap_analyzer_coverage.py
git commit -m "test(gap): characterize incremental backfill skip when history covered"
```

### Task 2: Bound the backfill start (no genesis re-walk for partial coverage)

**Files:**
- Test: `tests/test_gap_analyzer_coverage.py`
- Modify: `src/gmx_historical_data/gap_analyzer.py:75-108` (`_calculate_incremental_gap`)

- [ ] **Step 1: Write the failing test**

```python
def test_incremental_gap_bounds_start_to_feed_floor_not_genesis():
    """When older data IS missing, start must be bounded (feed floor), not None.

    `None` means 'walk from genesis' to the RPC collector. When we already hold
    a contiguous block, the backfill must target only the missing older slice.
    """
    analyzer = GapAnalyzer(chainlink_available=True)
    gmx = _df("2026-01-01", 24)
    existing = _df("2025-06-01", 100)      # our earliest 2025-06-01 > gmx earliest
    start, end = analyzer._calculate_incremental_gap(gmx, existing)
    # end is just before our earliest; start must be a concrete floor, not None
    assert end == int(pd.Timestamp("2025-06-01", tz=UTC).timestamp()) - 1
    assert start is not None and start < end
```

- [ ] **Step 2: Run to verify it fails**

Run: `poetry run pytest tests/test_gap_analyzer_coverage.py::test_incremental_gap_bounds_start_to_feed_floor_not_genesis -v`
Expected: FAIL — current code returns `start = None`.

- [ ] **Step 3: Implement bounded start**

In `_calculate_incremental_gap`, replace `return None, our_earliest_unix - 1` with a bounded start derived from a feed floor (constructor-injected, default the GMX-v2 genesis timestamp). Add a `feed_floor_timestamp` parameter to `GapAnalyzer.__init__` (Sphinx-documented), defaulting to the existing genesis constant converted to unix seconds. Return `(feed_floor_timestamp, our_earliest_unix - 1)`.

```python
# __init__ (add param, imports at top of file):
def __init__(self, chainlink_available: bool,
             gmx_v2_genesis_block: int = 120_000_000,
             feed_floor_timestamp: int | None = None) -> None:
    ...
    self.feed_floor_timestamp = feed_floor_timestamp or DEFAULT_FEED_FLOOR_TS

# in _calculate_incremental_gap:
if our_earliest > gmx_earliest:
    our_earliest_unix = int(our_earliest.timestamp())
    return self.feed_floor_timestamp, our_earliest_unix - 1
return None, None
```

Define `DEFAULT_FEED_FLOOR_TS` at module top (e.g. GMX v2 launch, 2021-07-13 UTC).

- [ ] **Step 4: Run to verify pass**

Run: `poetry run pytest tests/test_gap_analyzer_coverage.py -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add tests/test_gap_analyzer_coverage.py src/gmx_historical_data/gap_analyzer.py
git commit -m "fix(gap): bound incremental backfill start to feed floor, not genesis"
```

### Task 3: `_calculate_full_boundaries` is the only genesis path; incremental never is

**Files:**
- Test: `tests/test_fetch_boundary_incremental.py` (create)
- Modify: `src/gmx_historical_data/fetch_boundary_calculator.py:231,248,260-264`

- [ ] **Step 1: Write the failing test** — a symbol WITH stored candles + a NORMAL/ DATA_LOSS gap must produce `chainlink_start_timestamp is not None` (bounded) and must NOT be routed to `_calculate_full_boundaries`.

```python
# Use a fake AdaptiveGapDetector returning NORMAL_GAP and a storage stub with data.
def test_incremental_normal_gap_uses_bounded_chainlink_start(monkeypatch):
    calc = make_calculator_with(gap_status="NORMAL_GAP", existing_has_data=True,
                                our_earliest="2025-06-01", gmx_earliest="2026-01-01")
    b = calc.calculate("ETH", "1h", chainlink_available=True, gmx_earliest=...)
    assert b.mode.name == "INCREMENTAL"
    assert b.chainlink_needed is True
    assert b.chainlink_start_timestamp is not None     # bounded, not genesis
```

- [ ] **Step 2: Run to verify it fails**

Run: `poetry run pytest tests/test_fetch_boundary_incremental.py -v`
Expected: FAIL — current code hardcodes `chainlink_start_timestamp=None`.

- [ ] **Step 3: Implement** — in `_calculate_incremental_boundaries`, set `chainlink_start_timestamp` (and `oracle_start_block` analogue) from the bounded gap-analyzer output rather than literal `None`. Ensure the `NO_EXISTING_DATA` branch only triggers when storage truly has no rows for the symbol (guard with an explicit `existing_df.empty` check before falling back to full).

- [ ] **Step 4: Run to verify pass**

Run: `poetry run pytest tests/test_fetch_boundary_incremental.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_fetch_boundary_incremental.py src/gmx_historical_data/fetch_boundary_calculator.py
git commit -m "fix(boundaries): incremental uses bounded chainlink/oracle start; no false full fallback"
```

---

## Chunk 2: `--force` = re-fetch from genesis + overwrite

### Task 4: Thread `force` into `collect_symbol` → overwrite saves

**Files:**
- Test: `tests/test_cli_force_overwrite.py` (create)
- Modify: `src/gmx_historical_data/cli.py` — `collect_symbol` signature + the save calls at `:541-544` and `:627-632`; pass `overwrite=force` to `storage.save_candles` / use `save_raw_events` when `force`.

- [ ] **Step 1: Write the failing test** — calling the save path with `force=True` calls `storage.save_candles(..., overwrite=True)`; with `force=False` calls `overwrite=False`. Use a mock storage to assert the kwarg.

```python
def test_force_passes_overwrite_true(mock_storage):
    collector = make_collector(storage=mock_storage)
    collector._merge_and_save_candles("ETH", "1h", df, merge_with_existing=False, force=True)
    assert mock_storage.save_candles.call_args.kwargs["overwrite"] is True

def test_default_passes_overwrite_false(mock_storage):
    collector = make_collector(storage=mock_storage)
    collector._merge_and_save_candles("ETH", "1h", df, merge_with_existing=True, force=False)
    assert mock_storage.save_candles.call_args.kwargs.get("overwrite", False) is False
```

- [ ] **Step 2: Run to verify it fails**

Run: `poetry run pytest tests/test_cli_force_overwrite.py -v`
Expected: FAIL — `_merge_and_save_candles` has no `force` param.

- [ ] **Step 3: Implement** — add `force: bool = False` to `_merge_and_save_candles`; when `force`, skip the `merge_with_existing` load and call `self.storage.save_candles(df, tf, symbol, overwrite=True)`. Thread `force` from `collect_symbol` into the call at `:627`. (Keep the `_assert_history_preserved` guard active on the non-force path; it is bypassed naturally by `overwrite=True`.)

- [ ] **Step 4: Run to verify pass** — `poetry run pytest tests/test_cli_force_overwrite.py -v` → PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/test_cli_force_overwrite.py src/gmx_historical_data/cli.py
git commit -m "feat(collect): --force overwrites stored candles instead of merging"
```

### Task 5: `--force` forces genesis range + bypasses checkpoint skip

**Files:**
- Modify: `src/gmx_historical_data/cli.py` — `cli()` (`:1481` force option already exists), `collect_all_symbols` (`:919` checkpoint-skip already honours `force`), and the boundary call so `force=True` → full/genesis boundaries.

- [ ] **Step 1: Write the failing test** — with `force=True`, the boundary used for a symbol-with-data is FULL mode (genesis), not INCREMENTAL.

```python
def test_force_uses_full_boundaries_even_with_existing_data():
    calc = make_calculator_with(existing_has_data=True)
    b = calc.calculate("ETH", "1h", chainlink_available=True, gmx_earliest=..., force=True)
    assert b.mode.name == "FULL"
    assert b.chainlink_start_timestamp is None   # genesis re-walk intended under --force
```

- [ ] **Step 2: Run to verify it fails** — FAIL (calculator has no `force` param).

- [ ] **Step 3: Implement** — add `force: bool = False` to the boundary calculator entry method; when `force`, short-circuit to `_calculate_full_boundaries`. Thread `force` from `cli()` → collector → boundary calc. Confirm `collect_all_symbols` already bypasses the per-symbol checkpoint skip when `force` (`:919`).

- [ ] **Step 4: Run to verify pass** — PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/ src/gmx_historical_data/cli.py src/gmx_historical_data/fetch_boundary_calculator.py
git commit -m "feat(collect): --force re-fetches from genesis (full boundaries)"
```

---

## Chunk 3: Oracle path coverage + end-to-end regression

### Task 6: Oracle backfill resumes from coverage, genesis only under `--force`

**Files:**
- Test: `tests/test_fetch_boundary_incremental.py`
- Modify: `src/gmx_historical_data/cli.py:710` (`start = start_block or GMX_V2_GENESIS_BLOCK`) and the oracle boundary fields.

- [ ] **Step 1: Write the failing test** — for a non-Chainlink symbol with stored data, the oracle `start_block` passed to `collect_oracle_events` corresponds to the stored coverage boundary (not `GMX_V2_GENESIS_BLOCK`) unless `force`.

- [ ] **Step 2: Run to verify it fails** — FAIL.

- [ ] **Step 3: Implement** — derive an oracle resume `start_block` from stored coverage (map latest-stored-timestamp → block, or persist last oracle block in the checkpoint). Use `GMX_V2_GENESIS_BLOCK` only when `force` or no stored data.

- [ ] **Step 4: Run to verify pass** — PASS.

- [ ] **Step 5: Commit**

```bash
git add tests/ src/gmx_historical_data/cli.py
git commit -m "fix(oracle): resume oracle backfill from stored coverage, genesis only on --force"
```

### Task 7: End-to-end regression — current symbol fetches ~0 rounds

**Files:**
- Test: `tests/test_cli_force_overwrite.py` (or a new integration test with mocked collectors)

- [ ] **Step 1: Write the failing test** — a symbol whose stored candles already reach "now": running incremental collect must call the RPC/oracle collector with a range that yields zero (assert `collect_historical_rounds` is either not called or called with `chainlink_needed=False`).

```python
def test_incremental_current_symbol_skips_backfill(mock_rpc):
    run_incremental_collect("ETH", stored_up_to="now")
    assert mock_rpc.collect_historical_rounds.call_count == 0
```

- [ ] **Step 2: Run to verify it fails / passes** — verify it now PASSES with the Chunk 1 fix; if not, fix the gate in `collect_symbol:493`.

- [ ] **Step 3: Full suite + lint**

Run: `export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC && poetry run pytest -q && poetry run ruff format --check . && poetry run ruff check .`
Expected: PASS / clean.

- [ ] **Step 4: Commit**

```bash
git add tests/
git commit -m "test(collect): regression — current symbol performs no backfill"
```

### Task 8: Docs + manual review

- [ ] Update `README.md` / `Makefile` help: document that `collect --update` is append-by-default and `--force` re-fetches+overwrites.
- [ ] Manual verification: `make collect-update SYMBOL=ETH` on a current dataset prints "Chainlink backfill not needed - data is complete" and fetches 0 rounds; `make collect-update SYMBOL=ETH ARGS=--force` re-walks + overwrites.
- [ ] **STOP for manual review before merge** (per CLAUDE.md). Do not merge or push without explicit approval.

---

## Notes / risks

- The exact steady-state waste path (bounded-backfill vs `NO_EXISTING_DATA` fallback vs `API_UNAVAILABLE`) is pinned by Task 1/7; if the regression test reveals the culprit is the `NO_EXISTING_DATA` fallback, Task 3's `existing_df.empty` guard is the primary fix.
- `--force` + `overwrite=True` intentionally bypasses `_assert_history_preserved`. Keep it strictly flag-gated.
- Oracle resume (Task 6) may require persisting the last oracle block in the checkpoint if timestamp→block mapping is unreliable; prefer storing `last_block` in the existing `Checkpoint` (field already present).
