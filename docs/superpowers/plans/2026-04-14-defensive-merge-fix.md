# Defensive Merge Fix Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent `save_candles()` from silently overwriting historical parquet data when a partial (API-page-limited) dataset is written, and surgically fix the one confirmed call site that triggers the overwrite.

**Architecture:** Two-layer fix. Layer 1 (surgical): add `merge_with_existing=True` at `cli.py:1342` in `collect_non_chainlink_markets` — the confirmed root cause. Layer 2 (defensive): make `storage.save_candles()` merge-by-default with an explicit `overwrite: bool = False` escape hatch, so no future caller can accidentally destroy history. Callers that already pre-merge continue to work correctly (idempotent double-merge is safe).

**Tech Stack:** Python 3.11, Polars 1.x, pandas 2.x, pytest, poetry

---

## Investigation Summary (confirmed before plan)

- **Root cause:** `cli.py:1342` calls `_merge_and_save_candles(symbol, tf, oracle_df, gmx_df)` **without** `merge_with_existing=True`. The helper only merges the frames it receives — it never reads existing storage — so `gmx_df` (10 000-row GMX API page) is the only input, and `storage.save_candles()` overwrites the parquet unconditionally.
- **Trigger:** `make collect-full` / `make full-data` / CI `collect-full-history.yml` → runs `collect_non_chainlink_markets()`.
- **Why BTC/ETH/LINK/AVAX/SOL escaped:** Chainlink-fed symbols go through `collect_symbol()` → `_merge_and_save_candles(..., merge_with_existing=is_incremental)` and a Chainlink RPC backfill that supplies full history. They never hit `collect_non_chainlink_markets()`.
- **Why 1d survived:** 10 000 API rows ≥ 1 735 days of 1d candles → single page covers all history → overwrite is lossless. 1m/5m/1h lose history because `10 000 rows < full history`.
- **Other latent sites:** daemon `periodic_collector.py:291,415,562` pre-merge before calling `save_candles` (safe today, but fragile under concurrent runs). `storage.save_candles()` itself is the root enabler — no built-in safety net.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| Modify | `src/gmx_historical_data/storage.py:190-227` | Add merge-by-default logic to `save_candles()`, add `overwrite` param |
| Modify | `src/gmx_historical_data/cli.py:1342-1347` | Add `merge_with_existing=True` to the one unsafe call site |
| Modify | `tests/test_storage_list.py` | Add tests for merge-by-default behaviour |

No new files. Both production changes are backward-compatible — existing callers that already pre-merge produce an identical (idempotent) result.

---

## Chunk 1: Storage layer — defensive merge-by-default

### Task 1: Write failing tests for merge-by-default in `save_candles`

**Files:**
- Modify: `tests/test_storage_list.py`

- [ ] **Step 1: Add the tests at the bottom of `tests/test_storage_list.py`**

```python
def test_save_candles_merges_existing_by_default():
    """save_candles must preserve history already on disk (merge-by-default)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        historic = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2022-01-01", "2022-01-02", "2022-01-03"], utc=True
                ),
                "open": [100.0, 101.0, 102.0],
                "high": [105.0, 106.0, 107.0],
                "low": [99.0, 100.0, 101.0],
                "close": [104.0, 105.0, 106.0],
                "symbol": ["BTC", "BTC", "BTC"],
            }
        )
        storage.save_candles(historic, "1d", "BTC")

        # Second save: only the latest day (simulates a 10 000-row API page
        # that does not cover full history).
        recent = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-03", "2022-01-04"], utc=True),
                "open": [102.0, 103.0],
                "high": [107.0, 108.0],
                "low": [101.0, 102.0],
                "close": [106.0, 107.0],
                "symbol": ["BTC", "BTC"],
            }
        )
        storage.save_candles(recent, "1d", "BTC")

        result = storage.read_candles("1d", "BTC")
        assert len(result) == 4, (
            f"Expected 4 rows (full history merged), got {len(result)}. "
            "save_candles is overwriting instead of merging."
        )
        assert result["timestamp"].min() == pd.Timestamp("2022-01-01", tz="UTC")
        assert result["timestamp"].max() == pd.Timestamp("2022-01-04", tz="UTC")


def test_save_candles_overwrite_flag_replaces_data():
    """save_candles(overwrite=True) must discard existing data."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        historic = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(
                    ["2022-01-01", "2022-01-02", "2022-01-03"], utc=True
                ),
                "open": [100.0, 101.0, 102.0],
                "high": [105.0, 106.0, 107.0],
                "low": [99.0, 100.0, 101.0],
                "close": [104.0, 105.0, 106.0],
                "symbol": ["BTC", "BTC", "BTC"],
            }
        )
        storage.save_candles(historic, "1d", "BTC")

        recent = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-03", "2022-01-04"], utc=True),
                "open": [102.0, 103.0],
                "high": [107.0, 108.0],
                "low": [101.0, 102.0],
                "close": [106.0, 107.0],
                "symbol": ["BTC", "BTC"],
            }
        )
        storage.save_candles(recent, "1d", "BTC", overwrite=True)

        result = storage.read_candles("1d", "BTC")
        assert len(result) == 2, "overwrite=True should replace, not merge."
        assert result["timestamp"].min() == pd.Timestamp("2022-01-03", tz="UTC")


def test_save_candles_deduplicates_overlapping_timestamps():
    """Overlapping timestamps keep the latest-written value (keep='last')."""
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = ParquetStorage(Path(tmpdir))

        first = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-01"], utc=True),
                "open": [100.0],
                "high": [105.0],
                "low": [99.0],
                "close": [104.0],
                "symbol": ["ETH"],
            }
        )
        storage.save_candles(first, "1h", "ETH")

        updated = pd.DataFrame(
            {
                "timestamp": pd.to_datetime(["2022-01-01"], utc=True),
                "open": [200.0],  # corrected value
                "high": [210.0],
                "low": [190.0],
                "close": [205.0],
                "symbol": ["ETH"],
            }
        )
        storage.save_candles(updated, "1h", "ETH")

        result = storage.read_candles("1h", "ETH")
        assert len(result) == 1
        assert result.iloc[0]["open"] == 200.0, "Newer write should win on duplicate timestamp."
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run pytest tests/test_storage_list.py::test_save_candles_merges_existing_by_default \
    tests/test_storage_list.py::test_save_candles_overwrite_flag_replaces_data \
    tests/test_storage_list.py::test_save_candles_deduplicates_overlapping_timestamps -v
```

Expected: 3 × FAIL — first two fail because current `save_candles` overwrites; third may pass or fail.

---

### Task 2: Implement merge-by-default in `storage.save_candles()`

**Files:**
- Modify: `src/gmx_historical_data/storage.py:190-227`

- [ ] **Step 3: Replace the `save_candles` method body**

Current signature (line 190):
```python
def save_candles(
    self,
    df: pd.DataFrame,
    timeframe: str,
    symbol: str,
) -> Path:
```

New signature + body — replace lines 190–227 with:

```python
def save_candles(
    self,
    df: pd.DataFrame,
    timeframe: str,
    symbol: str,
    overwrite: bool = False,
) -> Path:
    """Save OHLCV candles to Parquet file.

    By default existing rows are preserved (merge-by-default). Overlapping
    timestamps resolve to the newer value (``keep='last'`` after
    ``[existing, new]`` concat). Pass ``overwrite=True`` to replace the file
    entirely — use only when you intend to discard history.

    :param df: DataFrame with OHLCV data.
    :param timeframe: Timeframe string (e.g., ``'1h'``).
    :param symbol: Token symbol (e.g., ``'ETH'``).
    :param overwrite: If ``True``, replace existing file instead of merging.
    :return: Path to saved Parquet file.
    """
    if df.empty:
        raise ValueError("Cannot save empty DataFrame")

    required_columns = ["timestamp", "open", "high", "low", "close", "symbol"]
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    symbol_dir = self._ensure_dir(self.candles_dir / symbol)
    filename = TIMEFRAME_TO_FILENAME.get(timeframe, timeframe)
    output_path = symbol_dir / f"{filename}.parquet"

    incoming = pl.from_pandas(df)

    if not overwrite and output_path.exists():
        try:
            existing = pl.read_parquet(output_path)
            incoming = (
                pl.concat([existing, incoming])
                .unique(subset=["timestamp"], keep="last", maintain_order=True)
                .sort("timestamp")
            )
        except Exception as exc:
            logger.error(
                "save_candles: merge FAILED for %s/%s (%s): %s — "
                "writing new data only; existing history may be lost",
                symbol,
                filename,
                output_path,
                exc,
                exc_info=True,
            )

    table = pa.Table.from_pandas(incoming.to_pandas(), schema=OHLCV_SCHEMA)
    pl.from_arrow(table).write_parquet(str(output_path), compression="zstd", compression_level=3)

    return output_path
```

> **Note:** `logger` is already imported in storage.py (check top of file; if absent add `import logging` + `logger = logging.getLogger(__name__)` near the top).

- [ ] **Step 4: Run the new tests**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run pytest tests/test_storage_list.py -v
```

Expected: all tests PASS including the 3 new ones.

- [ ] **Step 5: Run full test suite to check regressions**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run pytest tests/ -v --tb=short 2>&1 | tail -40
```

Expected: no new failures.

---

## Chunk 2: Surgical fix — `cli.py:1342`

### Task 3: Add `merge_with_existing=True` at the confirmed bad call site

**Files:**
- Modify: `src/gmx_historical_data/cli.py:1342-1347`

- [ ] **Step 1: Apply the one-line fix**

Current (lines 1342–1347):
```python
count = self._merge_and_save_candles(
    symbol,
    timeframe,
    oracle_df,
    gmx_df,
)
```

Replace with:
```python
count = self._merge_and_save_candles(
    symbol,
    timeframe,
    oracle_df,
    gmx_df,
    merge_with_existing=True,
)
```

- [ ] **Step 2: Verify no other unsafe direct `save_candles` calls remain in cli.py**

```bash
grep -n "save_candles" src/gmx_historical_data/cli.py
```

Expected output includes only line 330 (inside `_merge_and_save_candles` which already merges correctly) and no direct calls from `collect_non_chainlink_markets` or event-mode path at line 862 (event-mode path is a separate issue; confirm it still reads as a raw call — note for follow-up).

- [ ] **Step 3: Run tests**

```bash
export JSON_RPC_ARBITRUM=$ARBITRUM_CHAIN_JSON_RPC
poetry run pytest tests/ -v --tb=short 2>&1 | tail -40
```

Expected: all tests PASS.

- [ ] **Step 4: Ask for manual review**

**Pause here.** Surface for human review before any git operations:
- `git diff src/gmx_historical_data/storage.py`
- `git diff src/gmx_historical_data/cli.py`
- `git diff tests/test_storage_list.py`

Do **not** commit until the user confirms.

---

## Follow-up (out of scope for this plan — separate tickets)

1. **`cli.py:862` (event-mode)** — also calls `save_candles` directly without merge. Low priority: `--use-events` is not the default path, but should be fixed.
2. **`daemon/periodic_collector.py:291,415,562`** — pre-merge before calling `save_candles`, safe today but race-prone under concurrent daemon runs. Defensive merge in `save_candles` now covers this.
3. **`storage.py:save_candles` exception handler** — currently logs ERROR and writes new-only data. Acceptable degradation; can be changed to skip-write in future if preferred.
4. **Recovery for already-truncated symbols** — ADA/BCH/DOT/INJ/XLM +80 others have 1m/5m/1h truncated to API page. Re-run `collect-full` for those symbols (or oracle fallback) after this fix ships.
